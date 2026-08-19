package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

type requestPathResolverFn func(walkPath string) (inMountPath string, remotePath string, err error)
type addBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error)
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type planFileStateForWorkRequestsFn func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error)
type clearAccessFlagsFn func(ctx context.Context, path string, flags beegfs.AccessFlags) error
type setDirRstConfigFn func(ctx context.Context, inMountPath string) (isDir bool, err error)

// requestCleanupTimeout bounds how long cleaning up after an abandoned job request may take once the
// request context is gone. Cleanup (rolling back the applied file state plan, releasing the
// externalId) runs detached from that context so it can still reach BeeGFS and the remote target
// after cancellation, and this is what keeps it from stalling a shutdown that waits on it.
const requestCleanupTimeout = 1 * time.Minute

type jobRequestBuilder struct {
	mountPoint       filesystem.Provider
	RstMap           map[uint32]Provider
	submitRequest    SubmitRequestFn
	builderCfg       *flex.JobRequestCfg
	addBulkRequest   addBulkRequestFn
	getPathState     getPathStateFn
	planFileState    planFileStateForWorkRequestsFn
	clearAccessFlags clearAccessFlagsFn
	setDirRstConfig  setDirRstConfigFn
}

func (w *jobRequestBuilder) init() {
	w.initSetRstConfig()
}

func (w *jobRequestBuilder) initSetRstConfig() {
	if !(w.builderCfg.GetUpdate() || w.builderCfg.HasCooldownSecs()) {
		// When neither builder config Update nor CooldownSec are set then directories are not
		// included in the walk and we can safely ignore directory configuration updates.
		w.setDirRstConfig = func(context.Context, string) (bool, error) { return false, nil }
		return
	}

	var rstIds []uint32
	if w.builderCfg.GetUpdate() && IsValidRstId(w.builderCfg.RemoteStorageTarget) {
		rstIds = []uint32{w.builderCfg.RemoteStorageTarget}
	}

	var cooldownSecs *uint16
	if w.builderCfg.HasCooldownSecs() {
		v := uint16(math.MaxUint16)
		if w.builderCfg.GetCooldownSecs() <= math.MaxUint16 {
			v = uint16(w.builderCfg.GetCooldownSecs())
		}
		cooldownSecs = &v
	}

	w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) {
		stat, err := w.mountPoint.Lstat(inMountPath)
		if err != nil {
			return false, err
		}

		return stat.IsDir(), entry.SetDirRstPattern(ctx, inMountPath, rstIds, cooldownSecs)
	}
}

func (w *jobRequestBuilder) ProcessPathFromOriginalWalk(ctx context.Context, inMountPath string, remotePath string, failedPrecondition error) (activeSourceSubmissions int64, err error) {
	if isDir, err := w.setDirRstConfig(ctx, inMountPath); isDir || err != nil {
		// Abort the builder job since the beegfs was unable to set the directory's rst
		// configuration. The issue is likely systemic.
		return 0, err
	}

	var pathState PathState
	var skip bool
	var pathIssue error
	if pathState, skip, pathIssue, err = w.resolvePathStateForRequest(ctx, inMountPath); err != nil || skip {
		return
	} else if pathIssue != nil {
		failedPrecondition = appendError(failedPrecondition, pathIssue)
	}

	var keepLock bool
	if FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo) {
		keepLock = true
		failedPrecondition = appendError(failedPrecondition, fmt.Errorf("file access lock is already held"))
	}
	defer func() {
		if FileExists(pathState.LockedInfo) && !keepLock {
			if clearErr := w.clearAccessFlags(ctx, inMountPath, beegfs.LockedContentAccessFlags); clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) {
				err = appendError(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		request := w.buildRequest(ctx, cfg, failedPrecondition)
		canReleaseLock, submitted, processErr := w.processRequest(ctx, cfg, pathState, request)
		if !canReleaseLock {
			keepLock = true
		}
		if processErr != nil {
			err = processErr
			return
		}

		// Only count requests that were actually submitted and are still active. A request absorbed
		// into a bulk operation was not submitted at all, and one carrying a GenerationStatus is
		// already terminal on arrival, so neither occupies submission capacity.
		if submitted && !request.HasGenerationStatus() {
			activeSourceSubmissions++
		}
	}

	return
}

func (w *jobRequestBuilder) ProcessPathFromBulkOperation(
	ctx context.Context,
	inMountPath string,
	remotePath string,
	rstId uint32,
	BulkInfo *flex.BulkJobRequestInfo,
	failedPrecondition error,
) (err error) {
	pathState, pathStateErr := w.getPathState(ctx, w.mountPoint, inMountPath, PathStateWithLock)
	if errors.Is(pathStateErr, ErrGetPathStateFatal) {
		err = pathStateErr
		return
	}

	keepLock := FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo)
	defer func() {
		if FileExists(pathState.LockedInfo) && !keepLock {
			if clearErr := w.clearAccessFlags(ctx, inMountPath, beegfs.LockedContentAccessFlags); clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) {
				err = appendError(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	cfg := w.buildJobRequestCfg(inMountPath, remotePath, rstId, pathState.LockedInfo, w.builderCfg)
	request := w.buildRequest(ctx, cfg, failedPrecondition)
	request.SetRemoteStorageTarget(rstId)
	request.SetBulkInfo(BulkInfo)

	canReleaseLock, _, processErr := w.processRequest(ctx, cfg, pathState, request)
	if !canReleaseLock {
		keepLock = true
	}
	if processErr != nil {
		err = processErr
		return
	}

	return
}

// resolvePathStateForRequest retrieves the current path's state information and then update's it
// rstIds list before returning. If the builder job did not provide an rstId then the file's own rst
// configuration with be used; and if the configured rstIds list is empty, skip will be true.
//
// pathIssue will return any non-fatal errors returned while retrieving path state information or
// conflicts. All fatal errors will be reported via err.
func (w *jobRequestBuilder) resolvePathStateForRequest(ctx context.Context, inMountPath string) (pathState PathState, skip bool, pathIssue error, err error) {
	var pathStateErr error
	pathState, pathStateErr = w.getPathState(ctx, w.mountPoint, inMountPath, PathStateWithLock)
	if errors.Is(pathStateErr, ErrGetPathStateFatal) {
		// Returning err from this function aborts the entire builder job, so only fatal path
		// state errors are returned here. Non-fatal path state errors are attached to the
		// generated request when an rstId is available, allowing the builder job to continue.
		err = pathStateErr
		return
	}

	// If the caller specified a valid remote storage target, use it as the request's rstId.
	// Otherwise, rely on any rstIds discovered from the file state. If none are available,
	// skip the path so no request is created and the builder job does not fail. This is valid
	// because callers can trigger jobs from configured file rstIds without specifying a target.
	if IsValidRstId(w.builderCfg.RemoteStorageTarget) {
		if IsFileOffloaded(pathState.LockedInfo) && w.builderCfg.RemoteStorageTarget != pathState.LockedInfo.StubUrlRstId && !w.builderCfg.GetOverwrite() {
			pathIssue = fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag)
		}
		pathState.RstCfg.RSTIDs = []uint32{w.builderCfg.RemoteStorageTarget}
	} else if len(pathState.RstCfg.RSTIDs) == 0 && pathStateErr == nil {
		skip = true
		return
	}

	if pathStateErr != nil {
		pathIssue = pathStateErr
	} else if len(pathState.RstCfg.RSTIDs) > 1 && (w.builderCfg.Download || w.builderCfg.StubLocal) {
		pathIssue = ErrFileHasAmbiguousRSTs
	}

	return
}

// buildJobRequestCfgs returns a list of job request configurations for each rstId. Each
// configurations is a clone of cfg updated with the provided information.
func (w *jobRequestBuilder) buildJobRequestCfgs(
	inMountPath string,
	remotePath string,
	rstIds []uint32,
	lockedInfo *flex.JobLockedInfo,
	cfg *flex.JobRequestCfg,
) []*flex.JobRequestCfg {
	var requests []*flex.JobRequestCfg
	for _, rstId := range rstIds {
		request := w.buildJobRequestCfg(inMountPath, remotePath, rstId, lockedInfo, cfg)
		requests = append(requests, request)
	}
	return requests
}

// buildJobRequestCfgs returns a job request configuration for the rstId. The configuration is a
// clone of cfg updated with the provided information.
func (w *jobRequestBuilder) buildJobRequestCfg(
	inMountPath string,
	remotePath string,
	rstId uint32,
	lockedInfo *flex.JobLockedInfo,
	cfg *flex.JobRequestCfg,
) *flex.JobRequestCfg {
	request := proto.Clone(cfg).(*flex.JobRequestCfg)
	request.SetPath(inMountPath)
	request.SetRemotePath(remotePath)
	request.SetRemoteStorageTarget(rstId)
	request.SetLockedInfo(proto.Clone(lockedInfo).(*flex.JobLockedInfo))
	return request
}

// processRequest builds, prepares, and submits the job request for cfg. canReleaseLock is
// returned true only when this path produced no in-flight work that still depends on the lock.
func (w *jobRequestBuilder) processRequest(
	ctx context.Context,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
) (canReleaseLock bool, submitted bool, err error) {
	lockedInfo := cfg.GetLockedInfo()
	var applyPlan applyPlanFn
	if request.HasGenerationStatus() {
		canReleaseLock = true
	} else {
		var planErr error
		if applyPlan, planErr = w.planFileState(ctx, w.mountPoint, cfg); planErr != nil {
			canReleaseLock = true
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to prepare file state: %s", planErr.Error()),
			})
		}
	}

	if !request.HasGenerationStatus() {
		// request.HasBulkInfo()==true means that the request has already been added to a bulk
		// operation and is now ready to be submitted; otherwise, see if this request needs to be
		// processed as a bulk request.
		if !request.HasBulkInfo() {
			var skipSubmission bool
			if skipSubmission, err = w.addBulkRequest(ctx, request); err != nil || skipSubmission {
				// Whether there was an error while adding the bulk request or it was added and
				// we're skipping the submission, the lock must be allowed to be released. The
				// reason for this is there's currently no way to distinguish between who acquired a
				// lock except when acquiring the lock. So, when a bulk operation accepts
				// responsibility for a path, the lock must be released when possible. Then when the
				// bulk operation sends the path, it will know whether it's safe to release the file
				// access lock based on the locks acquisition.
				canReleaseLock = true
				return
			} else if request.HasGenerationStatus() {
				canReleaseLock = true
			}
		}
	}

	// newCleanupCtx returns a context detached from ctx so the undo functions can still complete
	// when ctx is already cancelled because sync is shutting down.
	newCleanupCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(ctx), requestCleanupTimeout)
	}

	planApplied := false
	applyUndo := noopUndo
	undoAppliedPlan := func() error {
		cleanupCtx, cancelCleanup := newCleanupCtx()
		defer cancelCleanup()
		return applyUndo(cleanupCtx)
	}

	var generatedExternalId string
	releaseExternalId := func() error {
		if generatedExternalId == "" {
			return nil
		}
		client, ok := w.RstMap[request.GetRemoteStorageTarget()]
		if !ok {
			return fmt.Errorf("%w: rstId %d", ErrConfigRSTTypeIsUnknown, request.GetRemoteStorageTarget())
		}
		cleanupCtx, cancelCleanup := newCleanupCtx()
		defer cancelCleanup()
		return client.ReleaseExternalId(cleanupCtx, cfg, generatedExternalId)
	}

	if !request.HasGenerationStatus() {
		undo, applyErr := applyPlan(&pathState)
		if applyErr != nil {
			if errors.Is(applyErr, ErrJobAlreadyComplete) {
				canReleaseLock = true
				request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
					State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
					Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
				}
			} else if errors.Is(applyErr, ErrJobAlreadyOffloaded) {
				canReleaseLock = false
				request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
					State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
				}
			} else if errors.Is(applyErr, ErrJobFailedPrecondition) {
				canReleaseLock = true
				request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
					State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
					Message: fmt.Sprintf("failed to prepare file state: %s", applyErr.Error()),
				})
			} else {
				canReleaseLock = false
				request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
					State:   beeremote.JobRequest_GenerationStatus_ERROR,
					Message: fmt.Sprintf("failed to prepare file state: %s", applyErr.Error()),
				})
			}
		} else {
			applyUndo = undo
			planApplied = true

			client := w.RstMap[request.GetRemoteStorageTarget()]
			externalId, externalIdErr := client.GenerateExternalId(ctx, cfg)
			if externalIdErr != nil {
				if undoErr := undoAppliedPlan(); undoErr != nil {
					canReleaseLock = false
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_ERROR,
						Message: fmt.Sprintf("failed to generate external id: %s; rollback also failed: %s", externalIdErr.Error(), undoErr.Error()),
					})
				} else {
					planApplied = false
					canReleaseLock = true
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
						Message: fmt.Sprintf("failed to generate external id: %s", externalIdErr.Error()),
					})
				}
			} else {
				generatedExternalId = externalId
				lockedInfo.SetExternalId(externalId)
				canReleaseLock = false
			}
		}
	}

	// undo reverts everything prepared for this request and must only be called once it is certain
	// no job will ever run it.
	undo := func() (err error) {
		if releaseErr := releaseExternalId(); releaseErr != nil {
			err = appendError(err, fmt.Errorf("unable to release external id %q for %s: %w", generatedExternalId, cfg.GetPath(), releaseErr))
		} else if generatedExternalId != "" {
			generatedExternalId = ""
			lockedInfo.SetExternalId("")
		}

		if planApplied {
			// A failed plan rollback is reported by leaving planApplied set rather than as an
			// error. The file is left mutated so it must stay locked for recovery to find. However,
			// one path failing to roll back must not fail the whole builder job.
			if undoErr := undoAppliedPlan(); undoErr == nil {
				planApplied = false
			}
		}

		// Bulk operations may need to track the outcome for each added request. In such cases, it
		// is important to notify the bulk operation of the rejected request since the job will
		// never be created.
		if request.HasBulkInfo() {
			if client, ok := w.RstMap[request.GetRemoteStorageTarget()]; !ok {
				err = appendError(err, fmt.Errorf("unable to resolve bulk request for %s: %w: rstId %d", cfg.GetPath(), ErrConfigRSTTypeIsUnknown, request.GetRemoteStorageTarget()))
			} else {
				cleanupCtx, cancelCleanup := newCleanupCtx()
				defer cancelCleanup()
				if bulkErr := client.ResolveBulkRequest(cleanupCtx, request); bulkErr != nil {
					err = appendError(err, fmt.Errorf("unable to resolve bulk request for %s: %w", cfg.GetPath(), bulkErr))
				}
			}
		}
		return err
	}

	if ctx.Err() == nil {
		if submitErr := w.submitRequest(request); submitErr == nil {
			submitted = true
			return
		}
	}

	// The submission failed or the parent context was cancelled. In either case revert all changes.
	planWasApplied := planApplied
	if undoErr := undo(); undoErr != nil {
		err = appendError(err, undoErr)
	}
	if planWasApplied {
		// The lock can only be released if the applied changes are successfully reverted.
		canReleaseLock = !planApplied
	}
	return
}

func (w *jobRequestBuilder) buildRequest(ctx context.Context, cfg *flex.JobRequestCfg, failedPrecondition error) *beeremote.JobRequest {
	rstId := cfg.GetRemoteStorageTarget()
	client, ok := w.RstMap[rstId]
	if !ok {
		// The rstId is from the file's RST config but has no matching client. This means it was
		// either removed after the file was configured, or was set incorrectly. Return a
		// FAILED_PRECONDITION so remote submits the job with the error message rather than
		// rejecting the job.
		return &beeremote.JobRequest{
			Path:                cfg.Path,
			RemoteStorageTarget: cfg.GetRemoteStorageTarget(),
			GenerationStatus: &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to build job request: %s: rstId %d", ErrConfigRSTTypeIsUnknown.Error(), rstId),
			},
		}
	}

	if failedPrecondition != nil {
		return BuildJobRequestWithFailedPrecondition(client, cfg, failedPrecondition.Error())
	}
	return BuildJobRequest(ctx, client, cfg)
}
