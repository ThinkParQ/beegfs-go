package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type requestPathResolverFn func(walkPath string) (inMountPath string, remotePath string, err error)
type addBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error)
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type planFileStateForWorkRequestsFn func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error)
type clearAccessFlagsFn func(ctx context.Context, path string, flags beegfs.AccessFlags) error
type setDirRstConfigFn func(ctx context.Context, inMountPath string) (isDir bool, err error)

type jobRequestBuilder struct {
	log              *zap.Logger
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
	if w.log == nil {
		w.log = zap.NewNop()
	}
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

const (
	// builderStepGrace bounds how long any single step of processing one path may keep running
	// after the builder job's context is cancelled. Each step checkpoints before it starts, so a
	// path that keeps making progress keeps earning time while a path wedged on a single meta or
	// remote operation is cut loose promptly. What bounds the total is that the steps below are
	// finite: one per path, one per rstId it fans out to, and the cleanup that follows.
	builderStepGrace = 30 * time.Second
	// builderCleanupGrace is granted to work that must finish to avoid leaving an entry in a bad
	// state: rolling back an applied plan, releasing a generated external id, and clearing a
	// path's content access lock. Cleanup gets its own window because by the time it runs the
	// forward steps may have already exhausted their grace, and abandoning it strands a modified
	// entry, leaks remote state, or leaves a lock that blocks later jobs for that path.
	builderCleanupGrace = 30 * time.Second
)

func (w *jobRequestBuilder) ProcessPathFromOriginalWalk(
	ctx context.Context,
	inMountPath string,
	remotePath string,
	failedPrecondition error,
) (activeSourceSubmissions int64, err error) {
	workCtx, cancel, checkpoint := WithCancellationDelay(ctx, builderStepGrace)
	defer cancel()

	if isDir, err := w.setDirRstConfig(workCtx, inMountPath); isDir || err != nil {
		// Abort the builder job since the beegfs was unable to set the directory's rst
		// configuration. The issue is likely systemic.
		return 0, err
	}

	var pathState PathState
	var skip bool
	var pathIssue error
	checkpoint(builderStepGrace)
	if pathState, skip, pathIssue, err = w.resolvePathStateForRequest(workCtx, inMountPath); err != nil || skip {
		return
	} else if pathIssue != nil {
		failedPrecondition = appendErrors(failedPrecondition, pathIssue)
	}

	var keepLock bool
	if FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo) {
		keepLock = true
		failedPrecondition = appendErrors(failedPrecondition, fmt.Errorf("file access lock is already held"))
	}
	defer func() {
		if FileExists(pathState.LockedInfo) && !keepLock {
			checkpoint(builderCleanupGrace)
			if clearErr := w.clearAccessFlags(workCtx, inMountPath, beegfs.LockedContentAccessFlags); clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) {
				err = appendErrors(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		// Each rstId is an independent round of remote work, so it earns its own grace.
		checkpoint(builderStepGrace)
		request := w.buildRequest(workCtx, cfg, failedPrecondition)
		canReleaseLock, submitted, processErr := w.processRequest(workCtx, checkpoint, cfg, pathState, request)
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
	workCtx, cancel, checkpoint := WithCancellationDelay(ctx, builderStepGrace)
	defer cancel()

	pathState, pathStateErr := w.getPathState(workCtx, w.mountPoint, inMountPath, PathStateWithLock)
	if errors.Is(pathStateErr, ErrGetPathStateFatal) {
		err = pathStateErr
		return
	}

	keepLock := FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo)
	defer func() {
		if FileExists(pathState.LockedInfo) && !keepLock {
			checkpoint(builderCleanupGrace)
			if clearErr := w.clearAccessFlags(workCtx, inMountPath, beegfs.LockedContentAccessFlags); clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) {
				err = appendErrors(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	checkpoint(builderStepGrace)
	cfg := w.buildJobRequestCfg(inMountPath, remotePath, rstId, pathState.LockedInfo, w.builderCfg)
	request := w.buildRequest(workCtx, cfg, failedPrecondition)
	request.SetRemoteStorageTarget(rstId)
	request.SetBulkInfo(BulkInfo)

	canReleaseLock, _, processErr := w.processRequest(workCtx, checkpoint, cfg, pathState, request)
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
	checkpoint CancellationCheckpoint,
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
		if applyPlan, planErr = w.planFileState(w.mountPoint, cfg); planErr != nil {
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
			checkpoint(builderStepGrace)
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

	if ctx.Err() != nil && (!request.HasGenerationStatus() || request.GetGenerationStatus().GetState() == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION) {
		// Shutting down and there's been no changes. Cancel immediately and don't apply the plan.
		canReleaseLock = true
		return
	}

	applyUndo := noopUndo
	var planApplied bool
	var terminalOutcome bool
	if !request.HasGenerationStatus() {

		var applyErr error
		checkpoint(builderStepGrace)
		planApplied, applyUndo, applyErr = applyPlan(ctx, &pathState)
		terminalOutcome = IsErrJobTerminalSentinel(applyErr)
		if applyErr != nil {
			if errors.Is(applyErr, ErrJobAlreadyComplete) {
				canReleaseLock = true
				request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
					State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
					Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
				})
			} else if errors.Is(applyErr, ErrJobAlreadyOffloaded) {
				canReleaseLock = false
				request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
					State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
				})
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
			canReleaseLock = false
			client := w.RstMap[request.GetRemoteStorageTarget()]

			checkpoint(builderStepGrace)
			externalId, externalErr := client.GenerateExternalId(ctx, cfg)
			if externalErr != nil {
				message := fmt.Sprintf("failed to generate external id: %s", externalErr.Error())
				checkpoint(builderCleanupGrace)
				if undoErr := applyUndo(ctx); undoErr != nil {
					canReleaseLock = false
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_ERROR,
						Message: fmt.Sprintf("%s; rollback also failed: %s", message, undoErr.Error()),
					})
				} else {
					planApplied = false
					canReleaseLock = true
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
						Message: message,
					})
				}
			} else {
				lockedInfo.SetExternalId(externalId)
			}
		}
	}

	if submitErr := w.submitRequest(request); submitErr != nil {
		// The submit is not governed by ctx, so it may have burned the whole grace on its own.
		// These three run concurrently, so one checkpoint covers the entire fan-out.
		checkpoint(builderCleanupGrace)
		wg := sync.WaitGroup{}
		var undoPlanErr, resolveBulkRequestErr error

		if planApplied && !terminalOutcome {
			wg.Go(func() {
				undoPlanErr = applyUndo(ctx)
				if undoPlanErr != nil {
					w.log.Warn("unable to revert what was prepared for a job request that could not be submitted, the entry is left modified and its lock retained",
						zap.String("path", cfg.GetPath()),
						zap.Uint32("rstId", request.GetRemoteStorageTarget()),
						zap.NamedError("submitError", submitErr),
						zap.Error(undoPlanErr))
				}
				canReleaseLock = undoPlanErr == nil
			})
		}

		if request.HasBulkInfo() {
			wg.Go(func() {
				if client, ok := w.RstMap[request.GetRemoteStorageTarget()]; ok {
					resolveBulkRequestErr = client.ResolveBulkRequest(ctx, request)
				} else {
					resolveBulkRequestErr = fmt.Errorf("%w: rstId %d", ErrConfigRSTTypeIsUnknown, request.GetRemoteStorageTarget())
				}
			})
		}
		if lockedInfo.ExternalId != "" {
			wg.Go(func() {
				client := w.RstMap[request.GetRemoteStorageTarget()]
				if err := client.ReleaseExternalId(ctx, cfg, lockedInfo.ExternalId); err != nil {
					// Leaked remote state for one path, which is not worth failing the builder job
					// over, but it may need to be cleaned up manually.
					w.log.Warn("unable to release external id",
						zap.String("path", cfg.GetPath()),
						zap.Uint32("rstId", request.GetRemoteStorageTarget()),
						zap.String("externalId", lockedInfo.ExternalId),
						zap.Error(err))
					return
				}
				lockedInfo.SetExternalId("")
			})
		}

		wg.Wait()
		err = appendErrors(err, undoPlanErr, resolveBulkRequestErr)
	} else {
		submitted = true
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
