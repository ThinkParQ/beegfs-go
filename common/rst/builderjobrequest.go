package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// addToBulkRequestFn offers a request to the bulk operation that handles its remote storage target.
// addedToBulk is true when an operation took the request. The operation then owns it and the caller
// must not submit it. The operation decides whether the request ever becomes a job: it may submit
// it later, or drop it so no job ever runs it.
type addToBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest, operation string) error
type updateBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type planFileStateForWorkRequestsFn func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error)
type clearAccessFlagsFn func(ctx context.Context, path string, flags beegfs.AccessFlags) error
type setDirRstConfigFn func(ctx context.Context, inMountPath string) error

type jobRequestBuilder struct {
	log               *zap.Logger
	mountPoint        filesystem.Provider
	RstMap            map[uint32]Provider
	submitRequest     SubmitRequestFn
	builderCfg        *flex.JobRequestCfg
	addToBulkRequest  addToBulkRequestFn
	updateBulkRequest updateBulkRequestFn
	getPathState      getPathStateFn
	planFileState     planFileStateForWorkRequestsFn
	clearAccessFlags  clearAccessFlagsFn
	setDirRstConfig   setDirRstConfigFn
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
		w.setDirRstConfig = func(context.Context, string) error { return nil }
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

	w.setDirRstConfig = func(ctx context.Context, inMountPath string) error {
		return entry.SetDirRstPattern(ctx, inMountPath, rstIds, cooldownSecs)
	}
}

func (w *jobRequestBuilder) ProcessPathFromOriginalWalk(
	ctx context.Context,
	inMountPath string,
	remotePath string,
	failedPrecondition error,
) (activeSourceSubmissions int64, err error) {
	// Adding a cancellation delay so files are left in a valid state regardless of the cancellation
	// reason. This provides any in-flight operation(s) a chance to finish.
	workCtx, cancel, checkpoint := WithCancellationDelay(ctx, checkpointGrace)
	defer cancel()

	var pathState PathState
	var skip bool
	var pathIssue error
	checkpoint(checkpointGrace)
	if pathState, skip, pathIssue, err = w.resolvePathStateForRequest(workCtx, inMountPath); skip || err != nil {
		return
	}

	if pathState.IsDir() {
		checkpoint(checkpointGrace)
		// Return any errors to abort the builder job since beegfs was unable to set the directory's
		// rst configuration. The underlining issue is likely systemic.
		err = w.setDirRstConfig(workCtx, inMountPath)
		return
	}

	if pathIssue != nil {
		failedPrecondition = appendErrors(failedPrecondition, pathIssue)
	}

	var keepLock bool
	if FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo) {
		// The file access lock is held by another so the lock must be kept.
		keepLock = true
		if w.builderCfg.Download || w.builderCfg.StubLocal {
			// The lock is held by something other than this builder. When a job holds it, remote's conflict
			// check refuses the request first and this message is dropped. Remote only records it when no job
			// on the path is live, which means the lock is orphaned.
			if w.builderCfg.Overwrite {
				failedPrecondition = appendErrors(failedPrecondition, errors.New("unable to overwrite the file because its access lock is already held (wait for the job holding it, or clear the lock if no job does)"))
			} else {
				failedPrecondition = appendErrors(failedPrecondition, fmt.Errorf("%w and its access lock is already held (wait for the job holding it, or clear the lock if no job does)", fs.ErrExist))
			}
		}
	}
	defer func() {
		if !keepLock {
			checkpoint(checkpointGrace)
			clearErr := w.clearAccessFlags(workCtx, inMountPath, beegfs.LockedContentAccessFlags)
			if clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) && !errors.Is(clearErr, syscall.ENOTDIR) && !errors.Is(clearErr, entry.ErrAccessFlagsUnchanged) {
				err = appendErrors(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		// Each rstId is an independent round of remote work, so it earns its own grace.
		checkpoint(checkpointGrace)
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
	reservedJobId string,
	BulkInfo *flex.BulkJobRequestInfo,
	failedPrecondition error,
) (err error) {
	// Adding a cancellation delay so files are left in a valid state regardless of the cancellation
	// reason. This provides any in-flight operation(s) a chance to finish.
	workCtx, cancel, checkpoint := WithCancellationDelay(ctx, checkpointGrace)
	defer cancel()

	pathState, pathStateErr := w.getPathState(workCtx, w.mountPoint, inMountPath, PathStateWithLock)
	if errors.Is(pathStateErr, ErrGetPathStateFatal) {
		err = pathStateErr
		return
	}
	if pathStateErr != nil {
		failedPrecondition = appendErrors(failedPrecondition, pathStateErr)
	}

	keepLock := FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo)
	defer func() {
		if !keepLock {
			checkpoint(checkpointGrace)
			clearErr := w.clearAccessFlags(workCtx, inMountPath, beegfs.LockedContentAccessFlags)
			if clearErr != nil && !errors.Is(clearErr, fs.ErrNotExist) && !errors.Is(clearErr, syscall.ENOTDIR) && !errors.Is(clearErr, entry.ErrAccessFlagsUnchanged) {
				err = appendErrors(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	checkpoint(checkpointGrace)
	cfg := w.buildJobRequestCfg(inMountPath, remotePath, rstId, pathState.LockedInfo, w.builderCfg)
	request := w.buildRequest(workCtx, cfg, failedPrecondition)
	request.SetRemoteStorageTarget(rstId)
	request.SetBulkInfo(BulkInfo)
	request.SetReserveJobId(reservedJobId)

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

// resolvePathStateForRequest retrieves the current path's state information and then updates its
// rstIds list before returning. If the builder job did not provide an rstId then the file's own rst
// configuration will be used; and if the configured rstIds list is empty, skip will be true so no
// request is created and the builder job does not fail.
//
// Directories are the exception since they return early with skip false and their rstIds untouched.
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

	if pathState.IsDir() {
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
	canReleaseLock = true
	var applyPlan applyPlanFn
	var workRequired bool
	if !request.HasGenerationStatus() {
		var planErr error
		if applyPlan, workRequired, planErr = w.planFileState(w.mountPoint, cfg); planErr != nil {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to prepare file state: %s", planErr.Error()),
			})
		}
	}

	// A request with bulk info was already added to a bulk operation and are being submitted.
	// Ignore when a request's path equals the builder's since there would only be one path for the
	// bulk operation.
	if workRequired && !request.HasGenerationStatus() && !request.HasBulkInfo() && cfg.GetPath() != w.builderCfg.GetPath() {
		if accepted, jobConflict, bulkErr := w.offerToBulkOperation(ctx, checkpoint, request); bulkErr != nil {
			err = bulkErr
			return
		} else if accepted {
			canReleaseLock = false
			return
		} else if jobConflict {
			// No job will run for this path, so the lock this builder took has nothing to guard. A lock
			// another job held is kept by the caller regardless.
			return
		}
	}

	planApplied := false
	applyUndo := noopUndo
	terminalOutcome := false
	if !request.HasGenerationStatus() {
		planApplied, applyUndo, terminalOutcome, canReleaseLock = w.prepareJobRequest(ctx, checkpoint, cfg, pathState, request, applyPlan)
	}

	checkpoint(checkpointGrace)
	submitErr := w.submitRequest(ctx, request)
	if submitErr != nil {
		checkpoint(checkpointGrace)

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
				// Notify the bulk operation that its request failed to be submitted.
				resolveBulkRequestErr = w.updateBulkRequest(ctx, request, BulkRequestFailed)
			})
		}

		lockedInfo := cfg.GetLockedInfo()
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
		return
	}

	submitted = true
	if request.HasBulkInfo() {
		// Notify the bulk operation that its request was submitted.
		if submittedErr := w.updateBulkRequest(ctx, request, BulkRequestSubmitted); submittedErr != nil {
			err = appendErrors(err, fmt.Errorf("unable to report the submission of %s to its bulk operation: %w", cfg.GetPath(), submittedErr))
		}
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

// offerToBulkOperation offers the request to the bulk operation that handles its remote storage
// target, and reserves the job that operation will later claim for the path.
//
// accepted is true only when an operation took the request. The operation then owns the path and
// resubmits it through its own walk, so the caller must not submit it.
//
// A non-nil err stops the builder job, so it is reserved for the failures that make every later
// path fail the same way: a target with no client, and a reservation the caller could not submit.
// An operation that refuses the request is not one of them. That is recorded on the request as a
// failed precondition and left for the caller to submit, because the next path may still be taken
// by a different operation, and an operation that failed for good is reported once at the end of
// the builder job by the registry's GetFailedOperationErrors.
func (w *jobRequestBuilder) offerToBulkOperation(ctx context.Context, checkpoint CancellationCheckpoint, request *beeremote.JobRequest) (accepted bool, jobConflict bool, err error) {
	rstId := request.GetRemoteStorageTarget()
	client, ok := w.RstMap[rstId]
	if !ok {
		err = fmt.Errorf("%w: rstId %d", ErrConfigRSTTypeIsUnknown, rstId)
		return
	}

	checkpoint(checkpointGrace)
	include, operation := client.IncludeRequestInBulkOperation(ctx, request)
	if !include {
		return
	}

	request.SetReserve(true)
	request.SetReserveJobId(uuid.New().String())

	checkpoint(checkpointGrace)
	if addErr := w.addToBulkRequest(ctx, request, operation); addErr != nil {
		request.SetReserve(false)
		request.ClearReserveJobId()
		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: fmt.Sprintf("failed to add request to bulk operation %q: %s", operation, addErr.Error()),
		})
		return
	}

	checkpoint(checkpointGrace)
	reserveErr := w.submitRequest(ctx, request)
	if reserveErr != nil {
		if errors.Is(reserveErr, ErrJobAlreadyExists) || errors.Is(reserveErr, ErrJobNotAllowed) {
			jobConflict = true
			// This would have been the job conflicts with another job so fail the bulk request.
			if markErr := w.updateBulkRequest(ctx, request, BulkRequestFailed); markErr != nil {
				err = fmt.Errorf("unable to release %s from bulk operation %q after its reservation was refused: %w", request.GetPath(), operation, markErr)
			}
		} else {
			// Failure to reserve a remote job is a systemic issue so fail the builder job.
			err = fmt.Errorf("failed to reserve job for bulk operation %q: %w", operation, reserveErr)
		}
		return
	}

	accepted = true
	return
}

func (w *jobRequestBuilder) prepareJobRequest(
	ctx context.Context,
	checkpoint CancellationCheckpoint,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
	applyPlan applyPlanFn,
) (planApplied bool, applyUndo undoFn, terminalOutcome bool, canReleaseLock bool) {
	lockedInfo := cfg.GetLockedInfo()
	canReleaseLock = false

	var applyErr error
	checkpoint(checkpointGrace)
	planApplied, applyUndo, applyErr = applyPlan(ctx, &pathState)
	if applyErr != nil {
		terminalOutcome = IsErrJobTerminalSentinel(applyErr)

		if errors.Is(applyErr, ErrJobAlreadyComplete) {
			canReleaseLock = true
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			})
		} else if errors.Is(applyErr, ErrJobAlreadyOffloaded) {
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
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("failed to prepare file state: %s", applyErr.Error()),
			})
		}
		return
	}

	client := w.RstMap[request.GetRemoteStorageTarget()]

	checkpoint(checkpointGrace)
	externalId, externalErr := client.GenerateExternalId(ctx, cfg)
	if externalErr != nil {
		message := fmt.Sprintf("failed to generate external id: %s", externalErr.Error())

		checkpoint(checkpointGrace)
		if undoErr := applyUndo(ctx); undoErr != nil {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("%s; rollback also failed: %s", message, undoErr.Error()),
			})
			return
		}

		planApplied = false
		canReleaseLock = true
		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: message,
		})
		return
	}

	lockedInfo.SetExternalId(externalId)
	return
}
