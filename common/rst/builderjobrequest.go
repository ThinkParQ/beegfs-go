package rst

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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
type setDirRstConfigFn func(ctx context.Context, inMountPath string) error

type jobRequestBuilder struct {
	log        *zap.Logger
	mountPoint filesystem.Provider
	RstMap     map[uint32]Provider
	// submitRequest and releaseUnusedFileLock are the builder's calls to remote, as RemoteJobCalls
	// describes. The builder never clears a content lock itself.
	submitRequest         SubmitRequestFn
	releaseUnusedFileLock ReleaseUnusedFileLockFn
	builderJobId          uuid.UUID
	builderCfg            *flex.JobRequestCfg
	addToBulkRequest      addToBulkRequestFn
	updateBulkRequest     updateBulkRequestFn
	getPathState          getPathStateFn
	planFileState         planFileStateForWorkRequestsFn
	setDirRstConfig       setDirRstConfigFn
	// dirsConfigured counts the directories whose RST configuration this builder updated. A
	// directory submits no job request, so no submission counter records it.
	dirsConfigured atomic.Int32
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
	pathState, skip, pathIssue, err = w.resolvePathStateForRequest(workCtx, inMountPath)

	lockAcquired := FileExists(pathState.LockedInfo) && pathState.LockAcquired
	outcome := notSubmitted
	defer func() {
		err = appendErrors(err, w.releaseIfNothingSubmitted(workCtx, checkpoint, inMountPath, lockAcquired, outcome))
	}()
	if skip || err != nil {
		return
	}

	if pathState.IsDir() {
		checkpoint(checkpointGrace)
		// Return any errors to fail the builder job since beegfs was unable to set the directory's
		// rst configuration. The underlining issue is likely systemic.
		if err = w.setDirRstConfig(workCtx, inMountPath); err == nil {
			w.dirsConfigured.Add(1)
		}
		return
	}

	if pathIssue != nil {
		failedPrecondition = appendErrors(failedPrecondition, pathIssue)
	}

	if FileExists(pathState.LockedInfo) && !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo) {
		if w.builderCfg.Download || w.builderCfg.StubLocal {
			// This job intends to alter the state by downloading or offloading a file and it did
			// not acquire the lock. Therefore, the lock must fail because it will conflict with any
			// existing job.
			if w.builderCfg.Overwrite {
				failedPrecondition = appendErrors(failedPrecondition, errors.New("unable to overwrite the file because its access lock is already held (wait for the job holding it, or clear the lock if no job does)"))
			} else {
				failedPrecondition = appendErrors(failedPrecondition, fmt.Errorf("%w and its access lock is already held (wait for the job holding it, or clear the lock if no job does)", fs.ErrExist))
			}
		}
	}

	if len(pathState.RstCfg.RSTIDs) == 0 && failedPrecondition != nil {
		// There are no rstIds and there is a failed precondition. So, in order to communicate the issue to
		// the user, submit the request with a generation status under the invalid rstId 0. No job
		// will be created just a record of the failure.
		checkpoint(checkpointGrace)
		cfg := w.buildJobRequestCfg(inMountPath, remotePath, 0, pathState.LockedInfo, w.builderCfg)
		request := BuildJobRequestWithFailedPrecondition(nil, cfg, failedPrecondition.Error())
		var requestOutcome submitOutcome
		requestOutcome, _, err = w.processRequest(workCtx, checkpoint, cfg, pathState, request)
		outcome = outcome.combine(requestOutcome)
		return
	}

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		// Each rstId is an independent round of remote work, so it earns its own grace.
		checkpoint(checkpointGrace)
		request := w.buildRequest(workCtx, cfg, failedPrecondition)
		requestOutcome, submitted, processErr := w.processRequest(workCtx, checkpoint, cfg, pathState, request)
		outcome = outcome.combine(requestOutcome)
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

func (w *jobRequestBuilder) ProcessPathFromBulkOperation(ctx context.Context, result *BulkStreamPathResult, failedPrecondition error) (err error) {
	// Adding a cancellation delay so files are left in a valid state regardless of the cancellation
	// reason. This provides any in-flight operation(s) a chance to finish.
	workCtx, cancel, checkpoint := WithCancellationDelay(ctx, checkpointGrace)
	defer cancel()

	pathState, pathStateErr := w.getPathState(workCtx, w.mountPoint, result.InMountPath, PathStateWithLock)
	if pathState.LockAcquired {
		// A reserved job keeps the lock the reservation took so finding it unset means the
		// reservation is gone or the lock was cleared by hand. The claim will most likely be refused.
		w.log.Warn("content lock was not held when claiming a reserved request, so it was taken again", zap.String("path", result.InMountPath), zap.String("jobId", result.ReservedJobId))
	}

	// If the file exists and is not offloaded then it is uncertain whether the lock was acquired so
	// assume it was. This is safe since remote only releases the lock when no job needs it.
	lockAcquired := FileExists(pathState.LockedInfo) && !IsFileOffloaded(pathState.LockedInfo)
	outcome := notSubmitted
	defer func() {
		err = appendErrors(err, w.releaseIfNothingSubmitted(workCtx, checkpoint, result.InMountPath, lockAcquired, outcome))
	}()

	if errors.Is(pathStateErr, ErrGetPathStateFatal) {
		err = pathStateErr
		return
	}
	if pathStateErr != nil {
		failedPrecondition = appendErrors(failedPrecondition, pathStateErr)
	}

	checkpoint(checkpointGrace)
	cfg := w.buildJobRequestCfg(result.InMountPath, result.RemotePath, result.RstId, pathState.LockedInfo, w.builderCfg)
	request := w.buildRequest(workCtx, cfg, failedPrecondition)
	request.SetRemoteStorageTarget(result.RstId)
	request.SetBulkInfo(result.BulkInfo)
	request.SetJobId(result.ReservedJobId)
	request.SetReservation(beeremote.JobRequest_CLAIM)

	outcome, _, err = w.processRequest(workCtx, checkpoint, cfg, pathState, request)
	return
}

// releaseIfNothingSubmitted runs when the builder is done with a path. It asks remote to release
// the content lock when this pass took it and no request for the path reached remote, because then
// no job relies on it. In every other case remote already decided: it released the lock at the end
// of each request if no job needed it, or a job holds it. If remote may not have received a
// request, the lock is kept, because a job may rely on it. The release is remote's to decide, so it
// can't pull the lock from a job that needs it.
func (w *jobRequestBuilder) releaseIfNothingSubmitted(ctx context.Context, checkpoint CancellationCheckpoint, inMountPath string, lockAcquired bool, outcome submitOutcome) error {
	if !lockAcquired || outcome != notSubmitted {
		return nil
	}
	checkpoint(checkpointGrace)
	if err := w.releaseUnusedFileLock(ctx, inMountPath); err != nil {
		if errors.Is(err, ErrRequestNotDelivered) {
			// releaseUnusedFileLock retries until remote answers so this is only reached when the
			// builder's context ended; Sync is shutting down or the builder job was cancelled, and
			// the grace period expired. Remote may not have seen the call. If it didn't, the lock
			// stays set with no job until a later request for the path reaches remote, or until a
			// job that shares the lock finishes and its release clears it.
			w.log.Warn("unable to ask remote to release the content lock, it is kept until a later release", zap.String("path", inMountPath), zap.Error(err))
			return nil
		}
		return fmt.Errorf("unable to release lock: %w", err)
	}
	return nil
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
		// Returning err from this function fails the builder job once its walk and bulk operations
		// finish, so only fatal path state errors are returned here. Non-fatal path state errors
		// are attached to the generated request when an rstId is available, allowing the builder
		// job to continue.
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

// jobIdFor derives the job ID for a request from the builder job ID, the request's path and its
// RST. The same builder job always derives the same ID for the same path and RST. So when a request
// is sent again, after a lost response or after a rescheduled builder job replays from an older
// resume token, remote recognizes it and answers with the job the first request created instead of
// creating a second one. A reservation and its claim share this ID too.
func (w *jobRequestBuilder) jobIdFor(request *beeremote.JobRequest) string {
	name := binary.BigEndian.AppendUint32([]byte(request.GetPath()), request.GetRemoteStorageTarget())
	return uuid.NewSHA1(w.builderJobId, name).String()
}

// submitOutcome says what remote knows about a path's requests once the builder is done with them.
// It decides whether the builder must ask remote to release a lock it took for the path.
type submitOutcome int

// The submitOutcome values are ordered intentionally so combine returns the highest ranked value.
const (
	// notSubmitted means no request for the path reached remote so no job relies on a lock the
	// builder took. The builder asks remote to release it.
	notSubmitted submitOutcome = iota
	// answered means remote answered every request. Remote released the lock at the end of each
	// call if no job still needed it.
	answered
	// notDelivered means remote may or may not have recorded a request. The builder leaves the
	// file, the lock and any external ID as they are, so a replay finds them unchanged.
	notDelivered
)

// combine returns the outcome of a path that produced both o and other. A request that may not
// have reached remote outranks one remote answered which outranks one that was never sent.
func (o submitOutcome) combine(other submitOutcome) submitOutcome {
	return max(o, other)
}

// processRequest prepares the job request for cfg and submits it. It returns what remote knows
// about the request, and whether remote accepted it.
//
// If remote refuses the request, the builder undoes whatever its plan changed in the file. Remote
// decides the content lock at the end of the submit, so the builder never clears it itself.
func (w *jobRequestBuilder) processRequest(
	ctx context.Context,
	checkpoint CancellationCheckpoint,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
) (outcome submitOutcome, submitted bool, err error) {
	if !request.HasJobId() {
		request.SetJobId(w.jobIdFor(request))
	}

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
		accepted, reserveOutcome, bulkErr := w.offerToBulkOperation(ctx, checkpoint, request)
		if bulkErr != nil || accepted || reserveOutcome != notSubmitted {
			return reserveOutcome, false, bulkErr
		}
	}

	var applyUndo undoFn
	terminalOutcome := false
	if !request.HasGenerationStatus() {
		applyUndo, terminalOutcome = w.prepareJobRequest(ctx, checkpoint, cfg, pathState, request, applyPlan)
	}

	// A plan that ends in a terminal outcome, such as already offloaded, keeps its changes, because
	// they are that outcome. Only a plan that prepared for a job has changes to undo.
	planToUndo := applyUndo != nil && !terminalOutcome
	checkpoint(checkpointGrace)
	submitErr := w.submitRequest(ctx, request)
	if submitErr == nil {
		if request.HasBulkInfo() {
			// Notify the bulk operation that its request was accepted by remote.
			checkpoint(checkpointGrace)
			if submittedErr := w.updateBulkRequest(ctx, request, BulkRequestAccepted); submittedErr != nil {
				err = fmt.Errorf("unable to report the submission of %s to its bulk operation: %w", cfg.GetPath(), submittedErr)
			}
		}
		return answered, true, err
	}

	if errors.Is(submitErr, ErrRequestNotDelivered) {
		// Remote may have recorded the request, and a job may now rely on the plan, the lock and
		// the external ID. Leave them all, so a replay finds them as they were. The bulk operation
		// keeps the request too, because its reservation may still be live.
		if request.HasBulkInfo() {
			w.log.Warn("unable to deliver job request to remote, notifying its bulk operation",
				zap.String("path", cfg.GetPath()),
				zap.Uint32("rstId", request.GetRemoteStorageTarget()),
				zap.String("operation", request.GetBulkInfo().GetOperation()),
				zap.Int64("jobIndex", request.GetBulkInfo().GetJobIndex()),
				zap.String("reservedJobId", request.GetJobId()),
				zap.Error(submitErr))
			checkpoint(checkpointGrace)
			if notifyErr := w.updateBulkRequest(ctx, request, BulkRequestNotDelivered); notifyErr != nil {
				err = notifyErr
			}
		}
		return notDelivered, false, err
	}

	// Remote refused the request, so no job will ever run it. Revert what the builder prepared
	// for it, and release the external ID it created for the request. The lock is not the
	// builder's to release: remote decided it at the end of the submit.
	//
	// The undo is a recovery step, not a guarantee. It puts the file back as it was before the plan
	// when remote refuses and the builder keeps running to the end of the undo. It does not cover
	// every failure, and does not try to:
	//   - Remote released the lock at the end of the submit if no other job needed it, so the undo
	//     runs unlocked. A client write in that window can race the undo. A new job, from a user or
	//     from watch, can also take the lock and start in that window. The undo can then delete,
	//     truncate or re-stub that job's file.
	//   - An error that hides remote's decision is treated as a refusal. If remote accepted the
	//     request and the answer was lost, the undo runs under that job. Releasing the external ID
	//     then aborts that job's upload. One such error is remote failing to commit its path entry
	//     after it dispatched the work.
	//   - A crash, or a shutdown that outlasts the grace period, stops the undo partway. The undo
	//     lives only in memory, so a restarted builder can't finish it, and the file stays partly
	//     changed.
	//   - An undo that fails is logged, and the file stays as the plan left it.
	//
	// These risks are accepted for three reasons:
	//   - Remote refuses few requests after the plan has run, because a pass that would change a
	//     file another job has locked is refused before any plan runs. A failure inside the undo
	//     that follows is rarer still.
	//   - Every plan step makes a change the user asked for. A download creates the file, restores
	//     a stub, or replaces a regular file only when the user set overwrite. stub_local turns the
	//     file into a stub. A file left partly changed is a failed request the user can retry.
	//   - Closing these gaps needs remote to record the job before the plan runs, and the undo
	//     written to a journal. That cost lands on every request to protect against a rare one.
	checkpoint(checkpointGrace)
	wg := sync.WaitGroup{}
	var undoErr, resolveBulkRequestErr error
	if planToUndo {
		wg.Go(func() {
			undoErr = applyUndo.run(ctx)
			if undoErr != nil {
				w.log.Warn("unable to revert what was prepared for a job request that was refused, the entry is left modified",
					zap.String("path", cfg.GetPath()),
					zap.Uint32("rstId", request.GetRemoteStorageTarget()),
					zap.NamedError("submitError", submitErr),
					zap.Error(undoErr))
			}
		})
	}
	if request.HasBulkInfo() {
		wg.Go(func() {
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
	err = appendErrors(undoErr, resolveBulkRequestErr)

	return answered, false, err
}

func (w *jobRequestBuilder) buildRequest(ctx context.Context, cfg *flex.JobRequestCfg, failedPrecondition error) *beeremote.JobRequest {
	rstId := cfg.GetRemoteStorageTarget()
	client, ok := w.RstMap[rstId]
	if !ok {
		// The rstId is from the file's RST config but has no matching client. This means it was
		// either removed after the file was configured, or was set incorrectly. Return a
		// FAILED_PRECONDITION so remote submits the job with the error message rather than
		// rejecting the job.
		return BuildJobRequestWithFailedPrecondition(nil, cfg, fmt.Sprintf("failed to build job request: %s: rstId %d", ErrConfigRSTTypeIsUnknown.Error(), rstId))
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
// resubmits it through its own walk, so the caller must not submit it. outcome says whether the
// reservation reached remote. notSubmitted when no reservation was sent, answered when remote
// reserved or refused it, and notDelivered when remote may not have received it.
//
// A non-nil err stops the builder job, so it is reserved for the failures that make every later
// path fail the same way: a target with no client, and a reservation the caller could not submit.
// An operation that refuses the request is not one of them. That is recorded on the request as a
// failed precondition and left for the caller to submit, because the next path may still be taken
// by a different operation, and an operation that failed for good is reported once at the end of
// the builder job by the registry's GetFailedOperationErrors.
func (w *jobRequestBuilder) offerToBulkOperation(ctx context.Context, checkpoint CancellationCheckpoint, request *beeremote.JobRequest) (accepted bool, outcome submitOutcome, err error) {
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

	// The reservation uses the request's own job ID, so the later claim names the same job.
	request.SetJobId(w.jobIdFor(request))
	request.SetReservation(beeremote.JobRequest_RESERVE)

	// The request joins the bulk operation before its job is reserved. The add assigns the
	// request's index in the operation, so the reserve request carries complete bulk info and
	// remote stores it on the reserved job. This provides remote with the information needed to
	// update the bulk operation when the request is cancelled.
	checkpoint(checkpointGrace)
	if addErr := w.addToBulkRequest(ctx, request, operation); addErr != nil {
		request.SetReservation(beeremote.JobRequest_NO_RESERVATION)
		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: fmt.Sprintf("failed to add request to bulk operation %q: %s", operation, addErr.Error()),
		})
		return
	}

	checkpoint(checkpointGrace)
	reserveErr := w.submitRequest(ctx, request)
	outcome = answered
	if errors.Is(reserveErr, ErrRequestNotDelivered) {
		outcome = notDelivered
	}
	if reserveErr != nil {
		if errors.Is(reserveErr, ErrJobAlreadyExists) || errors.Is(reserveErr, ErrJobNotAllowed) {
			// Remote refused the reservation because another job for this path conflicts with it.
			// The refusal is about this path alone, so only its request in the operation is failed.
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
) (applyUndo undoFn, terminalOutcome bool) {
	lockedInfo := cfg.GetLockedInfo()

	var applyErr error
	checkpoint(checkpointGrace)
	_, applyUndo, applyErr = applyPlan(ctx, &pathState)
	if applyErr != nil {
		terminalOutcome = IsErrJobTerminalSentinel(applyErr)

		if errors.Is(applyErr, ErrJobAlreadyComplete) {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			})
		} else if errors.Is(applyErr, ErrJobAlreadyOffloaded) {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
			})
		} else if errors.Is(applyErr, ErrJobFailedPrecondition) {
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
		if undoErr := applyUndo.run(ctx); undoErr != nil {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("%s; rollback also failed: %s", message, undoErr.Error()),
			})
			return
		}

		applyUndo = nil
		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: message,
		})
		return
	}

	lockedInfo.SetExternalId(externalId)
	return
}
