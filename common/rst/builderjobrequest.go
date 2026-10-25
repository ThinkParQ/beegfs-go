package rst

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync/atomic"
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

// terminalJobIdSuffix keeps the ID of a terminal request apart from the ID of a reservation for
// the same path and RST. terminalJobIdFor defines a terminal request.
const terminalJobIdSuffix = "\x00terminal"

// addToBulkRequestFn offers a request to the bulk operation that handles its remote storage target.
// addedToBulk is true when an operation took the request. The operation then owns it and the caller
// must not submit it. The operation decides whether the request ever becomes a job: it may submit
// it later, or drop it so no job ever runs it.
type addToBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest, operation string) error
type updateBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type planFileStateForWorkRequestsFn func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error)
type setDirRstConfigFn func(ctx context.Context, inMountPath string) error

// setContentLockFn sets the content lock on the file at path. pathState is the builder's read of
// the path, and its entry info and owner spare the lock a second lookup. It is setContentLock
// outside tests. It returns entry.ErrAccessFlagsUnchanged when the lock was already set, and an
// error wrapping beegfs.OpsErr_INUSE when a client has the file open in a way the lock conflicts
// with.
type setContentLockFn func(ctx context.Context, path string, pathState PathState) error

// setContentLock sets LockedContentAccessFlags with entry.SetAccessFlagsWithEntryInfo, using the
// entry info and owner in pathState.
func setContentLock(ctx context.Context, path string, pathState PathState) error {
	entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
	if entryInfoMsg == nil {
		return fmt.Errorf("original entry info is unavailable")
	}
	return entry.SetAccessFlagsWithEntryInfo(ctx, path, beegfs.LockedContentAccessFlags, *entryInfoMsg, pathState.OwnerNode)
}

// clearContentLock clears what setContentLock set, using the same entry info and owner.
func clearContentLock(ctx context.Context, path string, pathState PathState) error {
	entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
	if entryInfoMsg == nil {
		return fmt.Errorf("original entry info is unavailable")
	}
	return entry.ClearAccessFlagsWithEntryInfo(ctx, path, beegfs.LockedContentAccessFlags, *entryInfoMsg, pathState.OwnerNode)
}

type jobRequestBuilder struct {
	log        *zap.Logger
	mountPoint filesystem.Provider
	RstMap     map[uint32]Provider
	// submitRequest, reserveRequest and cancelReservation are the builder's calls to remote, as
	// RemoteJobCalls describes. The builder clears a content lock itself only in
	// lockAndCheckUnchanged, and only for a lock it just set for a request that writes the file.
	submitRequest     SubmitRequestFn
	reserveRequest    ReserveRequestFn
	cancelReservation CancelReservationFn
	setContentLock    setContentLockFn
	clearContentLock  setContentLockFn
	builderJobId      uuid.UUID
	builderCfg        *flex.JobRequestCfg
	addToBulkRequest  addToBulkRequestFn
	updateBulkRequest updateBulkRequestFn
	getPathState      getPathStateFn
	planFileState     planFileStateForWorkRequestsFn
	setDirRstConfig   setDirRstConfigFn
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

	if len(pathState.RstCfg.RSTIDs) == 0 && failedPrecondition != nil {
		// There are no rstIds and there is a failed precondition. So, in order to communicate the issue to
		// the user, submit the request with a generation status under the invalid rstId 0. No job
		// will be created just a record of the failure.
		checkpoint(checkpointGrace)
		cfg := w.buildJobRequestCfg(inMountPath, remotePath, 0, pathState.LockedInfo, w.builderCfg)
		request := BuildJobRequestWithFailedPrecondition(nil, cfg, failedPrecondition.Error())
		_, _, err = w.processRequest(workCtx, checkpoint, cfg, pathState, request)
		return
	}

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		// Each rstId is an independent round of remote work, so it earns its own grace.
		checkpoint(checkpointGrace)
		request := w.buildRequest(workCtx, cfg, failedPrecondition)
		_, submitted, processErr := w.processRequest(workCtx, checkpoint, cfg, pathState, request)
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

	// The reservation pass locked the file after remote recorded the reservation and checked the
	// file had not changed. The RESERVED job has kept the lock since, so no client could open the
	// file to change it, and the claim needs no second check. Two cases find no lock:
	//   - A file that did not exist when it was reserved, such as the target of a download. The
	//     plan creates it and locks it.
	//   - An existing file found unlocked. The reservation is gone or the lock was cleared by hand.
	//     The lock is taken again and logged, and the claim is most likely refused. Remote's release
	//     at the end of that claim clears the lock again, so this pass never has to release it.
	pathState, pathStateErr := w.getPathState(workCtx, w.mountPoint, result.InMountPath, PathStateWithLock)
	if pathState.LockAcquired {
		w.log.Warn("content lock was not held when claiming a reserved request, so it was taken again", zap.String("path", result.InMountPath), zap.String("jobId", result.ReservedJobId))
	}

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

	_, _, err = w.processRequest(workCtx, checkpoint, cfg, pathState, request)
	return
}

// resolvePathStateForRequest retrieves the current path's state information without taking the
// content lock, and then updates its rstIds list before returning. The lock is taken later, and
// only for a file whose plan changes it, once remote has reserved a job for it. If the builder job
// did not provide an rstId then the file's own rst configuration will be used; and if the
// configured rstIds list is empty, skip will be true so no request is created and the builder job
// does not fail.
//
// Directories are the exception since they return early with skip false and their rstIds untouched.
//
// pathIssue will return any non-fatal errors returned while retrieving path state information or
// conflicts. All fatal errors will be reported via err.
func (w *jobRequestBuilder) resolvePathStateForRequest(ctx context.Context, inMountPath string) (pathState PathState, skip bool, pathIssue error, err error) {
	var pathStateErr error
	pathState, pathStateErr = w.getPathState(ctx, w.mountPoint, inMountPath, PathStateNoLock)
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

// jobIdFor derives the job ID of a reservation from the builder job ID, the request's path and its
// RST. The same builder job always derives the same ID for the same path and RST. So when a request
// is sent again, after a lost response or after a rescheduled builder job replays from an older
// resume token, remote recognizes it and answers with the job the first request created instead of
// creating a second one. A reservation and its claim share this ID, and so does its cancel.
func (w *jobRequestBuilder) jobIdFor(request *beeremote.JobRequest) string {
	name := binary.BigEndian.AppendUint32([]byte(request.GetPath()), request.GetRemoteStorageTarget())
	return uuid.NewSHA1(w.builderJobId, name).String()
}

// terminalJobIdFor derives the job ID of a terminal request. A terminal request is one the builder
// submits without a reservation, because it carries an outcome rather than work: already
// complete, already offloaded, or a failed precondition. Remote may record it as a finished job,
// but it never runs work and never holds the lock.
//
// The ID is deterministic like jobIdFor, so a replay of the same terminal request gets the same
// answer. It differs from jobIdFor because one path can get both kinds within one builder job. For
// example, a pass reserves and claims an upload, the builder job is rescheduled after a crash, and
// its rewalk finds the file already complete. Remote refuses a terminal request under the claimed
// job's ID as a collision, so the path would be miscounted as not allowed.
func (w *jobRequestBuilder) terminalJobIdFor(request *beeremote.JobRequest) string {
	name := binary.BigEndian.AppendUint32([]byte(request.GetPath()), request.GetRemoteStorageTarget())
	return uuid.NewSHA1(w.builderJobId, append(name, terminalJobIdSuffix...)).String()
}

// submitOutcome says what remote knows about a path's requests once the builder is done with them.
// The builder takes a lock only under a reservation, so the outcome never decides a lock release.
// Tests use it to check which call ended the path.
type submitOutcome int

const (
	// notSubmitted means no request for the path reached remote.
	notSubmitted submitOutcome = iota
	// answered means remote answered every request. Remote released the lock at the end of each
	// call if no job still needed it.
	answered
	// notDelivered means remote may or may not have recorded a request. The builder leaves the
	// file, the lock and any external ID as they are, so a replay finds them unchanged.
	notDelivered
)

// processRequest prepares the job request for cfg and submits it. It returns what remote knows
// about the request, and whether remote accepted it.
//
// A request whose plan changes the file is reserved and locked first, by reserveAndLock, and then
// submitted as the claim of that reservation. A request whose plan changes nothing, or that already
// carries a generation status, is submitted as it is, without a lock. A claim from a bulk operation
// arrives with its reservation's lock already held.
//
// If remote refuses a claim, remote undoes whatever the plan changed in the file before it decides
// the content lock at the end of the submit. The builder never undoes a submitted plan, and never
// clears the lock itself.
func (w *jobRequestBuilder) processRequest(
	ctx context.Context,
	checkpoint CancellationCheckpoint,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
) (outcome submitOutcome, submitted bool, err error) {
	if !request.HasJobId() {
		// A request that reserves takes the reservation's ID in reserveAndLock instead.
		request.SetJobId(w.terminalJobIdFor(request))
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

	changesFile := workRequired || !planLeavesFileUnchanged(cfg)
	if changesFile && !request.HasGenerationStatus() && request.GetReservation() == beeremote.JobRequest_NO_RESERVATION {
		done, reserveOutcome, reserveErr := w.reserveAndLock(ctx, checkpoint, cfg, pathState, request, workRequired)
		if done {
			return reserveOutcome, false, reserveErr
		}
	}

	if !request.HasGenerationStatus() {
		w.prepareJobRequest(ctx, checkpoint, cfg, pathState, request, applyPlan)
	}

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

	// Remote refused the request, so no job will ever run it. The builder changes the file only for
	// a claim, and remote undoes a refused claim's changes with the RST abort before it decides the
	// lock. That abort also releases the external ID. So only the bulk operation is left to tell.
	if request.HasBulkInfo() {
		checkpoint(checkpointGrace)
		err = w.updateBulkRequest(ctx, request, BulkRequestFailed)
	}
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

// reserveAndLock runs for a request whose plan changes the file, before the plan is applied. It
// reserves the request's job with remote, takes the content lock, and checks that the file is still
// the one the request was built from. Remote records the job before the lock is set, so every lock
// the builder takes has a job that keeps it until the reservation is claimed or cancelled.
//
// Its inputs:
//   - cfg and request were built from pathState, which the builder read without the lock.
//   - workRequired says the plan needs a transfer. Only such a request can join a bulk operation.
//
// The rules, in order:
//   - A request that needs a transfer is offered to a bulk operation first. One the operation
//     takes is reserved with its bulk info, and is claimed later by the operation's own walk. One
//     the operation can't add is left with a failed precondition, and the caller submits it
//     without a reservation or a lock.
//   - A reservation remote refuses ends the path. Remote answered, and nothing was locked for it.
//   - A replayed reservation that was already claimed or cancelled ends the path. Its job already
//     holds the outcome, and the builder must not change the file under it.
//   - A file that changed before the lock was set, or that a client has open, gets its reservation
//     cancelled. Remote then releases the lock unless another job on the path needs it.
//   - Otherwise the request now carries CLAIM, and the caller applies the plan and claims the job.
//
// done is true when the path ends here, and outcome then says what remote knows. A non-nil err
// stops the builder job, so it is kept for failures that make every later path fail the same way:
// a target with no client, and a reservation remote answered with something other than a refusal.
// A bulk operation that refuses the request is not one of them, because the next path may still be
// taken by another operation, and an operation that failed for good is reported once at the end of
// the builder job by the registry's GetFailedOperationErrors.
func (w *jobRequestBuilder) reserveAndLock(
	ctx context.Context,
	checkpoint CancellationCheckpoint,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
	workRequired bool,
) (done bool, outcome submitOutcome, err error) {
	done = true
	rstId := request.GetRemoteStorageTarget()
	client, ok := w.RstMap[rstId]
	if !ok {
		err = fmt.Errorf("%w: rstId %d", ErrConfigRSTTypeIsUnknown, rstId)
		return
	}

	// The reservation uses the request's own job ID, so the claim names the same job.
	request.SetJobId(w.jobIdFor(request))
	request.SetReservation(beeremote.JobRequest_RESERVE)

	// A request joins its bulk operation before its job is reserved. The add assigns the request's
	// index in the operation, so the reservation carries complete bulk info and remote stores it on
	// the reserved job. A request on the builder's own path never joins one, because the operation
	// would hold that one path alone.
	operation := ""
	if workRequired && !request.HasBulkInfo() && cfg.GetPath() != w.builderCfg.GetPath() {
		checkpoint(checkpointGrace)
		include, includeOperation := client.IncludeRequestInBulkOperation(ctx, request)
		if include {
			checkpoint(checkpointGrace)
			if addErr := w.addToBulkRequest(ctx, request, includeOperation); addErr != nil {
				// Nothing was reserved, and the request now carries a failed precondition, so it goes
				// out as a terminal request under the terminal ID.
				request.SetReservation(beeremote.JobRequest_NO_RESERVATION)
				request.SetJobId(w.terminalJobIdFor(request))
				request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
					State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
					Message: fmt.Sprintf("failed to add request to bulk operation %q: %s", includeOperation, addErr.Error()),
				})
				done = false
				return
			}
			operation = includeOperation
		}
	}

	checkpoint(checkpointGrace)
	job, reserveErr := w.reserveRequest(ctx, request)
	outcome = answered
	switch {
	case errors.Is(reserveErr, ErrRequestNotDelivered):
		// Remote may hold the reservation. The builder took no lock. Only a crash replays the path:
		// the rescheduled builder job finds the reservation under the same ID and resumes from it.
		// After a shutdown or a cancel the walk has moved past the path, and the reservation stays
		// until a user runs `beegfs remote job cancel --force`.
		outcome = notDelivered
		return
	case IsClaimedReplay(request, job):
		// A replay of a reservation an earlier pass already claimed. The claim's job owns the file,
		// and sync counted the claim's answer when the claim was made.
		return
	case reserveErr != nil:
		if errors.Is(reserveErr, ErrJobAlreadyExists) || errors.Is(reserveErr, ErrJobNotAllowed) || errors.Is(reserveErr, ErrJobNotReserved) {
			// Another job on the path conflicts with this one, or this reservation was cancelled
			// before anything claimed it. Either way the refusal is about this path alone.
			err = w.failBulkRequest(ctx, request, operation)
			return
		}
		err = fmt.Errorf("failed to reserve job for %s: %w", request.GetPath(), reserveErr)
		return
	}

	// A replay checks the file against the request remote stored, because the file must still
	// match what the reservation was made for. A first reservation stored this request, and remote
	// may answer without echoing it, so the request's own locked info stands in.
	reserved := requestLockedInfo(job.GetRequest())
	if reserved == nil {
		reserved = cfg.GetLockedInfo()
	}
	checkpoint(checkpointGrace)
	if reason := w.lockAndCheckUnchanged(ctx, cfg, pathState, reserved); reason != nil {
		checkpoint(checkpointGrace)
		cancelErr := w.cancelReservation(ctx, request, reason)
		if errors.Is(cancelErr, ErrRequestNotDelivered) {
			// The reservation stays RESERVED. For a request that writes the file,
			// lockAndCheckUnchanged already cleared the lock it set. A read-only request keeps its
			// lock until the reservation ends. Only a crash replays the path, so after a shutdown or
			// a cancel the reservation stays until a user cancels it with --force.
			outcome = notDelivered
			w.log.Warn("unable to cancel the reservation of a file that changed, the reservation stays until the builder job is resumed after a crash or a user runs 'beegfs remote job cancel --force'", zap.String("path", request.GetPath()), zap.String("jobId", request.GetJobId()), zap.NamedError("reason", reason), zap.Error(cancelErr))
		} else if cancelErr != nil {
			// Remote refuses a claim of a reservation it no longer holds, and its release at the end
			// of that call decided the lock. An error from before that release, such as a path store
			// failure, leaves the reservation RESERVED and the file locked until a user cancels it
			// with --force.
			w.log.Warn("remote refused to cancel the reservation of a file that changed", zap.String("path", request.GetPath()), zap.String("jobId", request.GetJobId()), zap.NamedError("reason", reason), zap.Error(cancelErr))
		}
		err = w.failBulkRequest(ctx, request, operation)
		return
	}

	if operation != "" {
		// The operation claims the job later through its own walk.
		return
	}
	request.SetReservation(beeremote.JobRequest_CLAIM)
	done = false
	return
}

// IsClaimedReplay reports whether job, which remote answered a RESERVE for request with, is
// request's own reservation and was already claimed. Remote answers a replayed RESERVE with the
// job under the same ID. It also answers a RESERVE it refuses with the job that blocked it, and
// that job is another builder's or an earlier run's, often claimed too. Only the job ID tells the
// two apart, so a refusal is never mistaken for a replay and skipped without being counted.
func IsClaimedReplay(request *beeremote.JobRequest, job *beeremote.Job) bool {
	return job.GetId() != "" && job.GetId() == request.GetJobId() && job.GetRequest().GetReservation() == beeremote.JobRequest_CLAIM
}

// failBulkRequest tells the bulk operation that took request that no job will run it. It does
// nothing when operation is empty, because no operation took the request.
func (w *jobRequestBuilder) failBulkRequest(ctx context.Context, request *beeremote.JobRequest, operation string) error {
	if operation == "" {
		return nil
	}
	if err := w.updateBulkRequest(ctx, request, BulkRequestFailed); err != nil {
		return fmt.Errorf("unable to release %s from bulk operation %q after its reservation ended: %w", request.GetPath(), operation, err)
	}
	return nil
}

// lockAndCheckUnchanged runs after remote reserved a job for the file at cfg's path. It takes the
// content lock and then checks that the file is the one the reservation was made for. It returns
// nil when the file is locked and unchanged, and otherwise the reason the reservation must be
// cancelled. A reason wraps ErrFileChangedDuringBuild when the file changed or a client has it
// open.
//
// When the check fails, a lock this call set is cleared again for a request that writes the file,
// a download or an upload that leaves a stub. The file is not the one the reservation was made
// for, so the lock would otherwise sit on a file no job owns. If the cancel is then not delivered,
// a forced cancel's abort would find that lock and change the file. Clearing it is safe because
// remote refuses every other job on a path while a request that writes it holds a reservation, so
// no other job can rely on this lock. A lock is kept in two cases:
//   - The lock was already set. It is not this call's to clear. An offloaded file keeps its lock
//     for good, and an earlier pass of the same reservation may have set it.
//   - The request only reads the file. Remote lets read-only jobs to other RSTs share the path, so
//     one may have found this call's lock already set and relied on it. Its abort never changes
//     the file, so the lock does no harm. The rule mirrors Job.IsReadOnly in remote.
func (w *jobRequestBuilder) lockAndCheckUnchanged(ctx context.Context, cfg *flex.JobRequestCfg, pathState PathState, reserved *flex.JobLockedInfo) (reason error) {
	path := cfg.GetPath()
	lockedInfo := cfg.GetLockedInfo()
	if FileExists(pathState.LockedInfo) {
		lockErr := w.setContentLock(ctx, path, pathState)
		if lockErr == nil && (cfg.GetDownload() || cfg.GetStubLocal()) {
			// Clears the lock again if the check below fails. See the function comment.
			defer func() {
				if reason == nil {
					return
				}
				if clearErr := w.clearContentLock(ctx, path, pathState); clearErr != nil && !errors.Is(clearErr, entry.ErrAccessFlagsUnchanged) {
					w.log.Warn("unable to clear the content lock set on a file that changed", zap.String("path", path), zap.NamedError("reason", reason), zap.Error(clearErr))
				}
			}()
		}

		switch {
		case lockErr == nil, errors.Is(lockErr, entry.ErrAccessFlagsUnchanged):
		case errors.Is(lockErr, beegfs.OpsErr_INUSE):
			return fmt.Errorf("%w: a client has it open: %w", ErrFileChangedDuringBuild, lockErr)
		default:
			return fmt.Errorf("unable to lock %s: %w", path, lockErr)
		}
		lockedInfo.SetReadWriteLocked(true)
	}

	stat, statErr := w.mountPoint.Lstat(path)
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return fmt.Errorf("unable to check %s after locking it: %w", path, statErr)
	}
	exists := statErr == nil
	switch {
	case exists != FileExists(reserved):
		return fmt.Errorf("%w: it was created or removed", ErrFileChangedDuringBuild)
	case !exists:
		return nil
	case stat.Size() != reserved.GetSize():
		return fmt.Errorf("%w: its size changed from %d to %d", ErrFileChangedDuringBuild, reserved.GetSize(), stat.Size())
	case !stat.ModTime().Equal(reserved.GetMtime().AsTime()):
		return fmt.Errorf("%w: its mtime changed from %s to %s", ErrFileChangedDuringBuild, reserved.GetMtime().AsTime(), stat.ModTime())
	case uint32(stat.Mode()) != reserved.GetMode():
		return fmt.Errorf("%w: its mode changed from %s to %s", ErrFileChangedDuringBuild, fs.FileMode(reserved.GetMode()), stat.Mode())
	}
	if ino := statIno(stat); ino != 0 && pathState.Ino != 0 && ino != pathState.Ino {
		return fmt.Errorf("%w: the path now names another file", ErrFileChangedDuringBuild)
	}
	return nil
}

// requestLockedInfo returns the locked info a job request carries, or nil for a request type that
// carries none.
func requestLockedInfo(request *beeremote.JobRequest) *flex.JobLockedInfo {
	switch request.WhichType() {
	case beeremote.JobRequest_Sync_case:
		return request.GetSync().GetLockedInfo()
	case beeremote.JobRequest_Mock_case:
		return request.GetMock().GetLockedInfo()
	}
	return nil
}

func (w *jobRequestBuilder) prepareJobRequest(
	ctx context.Context,
	checkpoint CancellationCheckpoint,
	cfg *flex.JobRequestCfg,
	pathState PathState,
	request *beeremote.JobRequest,
	applyPlan applyPlanFn,
) {
	lockedInfo := cfg.GetLockedInfo()

	checkpoint(checkpointGrace)
	_, applyUndo, applyErr := applyPlan(ctx, &pathState)
	if applyErr != nil {
		// applyPlan undoes its own changes when a step fails, and returns an undo only when that
		// rollback failed too. The file may then be left changed, so the request reports ERROR,
		// and remote runs the RST abort on it. FAILED_PRECONDITION would tell remote that nothing
		// was changed.
		rollbackFailed := applyUndo != nil && !IsErrJobTerminalSentinel(applyErr)
		switch {
		case errors.Is(applyErr, ErrJobAlreadyComplete):
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			})
		case errors.Is(applyErr, ErrJobAlreadyOffloaded):
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
			})
		case errors.Is(applyErr, ErrJobFailedPrecondition) && !rollbackFailed:
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to prepare file state: %s", applyErr.Error()),
			})
		default:
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

		// Nothing was submitted yet, so the builder undoes its own plan here, under the lock the
		// reservation holds.
		checkpoint(checkpointGrace)
		if undoErr := applyUndo.run(ctx); undoErr != nil {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("%s; rollback also failed: %s", message, undoErr.Error()),
			})
			return
		}

		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: message,
		})
		return
	}

	lockedInfo.SetExternalId(externalId)
}
