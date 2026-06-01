package rst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

type requestPathResolverFn func(walkPath string) (inMountPath string, remotePath string, err error)
type tryRouteToBulkOperationFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error)
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type updateDirRstConfigFn func(ctx context.Context, rstID uint32, path string) error
type prepareFileStateForWorkRequestsFn func(ctx context.Context, mountPoint filesystem.Provider, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error)
type clearAccessFlagsFn func(ctx context.Context, path string, flags beegfs.AccessFlags) error

type jobRequestBuilder struct {
	mountPoint              filesystem.Provider
	RstMap                  map[uint32]Provider
	jobSubmissionCh         chan<- *beeremote.JobRequest
	jobRequestCountsCh      chan<- *jobRequestCounts
	builderCfg              *flex.JobRequestCfg
	tryRouteToBulkOperation tryRouteToBulkOperationFn
	getPathState            getPathStateFn
	updateDirRstConfig      updateDirRstConfigFn
	prepareFileState        prepareFileStateForWorkRequestsFn
	clearAccessFlags        clearAccessFlagsFn
}

type jobRequestCounts struct {
	Submitted int32
	Errors    int32
	Conflicts int32
}

func (w *jobRequestBuilder) Process(ctx context.Context, inMountPath string, remotePath string, failedPrecondition error) (err error) {

	var keepLock bool
	var pathState PathState

	if w.builderCfg.GetUpdate() {
		if stat, statErr := w.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
			// Directory update-mode entries do not produce a JobRequest. Apply the persistent RST
			// configuration on a best-effort basis and continue walking regardless of failure.
			w.updateDirRstConfig(ctx, w.builderCfg.RemoteStorageTarget, inMountPath)
			return
		}
	}

	var skip bool
	var pathIssue error
	if pathState, skip, pathIssue, err = w.resolvePathStateForRequest(ctx, inMountPath); err != nil || skip {
		return
	}
	failedPrecondition = appendError(failedPrecondition, pathIssue)

	result := &jobRequestCounts{}
	defer func() {
		w.jobRequestCountsCh <- result
		if !keepLock {
			if clearErr := w.clearAccessFlags(ctx, inMountPath, LockedAccessFlags); clearErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstIds, pathState.LockedInfo, w.builderCfg) {
		canReleaseLock, processErr := w.processJobRequestCfg(ctx, cfg, pathState, failedPrecondition, result)
		if !canReleaseLock {
			keepLock = true
		}
		if processErr != nil {
			err = processErr
			return
		}
	}

	return
}

func (w *jobRequestBuilder) resolvePathStateForRequest(ctx context.Context, inMountPath string) (state PathState, skip bool, pathIssue error, err error) {
	addPathIssue := func(err error) {
		pathIssue = appendError(pathIssue, err)
	}

	var pathStateErr error
	state, pathStateErr = w.getPathState(ctx, w.mountPoint, inMountPath, PathStateWithLock)

	if IsValidRstId(w.builderCfg.RemoteStorageTarget) {
		if IsFileOffloaded(state.LockedInfo) && w.builderCfg.RemoteStorageTarget != state.LockedInfo.StubUrlRstId && !w.builderCfg.GetOverwrite() {
			addPathIssue(fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag))
		}
		state.RstIds = []uint32{w.builderCfg.RemoteStorageTarget}
	} else if len(state.RstIds) == 0 {
		// If the user didn't specify any RSTs and the entry doesn't have any RSTs configured, just
		// silently ignore it. Otherwise pushing a subset of files based on their configured RST IDs
		// would always fail, whenever there is a file with no RSTs set on its entry info.
		skip = true
		return
	}

	if pathStateErr != nil {
		if errors.Is(pathStateErr, ErrGetPathStateFatal) {
			// If this function returns an error but it will also abort the entire builder job, which we
			// generally want to avoid outside fatal errors. Outside fatal errors, if there are any RST
			// IDs available for this inMountPath (either specified by the user, or determined
			// automatically), then report any errors as part of the generated requests for each file.
			// For non-fatal errors on paths that have no RSTs we must just return the error anyway to
			// avoid it being silently dropped.
			err = pathStateErr
			return
		}
		// All other errors are sent as failed preconditions
		addPathIssue(pathStateErr)
	} else if len(state.RstIds) > 1 && (w.builderCfg.Download || w.builderCfg.StubLocal) {
		addPathIssue(ErrFileHasAmbiguousRSTs)
	}

	return
}

// buildJobRequestCfgs returns a list of jobRequestCfgs for each rstId. Each cfg is a clone of the
// original cfg updated with the provided information.
func (w *jobRequestBuilder) buildJobRequestCfgs(inMountPath string, remotePath string, rstIds []uint32, lockedInfo *flex.JobLockedInfo, cfg *flex.JobRequestCfg) []*flex.JobRequestCfg {
	var requests []*flex.JobRequestCfg
	for _, rstId := range rstIds {
		request := proto.Clone(cfg).(*flex.JobRequestCfg)
		request.SetPath(inMountPath)
		request.SetRemotePath(remotePath)
		request.SetRemoteStorageTarget(rstId)
		request.SetLockedInfo(proto.Clone(lockedInfo).(*flex.JobLockedInfo))
		requests = append(requests, request)
	}
	return requests
}

// processJobRequestCfg builds, prepares, and submits the job request for cfg. canReleaseLock is
// returned true only when this path produced no in-flight work that still depends on the lock; once
// the request is routed, prepared, or submitted for real work, the lock must remain held.
func (w *jobRequestBuilder) processJobRequestCfg(ctx context.Context, cfg *flex.JobRequestCfg, pathState PathState, failedPrecondition error, result *jobRequestCounts) (canReleaseLock bool, err error) {

	request := w.buildJobRequest(ctx, cfg, failedPrecondition)

	if request.HasGenerationStatus() {
		canReleaseLock = true
	} else {
		if !request.HasBulkInfo() {
			// The file access lock must be acquired for the path. If the lock was already held, it
			// indicates a job conflict. Offloaded files are the exception because their lock is held
			// until their contents are retrieved.
			if !pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo) {
				result.Conflicts++
				return
			}

			if skip, routeErr := w.tryRouteToBulkOperation(ctx, request); routeErr != nil {
				// Request failed to be routed and since the file access lock was newly acquired, it
				// may be released.
				canReleaseLock = true
				err = routeErr
				return
			} else if skip {
				return
			}
		}
		canReleaseLock = w.prepareJobRequestForSubmission(ctx, request, cfg, pathState.EntryInfo, pathState.OwnerNode)
	}

	w.submitJobRequest(ctx, request, result)
	return
}

// prepareJobRequestForSubmission prepares a valid job request for submission and returns whether
// the caller may release the path lock afterward. request must have resolved to a valid client.
func (w *jobRequestBuilder) prepareJobRequestForSubmission(ctx context.Context, request *beeremote.JobRequest, cfg *flex.JobRequestCfg, entryInfo msg.EntryInfo, ownerNode beegfs.Node) (canReleaseLock bool) {
	canReleaseLock = true
	lockedInfo := cfg.GetLockedInfo()

	undoPrepare, prepareErr := w.prepareFileState(ctx, w.mountPoint, entryInfo, ownerNode, cfg)
	if prepareErr != nil {
		if errors.Is(prepareErr, ErrJobAlreadyComplete) {
			request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			}
		} else if errors.Is(prepareErr, ErrJobAlreadyOffloaded) {
			canReleaseLock = false
			request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
				State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
			}
		} else {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to prepare file state: %s", prepareErr.Error()),
			})
		}
		return
	}

	// Generating the externalId must be the last possible error to avoid situations where, once the
	// externalId is generated, it would be lost as a result of a subsequent preconditional failure.
	client, _ := w.RstMap[request.GetRemoteStorageTarget()]
	externalId, err := client.GenerateExternalId(ctx, cfg)
	if err != nil {
		if undoErr := undoPrepare(); undoErr != nil {
			canReleaseLock = false
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("failed to generate external id: %s; rollback also failed: %s", err.Error(), undoErr.Error()),
			})
			return
		}

		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: fmt.Sprintf("failed to generate external id: %s", err.Error()),
		})
		return
	}
	lockedInfo.SetExternalId(externalId)
	canReleaseLock = false

	return
}

func (w *jobRequestBuilder) buildJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, failedPrecondition error) *beeremote.JobRequest {
	rstId := cfg.GetRemoteStorageTarget()
	client, ok := w.RstMap[rstId]
	if !ok {
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

func (w *jobRequestBuilder) submitJobRequest(ctx context.Context, request *beeremote.JobRequest, result *jobRequestCounts) {
	select {
	case <-ctx.Done():
		return
	case w.jobSubmissionCh <- request:
		result.Submitted++
		if isStatusError(request.GetGenerationStatus()) {
			result.Errors++
		}
	}
}
