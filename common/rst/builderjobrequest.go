package rst

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

type requestPathResolverFn func(walkPath string) (inMountPath string, remotePath string, err error)
type addToBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error)
type getPathStateFn func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error)
type prepareFileStateForWorkRequestsFn func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error)
type clearAccessFlagsFn func(ctx context.Context, path string, flags beegfs.AccessFlags) error
type setDirRstConfigFn func(ctx context.Context, inMountPath string) (isDir bool, err error)

type jobRequestBuilder struct {
	mountPoint       filesystem.Provider
	RstMap           map[uint32]Provider
	jobSubmissionCh  chan<- *beeremote.JobRequest
	builderCfg       *flex.JobRequestCfg
	addToBulkRequest addToBulkRequestFn
	getPathState     getPathStateFn
	prepareFileState prepareFileStateForWorkRequestsFn
	clearAccessFlags clearAccessFlagsFn
	setDirRstConfig  setDirRstConfigFn
}

func (w *jobRequestBuilder) init() {
	w.initSetDirRstConfig()
}

func (w *jobRequestBuilder) initSetDirRstConfig() {
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

func (w *jobRequestBuilder) Process(ctx context.Context, inMountPath string, remotePath string, failedPrecondition error) (err error) {
	var isDir bool
	if isDir, err = w.setDirRstConfig(ctx, inMountPath); isDir || err != nil {
		return
	}

	var pathState PathState
	var skip bool
	var pathIssue error
	if pathState, skip, pathIssue, err = w.resolvePathStateForRequest(ctx, inMountPath); err != nil || skip {
		return
	}
	failedPrecondition = appendError(failedPrecondition, pathIssue)

	var keepLock bool
	defer func() {
		if !keepLock {
			if clearErr := w.clearAccessFlags(ctx, inMountPath, beegfs.LockedContentAccessFlags); clearErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to clear lock: %w", clearErr))
			}
		}
	}()

	for _, cfg := range w.buildJobRequestCfgs(inMountPath, remotePath, pathState.RstCfg.RSTIDs, pathState.LockedInfo, w.builderCfg) {
		canReleaseLock, processErr := w.processJobRequestCfg(ctx, cfg, pathState, failedPrecondition)
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

func (w *jobRequestBuilder) resolvePathStateForRequest(ctx context.Context, inMountPath string) (pathState PathState, skip bool, pathIssue error, err error) {
	addPathIssue := func(err error) {
		pathIssue = appendError(pathIssue, err)
	}

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
			addPathIssue(fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag))
		}
		pathState.RstCfg.RSTIDs = []uint32{w.builderCfg.RemoteStorageTarget}
	} else if len(pathState.RstCfg.RSTIDs) == 0 {
		skip = true
		return
	}

	if pathStateErr != nil {
		addPathIssue(pathStateErr)
	} else if len(pathState.RstCfg.RSTIDs) > 1 && (w.builderCfg.Download || w.builderCfg.StubLocal) {
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
func (w *jobRequestBuilder) processJobRequestCfg(ctx context.Context, cfg *flex.JobRequestCfg, pathState PathState, failedPrecondition error) (canReleaseLock bool, err error) {
	request := w.buildJobRequest(ctx, cfg, failedPrecondition)

	if request.HasGenerationStatus() {
		canReleaseLock = true
	} else {
		if !request.HasBulkInfo() {
			var skipSubmission bool
			if skipSubmission, err = w.addToBulkRequest(ctx, request); err != nil {
				// addToBulkRequest failed and since the file access lock was newly acquired, it may
				// be released.
				canReleaseLock = true
				return
			} else if skipSubmission {
				return
			}
		}
		canReleaseLock = w.prepareJobRequestForSubmission(ctx, request, cfg, pathState)
	}

	w.submitJobRequest(ctx, request)
	return
}

// prepareJobRequestForSubmission prepares a valid job request for submission and returns whether
// the caller may release the path lock afterward. request must have resolved to a valid client.
func (w *jobRequestBuilder) prepareJobRequestForSubmission(ctx context.Context, request *beeremote.JobRequest, cfg *flex.JobRequestCfg, pathState PathState) (canReleaseLock bool) {
	canReleaseLock = true
	lockedInfo := cfg.GetLockedInfo()

	undoPrepare, prepareErr := w.prepareFileState(ctx, w.mountPoint, pathState.RstCfg, pathState.EntryInfo, pathState.OwnerNode, cfg)
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
	client := w.RstMap[request.GetRemoteStorageTarget()]
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

func (w *jobRequestBuilder) submitJobRequest(ctx context.Context, request *beeremote.JobRequest) {
	select {
	case <-ctx.Done():
		return
	case w.jobSubmissionCh <- request:
	}
}
