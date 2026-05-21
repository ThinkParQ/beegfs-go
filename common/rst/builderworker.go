package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

type requestPathResolver func(walkPath string) (inMountPath string, remotePath string, err error)
type tryRouteToBulkOperationFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool)

func (c *JobBuilderClient) newRequestBuilderWorker(
	builderCfg *flex.JobRequestCfg,
	walkCh <-chan *filesystem.StreamPathResult,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	tryRouteToBulkOperation tryRouteToBulkOperationFn,
) *requestBuilderWorker {
	return &requestBuilderWorker{
		mountPoint:              c.mountPoint,
		RstMap:                  c.rstMap,
		jobSubmissionCh:         jobSubmissionCh,
		builderCfg:              builderCfg,
		walkCh:                  walkCh,
		getPaths:                c.getPathsFn(builderCfg),
		tryRouteToBulkOperation: tryRouteToBulkOperation,
		result:                  &requestBuilderWorkerResult{},
	}
}

func (c *JobBuilderClient) getPathsFn(cfg *flex.JobRequestCfg) requestPathResolver {
	if cfg.Download {
		if walkLocalPathInsteadOfRemote(cfg) {
			// Walking cfg.Path to support stub file download and files with a defined rst.
			return func(walkPath string) (string, string, error) {
				return walkPath, "", nil
			}
		}

		return func(walkPath string) (string, string, error) {
			// GetDownloadInMountPath should never return an error happen since remotePath and
			// remotePathDir are derived from cfg.RemotePath, so any error here indicates a bug
			// in the walking logic.
			remotePathDir, remotePathIsGlob := GetDownloadRemotePathDirectory(cfg.RemotePath)
			stat, err := c.mountPoint.Lstat(cfg.Path)
			isPathDir := err == nil && stat.IsDir()

			remotePath := walkPath
			inMountPath, err := GetDownloadInMountPath(cfg.Path, remotePath, remotePathDir, remotePathIsGlob, isPathDir, cfg.Flatten)
			if err == nil {
				// Ensure the local directory structure supports the object downloads
				err = c.mountPoint.CreateDir(filepath.Dir(inMountPath), 0755)
			}
			return inMountPath, remotePath, err
		}
	}

	return func(walkPath string) (string, string, error) {
		return walkPath, walkPath, nil
	}
}

type requestBuilderWorkerResult struct {
	Submitted   int32
	Errors      int32
	Conflicts   int32
	ResumeToken string
}

func (r *requestBuilderWorkerResult) Merge(result *requestBuilderWorkerResult) *requestBuilderWorkerResult {
	merged := &requestBuilderWorkerResult{
		Submitted: r.Submitted + result.Submitted,
		Errors:    r.Submitted + result.Submitted,
		Conflicts: r.Conflicts + result.Conflicts,
	}

	if r.ResumeToken == "" && result.ResumeToken != "" {
		merged.ResumeToken = result.ResumeToken
	}

	return merged
}

type requestBuilderWorker struct {
	mountPoint              filesystem.Provider
	RstMap                  map[uint32]Provider
	jobSubmissionCh         chan<- *beeremote.JobRequest
	builderCfg              *flex.JobRequestCfg
	tryRouteToBulkOperation tryRouteToBulkOperationFn
	walkCh                  <-chan *filesystem.StreamPathResult
	getPaths                requestPathResolver
	result                  *requestBuilderWorkerResult
}

func (w *requestBuilderWorker) Run(ctx context.Context, doneCh chan<- struct{}) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case walkResp, ok := <-w.walkCh:
			if !ok {
				select {
				case doneCh <- struct{}{}:
				default:
				}
				return nil
			}

			var failedPrecondition error
			if walkResp.Err != nil {
				if cancelErr, ok := errors.AsType[*RequestCancelError](walkResp.Err); ok {
					appendError(failedPrecondition, cancelErr.Reason)
				} else {
					return walkResp.Err
				}
			}

			if walkResp.ResumeToken != "" {
				w.result.ResumeToken = walkResp.ResumeToken
				return nil
			}

			inMountPath, remotePath, err := w.getPaths(walkResp.Path)
			if err != nil {
				return err
			}

			if w.builderCfg.GetUpdate() {
				if stat, statErr := w.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
					// Directory update-mode entries do not produce a JobRequest. Apply the persistent
					// RST configuration on a best-effort basis and continue walking regardless of failure.
					updateDirRstConfig(ctx, w.builderCfg.RemoteStorageTarget, inMountPath)
					return nil
				}
			}

			resolved, err := w.resolvePathStateForRequest(ctx, inMountPath)
			if err != nil {
				return err
			}
			if resolved.Skip {
				continue
			}
			if resolved.FailedPrecondition != nil {
				appendError(failedPrecondition, resolved.FailedPrecondition)
			}

			keepLock := false
			for _, requestCfg := range w.buildJobRequestCfgs(inMountPath, remotePath, resolved.PathState.RstIds, resolved.PathState.LockedInfo, w.builderCfg) {
				request := w.buildJobRequest(ctx, requestCfg, failedPrecondition)

				if !request.HasGenerationStatus() {
					if !request.HasBulkInfo() {
						// The file access lock must be acquired for the path. If the lock was already held, it
						// indicates a job conflict. Offloaded files are the exception because their lock is held
						// until their contents are retrieved.
						if !resolved.PathState.LockAcquired && !IsFileOffloaded(resolved.PathState.LockedInfo) {
							w.result.Conflicts++
							continue
						}

						if skip := w.tryRouteToBulkOperation(ctx, request); skip {
							keepLock = true
							continue
						}
					}

					w.prepareJobRequestForSubmission(ctx, request, requestCfg, resolved.PathState.EntryInfo, resolved.PathState.OwnerNode, &keepLock)
				}

				select {
				case <-ctx.Done():
					return nil
				case w.jobSubmissionCh <- request:
					if isStatusError(request.GetGenerationStatus()) {
						w.result.Errors++
					}
					w.result.Submitted++
				}
			}

			if !keepLock {
				if err := entry.ClearAccessFlags(ctx, inMountPath, LockedAccessFlags); err != nil {
					return errors.Join(err, fmt.Errorf("unable to clear lock: %w", err))
				}
			}
		}
	}
}

func (w *requestBuilderWorker) buildJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, failedPrecondition error) *beeremote.JobRequest {
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

func (w *requestBuilderWorker) Clone() *requestBuilderWorker {
	clone := *w
	clone.result = &requestBuilderWorkerResult{}
	return &clone
}

func (w *requestBuilderWorker) GetResult() *requestBuilderWorkerResult {
	return w.result
}

type resolvedPathState struct {
	PathState          PathState
	FailedPrecondition error
	Skip               bool
}

func (w *requestBuilderWorker) resolvePathStateForRequest(ctx context.Context, inMountPath string) (*resolvedPathState, error) {
	resolved := &resolvedPathState{}
	addFailedPrecondition := func(err error) {
		resolved.FailedPrecondition = appendError(resolved.FailedPrecondition, err)
	}

	pathState, pathStateErr := GetPathState(ctx, w.mountPoint, inMountPath, PathStateWithLock)
	resolved.PathState = pathState

	if IsValidRstId(w.builderCfg.RemoteStorageTarget) {
		if IsFileOffloaded(pathState.LockedInfo) && w.builderCfg.RemoteStorageTarget != pathState.LockedInfo.StubUrlRstId && !w.builderCfg.GetOverwrite() {
			addFailedPrecondition(fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag))
		}
		pathState.RstIds = []uint32{w.builderCfg.RemoteStorageTarget}
	} else if len(pathState.RstIds) == 0 {
		// If the user didn't specify any RSTs and the entry doesn't have any RSTs configured, just
		// silently ignore it. Otherwise pushing a subset of files based on their configured RST IDs
		// would always fail, whenever there is a file with no RSTs set on its entry info.
		resolved.Skip = true
		return resolved, nil
	}

	if pathStateErr != nil {
		if errors.Is(pathStateErr, ErrGetPathStateFatal) {
			// If this function returns an error but it will also abort the entire builder job, which we
			// generally want to avoid outside fatal errors. Outside fatal errors, if there are any RST
			// IDs available for this inMountPath (either specified by the user, or determined
			// automatically), then report any errors as part of the generated requests for each file.
			// For non-fatal errors on paths that have no RSTs we must just return the error anyway to
			// avoid it being silently dropped.
			return nil, pathStateErr
		}
		// All other errors are sent as failed preconditions
		addFailedPrecondition(pathStateErr)
	} else if len(pathState.RstIds) > 1 && (w.builderCfg.Download || w.builderCfg.StubLocal) {
		addFailedPrecondition(ErrFileHasAmbiguousRSTs)
	}

	return resolved, nil
}

// buildJobRequestCfgs returns a list of jobRequestCfgs for each rstId. Each cfg is a clone of the
// original cfg updated with the provided information.
func (w *requestBuilderWorker) buildJobRequestCfgs(inMountPath string, remotePath string, rstIds []uint32, lockedInfo *flex.JobLockedInfo, cfg *flex.JobRequestCfg) []*flex.JobRequestCfg {
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

// prepareJobRequestForSubmission prepares a valid job request for submission. request must have resolved to a valid client.
func (w *requestBuilderWorker) prepareJobRequestForSubmission(
	ctx context.Context,
	request *beeremote.JobRequest,
	cfg *flex.JobRequestCfg,
	entryInfo msg.EntryInfo,
	ownerNode beegfs.Node,
	keepLock *bool,
) {
	lockedInfo := cfg.GetLockedInfo()
	undoPrepare, prepareErr := PrepareFileStateForWorkRequests(ctx, w.mountPoint, entryInfo, ownerNode, cfg)
	if prepareErr != nil {
		if errors.Is(prepareErr, ErrJobAlreadyComplete) {
			request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			}
		} else if errors.Is(prepareErr, ErrJobAlreadyOffloaded) {
			*keepLock = true
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
			*keepLock = true
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
	*keepLock = true
}
