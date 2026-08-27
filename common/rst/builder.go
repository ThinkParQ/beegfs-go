package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// TODO: Add to remote a global builder section that has a maxRequests. Also, allow per remote storage target overrides
// in case some targets next more or less or it doesn't matter.
//   - 0 should be as many as possible.
//   - Add something like only-count-active-submissions-against-max-requests flag to remote-storage-target?
//	   This would ensure that only jobs that were submitted with a non-terminal state would be counted against maxRequests.

const maxRequests = 1000

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
}

var _ Provider = &JobBuilderClient{}

func NewJobBuilderClient(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider) *JobBuilderClient {
	return &JobBuilderClient{
		ctx:        ctx,
		rstMap:     rstMap,
		mountPoint: mountPoint,
	}
}

func (c *JobBuilderClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: 0,
		StubLocal:           cfg.StubLocal,
		RestorePolicy:       cfg.RestorePolicy,
		CooldownSecs:        cfg.CooldownSecs,
		Priority:            cfg.GetPriority(),
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Builder{
			Builder: &flex.BuilderJob{
				Cfg: cfg,
			},
		},
		Update: cfg.Update,
	}
}

func (c *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (workRequests []*flex.WorkRequest, err error) {
	if !job.Request.HasBuilder() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	workRequests = RecreateWorkRequests(job, nil)
	return
}

func (c *JobBuilderClient) ExecuteJobBuilderRequest(
	shutdownCtx context.Context,
	workCtx context.Context,
	log *zap.Logger,
	workRequest *flex.WorkRequest,
	submitRequest SubmitRequestFn,
	workerSaturation []func() float64,
) (result *SchedulingResult) {
	if !workRequest.HasBuilder() {
		return &SchedulingResult{Err: ErrReqAndRSTTypeMismatch}
	}

	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	registry, err := c.newBulkOperationRegistry(workCtx, workRequest.GetJobId())
	if err != nil {
		return &SchedulingResult{Err: err}
	}
	defer func() {
		if closeErr := registry.Close(workCtx); closeErr != nil {
			result.Err = appendErrors(result.Err, closeErr)
		}
	}()

	controller := c.newRequestBuildController(shutdownCtx, workCtx, log, cfg, submitRequest, registry.AddRequest, workerSaturation)
	resumeToken := workRequest.GetExternalId()

	var walkErr error
	if workCtx.Err() == nil {
		var walkComplete bool
		if walkComplete, walkErr = parseResumeToken(resumeToken, workRequest.JobId); !walkComplete {
			if walk, stopWalk, err := c.getWalk(workCtx, workRequest, walkBufferSize); err != nil {
				walkErr = err
			} else if err = controller.AddWalk(walk, stopWalk, maxRequests); err != nil {
				walkErr = err
			} else if err = controller.WaitForWalk(); err != nil {
				walkErr = err
			}
		}
	}

	var fatalBulkOperationsError error
	managers := registry.GetManagersSnapshot()
	if workCtx.Err() == nil {
		for _, manager := range managers {
			controller.ExecuteBulkOperation(manager)
		}
		fatalBulkOperationsError = controller.WaitForBulkOperations()
	}

	result, resumeToken = controller.GetResults()
	if resumeToken == walkCompleteToken || walkErr != nil {
		resumeToken = buildWalkCompleteSentinel(workRequest.JobId, walkErr)
	}
	workRequest.SetExternalId(resumeToken)

	// A shutdown must never cancel the bulk operations since the builder job is not complete and
	// will resume after the restart. Only a deliberate cancellation or a fatal bulk failure should
	// cancel the bulk operations.
	if shutdownCtx.Err() == nil && (workCtx.Err() != nil || fatalBulkOperationsError != nil) {
		var reason error
		if fatalBulkOperationsError != nil {
			reason = fmt.Errorf("bulk operation failed: %w", fatalBulkOperationsError)
		} else {
			reason = fmt.Errorf("builder job work request was cancelled")
		}
		for _, manager := range managers {
			controller.CancelBulkOperation(manager, reason)
		}
		result.Err = appendErrors(result.Err, reason, controller.WaitForBulkOperations())
	} else if !result.Reschedule {
		result.Err = appendErrors(result.Err, walkErr, registry.GetFailedOperationErrors())
	}

	return
}

const walkBufferSize = 256

func (c *JobBuilderClient) getWalk(ctx context.Context, workRequest *flex.WorkRequest, chanSize int) (walk <-chan *filesystem.StreamPathResult, stopWalk func(), err error) {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken := workRequest.GetExternalId()
	stopWalk = func() {}

	var filter filesystem.FileInfoFilter
	filterExpr := cfg.GetFilterExpr()
	if filterExpr != "" {
		if filter, err = filesystem.CompileFilter(filterExpr); err != nil {
			err = fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			return
		}
	}

	walkPaths := filesystem.StreamPathsLexicographically
	if cfg.GetUpdate() || cfg.HasCooldownSecs() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}

	if cfg.GetDownload() {
		if filter != nil {
			err = fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
			return
		}

		if WalkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			return walkPaths(ctx, c.mountPoint, workRequest.GetPath(), resumeToken, chanSize, nil)
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}
			return client.GetWalk(ctx, client.SanitizeRemotePath(cfg.GetRemotePath()), chanSize, resumeToken)
		}
	} else {
		return walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, chanSize, filter)
	}
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

// ResolveBulkRequest is a no-op because builder jobs are never part of a bulk operation themselves.
// The requests a builder job generates are resolved by the provider that owns their bulk operation.
func (c *JobBuilderClient) ResolveBulkRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return nil
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) (err error) {
	workState := GetWorkResultsState(workResults)
	if !abort {
		switch workState {
		case flex.Work_COMPLETED, flex.Work_CANCELLED:
		default:
			return fmt.Errorf("unable to resolve failure")
		}
	}

	registry, err := c.newBulkOperationRegistry(ctx, job.GetId())
	if err != nil {
		return err
	}
	if len(registry.GetManagersSnapshot()) == 0 {
		return nil
	}
	defer func() {
		err = appendErrors(err, registry.Close(ctx))
	}()

	cancelled := workState == flex.Work_CANCELLED
	if cancelled || abort {
		var reason error
		if cancelled {
			reason = fmt.Errorf("builder job was cancelled")
		} else {
			reason = fmt.Errorf("builder job was aborted")
		}

		cancelWaits := map[*bulkOperationManager]BulkCancelResultFn{}
		for _, manager := range registry.GetManagersSnapshot() {
			walkCh, wait, cancelErr := manager.Cancel(ctx, reason)
			if cancelErr != nil {
				err = appendErrors(err, fmt.Errorf("failed to cancel bulk operation %s: %w", manager.Key(), cancelErr))
				continue
			}

			cancelWaits[manager] = wait
			go func() {
				for range walkCh {
				}
			}()
		}

		for manager, wait := range cancelWaits {
			if cancelErr := wait(); cancelErr != nil {
				err = appendErrors(err, fmt.Errorf("failed to wait for bulk operation %s to cancel: %w", manager.Key(), cancelErr))
			} else if destroyErr := manager.Destroy(ctx); destroyErr != nil {
				err = appendErrors(err, fmt.Errorf("failed to destroy bulk operation %s: %w", manager.Key(), destroyErr))
			}
		}
		return
	}

	for _, manager := range registry.GetManagersSnapshot() {
		if destroyErr := manager.Destroy(ctx); destroyErr != nil {
			err = appendErrors(err, fmt.Errorf("failed to destroy bulk operation %s: %w", manager.Key(), destroyErr))
		}
	}

	return
}

// GetConfig is not implemented and should never be called.
func (c *JobBuilderClient) GetConfig() *flex.RemoteStorageTarget {
	return nil
}

// GetWalk is not implemented and should never be called.
func (c *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string) (<-chan *filesystem.StreamPathResult, func(), error) {
	return nil, func() {}, ErrUnsupportedOpForRST
}

// SanitizeRemotePath should never be called.
func (c *JobBuilderClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

// GetRemotePathInfo is not implemented and should never be called.
func (c *JobBuilderClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, bool, bool, error) {
	return 0, time.Time{}, false, false, ErrUnsupportedOpForRST
}

// GenerateExternalId is not implemented and should never be called.
func (c *JobBuilderClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	return "", ErrUnsupportedOpForRST
}

// ReleaseExternalId is a no-op because GenerateExternalId never hands out an id to release.
func (c *JobBuilderClient) ReleaseExternalId(ctx context.Context, cfg *flex.JobRequestCfg, externalId string) error {
	return nil
}

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return true, 0, nil
}

func (c *JobBuilderClient) IncludeRequestInBulkOperation(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	return nil, ErrUnsupportedOpForRST
}

// newBulkOperationRegistry creates the registry for builderJobId and reopens every bulk operation
// the job already started by loading the entries saved on the mount. Records are persistent so
// operations are never lost when sync crashes or shuts down.
func (c *JobBuilderClient) newBulkOperationRegistry(ctx context.Context, builderJobId string) (*bulkOperationRegistry, error) {
	registry := &bulkOperationRegistry{
		managers:     make(map[string]*bulkOperationManager),
		managersMu:   sync.Mutex{},
		mountPath:    c.mountPoint.GetMountPath(),
		rstMap:       c.rstMap,
		builderJobId: builderJobId,
	}
	if err := registry.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to load the saved bulk operations of builder job %s: %w", builderJobId, err)
	}

	return registry, nil
}

const (
	// requestBuildControllerWorkerMultiplier scales GOMAXPROCS to set the maximum number of
	// concurrent path-processing goroutines. Each path always blocks on at least one BeeGFS
	// metadata operation (lock acquisition via getPathState) and on submitting its request to
	// remote which is why per-path goroutines the is right model.
	//
	// Each goroutines are parked during the blocking I/O, freeing OS threads for other work. The
	// multiplier must be large enough to keep hardware threads busy, but small enough to avoid
	// excessive concurrent pressure on the metadata server and on remote.
	requestBuildControllerWorkerMultiplier = 8.0
)

func (c *JobBuilderClient) newRequestBuildController(
	shutdownCtx context.Context,
	workCtx context.Context,
	log *zap.Logger,
	builderCfg *flex.JobRequestCfg,
	submitRequest SubmitRequestFn,
	addBulkRequest addBulkRequestFn,
	workerSaturation []func() float64,
) *requestBuildController {
	maxWorkers := max(1, int(requestBuildControllerWorkerMultiplier*float32(runtime.GOMAXPROCS(0))))
	requestBuilder := c.newJobRequestBuilder(log, builderCfg, submitRequest, addBulkRequest)
	return &requestBuildController{
		shutdownCtx:      shutdownCtx,
		workCtx:          workCtx,
		requestBuilder:   requestBuilder,
		getPaths:         c.getPathsFn(builderCfg),
		maxWorkersCh:     make(chan struct{}, maxWorkers),
		workerSaturation: workerSaturation,
	}
}

func (c *JobBuilderClient) newJobRequestBuilder(
	log *zap.Logger,
	builderCfg *flex.JobRequestCfg,
	submitRequest SubmitRequestFn,
	addBulkRequest addBulkRequestFn,
) *jobRequestBuilder {
	requestBuilder := &jobRequestBuilder{
		log:              log,
		mountPoint:       c.mountPoint,
		RstMap:           c.rstMap,
		submitRequest:    submitRequest,
		builderCfg:       builderCfg,
		getPathState:     GetPathState,
		planFileState:    PlanFileStateForWorkRequests,
		clearAccessFlags: entry.ClearAccessFlags,
		addBulkRequest:   addBulkRequest,
	}
	requestBuilder.init()

	return requestBuilder
}

func (c *JobBuilderClient) getPathsFn(cfg *flex.JobRequestCfg) requestPathResolverFn {
	if cfg.Download {
		if WalkLocalPathInsteadOfRemote(cfg) {
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

const walkCompleteToken = ""

func buildWalkCompleteSentinel(jobId string, err error) string {
	if err != nil {
		return fmt.Sprintf("sentinel:%s:%s", jobId, err.Error())
	}
	return fmt.Sprintf("sentinel:%s:", jobId)
}

func parseResumeToken(token string, jobId string) (walkComplete bool, sentinelErr error) {
	if token == walkCompleteToken {
		return
	}

	var errMessage string
	if errMessage, walkComplete = strings.CutPrefix(token, fmt.Sprintf("sentinel:%s:", jobId)); !walkComplete {
		return
	}
	if errMessage != "" {
		sentinelErr = errors.New(errMessage)
	}
	return
}

func WalkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
