package rst

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

const checkpointGrace = 30 * time.Second

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
	stateRoot  string
}

var _ Provider = &JobBuilderClient{}

func NewJobBuilderClient(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider, stateRoot string) *JobBuilderClient {
	return &JobBuilderClient{
		ctx:        ctx,
		rstMap:     rstMap,
		mountPoint: mountPoint,
		stateRoot:  stateRoot,
	}
}

func (c *JobBuilderClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: JobBuilderRstId,
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

func GetBuilderJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	client := NewJobBuilderClient(context.Background(), nil, nil, "")
	return client.GetJobRequest(cfg)
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
	result = &SchedulingResult{}
	if !workRequest.HasBuilder() {
		result.Err = ErrReqAndRSTTypeMismatch
		return
	}
	builder := workRequest.GetBuilder()

	registry, err := c.newBulkOperationRegistry(workCtx, workRequest.GetJobId())
	if err != nil {
		result.Err = err
		return
	}
	requestBuilder := c.newJobRequestBuilder(log, builder.Cfg, submitRequest, registry.AddRequest, registry.UpdateBulkRequest)
	controller := c.newRequestBuildController(shutdownCtx, workCtx, requestBuilder, workerSaturation)

	defer func() {
		if closeErr := registry.Close(workCtx); closeErr != nil {
			// Close only releases resources, so a failure here means an operation's handles could not
			// be closed cleanly, not that any of its state was lost. Every write is already durable
			// before the call that made it returned. The job is left to finish on its own terms.
			log.Warn("failed to close one or more bulk operations", zap.String("jobId", workRequest.GetJobId()), zap.Error(closeErr))
		}

		// A shutdown must never end the builder job, so nothing is reported here. The job must be
		// rescheduled. The walk and bulk operations errors will persist through a restart.
		if shutdownCtx.Err() != nil {
			if result.Err != nil {
				log.Warn("discarding a builder job error because the node is shutting down, the work is retried after the restart", zap.String("jobId", workRequest.GetJobId()), zap.Error(result.Err))
			}
			result.Err = nil
			result.Reschedule = true
			return
		}

		if !result.Reschedule {
			result.Err = appendErrors(result.Err, controller.GetWalkErrors(), registry.GetFailedOperationErrors())
			return
		}
	}()

	if err = controller.Init(c, workRequest); err != nil {
		result.Err = err
		return
	}
	controller.RunWalk()

	managers := registry.GetManagersSnapshot()
	var bulkErr error
	if workCtx.Err() == nil {
		for _, manager := range managers {
			controller.ExecuteBulkOperation(manager)
		}
		bulkErr = controller.WaitForBulkOperations()
	}

	controller.UpdateResult(result)

	var reason error
	if bulkErr != nil {
		reason = fmt.Errorf("bulk operation failed: %w", bulkErr)
	} else if shutdownCtx.Err() == nil && workCtx.Err() != nil {
		reason = fmt.Errorf("builder job work request was cancelled")
	}
	if reason != nil {
		for _, manager := range managers {
			controller.CancelBulkOperation(manager, reason)
		}
		result.Err = appendErrors(result.Err, reason, controller.WaitForBulkOperations())
	}

	return
}

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
func (c *JobBuilderClient) ExecuteWorkRequestPart(shutdownCtx context.Context, workCtx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) *SchedulingResult {
	return &SchedulingResult{Err: ErrUnsupportedOpForRST}
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) (err error) {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteJobBuilderRequest(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, cancelRequest CancelRequestFn, abort bool) (err error) {
	if !job.GetRequest().HasBuilder() {
		return ErrReqAndRSTTypeMismatch
	}
	builder := job.GetRequest().GetBuilder()

	workState := GetWorkResultsState(workResults)
	if !abort {
		switch workState {
		case flex.Work_COMPLETED, flex.Work_CANCELLED:
		default:
			return fmt.Errorf("unable to resolve failure")
		}
	}

	// It's critical that a context cancellation doesn't prevent the following from completing so
	// add a grace period to complete.
	ctx, cancel, checkpoint := WithCancellationDelay(ctx, time.Minute)
	defer cancel()

	registry, err := c.newBulkOperationRegistry(ctx, job.GetId())
	if err != nil {
		return err
	}
	defer func() {
		checkpoint(checkpointGrace)
		err = appendErrors(err, registry.Close(ctx))
	}()

	managers := registry.GetManagersSnapshot()
	if len(managers) == 0 {
		return nil
	}

	var reason error
	if workState == flex.Work_CANCELLED {
		reason = fmt.Errorf("builder job was cancelled")
	} else if abort {
		reason = fmt.Errorf("builder job was aborted")
	}

	if reason != nil {
		// The cancel walk reports each request by its walk path. Remote keys reserved jobs by the
		// path in the mount so each walk path is mapped the way the builder's walk mapped it.
		cfg := builder.GetCfg()
		if cfg == nil {
			return fmt.Errorf("unable to cancel bulk operations: builder job %s has no configuration (this is probably a bug)", job.GetId())
		}
		getPaths, pathsErr := c.getPathsFn(cfg)
		if pathsErr != nil {
			return fmt.Errorf("unable to map bulk requests to their paths in the mount: %w", pathsErr)
		}

		var errMu sync.Mutex
		var wg sync.WaitGroup
		for _, manager := range managers {
			wg.Go(func() {
				cancelErr := cancelBulkOperation(ctx, checkpoint, manager, reason, getPaths, cancelRequest)
				if cancelErr == nil {
					return
				}
				errMu.Lock()
				err = appendErrors(err, cancelErr)
				errMu.Unlock()
			})
		}
		wg.Wait()

		// Return if any bulk operations failed to be cancelled. This leaves keeps the bulk
		// operation managers from being destroyed.
		if err != nil {
			return
		}
	}

	var errMu sync.Mutex
	var wg sync.WaitGroup
	for _, manager := range managers {
		wg.Go(func() {
			checkpoint(checkpointGrace)
			if destroyErr := manager.Destroy(ctx); destroyErr != nil {
				errMu.Lock()
				err = appendErrors(err, fmt.Errorf("failed to destroy bulk operation %s: %w", manager.Key(), destroyErr))
				errMu.Unlock()
			}
		})
	}
	wg.Wait()
	return
}

// cancelBulkOperation cancels one bulk operation and cancels every request that operation reserved.
//
// It runs from CompleteJobBuilderRequest when a builder job ended in a state that must release what
// the operation reserved remotely. That is either a cancelled job or an aborted one. The manager
// comes from the registry snapshot of the builder job. getPaths maps a walk path to the path in the
// mount, the same way the builder's walk did, because remote keys reserved jobs by that path.
// cancelRequest cancels a single reserved job and is provided by the caller of
// CompleteJobBuilderRequest. checkpoint extends the cancellation grace period, so it runs before
// each step that can block.
//
// The rules it applies:
//   - A cancel that cannot be started is reported and nothing else happens for that operation.
//   - The cancel walk is always drained to the end, even after a request fails to cancel. The
//     operation only finishes once nothing is left to read, so stopping early would hang wait.
//   - wait runs last, after the walk is drained, so the operation reports its own errors.
//
// The errors from all three steps are joined into the returned error. Managers are cancelled in
// parallel by the caller, so nothing here may touch state shared between managers.
func cancelBulkOperation(ctx context.Context, checkpoint CancellationCheckpoint, manager *bulkOperationManager, reason error, getPaths requestPathResolverFn, cancelRequest CancelRequestFn) (err error) {
	checkpoint(checkpointGrace)
	walkCh, wait, cancelErr := manager.Cancel(ctx, reason)
	if cancelErr != nil {
		return fmt.Errorf("failed to cancel bulk operation %s: %w", manager.Key(), cancelErr)
	}

	for result := range walkCh {
		checkpoint(checkpointGrace)
		inMountPath, _ := getPaths(result.Path)
		if cancelRequestErr := cancelRequest(inMountPath, result.ReservedJobId); cancelRequestErr != nil {
			err = appendErrors(err, fmt.Errorf("failed to cancel request from bulk operation %s: %w", manager.Key(), cancelRequestErr))
		}
	}

	checkpoint(checkpointGrace)
	if waitErr := wait(); waitErr != nil {
		err = appendErrors(err, fmt.Errorf("failed to wait for bulk operation %s to cancel: %w", manager.Key(), waitErr))
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

func (c *JobBuilderClient) IsWorkRequestReady(shutdownCtx context.Context, workCtx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
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
		stateRoot:    c.stateRoot,
		rstMap:       c.rstMap,
		builderJobId: builderJobId,
	}
	if err := registry.Init(ctx); err != nil {
		// Init opens every operation it loads, so the ones opened before it failed have to be released
		// here. The caller gets no registry back and has nothing to close.
		if closeErr := registry.Close(ctx); closeErr != nil {
			err = appendErrors(err, closeErr)
		}
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
	requestBuilder *jobRequestBuilder,
	workerSaturation []func() float64,
) *requestBuildController {
	return &requestBuildController{
		shutdownCtx:      shutdownCtx,
		workCtx:          workCtx,
		requestBuilder:   requestBuilder,
		workerSaturation: workerSaturation,
	}
}

func (c *JobBuilderClient) newJobRequestBuilder(
	log *zap.Logger,
	builderCfg *flex.JobRequestCfg,
	submitRequest SubmitRequestFn,
	addToBulkRequest addToBulkRequestFn,
	updateBulkRequest updateBulkRequestFn,
) *jobRequestBuilder {
	requestBuilder := &jobRequestBuilder{
		log:               log,
		mountPoint:        c.mountPoint,
		RstMap:            c.rstMap,
		submitRequest:     submitRequest,
		builderCfg:        builderCfg,
		getPathState:      GetPathState,
		planFileState:     PlanFileStateForWorkRequests,
		clearAccessFlags:  entry.ClearAccessFlags,
		addToBulkRequest:  addToBulkRequest,
		updateBulkRequest: updateBulkRequest,
	}
	requestBuilder.init()

	return requestBuilder
}

func (c *JobBuilderClient) getPathsFn(cfg *flex.JobRequestCfg) (requestPathResolverFn, error) {
	if cfg.Download {
		if WalkLocalPathInsteadOfRemote(cfg) {
			// Walking cfg.Path to support stub file download and files with a defined rst.
			return func(walkPath string) (string, string) {
				return walkPath, ""
			}, nil
		}

		remotePathDir, remotePathIsGlob := GetDownloadRemotePathDirectory(cfg.RemotePath)

		isPathDir := false
		stat, err := c.mountPoint.Lstat(cfg.Path)
		if err == nil {
			isPathDir = stat.IsDir()
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		// The directory structure the downloads land in is not created here. The file creation in
		// PlanFileStateForWorkRequests already reports a missing parent, so it creates the
		// directory then (see createWithParentDir), which costs nothing for the paths whose parent
		// already exists and correctly recreates one removed since an earlier pass.
		return func(walkPath string) (string, string) {
			remotePath := walkPath
			inMountPath := GetDownloadInMountPath(cfg.Path, remotePath, remotePathDir, remotePathIsGlob, isPathDir, cfg.Flatten)
			return inMountPath, remotePath
		}, nil
	}

	return func(walkPath string) (string, string) {
		return walkPath, walkPath
	}, nil
}

func WalkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
