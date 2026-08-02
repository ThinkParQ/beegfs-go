package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
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

// GenerateWorkRequests for JobBuilderClient should simply pass a single
func (c *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (workRequests []*flex.WorkRequest, err error) {
	if !job.Request.HasBuilder() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	workRequests = RecreateWorkRequests(job, nil)
	return
}

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest) *SchedulingResult {
	if !workRequest.HasBuilder() {
		return &SchedulingResult{Err: ErrReqAndRSTTypeMismatch}
	}

	return c.executeBuilderRequest(ctx, workRequest, jobSubmissionCh)
}

// ExcludeRequestFromBulkOperation is not implemented and should never be called since JobBuilderClient never
// generates bulk operation requests (see IncludeRequestInBulkOperation).
func (c *JobBuilderClient) ExcludeRequestFromBulkOperation(ctx context.Context, request *beeremote.JobRequest, reason error) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) IncludeRequestInBulkOperation(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	return nil, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return true, 0, nil
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	if abort {
		bulkOperations := getBulkOperations(workResults)
		if len(bulkOperations) > 0 {
			bulkOperationsManager, err := c.newBulkOperationsManager(ctx, job.GetId(), &bulkOperations)
			if err != nil {
				return err
			}

			waits := []BulkWaitFn{}
			for _, manager := range bulkOperationsManager.getManagersSnapshot() {
				walkCh, wait, err := manager.Cancel(ctx, nil)
				if err != nil {
					return err
				}
				waits = append(waits, wait)
				go func() {
					// Discard any walk paths
					for range walkCh {
					}
				}()
			}

			for _, wait := range waits {
				err = errors.Join(err, wait())
			}
			return err
		}
	}

	return nil
}

func getBulkOperations(workResults []*flex.Work) []*flex.BulkOperation {
	jobBuilderOperations := []*flex.BulkOperation{}
	for _, workResult := range workResults {
		if workResult.HasJobBuilderInfo() {
			jobBuilderOperations = append(jobBuilderOperations, workResult.JobBuilderInfo.BulkOperations...)
		}
	}
	return jobBuilderOperations
}

// GetConfig is not implemented and should never be called.
func (c *JobBuilderClient) GetConfig() *flex.RemoteStorageTarget {
	return nil
}

// GetWalk is not implemented and should never be called.
func (c *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string, maxRequests int) (<-chan *filesystem.StreamPathResult, error) {
	return nil, ErrUnsupportedOpForRST
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

func (c *JobBuilderClient) executeBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest) (result *SchedulingResult) {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	bulkOperationsManager, bulkErr := c.newBulkOperationsManager(ctx, workRequest.GetJobId(), &builder.BulkOperations)
	if bulkErr != nil {
		result = &SchedulingResult{Err: MarkBuilderFailed(bulkErr)}
		return
	}
	defer func() {
		if closeErr := bulkOperationsManager.Close(ctx); closeErr != nil {
			result.Err = errors.Join(result.Err, closeErr)
		}
	}()

	requestBuildController := c.newRequestBuildController(ctx, cfg, jobSubmissionCh, bulkOperationsManager.AddRequest)
	requestBuildController.Start()
	abort := func(err error) *SchedulingResult {
		err = fmt.Errorf("job builder request was aborted: %w", err)
		if bulkErr := bulkOperationsManager.Abort(ctx, requestBuildController, err); bulkErr != nil {
			return &SchedulingResult{Err: MarkBuilderFailed(errors.Join(err, bulkErr))}
		}
		return &SchedulingResult{Err: MarkBuilderCancelled(err)}
	}

	if !isWalkComplete(workRequest.GetExternalId(), workRequest.JobId) {
		walkSize := min(cap(jobSubmissionCh), maxRequests+1) // maxRequests+1 is for ResumeToken when there is more work
		walkCh, err := c.getWalkCh(ctx, workRequest, walkSize)
		if err != nil {
			result = abort(err)
			return
		}
		requestBuildController.AddSourceWalk(walkCh)
		requestBuildController.WaitForSourceWalkProcessing()
	}

	result = bulkOperationsManager.Execute(ctx, requestBuildController)
	if result.Err != nil {
		return abort(result.Err)
	}

	requestBuildController.Close()
	resumeToken, err := requestBuildController.Wait()
	if err != nil {
		result = abort(err)
	} else if resumeToken != "" {
		result.Reschedule = true
		result.Delay = 0
		workRequest.SetExternalId(resumeToken)
	} else {
		walkCompleteSentinel := makeWalkCompleteSentinel(workRequest.JobId)
		workRequest.SetExternalId(walkCompleteSentinel)
	}

	return result
}

func (c *JobBuilderClient) getWalkCh(ctx context.Context, workRequest *flex.WorkRequest, chanSize int) (walkCh <-chan *filesystem.StreamPathResult, err error) {
	maxFiles := maxRequests
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken := workRequest.GetExternalId()
	if isWalkComplete(resumeToken, workRequest.JobId) {
		return
	}

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
			return walkPaths(ctx, c.mountPoint, workRequest.GetPath(), resumeToken, maxFiles, chanSize, nil)
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			if walkCh, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.GetRemotePath()), chanSize, resumeToken, maxFiles); err != nil {
				return
			}
		}
	} else {
		walkCh, err = walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxFiles, chanSize, filter)
		if err != nil {
			return
		}
	}

	return
}

func (c *JobBuilderClient) newBulkOperationsManager(ctx context.Context, builderJobId string, builderBulkOperations *[]*flex.BulkOperation) (*jobBuilderBulkOperationsManager, error) {
	manager := &jobBuilderBulkOperationsManager{
		managers:              make(map[string]*bulkOperationManager),
		managersMu:            sync.Mutex{},
		rstMap:                c.rstMap,
		builderBulkOperations: builderBulkOperations,
		builderJobId:          builderJobId,
	}

	var err error
	for _, bulkOperation := range *builderBulkOperations {
		key := fmt.Sprintf("%d-%s", bulkOperation.RstId, bulkOperation.Operation)
		client, ok := manager.rstMap[bulkOperation.RstId]
		if !ok {
			return nil, fmt.Errorf("unable to create bulk operation manager: remote storage target ID %d does not exist in the configuration", bulkOperation.RstId)
		}

		var createErr error
		if manager.managers[key], createErr = newBulkOperationManager(ctx, client, builderJobId, bulkOperation); createErr != nil {
			err = errors.Join(err, createErr)
		}
	}
	return manager, err
}

const (
	// requestBuildControllerWorkerMultiplier scales GOMAXPROCS to set the maximum number of
	// concurrent path-processing goroutines. Each path always blocks on at least one BeeGFS
	// metadata operation (lock acquisition via getPathState), making per-path goroutines the right
	// model: goroutines are parked during the blocking I/O, freeing OS threads for other work.
	// The multiplier must be large enough that enough goroutines are in flight to keep hardware
	// threads busy, but small enough to avoid excessive concurrent pressure on the metadata server.
	requestBuildControllerWorkerMultiplier = 4.0
	// requestBuildControllerQueueDepthPerWorker controls the job submission backpressure threshold:
	// threshold = min(cap(jobSubmissionCh), maxWorkers*queueDepthPerWorker). Once the submission
	// queue reaches the threshold, processWalk stops spawning new path goroutines until it drains.
	// Higher values allow more in-flight submissions before throttling, which smooths throughput but
	// buffers more work in memory. Lower values throttle more tightly and respond faster to a slow
	// downstream consumer.
	requestBuildControllerQueueDepthPerWorker = 1.5
)

func (c *JobBuilderClient) newRequestBuildController(
	ctx context.Context,
	builderCfg *flex.JobRequestCfg,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	addBulkRequest addBulkRequestFn,
) *requestBuildController {
	cpuLimit := max(1, int(requestBuildControllerWorkerMultiplier*float32(runtime.GOMAXPROCS(0))))
	queueLimit := max(1, cap(jobSubmissionCh))
	maxWorkers := min(cpuLimit, queueLimit)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(maxWorkers + 1) // Reserve maxWorkers for path processors; processWalk uses one slot.
	submissionBackpressureThreshold := max(1, min(cap(jobSubmissionCh), int(requestBuildControllerQueueDepthPerWorker*float32(maxWorkers))))

	requestBuilder := c.newJobRequestBuilder(builderCfg, jobSubmissionCh, addBulkRequest)
	bulkWalks := NewBulkStreamPathResultMultiplexer(groupCtx, cap(jobSubmissionCh))

	return &requestBuildController{
		group:                           group,
		ctx:                             groupCtx,
		parentCtx:                       ctx,
		requestBuilder:                  requestBuilder,
		submissionBackpressureThreshold: submissionBackpressureThreshold,
		bulkWalks:                       bulkWalks,
		getPaths:                        c.getPathsFn(builderCfg),
	}
}

func (c *JobBuilderClient) newJobRequestBuilder(
	builderCfg *flex.JobRequestCfg,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	addBulkRequest addBulkRequestFn,
) *jobRequestBuilder {
	requestBuilder := &jobRequestBuilder{
		mountPoint:       c.mountPoint,
		RstMap:           c.rstMap,
		jobSubmissionCh:  jobSubmissionCh,
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

const builderWalkCompletePrefix = "builder:walk-complete:"

func makeWalkCompleteSentinel(jobID string) string {
	return builderWalkCompletePrefix + jobID
}

func isWalkComplete(externalID, jobID string) bool {
	return externalID == makeWalkCompleteSentinel(jobID)
}

func appendError(accumulatedErr error, nextErr error) error {
	if accumulatedErr == nil {
		return nextErr
	}
	return fmt.Errorf("%w; %w", accumulatedErr, nextErr)
}

func WalkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
