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

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest, workerSaturation []func() float64) *SchedulingResult {
	if !workRequest.HasBuilder() {
		return &SchedulingResult{Err: ErrReqAndRSTTypeMismatch}
	}

	return c.executeBuilderRequest(ctx, workRequest, jobSubmissionCh, workerSaturation)
}

func (c *JobBuilderClient) executeBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest, workerSaturation []func() float64) (result *SchedulingResult) {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	registry := c.newBulkOperationRegistry(ctx, workRequest.GetJobId(), &builder.BulkOperations)
	defer func() {
		if closeErr := registry.Close(ctx); closeErr != nil {
			result.Err = appendError(result.Err, closeErr)
		}
	}()

	controller := c.newRequestBuildController(ctx, cfg, jobSubmissionCh, registry.AddRequest, workerSaturation)
	abort := func(reason error) *SchedulingResult {
		reason = fmt.Errorf("request was aborted: %w", reason)
		managers := registry.GetManagersSnapshot()
		if len(managers) == 0 {
			return &SchedulingResult{Err: MarkBuilderCancelled(reason)}
		}

		for _, manager := range managers {
			controller.CancelBulkOperation(manager, reason)
		}
		err := controller.WaitForBulkOperations()
		if registry.IsFailedManager() {
			return &SchedulingResult{Err: MarkBuilderFailed(reason, err)}
		}
		return &SchedulingResult{Err: MarkBuilderCancelled(reason, err)}
	}

	resumeToken := workRequest.GetExternalId()
	walkComplete, walkErr := parseResumeToken(resumeToken, workRequest.JobId)
	if !walkComplete {
		walkSize := min(cap(jobSubmissionCh), maxRequests+1) // +1 is for ResumeToken when there is more work
		walkChGenerator, resumeToken, err := c.getNextWalkChGenerator(ctx, workRequest, walkSize)
		if err != nil {
			if len(registry.GetManagersSnapshot()) == 0 {
				return abort(err)
			}
			walkErr = err
		} else {
			controller.WalkSourceGenerator(walkChGenerator, resumeToken, maxRequests)
			if err = controller.WaitForWalkSources(); err != nil {
				if errors.Is(ctx.Err(), context.Canceled) || len(registry.GetManagersSnapshot()) == 0 {
					return abort(err)
				}
				walkErr = err
			}
		}
	}

	for _, manager := range registry.GetManagersSnapshot() {
		controller.ExecuteBulkOperation(manager)
	}
	if err := controller.WaitForBulkOperations(); err != nil {
		return abort(err)
	}

	result, resumeToken = controller.GetResults()
	if resumeToken == "" || walkErr != nil {
		resumeToken = buildWalkCompleteSentinel(workRequest.JobId, walkErr)
	}
	workRequest.SetExternalId(resumeToken)
	if result.Reschedule {
		return
	}

	if registry.IsFailedManager() {
		result.Err = MarkBuilderFailed(result.Err)
	} else if walkErr != nil {
		result.Err = MarkBuilderCancelled(result.Err, walkErr)
	}
	return
}

type nextWalkChGenerator func(resumeToken string) (walkCh <-chan *filesystem.StreamPathResult, err error)

func (c *JobBuilderClient) getNextWalkChGenerator(ctx context.Context, workRequest *flex.WorkRequest, chanSize int) (generator nextWalkChGenerator, resumeToken string, err error) {
	maxFiles := maxRequests
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken = workRequest.GetExternalId()

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
			generator = func(token string) (walkCh <-chan *filesystem.StreamPathResult, err error) {
				return walkPaths(ctx, c.mountPoint, workRequest.GetPath(), token, maxFiles, chanSize, nil)
			}
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			generator = func(token string) (walkCh <-chan *filesystem.StreamPathResult, err error) {
				return client.GetWalk(ctx, client.SanitizeRemotePath(cfg.GetRemotePath()), chanSize, token, maxFiles)
			}
		}
	} else {
		generator = func(token string) (walkCh <-chan *filesystem.StreamPathResult, err error) {
			return walkPaths(ctx, c.mountPoint, workRequest.Path, token, maxFiles, chanSize, filter)
		}
	}

	return
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) (err error) {

	// All finished builder jobs should immediately try to complete
	//		If they're

	bulkOperations := getBulkOperations(workResults)
	if len(bulkOperations) == 0 {
		return
	}

	registry := c.newBulkOperationRegistry(ctx, job.GetId(), &bulkOperations)

	if abort {
		reason := fmt.Errorf("builder job %q was aborted", job.GetId())

		cancelWaits := map[*bulkOperationManager]BulkCancelResultFn{}
		for _, manager := range registry.GetManagersSnapshot() {
			walkCh, wait, cancelErr := manager.Cancel(ctx, reason)
			if cancelErr != nil {
				err = appendError(err, fmt.Errorf("failed to cancel bulk operation %s: %w", manager.Key(), cancelErr))
				continue
			}

			cancelWaits[manager] = wait
			go func() {
				for range walkCh {
				}
			}()
		}

		for manager, wait := range cancelWaits {
			if waitErr := wait(); waitErr != nil {
				err = appendError(err, fmt.Errorf("failed to wait for bulk operation %s to cancel: %w", manager.Key(), waitErr))
			} else if destroyErr := manager.Destroy(ctx); destroyErr != nil {
				err = appendError(err, fmt.Errorf("failed to destroy bulk operation %s: %w", manager.Key(), destroyErr))
			}
		}
		return
	}

	for _, manager := range registry.GetManagersSnapshot() {
		if destroyErr := manager.Destroy(ctx); destroyErr != nil {
			err = appendError(err, fmt.Errorf("failed to destroy bulk operation %s: %w", manager.Key(), destroyErr))
		}
	}

	return
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

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return true, 0, nil
}

func (c *JobBuilderClient) IncludeRequestInBulkOperation(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	return nil, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) newBulkOperationRegistry(ctx context.Context, builderJobId string, builderBulkOperations *[]*flex.BulkOperation) *bulkOperationRegistry {
	manager := &bulkOperationRegistry{
		managers:              make(map[string]*bulkOperationManager),
		managersMu:            sync.Mutex{},
		rstMap:                c.rstMap,
		builderBulkOperations: builderBulkOperations,
		builderJobId:          builderJobId,
	}

	for _, bulkOperation := range *builderBulkOperations {
		key := bulkOperationKey(bulkOperation.RstId, bulkOperation.Operation)
		client, _ := manager.rstMap[bulkOperation.RstId]
		manager.managers[key] = newBulkOperationManager(ctx, client, builderJobId, bulkOperation)
	}
	return manager
}

const (
	// requestBuildControllerWorkerMultiplier scales GOMAXPROCS to set the maximum number of
	// concurrent path-processing goroutines. Each path always blocks on at least one BeeGFS
	// metadata operation (lock acquisition via getPathState), making per-path goroutines the right
	// model: goroutines are parked during the blocking I/O, freeing OS threads for other work. The
	// multiplier must be large enough that enough goroutines are in flight to keep hardware threads
	// busy, but small enough to avoid excessive concurrent pressure on the metadata server.
	requestBuildControllerWorkerMultiplier = 8.0
	// requestBuildControllerQueueDepthPerWorker controls the job submission backpressure threshold:
	// threshold = min(cap(jobSubmissionCh), maxWorkers*queueDepthPerWorker). Once the submission
	// queue reaches the threshold, processWalk stops spawning new path goroutines until it drains.
	// Higher values allow more in-flight submissions before throttling, which smooths throughput
	// but buffers more work in memory. Lower values throttle more tightly and respond faster to a
	// slow downstream consumer.
	requestBuildControllerQueueDepthPerWorker = 2.0
)

func (c *JobBuilderClient) newRequestBuildController(
	ctx context.Context,
	builderCfg *flex.JobRequestCfg,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	addBulkRequest addBulkRequestFn,
	workerSaturation []func() float64,
) *requestBuildController {
	cpuLimit := max(1, int(requestBuildControllerWorkerMultiplier*float32(runtime.GOMAXPROCS(0))))
	queueLimit := max(1, cap(jobSubmissionCh))
	maxWorkers := min(cpuLimit, queueLimit)
	submissionBackpressureThreshold := max(1, min(cap(jobSubmissionCh), int(requestBuildControllerQueueDepthPerWorker*float32(maxWorkers))))
	requestBuilder := c.newJobRequestBuilder(builderCfg, jobSubmissionCh, addBulkRequest)
	return &requestBuildController{
		ctx:                   ctx,
		requestBuilder:        requestBuilder,
		backpressureThreshold: submissionBackpressureThreshold,
		getPaths:              c.getPathsFn(builderCfg),
		maxWorkersCh:          make(chan struct{}, maxWorkers),
		workerSaturation:      workerSaturation,
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

func buildWalkCompleteSentinel(jobId string, err error) string {
	if err != nil {
		return fmt.Sprintf("sentinel:%s:%s", jobId, err.Error())
	}
	return fmt.Sprintf("sentinel:%s:", jobId)
}

func parseResumeToken(token string, jobId string) (walkComplete bool, sentinelErr error) {
	if token == "" {
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
