package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
}

var _ Provider = &JobBuilderClient{}

const (
	defaultJobBuilderStateRoot   = ".beegfs-rst"
	defaultJobBuilderMaxRequests = 1000
)

type JobBuilderConfig struct {
	StateRoot   string
	MaxRequests int
}

var jobBuilderConfig = JobBuilderConfig{
	StateRoot:   defaultJobBuilderStateRoot,
	MaxRequests: defaultJobBuilderMaxRequests,
}

func SetJobBuilderConfig(cfg JobBuilderConfig) {
	if cfg.StateRoot == "" {
		cfg.StateRoot = defaultJobBuilderStateRoot
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = defaultJobBuilderMaxRequests
	}
	jobBuilderConfig = cfg
}

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

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest) *ExecuteJobBuilderRequestResult {
	if !workRequest.HasBuilder() {
		return &ExecuteJobBuilderRequestResult{Err: ErrReqAndRSTTypeMismatch}
	}

	result := c.executeBuilderRequest(ctx, workRequest, jobSubmissionCh)
	if result.Err != nil || result.Reschedule {
		return result
	}

	result.Err = c.getBuilderResults(workRequest.GetBuilder())
	return result
}

func (c *JobBuilderClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	return nil, ErrFileTypeUnsupported
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
			for _, bulkOperation := range workResult.JobBuilderInfo.BulkOperations {
				jobBuilderOperations = append(jobBuilderOperations, bulkOperation)
			}
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

func (c *JobBuilderClient) executeBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest) *ExecuteJobBuilderRequestResult {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	bulkOperationsManager, bulkErr := c.newBulkOperationsManager(ctx, workRequest.GetJobId(), &builder.BulkOperations)
	if bulkErr != nil {
		return &ExecuteJobBuilderRequestResult{Err: MarkBuilderFailed(bulkErr)}
	}

	requestBuildController := c.newRequestBuildController(ctx, cfg, jobSubmissionCh, bulkOperationsManager.AddRequest)
	requestBuildController.Start()

	abort := func(err error) *ExecuteJobBuilderRequestResult {
		err = fmt.Errorf("job builder request was aborted: %w", err)
		if bulkErr := bulkOperationsManager.Abort(ctx, requestBuildController, err); bulkErr != nil {
			return &ExecuteJobBuilderRequestResult{Err: MarkBuilderFailed(errors.Join(err, bulkErr))}
		}
		return &ExecuteJobBuilderRequestResult{Err: MarkBuilderCancelled(err)}
	}

	waitForBulkResume, err := bulkOperationsManager.Resume(ctx, requestBuildController)
	if err != nil {
		return abort(err)
	}

	walkReschedule := false
	if !isWalkComplete(workRequest.GetExternalId(), workRequest.JobId) {
		maxRequests := jobBuilderConfig.MaxRequests
		walkSize := min(cap(jobSubmissionCh), maxRequests+1) // maxRequests +1 is for ResumeToken when there is more work
		walkCh, err := c.getWalkCh(ctx, workRequest, walkSize)
		if err != nil {
			return abort(err)
		}
		waitForWalk := requestBuildController.AddWalks([]<-chan *filesystem.StreamPathResult{walkCh})
		waitForWalk()
	}

	if err = waitForBulkResume(); err != nil {
		return abort(err)
	}

	bulkReschedule, bulkDelay, err := bulkOperationsManager.Execute(ctx, requestBuildController)
	if err != nil {
		return abort(err)
	}

	// Close the request build controller and wait for the results. Be sure to update the builder
	// counters before processing err.
	requestBuildController.Close()
	results, err := requestBuildController.Wait()
	builder.Submitted += results.Submitted
	builder.Errors += results.Errors
	builder.Conflicts += results.Conflicts
	if err != nil {
		return abort(err)
	} else if results.ResumeToken != "" {
		walkReschedule = true
		workRequest.SetExternalId(results.ResumeToken)
	} else {
		walkCompleteSentinel := makeWalkCompleteSentinel(workRequest.JobId)
		workRequest.SetExternalId(walkCompleteSentinel)
	}

	result := &ExecuteJobBuilderRequestResult{}
	result.Reschedule = walkReschedule || bulkReschedule
	if !walkReschedule && bulkDelay != 0 {
		result.Delay = bulkDelay
	}

	return result
}

func (c *JobBuilderClient) getBuilderResults(builder *flex.BuilderJob) (err error) {
	cfg := builder.GetCfg()
	totalSubmitted := builder.GetSubmitted()
	totalSubmittedErrors := builder.GetErrors()
	totalConflicts := builder.GetConflicts()

	if totalSubmitted == 0 {
		if totalConflicts > 0 {
			err = appendError(err, fmt.Errorf("all %d matched path(s) conflicted with other jobs that already held their locks; no requests were submitted", totalConflicts))
		} else if cfg.Download {
			if walkLocalPathInsteadOfRemote(cfg) {
				err = appendError(err, fmt.Errorf("walking local path since --%s was not provided; No matches found in path: %s", RemotePathFlag, cfg.Path))
			} else {
				err = appendError(err, fmt.Errorf("no matches found in remote path: %s", cfg.RemotePath))
			}
		} else {
			err = appendError(err, fmt.Errorf("no matches found in local path: %s", cfg.Path))
		}
	} else {
		if totalSubmittedErrors > 0 {
			err = appendError(err, fmt.Errorf("%d of %d requests were submitted with errors", totalSubmittedErrors, totalSubmitted))
		}
		if totalConflicts > 0 {
			err = appendError(err, fmt.Errorf("%d request(s) could not be submitted due to a conflicting job request already holding the lock", totalConflicts))
		}
	}

	if err != nil {
		if !IsValidRstId(cfg.GetRemoteStorageTarget()) {
			err = appendError(err, fmt.Errorf("--%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag))
		}
		err = MarkBuilderCancelled(err)
	}
	return
}

func (c *JobBuilderClient) getWalkCh(ctx context.Context, workRequest *flex.WorkRequest, chanSize int) (walkCh <-chan *filesystem.StreamPathResult, err error) {
	maxPaths := jobBuilderConfig.MaxRequests
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
	if cfg.GetUpdate() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}

	if cfg.GetDownload() {
		if filter != nil {
			err = fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
			return
		}

		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			return walkPaths(ctx, c.mountPoint, workRequest.GetPath(), resumeToken, maxPaths, chanSize, nil)
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			if walkCh, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.GetRemotePath()), chanSize, resumeToken, maxPaths); err != nil {
				return
			}
		}
	} else {
		walkCh, err = walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxPaths, chanSize, filter)
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
		client := manager.rstMap[bulkOperation.RstId]
		var createErr error
		if manager.managers[key], createErr = newBulkOperationManager(ctx, client, builderJobId, bulkOperation); createErr != nil {
			err = errors.Join(err, createErr)
		}
	}
	return manager, err
}

func (c *JobBuilderClient) newRequestBuildController(
	ctx context.Context,
	cfg *flex.JobRequestCfg,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	tryRouteToBulkOperation tryRouteToBulkOperationFn,
) *requestBuildController {
	g, gCtx := errgroup.WithContext(ctx)
	walkMultiplexer := newRequestBuildWalkMultiplexer(gCtx, cap(jobSubmissionCh))
	worker := c.newRequestBuilderWorker(cfg, walkMultiplexer.Output(), jobSubmissionCh, tryRouteToBulkOperation)
	workerPool := &requestBuildWorkerPool{
		group:                   g,
		ctx:                     gCtx,
		parentCtx:               ctx,
		workerBase:              worker,
		getSubmissionQueueDepth: func() int { return len(jobSubmissionCh) },
	}

	return &requestBuildController{
		walkMultiplexer: walkMultiplexer,
		workerPool:      workerPool,
	}
}

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
		getPathState:            GetPathState,
		updateDirRstConfig:      updateDirRstConfig,
		prepareFileState:        PrepareFileStateForWorkRequests,
		clearAccessFlags:        entry.ClearAccessFlags,
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

const builderWalkCompletePrefix = "builder:walk-complete:"

func makeWalkCompleteSentinel(jobID string) string {
	return builderWalkCompletePrefix + jobID
}

func isWalkComplete(externalID, jobID string) bool {
	return externalID == makeWalkCompleteSentinel(jobID)
}

func isStatusError(status *beeremote.JobRequest_GenerationStatus) bool {
	return status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION)
}

func appendError(accumulatedErr error, nextErr error) error {
	if accumulatedErr == nil {
		return nextErr
	}
	return fmt.Errorf("%w; %w", accumulatedErr, nextErr)
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
