package rst

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
	stateMu    *sync.Mutex
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

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, delay time.Duration, bulkErr error, err error) {
	if !workRequest.HasBuilder() {
		err = ErrReqAndRSTTypeMismatch
		return
	}

	reschedule, delay, bulkErr, err = c.executeBuilderRequest(ctx, workRequest, jobSubmissionChan)
	if err != nil || bulkErr != nil || reschedule {
		return
	}

	builder := workRequest.GetBuilder()
	err = c.getBuilderResults(builder)
	return
}

func (c *JobBuilderClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest) (walkChan chan *filesystem.StreamPathResult, getResults BulkRequestResultFn, err error) {
	return nil, nil, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error) (walkChan chan *filesystem.StreamPathResult, wait BulkCancelResultFn, err error) {
	return nil, nil, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return true, 0, nil
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return nil
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

func (c *JobBuilderClient) executeBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest) (bool, time.Duration, error, error) {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	// Setup bulk operations manager which will oversee any bulk operation started.
	bulkOperationsManager := c.newBulkOperationsManager(workRequest)
	defer func() {
		builder.SetBulkOperations(bulkOperationsManager.GetBulkOperations())
	}()

	// Create and start request build controller.
	controller := c.newRequestBuildController(ctx, cfg, jobSubmissionCh, bulkOperationsManager.AddRequest)
	wait := controller.Start()

	abort := func(abortErr error) (bool, time.Duration, error, error) {
		err := fmt.Errorf("job builder request was aborted: %w", abortErr)
		bulkErr := bulkOperationsManager.Abort(ctx, controller, err)
		return false, 0, bulkErr, err
	}

	walkReschedule := false
	walkComplete := isWalkComplete(workRequest.GetExternalId(), workRequest.JobId)
	if !walkComplete {
		maxRequests := jobBuilderConfig.MaxRequests
		walkSize := min(cap(jobSubmissionCh), maxRequests+1) // maxRequests +1 is for ResumeToken when there is more work
		walkCh, walkErr := c.getWalkCh(ctx, workRequest, walkSize)
		if walkErr != nil {
			return abort(walkErr)
		}
		waitForWalk := controller.AddWalks([]<-chan *filesystem.StreamPathResult{walkCh})

		// TODO: It would be better if the builder walk was allowed to continue until the end where the bulk
		// operation could be executed in parallel and not be handled all together at the same time at the end.
		// So that,
		//  - Builder walk starts
		//		- Summits requests that can be
		//		- Routes request to bulk operations
		//  - Bulk manager can wait for new bulk operations
		//		- Each new bulk operation is executed and starts processing routed requests (ExecuteBulkRequest)
		//	- Builder walk completes or is rescheduled with no delay (just momentarily pushed to the wait-queue)
		//  - Bulk operation manager tells each bulk operation to finish (CompleteBulkRequest)
		//  - Bulk operation manager completes (all bulk operations are finished) OR reschedules with delay (minimum bulk operation delay required)
		//  - Wait for aggregate results (builder state results (jobs submitted, errors, conflicts))
		//  - (...)

		waitForWalk()
	}

	walkResultBuilder := bulkRequestHandles{}
	for managerKey, manager := range bulkOperationsManager.managers {
		client := c.rstMap[manager.rstId]
		walkCh, getResult, err := client.ExecuteBulkRequest(ctx, manager.StateMountPath, manager.Operation, manager.JobRequests)
		if err != nil {
			manager.AppendError(err)
			continue
		}
		walkResultBuilder.add(managerKey, walkCh, getResult)
	}

	waitForBulkWalks := controller.AddWalks(walkResultBuilder.getWalkChs())
	waitForBulkWalks()
	bulkReschedule, bulkDelay, executeErrs := walkResultBuilder.getMergedResults()
	for key, err := range executeErrs {
		bulkOperationsManager.managers[key].AppendError(err)
	}

	// Close the controller and wait for the results.
	_ = controller.Close()
	results, err := wait()
	builder.Submitted += results.Submitted
	builder.Errors += results.Errors
	builder.Conflicts += results.Conflicts
	if err != nil {
		return abort(err)
	} else if results.ResumeToken != "" {
		walkReschedule = true
		workRequest.SetExternalId(results.ResumeToken)
	} else {
		walkComplete = true
		walkCompleteSentinel := makeWalkCompleteSentinel(workRequest.JobId)
		workRequest.SetExternalId(walkCompleteSentinel)
	}

	reschedule := walkReschedule || bulkReschedule
	var delay time.Duration
	if !walkReschedule && bulkDelay != 0 {
		delay = bulkDelay
	}
	return reschedule, delay, nil, nil
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
