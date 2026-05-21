package rst

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
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

	builder := workRequest.GetBuilder()

	reschedule, delay, bulkErr, err = c.executeBuilderRequest(ctx, workRequest, jobSubmissionChan)
	if err != nil || bulkErr != nil || reschedule {
		return
	}

	err = c.getBuilderResults(builder)
	return
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

type bulkRequestHandles struct {
	walkChs    map[string]<-chan *filesystem.StreamPathResult
	getResults map[string]BulkRequestResultFn
}

func (b *bulkRequestHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, getResult BulkRequestResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.getResults == nil {
		b.getResults = map[string]BulkRequestResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.getResults[managerKey] = getResult
}

func (b *bulkRequestHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkRequestHandles) getMergedResults() (reschedule bool, delay time.Duration, errs map[string]error) {
	errs = map[string]error{}
	for managerKey, getResult := range b.getResults {
		resultReschedule, resultDelay, resultErr := getResult()
		if resultReschedule {
			reschedule = true
			if delay == 0 || delay > resultDelay {
				delay = resultDelay
			}
		}
		if resultErr != nil {
			errs[managerKey] = resultErr
		}
	}
	return
}

type bulkCancelHandles struct {
	walkChs map[string]<-chan *filesystem.StreamPathResult
	waits   map[string]BulkCancelResultFn
}

func (b *bulkCancelHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, wait BulkCancelResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.waits == nil {
		b.waits = map[string]BulkCancelResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.waits[managerKey] = wait
}

func (b *bulkCancelHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkCancelHandles) getMergedResults() map[string]error {
	errs := map[string]error{}
	for managerKey, wait := range b.waits {
		if err := wait(); err != nil {
			errs[managerKey] = err
		}
	}
	return errs
}

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

type requestBuildControllerResult func() (*requestBuilderWorkerResult, error)
type submissionQueueDepthFn func() int
type requestBuildController struct {
	group                   *errgroup.Group
	ctx                     context.Context
	parentCtx               context.Context
	workers                 []*requestBuilderWorker
	getSubmissionQueueDepth submissionQueueDepthFn
	workersStarted          int
	walkCh                  chan *filesystem.StreamPathResult
	closeOnce               sync.Once
}

func (c *JobBuilderClient) newRequestBuildController(ctx context.Context, cfg *flex.JobRequestCfg, jobSubmissionCh chan<- *beeremote.JobRequest, tryRouteToBulkOperation tryRouteToBulkOperationFn) *requestBuildController {
	g, gCtx := errgroup.WithContext(ctx)
	controller := &requestBuildController{
		group:                   g,
		ctx:                     gCtx,
		parentCtx:               ctx,
		getSubmissionQueueDepth: func() int { return len(jobSubmissionCh) },
		walkCh:                  make(chan *filesystem.StreamPathResult, max(1, cap(jobSubmissionCh))),
	}
	worker := c.newRequestBuilderWorker(cfg, controller.walkCh, jobSubmissionCh, tryRouteToBulkOperation)
	controller.workers = []*requestBuilderWorker{worker}

	return controller
}

func (c *requestBuildController) Close() error {
	c.closeOnce.Do(func() {
		close(c.walkCh)
	})
	return nil
}

func (c *requestBuildController) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	if len(walkChs) == 0 {
		return func() {}
	}

	var wg sync.WaitGroup
	for _, ch := range walkChs {
		wg.Add(1)
		go func(ch <-chan *filesystem.StreamPathResult) {
			defer wg.Done()
			for {
				select {
				case <-c.ctx.Done():
					return
				case walkPath, ok := <-ch:
					if !ok {
						return
					}

					select {
					case <-c.ctx.Done():
						return
					case c.walkCh <- walkPath:
					}
				}
			}
		}(ch)
	}

	return func() {
		wg.Wait()
	}
}

func (c *requestBuildController) addWorker(doneCh chan<- struct{}) {
	if len(c.workers) == 0 {
		return
	}

	var worker *requestBuilderWorker
	if c.workersStarted == 0 {
		worker = c.workers[0]
	} else {
		worker = c.workers[0].Clone()
		c.workers = append(c.workers, worker)
	}

	c.group.Go(func() error {
		return worker.Run(c.ctx, doneCh)
	})
	c.workersStarted++
}

// One worker starts immediately. Additional workers are added only when the job submission channel
// appears underutilized, meaning the current workers are not generating enough work to keep it
// filled. The controller uses a low `len(jobSubmissionChan)` relative to the number of active
// workers as a signal that downstream pressure is low, and increases parallelism in response.
//
// Processing finishes when input work is exhausted, the context is canceled, or any worker or the
// controller returns an error
func (c *requestBuildController) Start() requestBuildControllerResult {
	maxWorkers := runtime.GOMAXPROCS(0)
	doneCh := make(chan struct{}, maxWorkers)
	c.group.Go(func() error {
		lowThresholdTicks := 0

		c.addWorker(doneCh)
		for {
			select {
			case <-c.ctx.Done():
				if c.parentCtx.Err() != nil {
					return c.parentCtx.Err()
				}
				return nil
			case <-doneCh:
				return nil
			case <-time.After(100 * time.Millisecond):
				queueDepth := c.getSubmissionQueueDepth()
				workers := len(c.workers)
				if workers < maxWorkers && queueDepth <= 2*workers {
					if queueDepth <= workers {
						lowThresholdTicks += 3
					} else {
						lowThresholdTicks++
					}

					if lowThresholdTicks >= 3 {
						c.addWorker(doneCh)
						lowThresholdTicks = 0
					}
				} else {
					lowThresholdTicks = 0
				}

			}
		}
	})

	return func() (*requestBuilderWorkerResult, error) {
		err := c.group.Wait()

		aggregate := &requestBuilderWorkerResult{}
		for _, worker := range c.workers {
			aggregate = aggregate.Merge(worker.GetResult())
		}
		return aggregate, err
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

type jobBuilderBulkOperationsManager struct {
	managers     map[string]*bulkOperationManager
	managersMu   sync.Mutex
	rstMap       map[uint32]Provider
	workRequest  *flex.WorkRequest
	builder      *flex.BuilderJob
	builderJobId string
}

func (c *JobBuilderClient) newBulkOperationsManager(workRequest *flex.WorkRequest) *jobBuilderBulkOperationsManager {
	builder := workRequest.GetBuilder()
	builderJobId := workRequest.GetJobId()
	manager := &jobBuilderBulkOperationsManager{
		managers:     make(map[string]*bulkOperationManager),
		managersMu:   sync.Mutex{},
		rstMap:       c.rstMap,
		workRequest:  workRequest,
		builder:      builder,
		builderJobId: builderJobId,
	}

	for _, bulkOperation := range builder.GetBulkOperations() {
		key := fmt.Sprintf("%d-%s", bulkOperation.RstId, bulkOperation.Operation)
		manager.managers[key] = newBulkOperationManager(builderJobId, bulkOperation)
	}
	return manager
}

func (m *jobBuilderBulkOperationsManager) GetBulkOperations() []*flex.BuilderJob_BulkOperation {
	var bulkOperations []*flex.BuilderJob_BulkOperation
	for _, manager := range m.managers {
		bulkOperations = append(bulkOperations, manager.GetBulkOperation())
	}
	return bulkOperations
}

func (m *jobBuilderBulkOperationsManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool) {
	if request.GetGenerationStatus() != nil {
		return
	}

	client := m.rstMap[request.GetRemoteStorageTarget()]
	include, operation := client.IncludeInBulkRequest(ctx, request)
	if include {
		manager := m.getManager(request.RemoteStorageTarget, operation)
		manager.AddRequest(request)
		skipSubmit = true
	}
	return
}

// Abort cancels all bulk operation. All errors will be added to the bulk operation manager.
func (m *jobBuilderBulkOperationsManager) Abort(ctx context.Context, controller *requestBuildController, reason error) error {
	if len(m.managers) == 0 {
		return nil
	}

	walkResultBuilder := bulkCancelHandles{}
	for managerKey, manager := range m.managers {
		client := m.rstMap[manager.rstId]
		walkCh, wait, err := client.CancelBulkRequest(ctx, manager.StateMountPath, manager.Operation, reason)
		if err != nil {
			manager.AppendError(err)
			continue
		}
		walkResultBuilder.add(managerKey, walkCh, wait)
	}

	waitForWalks := controller.AddWalks(walkResultBuilder.getWalkChs())
	waitForWalks()

	results := walkResultBuilder.getMergedResults()
	for key, err := range results {
		m.managers[key].AppendError(err)
	}

	return nil
}

// getManager returns the bulk operation manager and creates it if it does not exists.
func (m *jobBuilderBulkOperationsManager) getManager(rstId uint32, operation string) *bulkOperationManager {
	key := fmt.Sprintf("%d-%s", rstId, operation)
	m.managersMu.Lock()
	defer m.managersMu.Unlock()

	manager, ok := m.managers[key]
	if !ok {
		bulkOperation := &flex.BuilderJob_BulkOperation{RstId: rstId, Operation: operation}
		manager = newBulkOperationManager(m.builderJobId, bulkOperation)
		m.managers[key] = manager
	}
	return manager
}

const (
	// TODO: ./beegfs-rst/job/<job-id>/bulk/<session-file>.json
	// TODO: ./beegfs-rst/job/<job-id>/for-path - associates with the builder job localPath/remotePath

	bulkManagerPath = "job"
)

type bulkOperationManager struct {
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	nextJobIndex   int64
	mu             sync.Mutex
	errors         string
	Completed      bool
}

func newBulkOperationManager(jobId string, bulkOperation *flex.BuilderJob_BulkOperation) *bulkOperationManager {
	stateMountPath := path.Join(jobBuilderConfig.StateRoot, bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
	manager := &bulkOperationManager{
		StateMountPath: stateMountPath,
		rstId:          bulkOperation.RstId,
		Operation:      bulkOperation.Operation,
		nextJobIndex:   bulkOperation.NextJobIndex,
		JobRequests:    []*beeremote.JobRequest{},
	}
	if bulkOperation.Errors != nil {
		manager.errors = *bulkOperation.Errors
	}
	return manager
}

func (m *bulkOperationManager) GetBulkOperation() *flex.BuilderJob_BulkOperation {
	operation := &flex.BuilderJob_BulkOperation{
		RstId:        m.rstId,
		Operation:    m.Operation,
		NextJobIndex: m.nextJobIndex,
	}

	if m.errors != "" {
		operation.Errors = new(m.errors)
	}

	return operation
}

func (m *bulkOperationManager) AddRequest(request *beeremote.JobRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()

	request.SetBulkInfo(&flex.BulkJobRequestInfo{
		StateMountPath: m.StateMountPath,
		Operation:      m.Operation,
		JobIndex:       m.nextJobIndex,
	})
	m.nextJobIndex++

	m.JobRequests = append(m.JobRequests, request)
}

func (m *bulkOperationManager) AppendError(err error) {
	if err == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errors == "" {
		m.errors = err.Error()
	} else {
		m.errors = fmt.Sprintf("%s. %s", m.errors, err.Error())
	}
}

const builderWalkCompletePrefix = "builder:walk-complete:"

func makeWalkCompleteSentinel(jobID string) string {
	return builderWalkCompletePrefix + jobID
}

func isWalkComplete(externalID, jobID string) bool {
	return externalID == makeWalkCompleteSentinel(jobID)
}

func (m *bulkOperationManager) GetErrors() error {
	if m.errors == "" {
		return nil
	}

	return fmt.Errorf("bulk operation %s: (%s)", m.Operation, m.errors)
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
