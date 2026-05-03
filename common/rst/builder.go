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

	"github.com/thinkparq/beegfs-go/common/filesystem"
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
func (c *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
	if !job.Request.HasBuilder() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	workRequests := RecreateWorkRequests(job, nil)
	return workRequests, nil
}

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, delay time.Duration, err error) {
	if !workRequest.HasBuilder() {
		err = ErrReqAndRSTTypeMismatch
		return
	}

	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken := workRequest.GetExternalId()
	if isWalkCompleteSentinel(resumeToken, workRequest.JobId) {
		return c.executeJobBuilderRequest(ctx, workRequest, nil, jobSubmissionChan, cfg)
	}

	var filter filesystem.FileInfoFilter
	filterExpr := cfg.GetFilterExpr()
	if filterExpr != "" {
		if filter, err = filesystem.CompileFilter(filterExpr); err != nil {
			err = fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			return
		}
	}

	// TODO: maxRequests limits the number of requests that can be created at a time before the
	// builder job is rescheduled. This should probably be based on the client if possible;
	// otherwise, client based metric that are based on builder short/long-term data collection.
	// Each client should at least have some input since there may be costs associated with the
	// requests as in s3.
	maxRequests := 1000
	walkChanSize := min(cap(jobSubmissionChan), maxRequests+1) // maxRequests +1 is for ResumeToken when there is more work
	var walkChan <-chan *filesystem.StreamPathResult
	walkPaths := filesystem.StreamPathsLexicographically
	if cfg.GetUpdate() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}
	if cfg.Download {

		if filter != nil {
			return false, 0, fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
		}

		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			if walkChan, err = walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxRequests, walkChanSize, nil); err != nil {
				return
			}
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			if walkChan, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.RemotePath), walkChanSize, resumeToken, maxRequests); err != nil {
				return
			}
		}
	} else {
		walkChan, err = walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxRequests, walkChanSize, filter)
		if err != nil {
			return
		}
	}

	return c.executeJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan, cfg)
}

func (c *JobBuilderClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest) (reschedule bool, delay time.Duration, err error) {
	return false, 0, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteBulkRequest(ctx context.Context, stateMountPath string, operation string) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	return true, 0, nil
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
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

func isStatusError(status *beeremote.JobRequest_GenerationStatus) bool {
	return status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION)
}

func (c *JobBuilderClient) executeJobBuilderRequest(
	ctx context.Context,
	request *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	cfg *flex.JobRequestCfg,
) (bool, time.Duration, error) {

	builder := request.GetBuilder()
	walkLocal := walkLocalPathInsteadOfRemote(cfg)
	builderStateMu := sync.Mutex{}
	reschedule := false
	var rescheduleDelay time.Duration

	bulkManagers := make(map[string]*bulkManager)
	bulkManagersMu := sync.Mutex{}
	for _, bulkOperation := range builder.GetBulkOperations() {
		key := fmt.Sprintf("%d-%s", bulkOperation.RstId, bulkOperation.Operation)
		manager := newBulkManager(request.JobId, bulkOperation)
		bulkManagers[key] = manager
	}
	abortBuilderJob := func(reason error) (bool, time.Duration, error) {
		var bulkManagerErrs []error
		for _, manager := range bulkManagers {
			client := c.rstMap[manager.rstId]
			bulkManagerErrs = append(bulkManagerErrs, client.CancelBulkRequest(ctx, manager.StateMountPath, manager.Operation, reason))
		}
		err := errors.Join(reason, errors.Join(bulkManagerErrs...))
		err = fmt.Errorf("job builder request was aborted: %w", err)
		return false, 0, err
	}

	if walkChan != nil {
		getPaths := c.getPathsFn(cfg, walkLocal)

		g, walkCtx := errgroup.WithContext(ctx)
		maxWorkers := runtime.GOMAXPROCS(0)
		walkDoneChan := make(chan struct{}, maxWorkers)
		defer close(walkDoneChan)
		createJobRequests := func() error {
			jobsWithErrors := int32(0)
			jobsSubmitted := int32(0)
			defer func() {
				builderStateMu.Lock()
				builder.Submitted += jobsSubmitted
				builder.Errors += jobsWithErrors
				builderStateMu.Unlock()
			}()

			var walkPath string
			for {
				select {
				case <-walkCtx.Done():
					return walkCtx.Err()
				case walkResp, ok := <-walkChan:
					if !ok {
						select {
						case walkDoneChan <- struct{}{}:
						default:
						}
						return nil
					}

					if walkResp.Err != nil {
						return walkResp.Err
					}

					if walkResp.ResumeToken != "" {
						builderStateMu.Lock()
						reschedule = true
						rescheduleDelay = time.Millisecond
						request.SetExternalId(walkResp.ResumeToken)
						builderStateMu.Unlock()
						return nil
					}
					walkPath = walkResp.Path
				}

				inMountPath, remotePath, err := getPaths(walkPath)
				if err != nil {
					return err
				}

				if cfg.GetUpdate() {
					if stat, statErr := c.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
						dirErr := updateDirRstConfig(walkCtx, cfg.RemoteStorageTarget, inMountPath)
						jobsSubmitted++
						if dirErr != nil {
							jobsWithErrors++
						}
						continue
					}
				}

				jobRequests, err := BuildJobRequests(walkCtx, c.rstMap, c.mountPoint, inMountPath, remotePath, cfg)
				if err != nil {
					// BuildJobRequest should only return fatal errors, or if there are no RSTs
					// specified/configured on an entry and there is no other way to return the
					// error other then aborting the builder job entirely.
					return err
				}

				for _, jobRequest := range jobRequests {
					jobWithError := false
					status := jobRequest.GetGenerationStatus()
					if isStatusError(status) {
						jobWithError = true
					} else if status == nil {
						client := c.rstMap[jobRequest.GetRemoteStorageTarget()]
						include, operation := client.IncludeInBulkRequest(walkCtx, jobRequest)
						if include {
							managerKey := fmt.Sprintf("%d-%s", jobRequest.RemoteStorageTarget, operation)

							bulkManagersMu.Lock()
							manager, ok := bulkManagers[managerKey]
							if !ok {
								manager = newBulkManager(request.JobId, &flex.BuilderJob_BulkOperation{
									RstId:     jobRequest.RemoteStorageTarget,
									Operation: operation,
								})
								bulkManagers[managerKey] = manager
							}
							bulkManagersMu.Unlock()

							manager.AddRequest(jobRequest)
							continue
						}
					}

					select {
					case <-walkCtx.Done():
						return walkCtx.Err()
					case jobSubmissionChan <- jobRequest:
						if jobWithError {
							jobsWithErrors++
						}
						jobsSubmitted++
					}
				}
			}
		}

		// Start worker(s) that process walk paths and enqueue job requests. Begin with one and add more
		// (up to GOMAXPROCS) when the job submission channel stays near empty, indicating the consumer is
		// draining faster than we can fill it. This keeps throughput balanced without over saturating
		// the system.
		g.Go(func() error {
			workers := 1
			lowThresholdTicks := 0
			g.Go(createJobRequests)
			for {
				select {
				case <-walkCtx.Done():
					return nil
				case <-walkDoneChan:
					return nil
				case <-time.After(100 * time.Millisecond):
					size := len(jobSubmissionChan)
					if workers < maxWorkers && size <= 2*workers {
						if size <= workers {
							lowThresholdTicks += 3
						} else {
							lowThresholdTicks++
						}

						if lowThresholdTicks >= 3 {
							g.Go(createJobRequests)
							workers++
							lowThresholdTicks = 0
						}
					} else {
						lowThresholdTicks = 0
					}
				}
			}
		})

		err := g.Wait()
		if err != nil {
			return abortBuilderJob(err)
		}

		if !reschedule {
			request.SetExternalId(makeWalkCompleteSentinel(request.JobId))
		}
	}

	bulkOperations := []*flex.BuilderJob_BulkOperation{}
	g, bulkCtx := errgroup.WithContext(ctx)
	for _, manager := range bulkManagers {
		g.Go(func() error {
			client := c.rstMap[manager.rstId]
			// bulkReschedule, bulkDelay, err := client.ManageBulkRequest(bulkCtx, request.JobId, manager.operation, manager.jobRequests)
			bulkReschedule, bulkDelay, err := client.ExecuteBulkRequest(bulkCtx, manager.StateMountPath, manager.Operation, manager.JobRequests)
			if err != nil {
				return err
			}

			jobsSubmitted, jobsWithErrors := manager.SubmitJobRequests(bulkCtx, jobSubmissionChan)

			builderStateMu.Lock()
			builder.Submitted += jobsSubmitted
			builder.Errors += jobsWithErrors
			if bulkReschedule {
				reschedule = true
				if rescheduleDelay == 0 {
					rescheduleDelay = bulkDelay
				} else {
					rescheduleDelay = min(rescheduleDelay, bulkDelay)
				}
			}
			builderStateMu.Unlock()

			return nil
		})
		bulkOperations = append(bulkOperations, manager.GetBulkOperation())
	}
	err := g.Wait()
	builder.SetBulkOperations(bulkOperations)
	if err != nil {
		return abortBuilderJob(err)
	}

	if reschedule {
		return true, rescheduleDelay, nil
	}

	managerErrIndex := 0
	managerErrs := make([]error, len(bulkManagers))
	wg := sync.WaitGroup{}
	for _, manager := range bulkManagers {
		index := managerErrIndex
		mgr := manager
		wg.Go(func() {
			client := c.rstMap[mgr.rstId]
			if err := client.CompleteBulkRequest(ctx, mgr.StateMountPath, mgr.Operation); err != nil {
				managerErrs[index] = err
			}
		})
		managerErrIndex++
	}
	wg.Wait()
	bulkManagerErr := errors.Join(managerErrs...)

	var errMessage string
	totalSubmitted := builder.GetSubmitted()
	totalErrors := builder.GetErrors()
	if totalSubmitted == 0 {
		if cfg.Download {
			if walkLocal {
				errMessage = fmt.Sprintf("walking local path since --%s was not provided; No matches found in path: %s", RemotePathFlag, cfg.Path)
			} else {
				errMessage = fmt.Sprintf("no matches found in remote path: %s", cfg.RemotePath)
			}
		} else {
			errMessage = fmt.Sprintf("no matches found in local path: %s", cfg.Path)
		}
	} else if totalErrors > 0 {
		errMessage = fmt.Sprintf("%d of %d requests were submitted with errors", totalErrors, totalSubmitted)
	}

	if bulkManagerErr != nil {
		if errMessage != "" {
			errMessage += fmt.Sprintf("; %s", bulkManagerErr)
		} else {
			errMessage = bulkManagerErr.Error()
		}
	}

	if errMessage != "" {
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			errMessage += fmt.Sprintf("; --%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag)
		}
		return false, 0, errors.New(errMessage)
	}
	return false, 0, nil
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}

const builderWalkCompletePrefix = "builder:walk-complete:"

func makeWalkCompleteSentinel(jobID string) string {
	return builderWalkCompletePrefix + jobID
}

func isWalkCompleteSentinel(externalID, jobID string) bool {
	return externalID == makeWalkCompleteSentinel(jobID)
}

func (c *JobBuilderClient) getPathsFn(cfg *flex.JobRequestCfg, walkingLocalPath bool) func(string) (string, string, error) {
	var remotePathDir string
	var remotePathIsGlob bool
	var isPathDir bool
	if cfg.Download {
		remotePathDir, remotePathIsGlob = GetDownloadRemotePathDirectory(cfg.RemotePath)
		stat, err := c.mountPoint.Lstat(cfg.Path)
		isPathDir = err == nil && stat.IsDir()
	}

	return func(walkPath string) (inMountPath, remotePath string, err error) {
		if cfg.Download {
			if walkingLocalPath {
				// Walking cfg.Path to support stub file download and files with a defined rst.
				inMountPath = walkPath
			} else {
				remotePath = walkPath
				// GetDownloadInMountPath should never return an error happen since remotePath and
				// remotePathDir are derived from cfg.RemotePath, so any error here indicates a bug
				// in the walking logic.
				inMountPath, err = GetDownloadInMountPath(cfg.Path, remotePath, remotePathDir, remotePathIsGlob, isPathDir, cfg.Flatten)
				if err == nil {
					// Ensure the local directory structure supports the object downloads
					err = c.mountPoint.CreateDir(filepath.Dir(inMountPath), 0755)
				}
			}
		} else {
			inMountPath = walkPath
			remotePath = inMountPath
		}
		return
	}
}

const (
	bulkManagerPath = ".beegfs-builder-job/bulkManager"
)

type bulkManager struct {
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	nextJobIndex   int64
	mu             sync.Mutex
}

func newBulkManager(jobId string, bulkOperation *flex.BuilderJob_BulkOperation) *bulkManager {
	stateMountPath := path.Join(bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
	return &bulkManager{
		StateMountPath: stateMountPath,
		rstId:          bulkOperation.RstId,
		Operation:      bulkOperation.Operation,
		nextJobIndex:   bulkOperation.NextRequestId,
		JobRequests:    []*beeremote.JobRequest{},
	}
}

func (m *bulkManager) GetBulkOperation() *flex.BuilderJob_BulkOperation {
	return &flex.BuilderJob_BulkOperation{
		RstId:         m.rstId,
		Operation:     m.Operation,
		NextRequestId: m.nextJobIndex,
	}
}

func (m *bulkManager) AddRequest(request *beeremote.JobRequest) {
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

func (m *bulkManager) SubmitJobRequests(ctx context.Context, jobSubmissionChan chan<- *beeremote.JobRequest) (jobsSubmitted int32, jobsWithErrors int32) {
	for _, request := range m.JobRequests {
		jobWithError := false
		status := request.GetGenerationStatus()
		if isStatusError(status) {
			jobWithError = true
		}

		select {
		case <-ctx.Done():
			return
		case jobSubmissionChan <- request:
			if jobWithError {
				jobsWithErrors++
			}
			jobsSubmitted++
		}
	}

	return
}
