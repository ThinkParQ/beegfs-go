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

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, err error) {
	if !workRequest.HasBuilder() {
		err = ErrReqAndRSTTypeMismatch
		return
	}

	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken := workRequest.GetExternalId()

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

	walkChanSize := cap(jobSubmissionChan)
	var walkChan <-chan *filesystem.StreamPathResult
	walkPaths := filesystem.StreamPathsLexicographically
	if cfg.GetUpdate() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}
	if cfg.Download {

		if filter != nil {
			return false, fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
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

func (r *JobBuilderClient) PlanBulkRequest(ctx context.Context, cfg *flex.JobRequestCfg) (includeInBulk bool, skipIndividual bool, waitQueueDelay time.Duration) {
	return false, false, 0
}

func (r *JobBuilderClient) BuildBulkRequest(ctx context.Context) (submitBulkRequest SubmitBulkRequestFn, appendBulkRequestCfg AppendBulkRequestCfgFn, err error) {
	return nil, nil, ErrUnsupportedOpForRST
}

func (r *JobBuilderClient) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
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

func (c *JobBuilderClient) executeJobBuilderRequest(
	ctx context.Context,
	request *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	cfg *flex.JobRequestCfg,
) (bool, error) {

	var walkingLocalPath bool
	var remotePathDir string
	var remotePathIsGlob bool
	var isPathDir bool
	if cfg.Download {
		walkingLocalPath = walkLocalPathInsteadOfRemote(cfg)
		remotePathDir, remotePathIsGlob = GetDownloadRemotePathDirectory(cfg.RemotePath)
		stat, err := c.mountPoint.Lstat(cfg.Path)
		isPathDir = err == nil && stat.IsDir()
	}

	reschedule := false
	builder := request.GetBuilder()
	if builder.BulkStats == nil {
		builder.BulkStats = make(map[uint32]*flex.BuilderJob_BulkStat)
	}
	builderStateMu := sync.Mutex{}

	bulkRequestsChan := make(chan *flex.JobRequestCfg, 1024)
	createBulkJobRequests := func() (err error) {
		bulkRequestAdders := make(map[uint32]AppendBulkRequestCfgFn)
		for {

			select {
			case <-ctx.Done():
				return ctx.Err()
			case cfg, ok := <-bulkRequestsChan:
				if !ok {
					return nil
				}

				rstId := cfg.RemoteStorageTarget
				if _, ok := bulkRequestAdders[rstId]; !ok {
					builder.BulkStats[rstId] = &flex.BuilderJob_BulkStat{}
					client := c.rstMap[rstId]
					submitBulkRequest, appendBulkRequestCfg, err := client.BuildBulkRequest(ctx)
					if err != nil {
						// BuildBulkRequests should only return fatal errors, or cases where there
						// is no way to surface the problem on a generated request, because any
						// returned error aborts the builder job entirely.
						return err
					}

					defer func() {
						if err := submitBulkRequest(); err != nil {
							builder.BulkStats[rstId].SetError(err.Error())
						}
					}()
					bulkRequestAdders[rstId] = appendBulkRequestCfg
				}

				bulkRequestAdders[rstId](cfg)
				builder.BulkStats[rstId].Paths++
			}
		}
	}

	maxWorkers := runtime.GOMAXPROCS(0)
	createJobRequests := func() error {
		var err error
		var inMountPath string
		var remotePath string
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case walkResp, ok := <-walkChan:
				if !ok {
					return nil
				}

				if walkResp.Err != nil {
					return walkResp.Err
				}

				if walkResp.ResumeToken != "" {
					builderStateMu.Lock()
					reschedule = true
					request.SetExternalId(walkResp.ResumeToken)
					builderStateMu.Unlock()
					return nil
				}

				if cfg.Download {
					if walkingLocalPath {
						// Walking cfg.Path to support stub file download and files with a defined rst.
						inMountPath = walkResp.Path
					} else {
						remotePath = walkResp.Path
						inMountPath, err = GetDownloadInMountPath(cfg.Path, remotePath, remotePathDir, remotePathIsGlob, isPathDir, cfg.Flatten)
						if err != nil {
							// This should never happen since both remotePath and remotePathDir
							// come directly from cfg.RemotePath, so any error here indicates a
							// bug in the walking logic.
							return err
						}

						// Ensure the local directory structure supports the object downloads
						if err := c.mountPoint.CreateDir(filepath.Dir(inMountPath), 0755); err != nil {
							return err
						}
					}
				} else {
					inMountPath = walkResp.Path
					remotePath = inMountPath
				}
			}

			if cfg.GetUpdate() {
				if stat, statErr := c.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
					dirErr := updateDirRstConfig(ctx, cfg.RemoteStorageTarget, inMountPath)
					builderStateMu.Lock()
					builder.Submitted++
					if dirErr != nil {
						builder.Errors++
					}
					builderStateMu.Unlock()
					continue
				}
			}

			builtJobRequests, err := BuildJobRequests(ctx, c.rstMap, c.mountPoint, inMountPath, remotePath, cfg)
			if err != nil {
				// BuildJobRequest should only return fatal errors, or if there are no RSTs
				// specified/configured on an entry and there is no other way to return the
				// error other then aborting the builder job entirely.
				return err
			}
			errorCount := int32(0)
			submitted := int32(0)
			for _, request := range builtJobRequests {
				requestCfg := request.cfg
				jobRequest := request.request

				status := jobRequest.GetGenerationStatus()
				if status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION) {
					errorCount++
				} else if status == nil || status.State == beeremote.JobRequest_GenerationStatus_UNSPECIFIED {
					client := c.rstMap[requestCfg.GetRemoteStorageTarget()]
					includeInBulk, skipIndividual, waitQueueDelay := client.PlanBulkRequest(ctx, requestCfg)
					if !includeInBulk && skipIndividual {
						errorCount++
						jobRequest.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
							State:   beeremote.JobRequest_GenerationStatus_ERROR,
							Message: "request was not submitted: it was neither added to a bulk request nor submitted individually (this is probably a bug)",
						})
					} else {
						if includeInBulk {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case bulkRequestsChan <- requestCfg:
							}
						}
						if skipIndividual {
							continue
						}
						if waitQueueDelay > 0 {
							jobRequest.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
								State:   beeremote.JobRequest_GenerationStatus_NOT_READY,
								Message: waitQueueDelay.String(),
							})
						}
					}
				}

				select {
				case <-ctx.Done():
				case jobSubmissionChan <- jobRequest:
					submitted++
				}
			}

			builderStateMu.Lock()
			builder.Submitted += submitted
			builder.Errors += errorCount
			builderStateMu.Unlock()
		}
	}

	// Start worker(s) that process walk paths and enqueue job requests. Begin with one and add more
	// (up to GOMAXPROCS) when the job submission channel stays near empty, indicating the consumer is
	// draining faster than we can fill it. This keeps throughput balanced without over saturating
	// the system.
	g, ctx := errgroup.WithContext(ctx)
	createJobRequestsWg := sync.WaitGroup{}
	createJobRequestsDoneChan := make(chan struct{})
	startCreateJobRequests := func() {
		createJobRequestsWg.Add(1)
		g.Go(func() error {
			defer createJobRequestsWg.Done()
			return createJobRequests()
		})
	}

	startCreateJobRequests()
	g.Go(func() error {
		createJobRequestsWg.Wait()
		close(createJobRequestsDoneChan)
		close(bulkRequestsChan)
		return nil
	})
	g.Go(createBulkJobRequests)
	g.Go(func() error {
		workers := 1
		lowThresholdTicks := 0
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-createJobRequestsDoneChan:
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
						startCreateJobRequests()
						workers++
						lowThresholdTicks = 0
					}
				} else {
					lowThresholdTicks = 0
				}
			}
		}
	})

	if err := g.Wait(); err != nil {
		return false, fmt.Errorf("job builder request was aborted: %w", err)
	}
	if reschedule {
		return true, nil
	}

	bulkRequestSubmitted := int32(0)
	bulkErrMessages := make([]string, 0)
	for rstId, stats := range builder.GetBulkStats() {
		if stats == nil {
			continue
		}

		includedPaths := stats.GetPaths()
		bulkRequestSubmitted += includedPaths
		if !stats.HasError() {
			continue
		}
		bulkErrMessage := fmt.Sprintf("rst %d: paths=%d, error=%q", rstId, includedPaths, stats.GetError())
		bulkErrMessages = append(bulkErrMessages, bulkErrMessage)
	}

	errMessages := make([]string, 0)
	individualRequestSubmitted := builder.GetSubmitted()
	individualRequestErrors := builder.GetErrors()
	if individualRequestSubmitted == 0 && bulkRequestSubmitted == 0 {
		if cfg.Download {
			if walkingLocalPath {
				errMessages = append(errMessages, fmt.Sprintf("walking local path since --%s was not provided; No matches found in path: %s", RemotePathFlag, cfg.Path))
			} else {
				errMessages = append(errMessages, fmt.Sprintf("no matches found in remote path: %s", cfg.RemotePath))
			}
		} else {
			errMessages = append(errMessages, fmt.Sprintf("no matches found in local path: %s", cfg.Path))
		}
	} else {
		if individualRequestErrors > 0 {
			errMessages = append(errMessages, fmt.Sprintf("%d of %d requests were submitted with errors", individualRequestErrors, individualRequestSubmitted))
		}

		if len(bulkErrMessages) > 0 {
			bulkErrMessage := fmt.Sprintf("bulk submission errors: %s", strings.Join(bulkErrMessages, " | "))
			errMessages = append(errMessages, bulkErrMessage)
		}
	}

	if len(errMessages) > 0 {
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			errMessages = append(errMessages, fmt.Sprintf("--%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag))
		}
		return false, errors.New(strings.Join(errMessages, "; "))
	}
	return false, nil
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
