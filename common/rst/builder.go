package rst

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	log        *zap.Logger
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
}

var _ Provider = &JobBuilderClient{}

func NewJobBuilderClient(ctx context.Context, log *zap.Logger, rstMap map[uint32]Provider, mountPoint filesystem.Provider) *JobBuilderClient {
	if log == nil {
		log = zap.NewNop()
	}
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[JobBuilderClient]().PkgPath())))

	return &JobBuilderClient{
		ctx:        ctx,
		log:        log,
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
	if cfg.Download {

		if filter != nil {
			return false, fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
		}

		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			if walkChan, err = filesystem.StreamPathsLexicographically(ctx, c.mountPoint, workRequest.Path, resumeToken, maxRequests, walkChanSize, nil); err != nil {
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
		walkChan, err = filesystem.StreamPathsLexicographically(ctx, c.mountPoint, workRequest.Path, resumeToken, maxRequests, walkChanSize, filter)
		if err != nil {
			return
		}
	}

	return c.executeJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan, cfg)
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
func (c *JobBuilderClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (*RemotePathInfo, error) {
	return nil, ErrUnsupportedOpForRST
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
	builder := request.GetBuilder()

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

	var (
		// jobRequestsBuffer holds job requests that need to be sorted before streaming to the caller.
		// In practice, a thousand job requests occupy tens to a few hundred megabytes contingent on
		// string lengths.
		jobRequestsBuffer []*beeremote.JobRequest
		// jobRequestsBufferSize maintains the estimated byte size of buffered job requests.
		jobRequestsBufferSize int64
		// jobRequestsBufferSizeThreshold forces the jobRequestsBuffer to be flushed; this is
		// intended as a safety valve to ensure memory utilization remains controlled. This value
		// should large enough to never be reached.
		jobRequestsBufferSizeThreshold = int64(100 * 1024 * 1024)
		// jobRequestsBufferMu mutex guards jobRequestsBuffer updates.
		jobRequestsBufferMu = sync.Mutex{}
	)

	// submitJobRequests sends jobRequests to jobSubmissionChan unless skipBuffering is true. When
	// buffering, requests with SortValues go into jobRequestsBuffer, which is sorted and flushed
	// after all requests have been received.
	//
	// Only the parent context should be used to ensure the job requests are submitted and only failed
	// when the caller's context is cancelled.
	var submitJobRequests func(jobRequests []*beeremote.JobRequest, skipBuffering bool) (submitCount int, errorCount int)
	submitJobRequests = func(jobRequests []*beeremote.JobRequest, skipBuffering bool) (submitCount int, errorCount int) {
		for _, jobRequest := range jobRequests {
			if !skipBuffering && len(jobRequest.SortValues) > 0 {
				jobRequestsBufferMu.Lock()
				jobRequestsBuffer = append(jobRequestsBuffer, jobRequest)
				jobRequestsBufferSize += sizeOfJobRequest(jobRequest)

				// Sort and flush jobRequestsBuffer when memory usage exceeds threshold. This will
				// block other processes attempting to defer jobRequests until complete.
				if jobRequestsBufferSize >= jobRequestsBufferSizeThreshold {
					c.log.Warn("exceeded job request buffer memory usage threshold", zap.Int("jobRequests", len(jobRequestsBuffer)),
						zap.Int64("bufferSizeBytes", jobRequestsBufferSize), zap.Int64("thresholdBytes", jobRequestsBufferSizeThreshold))

					sortJobRequests(jobRequestsBuffer)
					submits, errors := submitJobRequests(jobRequestsBuffer, true)
					errorCount += errors
					submitCount += submits
					jobRequestsBuffer = []*beeremote.JobRequest{}
					jobRequestsBufferSize = 0
				}
				jobRequestsBufferMu.Unlock()
				continue
			}

			status := jobRequest.GetGenerationStatus()
			if status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION) {
				errorCount++
			}
			select {
			case <-ctx.Done():
			case jobSubmissionChan <- jobRequest:
				submitCount++
			}
		}
		return
	}

	g, gCtx := errgroup.WithContext(ctx)
	builderStateMu := sync.Mutex{}
	reschedule := false

	maxWorkers := runtime.GOMAXPROCS(0)
	walkDoneChan := make(chan struct{}, maxWorkers)
	defer close(walkDoneChan)
	createJobRequests := func() error {
		var err error
		var inMountPath string
		var remotePath string

		for {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
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

			jobRequests, err := BuildJobRequests(gCtx, c.rstMap, c.mountPoint, inMountPath, remotePath, cfg)
			if err != nil {
				// BuildJobRequest should only return fatal errors, or if there are no RSTs
				// specified/configured on an entry and there is no other way to return the
				// error other then aborting the builder job entirely.
				return err
			}

			submitCount, errorCount := submitJobRequests(jobRequests, false)
			builderStateMu.Lock()
			builder.Submitted += int32(submitCount)
			builder.Errors += int32(errorCount)
			builderStateMu.Unlock()
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
			case <-gCtx.Done():
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

	// Sort and submit any remaining job requests. These requests need to be submitted regardless of
	// any errors that may have occurred since the job request was successfully created.
	if len(jobRequestsBuffer) > 0 {
		sortJobRequests(jobRequestsBuffer)
		submitCount, errorCount := submitJobRequests(jobRequestsBuffer, true)
		builder.Submitted += int32(submitCount)
		builder.Errors += int32(errorCount)
	}

	if err != nil {
		return false, fmt.Errorf("job builder request was aborted: %w", err)
	}

	if reschedule {
		return true, nil
	}

	var errMessage string
	totalSubmitted := builder.GetSubmitted()
	totalErrors := builder.GetErrors()
	if totalSubmitted == 0 {
		if cfg.Download {
			if walkingLocalPath {
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

	if errMessage != "" {
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			errMessage += fmt.Sprintf("; --%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag)
		}
		return false, errors.New(errMessage)
	}
	return false, nil
}

func sortJobRequests(buffer []*beeremote.JobRequest) {
	sort.SliceStable(buffer, func(i, j int) bool {
		a := buffer[i].SortValues
		b := buffer[j].SortValues
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] == b[k] {
				continue
			}
			return a[k] < b[k]
		}
		return len(a) < len(b)
	})
}

// sizeOfJobRequest approximates the size of jobRequest. The actual heap usage may end up higher or
// lower than the estimate based on runtime overhead and whether heap usages were reused.
func sizeOfJobRequest(jobRequests *beeremote.JobRequest) int64 {
	size := int64(proto.Size(jobRequests))
	size += int64(unsafe.Sizeof(*jobRequests))
	size += int64(len(jobRequests.SortValues)) * int64(unsafe.Sizeof(""))
	return size
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
