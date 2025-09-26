package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
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

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	if !workRequest.HasBuilder() {
		return ErrReqAndRSTTypeMismatch
	}

	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	walkChanSize := len(jobSubmissionChan)
	var walkChan <-chan *WalkResponse
	var err error
	if cfg.Download {
		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			if walkChan, err = WalkPath(ctx, c.mountPoint, workRequest.Path, walkChanSize); err != nil {
				return err
			}
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				return fmt.Errorf("failed to determine rst client")
			}
			if walkChan, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.RemotePath), walkChanSize); err != nil {
				return err
			}
		}
	} else if walkChan, err = WalkPath(ctx, c.mountPoint, workRequest.Path, walkChanSize); err != nil {
		return err
	}

	return c.executeJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan, cfg)
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
func (c *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int) (<-chan *WalkResponse, error) {
	return nil, ErrUnsupportedOpForRST
}

// SanitizeRemotePath should never be called.
func (c *JobBuilderClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

// GetRemotePathInfo is not implemented and should never be called.
func (c *JobBuilderClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, map[string]string, error) {
	return 0, time.Time{}, nil, ErrUnsupportedOpForRST
}

// GenerateExternalId is not implemented and should never be called.
func (c *JobBuilderClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	return "", ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) executeJobBuilderRequest(ctx context.Context, request *flex.WorkRequest, walkChan <-chan *WalkResponse, jobSubmissionChan chan<- *beeremote.JobRequest, cfg *flex.JobRequestCfg) error {
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

	var err error
	var submittedTotal atomic.Uint32
	var submittedWithErrors atomic.Uint32
	workers := 2
	g, ctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {

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

				jobRequests, err := BuildJobRequests(ctx, c.rstMap, c.mountPoint, inMountPath, remotePath, cfg)
				if err != nil {
					// BuildJobRequest should only return fatal errors, or if there are no RSTs
					// specified/configured on an entry and there is no other way to return the
					// error other then aborting the builder job entirely.
					return err
				}

				for _, jobRequest := range jobRequests {
					status := jobRequest.GetGenerationStatus()
					if status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION) {
						submittedWithErrors.Add(1)
					}
					jobSubmissionChan <- jobRequest
					submittedTotal.Add(1)
				}
			}
		})
	}
	if err = g.Wait(); err != nil {
		return fmt.Errorf("job builder request was aborted: %w", err)
	}

	var errMessage string
	if submittedTotal.Load() == 0 {
		if cfg.Download {
			if walkingLocalPath {
				errMessage = fmt.Sprintf("walking local path since --remote-path was not provided; No matches found in path: %s", cfg.Path)
			} else {
				errMessage = fmt.Sprintf("no matches found in remote path: %s", cfg.RemotePath)
			}
		} else {
			errMessage = fmt.Sprintf("no matches found in local path: %s", cfg.Path)
		}
	} else if submittedWithErrors.Load() > 0 {
		errMessage = fmt.Sprintf("%d of %d requests were submitted with errors", submittedWithErrors.Load(), submittedTotal.Load())
	}

	if errMessage != "" {
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			errMessage += "; --remote-target was not provided so relying on configured rstIds and stub urls"
		}
		return errors.New(errMessage)
	}
	return nil
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}
