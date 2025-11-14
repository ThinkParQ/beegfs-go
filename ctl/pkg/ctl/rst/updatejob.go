package rst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

type UpdateJobCfg struct {
	// The path or path prefix to update (when Recurse is set).
	Path           string
	JobID          string
	RemoteTargets  []uint
	Force          bool
	NewState       beeremote.UpdateJobsRequest_NewState
	Recurse        bool
	DeleteOrphaned bool
}

type UpdateJobsResponse struct {
	Path   string
	Result *beeremote.UpdateJobsResponse
	Err    error
}

// UpdateJobsByPaths updates the state of one or more job(s) for one or more path(s). This function
// intentionally does not check if the provided path still exists in BeeGFS because a job could be
// triggered then the file deleted/renamed. For similar reasons it does not check if the path is a
// directory (which shouldn't normally have jobs), since it is possible a file was uploaded,
// deleted, then a directory created in its place with the same name. In all of these cases we need
// to be able to update any existing jobs for any path. If no jobs exist, Remote will simply return
// not found.
func UpdateJobsByPaths(ctx context.Context, cfg *UpdateJobCfg, respChan chan<- *UpdateJobsResponse) error {

	if cfg.Recurse && cfg.JobID != "" {
		return errors.New("invalid configuration: cannot both recursively update jobs and update a specific job ID")
	}

	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil && !errors.Is(err, filesystem.ErrUnmounted) {
		return err
	}
	pathInMount, err := beegfs.GetRelativePathWithinMount(cfg.Path)
	if err != nil {
		return err
	}

	request := beeremote.UpdateJobsRequest_builder{
		NewState:    cfg.NewState,
		ForceUpdate: cfg.Force,
		Path:        pathInMount,
	}.Build()

	if cfg.JobID != "" {
		request.SetJobId(*proto.String(cfg.JobID))
	}
	targetFilter, err := buildRemoteTargetFilter(cfg.RemoteTargets)
	if err != nil {
		return err
	}
	if len(targetFilter) > 0 {
		remoteTargets := make(map[uint32]bool, len(targetFilter))
		for tgt := range targetFilter {
			remoteTargets[tgt] = true
		}
		request.SetRemoteTargets(remoteTargets)
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return err
	}

	// For a single path just use the unary RPC to update jobs. Runs in a separate goroutine so the
	// caller can wait for UpdateJobs() to return before reading from the channel.
	if !cfg.Recurse {
		go func() {
			defer close(respChan)
			resp, err := beeRemote.UpdateJobs(ctx, request)
			if err != nil {
				respChan <- &UpdateJobsResponse{
					Err: err,
				}
				return
			}
			respChan <- &UpdateJobsResponse{
				Path:   request.Path,
				Result: resp,
			}
		}()
		return nil
	}

	// Otherwise use the streaming RPC to recursively update all paths with this prefix.
	stream, err := beeRemote.UpdatePaths(ctx, beeremote.UpdatePathsRequest_builder{
		PathPrefix:      pathInMount,
		RequestedUpdate: request,
	}.Build())

	if err != nil {
		return err
	}

	go func() {
		defer close(respChan)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				respChan <- &UpdateJobsResponse{
					Err: err,
				}
				return
			}
			respChan <- &UpdateJobsResponse{
				Path:   resp.GetPath(),
				Result: resp.GetUpdateResult(),
			}
		}
	}()
	return nil
}

// DeleteOrphanedJobs walks the Remote database for the specified path or prefix and deletes any
// jobs whose BeeGFS path no longer exists locally. It respects the jobID and remote target filters
// provided in cfg. Unlike UpdateJobsByPaths, this function requires BeeGFS to be mounted locally so
// it can verify each path before requesting it be deleted.
func DeleteOrphanedJobs(ctx context.Context, cfg *UpdateJobCfg, respChan chan<- *UpdateJobsResponse) error {

	if cfg.Recurse && cfg.JobID != "" {
		return errors.New("invalid configuration: cannot both recursively update jobs and update a specific job ID")
	}
	if cfg.Force {
		return errors.New("invalid configuration: the force flag cannot be used with --delete-orphaned-jobs")
	}

	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil {
		if errors.Is(err, filesystem.ErrUnmounted) {
			return fmt.Errorf("unable to delete orphaned jobs without a BeeGFS mount (set --%s to the BeeGFS mount point)", config.BeeGFSMountPointKey)
		}
		return err
	}
	if beegfs.GetMountPath() == "" {
		return fmt.Errorf("unable to determine BeeGFS mount point (required for --delete-orphaned-jobs)")
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	jobStream := make(chan *GetJobsResponse, 1024)
	getJobsCfg := GetJobsConfig{
		Path:  cfg.Path,
		JobID: cfg.JobID,
	}
	switch {
	case cfg.JobID != "":
		// Query is restricted to a single job ID.
	case cfg.Recurse:
		getJobsCfg.Recurse = true
	default:
		getJobsCfg.exactPath = true
	}

	if err := GetJobs(pipelineCtx, getJobsCfg, jobStream); err != nil {
		cancel()
		return err
	}

	targetFilter, err := buildRemoteTargetFilter(cfg.RemoteTargets)
	if err != nil {
		cancel()
		return err
	}
	numWorkers := viper.GetInt(config.NumWorkersKey)
	if numWorkers < 1 {
		numWorkers = 1
	}
	mountPath := beegfs.GetMountPath()

	go func() {
		defer close(respChan)
		defer cancel()

		deleteQueue := make(chan string, numWorkers*4)
		group, groupCtx := errgroup.WithContext(ctx)

		group.Go(func() error {
			defer close(deleteQueue)

			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case jobResp, ok := <-jobStream:
					if !ok {
						return nil
					}
					if jobResp.Err != nil {
						if errors.Is(jobResp.Err, rst.ErrEntryNotFound) {
							continue
						}
						return jobResp.Err
					}
					if !matchesTargetFilter(jobResp.Results, targetFilter) {
						continue
					}
					missing, err := pathMissingOnClient(mountPath, jobResp.Path)
					if err != nil {
						return err
					}
					if !missing {
						continue
					}
					select {
					case <-groupCtx.Done():
						return groupCtx.Err()
					case deleteQueue <- jobResp.Path:
					}
				}
			}
		})

		for i := 0; i < numWorkers; i++ {
			group.Go(func() error {
				for {
					select {
					case <-groupCtx.Done():
						return groupCtx.Err()
					case deletePath, ok := <-deleteQueue:
						if !ok {
							return nil
						}
						absPath := makeAbsolutePath(mountPath, deletePath)
						pathCfg := cloneUpdateJobCfg(cfg, absPath)
						responses := make(chan *UpdateJobsResponse, 1)
						if err := UpdateJobsByPaths(groupCtx, pathCfg, responses); err != nil {
							return err
						}
						for {
							select {
							case <-groupCtx.Done():
								return groupCtx.Err()
							case updateResp, ok := <-responses:
								if !ok {
									goto nextPath
								}
								respChan <- updateResp
							}
						}
					nextPath:
					}
				}
			})
		}

		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			respChan <- &UpdateJobsResponse{Err: err}
		}
	}()

	return nil
}

func buildRemoteTargetFilter(targets []uint) (map[uint32]struct{}, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	filter := make(map[uint32]struct{}, len(targets))
	for _, tgt := range targets {
		if tgt > math.MaxUint32 {
			return nil, fmt.Errorf("specified remote target ID is larger than the uint32 maximum: %d", tgt)
		}
		filter[uint32(tgt)] = struct{}{}
	}
	return filter, nil
}

func matchesTargetFilter(results []*beeremote.JobResult, filter map[uint32]struct{}) bool {
	if len(filter) == 0 {
		return true
	}
	for _, job := range results {
		if job.GetJob() == nil || job.GetJob().GetRequest() == nil {
			continue
		}
		if _, ok := filter[job.GetJob().GetRequest().GetRemoteStorageTarget()]; ok {
			return true
		}
	}
	return false
}

func pathMissingOnClient(mountPath, dbPath string) (bool, error) {
	absPath := makeAbsolutePath(mountPath, dbPath)
	if _, err := os.Lstat(absPath); err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func makeAbsolutePath(mountPath, dbPath string) string {
	trimmed := strings.TrimPrefix(dbPath, "/")
	if trimmed == "" {
		return mountPath
	}
	if mountPath == "" {
		return "/" + trimmed
	}
	return filepath.Join(mountPath, trimmed)
}

func cloneUpdateJobCfg(cfg *UpdateJobCfg, path string) *UpdateJobCfg {
	return &UpdateJobCfg{
		Path:           path,
		JobID:          cfg.JobID,
		RemoteTargets:  cfg.RemoteTargets,
		Force:          false,
		NewState:       cfg.NewState,
		Recurse:        false,
		DeleteOrphaned: false,
	}
}
