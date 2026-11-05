package rst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type UpdateJobCfg struct {
	// The path or path prefix to update (when Recurse is set).
	Path          string
	JobID         string
	RemoteTargets []uint
	Force         bool
	NewState      beeremote.UpdateJobsRequest_NewState
	Recurse       bool
	// Reason is why the jobs are cancelled. Remote appends it to the message of every job the
	// update cancels. It is ignored for other states.
	Reason string
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
	if cfg.Reason != "" {
		request.SetReason(cfg.Reason)
	}

	if cfg.JobID != "" {
		request.SetJobId(*new(cfg.JobID))
	}
	if cfg.RemoteTargets != nil {
		remoteTargets := map[uint32]bool{}
		for _, tgt := range cfg.RemoteTargets {
			if tgt > math.MaxUint32 {
				return fmt.Errorf("specified remote target ID is larger than the uint32 maximum: %d", tgt)
			}
			remoteTargets[uint32(tgt)] = true
		}
		request.SetRemoteTargets(remoteTargets)
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return err
	}
	// An older Remote ignores the reason without an error, so the jobs would be cancelled without
	// it. The reason came with target management, so a Remote that offers that also keeps it.
	if cfg.Reason != "" {
		if _, err := requireRSTManagement(ctx); err != nil {
			return fmt.Errorf("this Remote does not record a reason with cancelled jobs (hint: upgrade beegfs-remote, or omit --message): %w", err)
		}
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
