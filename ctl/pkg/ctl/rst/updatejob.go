package rst

import (
	"context"
	"errors"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type UpdateJobCfg struct {
	Path     string
	JobID    string
	Force    bool
	NewState beeremote.UpdateJobRequest_NewState
}

// UpdateJobs updates the state of all jobs at the specified path. This function intentionally does
// not check if the provided path still exists in BeeGFS because a job could be triggered then the
// file deleted/renamed. For similar reasons it does not check if the path is a directory (which
// shouldn't normally have jobs), since it is possible a file was uploaded, deleted, then a
// directory created in its place with the same name. In all of these cases we need to be able to
// update any existing jobs for any path. If no jobs exist, Remote will simply return not found.
//
// TODO (https://github.com/ThinkParQ/bee-remote/issues/15): Support updating multiple paths based
// on a provided prefix.
func UpdateJobs(ctx context.Context, cfg UpdateJobCfg) (*beeremote.UpdateJobResponse, error) {

	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil && !errors.Is(err, filesystem.ErrUnmounted) {
		return nil, err
	}
	pathInMount, err := beegfs.GetRelativePathWithinMount(cfg.Path)
	if err != nil {
		return nil, err
	}

	request := &beeremote.UpdateJobRequest{
		NewState:    cfg.NewState,
		ForceUpdate: cfg.Force,
	}

	switch {
	case cfg.JobID != "":
		request.Query = &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: cfg.JobID,
				Path:  pathInMount,
			},
		}
	case pathInMount != "":
		request.Query = &beeremote.UpdateJobRequest_ByExactPath{ByExactPath: pathInMount}
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}

	return beeRemote.UpdateJob(ctx, request)
}
