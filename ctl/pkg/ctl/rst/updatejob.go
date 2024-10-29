package rst

import (
	"context"
	"fmt"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type UpdateJobCfg struct {
	Path     string
	JobID    string
	Force    bool
	NewState beeremote.UpdateJobRequest_NewState
}

func UpdateJobs(ctx context.Context, cfg UpdateJobCfg) (*beeremote.UpdateJobResponse, error) {

	pathInMount := cfg.Path
	if !cfg.Force {
		beegfs, err := config.BeeGFSClient(cfg.Path)
		if err != nil {
			return nil, err
		}
		pathInMount, err = beegfs.GetRelativePathWithinMount(cfg.Path)
		if err != nil {
			return nil, err
		}
		pathStat, err := beegfs.Stat(pathInMount)
		if err != nil {
			return nil, err
		}
		if pathStat.IsDir() {
			return nil, fmt.Errorf("updating jobs by directory is not supported (yet)")
		}
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
