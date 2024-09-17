package rst

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// GetJobsConfig contains all user facing flags needed to generate a
// beeremote.GetJobsRequest.
type GetJobsConfig struct {
	JobID            string
	Path             string
	WithWorkRequests bool
	WithWorkResults  bool
}

type GetJobsResponse struct {
	Path    string
	Results []*beeremote.JobResult
	Err     error
}

// GetJobs asynchronously retrieves jobs based on the provided cfg and sends them to respChan.
// It will immediately return an error if anything goes wrong during setup. Subsequent errors
// will be returned over the respChan. Currently all errors are fatal.
func GetJobs(ctx context.Context, cfg GetJobsConfig, respChan chan<- *GetJobsResponse) error {

	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil {
		return err
	}
	pathInMount, err := beegfs.GetRelativePathWithinMount(cfg.Path)
	if err != nil {
		return err
	}
	pathStat, err := beegfs.Stat(pathInMount)
	if err != nil {
		return err
	}

	request := &beeremote.GetJobsRequest{
		IncludeWorkRequests: cfg.WithWorkRequests || viper.GetBool(config.DebugKey),
		IncludeWorkResults:  cfg.WithWorkResults || viper.GetBool(config.DebugKey),
	}

	switch {
	case cfg.JobID != "":
		if pathStat.IsDir() {
			return fmt.Errorf("specifying a job ID is only allowed if the provided path is a file (not a directory)")
		}
		request.Query = &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: cfg.JobID,
				Path:  pathInMount,
			},
		}
	case pathStat.IsDir():
		request.Query = &beeremote.GetJobsRequest_ByPathPrefix{ByPathPrefix: pathInMount}
	default:
		request.Query = &beeremote.GetJobsRequest_ByExactPath{ByExactPath: pathInMount}
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return err
	}

	stream, err := beeRemote.GetJobs(ctx, request)
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
				respChan <- &GetJobsResponse{
					Err: err,
				}
				return
			}
			respChan <- &GetJobsResponse{
				Path:    resp.Path,
				Results: resp.Results,
			}
		}
	}()

	return nil
}
