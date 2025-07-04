package rst

import (
	"context"
	"errors"
	"io"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetJobsConfig contains all user facing flags needed to generate a
// beeremote.GetJobsRequest.
type GetJobsConfig struct {
	JobID            string
	Path             string
	Recurse          bool
	WithWorkRequests bool
	WithWorkResults  bool
	// Currently exactPath is not exported to only allow this mode to be used from within the rst
	// package, notably for GetStatus. This is because it might be confusing when a user had a
	// directory containing files that were uploaded, but then later deleted or renamed the
	// directory and replaced it with an identically named file, making it impossible to display
	// jobs for the previously uploaded files under the now deleted directory. This is not a problem
	// for GetStatus because it is only used when we want to get the jobs for at most a single path,
	// and will be comparing mtime to determine if the dbPath matches the current fsPath.
	exactPath bool
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
	if err != nil && !errors.Is(err, filesystem.ErrUnmounted) {
		return err
	}
	pathInMount, err := beegfs.GetRelativePathWithinMount(cfg.Path)
	if err != nil {
		return err
	}

	request := &beeremote.GetJobsRequest{
		IncludeWorkRequests: cfg.WithWorkRequests || viper.GetBool(config.DebugKey),
		IncludeWorkResults:  cfg.WithWorkResults || viper.GetBool(config.DebugKey),
	}

	switch {
	case cfg.JobID != "":
		// If the user provides a job ID, we can at most update a single path. If for some reason
		// they provided a directory, they will simply get a not found error. We don't verify if the
		// specified path is a directory in case a file was deleted and a new directory created with
		// the same name, which would prevent updating updating the previously executed job.
		request.Query = &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: cfg.JobID,
				Path:  pathInMount,
			},
		}
	case cfg.exactPath:
		request.Query = &beeremote.GetJobsRequest_ByExactPath{ByExactPath: cfg.Path}
	case cfg.Recurse:
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
				if rpcStatus, ok := status.FromError(err); ok {
					if rpcStatus.Code() == codes.NotFound {
						err = ErrEntryNotFound
					}
				}
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
