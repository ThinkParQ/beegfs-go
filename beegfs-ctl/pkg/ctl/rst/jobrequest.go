package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type SyncJobRequestCfg struct {
	RSTID      string
	Path       string
	Overwrite  bool
	RemotePath string
	Download   bool
	ChanSize   int
}

type SyncJobResponse struct {
	Result   *beeremote.JobResult
	Err      error
	FatalErr bool
}

// SubmitSyncJobRequests asynchronously submits jobs returning the responses over the provided
// channel. It will immediately return an error if anything went wrong during setup. Subsequent
// errors that occur while submitting requests will be returned over the respChan. Most errors
// returned over respChan are likely limited to the specific request (i.e., an invalid request) and
// it is up to the caller to decide to continue making requests or cancel the context. If a fatal
// error occurs that is likely to prevent scheduling all future requests (such as errors walking a
// directory or connecting to BeeRemote) then the response will have FatalError=true and all
// outstanding goroutines will be immediately cancelled. In all cases the channel is closed once
// there are no more responses to receive.
func SubmitSyncJobRequests(ctx context.Context, cfg SyncJobRequestCfg) (<-chan *SyncJobResponse, error) {

	respChan := make(chan *SyncJobResponse, cfg.ChanSize)
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/40 Support directory downloads. For
	// downloads we cannot just setup the BeeGFSClient using the path, since it is likely the file
	// doesn't yet exist in BeeGFS. For now since we only support file downloads the parent
	// directory must always exist, so we can use it to setup the BeeGFS client. This also works for
	// uploads regardless if they are by path or file.
	beegfs, err := config.BeeGFSClient(filepath.Dir(cfg.Path))
	if err != nil {
		return nil, fmt.Errorf("unable to setup BeeGFS client: %w", err)
	}
	pathInMount, err := beegfs.GetRelativePathWithinMount(cfg.Path)
	if err != nil {
		return nil, err
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}

	var operation flex.SyncJob_Operation
	var isDir bool
	pathStat, err := beegfs.Stat(pathInMount)
	switch {
	case cfg.Download:
		operation = flex.SyncJob_DOWNLOAD
		if err != nil {
			// For downloads its expected the file doesn't already exist in BeeGFS. All other errors
			// should be returned.
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
		} else if !cfg.Overwrite {
			return nil, fmt.Errorf("download would overwrite an existing file in BeeGFS (set the overwrite flag to ignore)")
		} else {
			if pathStat.IsDir() {
				return nil, fmt.Errorf("downloading a directory is not supported (yet)")
			}
		}
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/40
		// Support directory downloads. For now assume all downloads are files.
		isDir = false
	default:
		operation = flex.SyncJob_UPLOAD
		if err != nil {
			return nil, err
		}
		isDir = pathStat.IsDir()
	}

	baseRequest := &beeremote.JobRequest{
		Path:                "",
		RemoteStorageTarget: cfg.RSTID,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation:  operation,
				Overwrite:  cfg.Overwrite,
				RemotePath: cfg.RemotePath,
			},
		},
	}

	if !isDir {
		// Complete the request in a separate goroutine otherwise if the caller provided an
		// unbuffered channel we would block here.
		go func() {
			baseRequest.Path = pathInMount
			resp, err := beeRemote.SubmitJob(ctx, &beeremote.SubmitJobRequest{Request: baseRequest})
			if err != nil {
				respChan <- &SyncJobResponse{
					Result: nil,
					Err:    err,
				}
			} else {
				respChan <- &SyncJobResponse{
					// We have to check the error because resp could be nil.
					Result: resp.Result,
					Err:    nil,
				}
			}
			close(respChan)
		}()
		return respChan, nil
	}

	// The channel size here is somewhat of an arbitrary selection. We mainly want to avoid blocking
	// walkDir() if possible. We could expose it to the user but then its one more thing to think
	// about. Testing shows with BR and CTL running on the same system with 4 workers submitting
	// requests and a channel size of 100 we are unlikely to ever block walkDir(). Possibly this
	// would be a problem if the number of workers was very low (due to few CPUs) and/or network
	// latency between CTL and BR is high. 2048 seems like a reasonable enough channel size to
	// smooth out bumps in network latency, while using a reasonable amount of memory.
	pathChan := make(chan string, 2048)
	// Because we have multiple senders to the respChan we have the goroutine that handles walking the
	// directory wait and close the channel once requests have been submitted for all paths.
	wg := sync.WaitGroup{}
	// This context signals if the goroutine walking the directory tree and the goroutines
	// submitting job requests should shutdown early.
	ctx, cancel := context.WithCancel(ctx)

	submitRequestFn := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case path, ok := <-pathChan:
				if !ok {
					return
				}
				req := proto.Clone(baseRequest).(*beeremote.JobRequest)
				req.Path, err = beegfs.GetRelativePathWithinMount(path)
				if err != nil {
					respChan <- &SyncJobResponse{
						Result: nil,
						Err:    err,
					}
					// Most likely we were provided a relative path, and we were unable to convert
					// it to a relative path within the mount point. Likely because we couldn't get
					// the cwd. Don't keep trying if this happens.
					cancel()
					return
				}
				resp, err := beeRemote.SubmitJob(ctx, &beeremote.SubmitJobRequest{Request: req})
				if err != nil {
					if st, ok := status.FromError(err); ok {
						switch st.Code() {
						case codes.Unavailable:
							respChan <- &SyncJobResponse{
								Result:   nil,
								Err:      fmt.Errorf("fatal error sending request to BeeRemote: %w", err),
								FatalErr: true,
							}
							// This would happen if there is an error connecting to BeeRemote.
							// Don't keep trying and signal other goroutines to shutdown as well.
							cancel()
							return
						}
					}
					respChan <- &SyncJobResponse{
						Result: nil,
						Err:    err,
					}
				} else {
					respChan <- &SyncJobResponse{
						// We have to check the error because resp could be nil, and we want to
						// handle certain errors specially.
						Result: resp.Result,
						Err:    err,
					}
				}
			}
		}
	}

	numWorkers := viper.GetInt(config.NumWorkersKey)
	if numWorkers > 1 {
		// One worker will be dedicated for walkDir (this function) unless there is only one CPU.
		numWorkers = viper.GetInt(config.NumWorkersKey) - 1
	}

	for range numWorkers {
		wg.Add(1)
		go submitRequestFn()
	}

	go func() {
		walkDirFunc := func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if d.Type()&fs.ModeSymlink != 0 {
					// TODO: https://github.com/ThinkParQ/bee-remote/issues/25
					// Support symbolic links.
					respChan <- &SyncJobResponse{
						Err: fmt.Errorf("skipping symbolic link: %s", path),
					}
				} else if !d.IsDir() {
					// Only send file paths to the channel.
					pathChan <- path
				}
			}
			return nil
		}

		err := beegfs.WalkDir(pathInMount, walkDirFunc)
		if err != nil {
			respChan <- &SyncJobResponse{
				Err:      fmt.Errorf("fatal error walking directory tree: %w", err),
				FatalErr: true,
			}
			cancel()
		}
		close(pathChan)
		// Wait to close the response channel until we finish creating requests for all paths.
		wg.Wait()
		close(respChan)
	}()

	return respChan, nil
}
