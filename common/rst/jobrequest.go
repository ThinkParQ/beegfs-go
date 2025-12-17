package rst

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/common/filesystem"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobResponse struct {
	Path     string
	Result   *beeremote.JobResult
	Status   beeremote.SubmitJobResponse_ResponseStatus
	Err      error
	FatalErr bool
}

// SubmitJobRequest asynchronously submits jobs returning the responses over the JobResponse
// channel. It will immediately return an error if anything went wrong during setup. Subsequent
// errors that occur while submitting requests will be returned over the JobResponse channel. Most
// errors returned over the channel are likely limited to the specific request (i.e., an invalid
// request) and it is up to the caller to decide to continue making requests or cancel the context.
// If a fatal error occurs that is likely to prevent scheduling all future requests (such as errors
// walking a directory or connecting to BeeRemote) then the response will have FatalError=true and
// all outstanding goroutines will be immediately cancelled. In all cases the channel is closed once
// there are no more responses to receive.
func SubmitJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, chanSize int) (<-chan *JobResponse, error) {
	remote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}

	respChan := make(chan *JobResponse, chanSize)
	go func() {
		defer close(respChan)

		requests, err := prepareJobRequests(ctx, remote, cfg)
		if err != nil {
			respChan <- &JobResponse{Path: cfg.Path, Err: err}
		}

		for _, request := range requests {
			resp := &JobResponse{Path: request.Path}
			submission, err := remote.SubmitJob(ctx, &beeremote.SubmitJobRequest{Request: request})
			if err != nil {
				resp.Err = err
				// We have to check the error because submission could be nil so we shouldn't try and
				// deference it to set the resp.Result and some errors are handled specially.
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.Unavailable:
						resp.Err = fmt.Errorf("fatal error sending request to BeeRemote: %w", err)
						resp.FatalErr = true
						respChan <- resp
						return
					}
				}
			} else {
				resp.Result = submission.Result
				resp.Status = submission.GetStatus()
			}

			respChan <- resp
		}
	}()

	return respChan, nil
}

// prepareJobRequests creates all job requests required. If the path does not exist, unknown, glob
// pattern or a directory then a job-builder request will be returned. Otherwise, the supplied
// cfg.rstId, stub file url, or the file's rstIds will be used to generate rst specific job
// requests.
func prepareJobRequests(ctx context.Context, remote beeremote.BeeRemoteClient, cfg *flex.JobRequestCfg) ([]*beeremote.JobRequest, error) {
	mountPoint, err := config.BeeGFSClient(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("unable to acquire BeeGFS client: %w", err)
	}

	pathInfo, err := getMountPathInfo(mountPoint, cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("unable to determine information for path: %w", err)
	}

	cfg.SetPath(pathInfo.Path)
	if !cfg.Download && cfg.RemotePath == "" {
		cfg.SetRemotePath(pathInfo.Path)
	}

	jobBuilder := false
	if pathInfo.IsGlob {
		jobBuilder = true
	} else if pathInfo.IsDir {
		jobBuilder = true
		if cfg.Download {
			// Check if the downloaded file already exists
			remotePathDir, _ := GetDownloadRemotePathDirectory(cfg.RemotePath)
			inMountPath, err := GetDownloadInMountPath(cfg.Path, cfg.RemotePath, remotePathDir, false, true, cfg.Flatten)
			if err != nil {
				// This should never happen since both remotePath and remotePathDir come directly
				// from cfg.RemotePath, so any error here indicates a bug in the walking logic.
				return nil, err
			}
			inMountPathInfo, err := getMountPathInfo(mountPoint, inMountPath)
			if err == nil && inMountPathInfo.Exists && !inMountPathInfo.IsDir {
				pathInfo = inMountPathInfo
				cfg.SetPath(pathInfo.Path)
				jobBuilder = false
			}
		}
	} else if !pathInfo.Exists {
		if !cfg.Download {
			return nil, fmt.Errorf("unable to upload file: %w", os.ErrNotExist)
		}
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			return nil, fmt.Errorf("unable to send job requests: %w", ErrFileHasNoRSTs)
		}
		jobBuilder = true
	}

	if cfg.Priority == nil {
		var priority int32 = 3
		if jobBuilder {
			priority = 4
		}
		cfg.Priority = &priority
	}

	if jobBuilder {
		client := NewJobBuilderClient(ctx, nil, nil)
		request := client.GetJobRequest(cfg)
		return []*beeremote.JobRequest{request}, nil
	}

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve RST mappings: %w", err)
	}
	rstMap, err := GetRstMap(ctx, mountPoint, mappings.RstIdToConfig)
	if err != nil {
		return nil, err
	}

	if IsValidRstId(cfg.RemoteStorageTarget) {
		client, ok := rstMap[cfg.RemoteStorageTarget]
		if !ok {
			return nil, fmt.Errorf("remote storage target does not exist! Check --%s or BeeGFS Remote configuration before retrying", RemoteTargetFlag)
		}
		request := client.GetJobRequest(cfg)
		return []*beeremote.JobRequest{request}, nil
	}

	entryInfo, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{}, pathInfo.Path)
	if err != nil {
		return nil, err
	}
	entry := entryInfo.Entry

	if entry.FileState.GetDataState() == DataStateOffloaded {
		// Attempt to retrieve the stub file content from remote and use the rstId to create the job
		// requests. Otherwise, send the request to job-builder to complete.
		resp, err := remote.GetStubContents(ctx, &beeremote.GetStubContentsRequest{Path: pathInfo.Path})
		if err == nil {
			cfg.SetRemoteStorageTarget(*resp.RstId)
			client, ok := rstMap[cfg.RemoteStorageTarget]
			if !ok {
				return nil, fmt.Errorf("remote storage target ID %d from stub file does not exist in the configuration: %w", cfg.RemoteStorageTarget, ErrFileHasNoRSTs)
			}
			request := client.GetJobRequest(cfg)
			return []*beeremote.JobRequest{request}, nil
		}

		client := NewJobBuilderClient(ctx, nil, nil)
		request := client.GetJobRequest(cfg)
		return []*beeremote.JobRequest{request}, nil
	}

	if len(entry.Remote.RSTIDs) == 0 {
		return nil, fmt.Errorf("unable to build job request(s)! --%s must be specified: %w", RemoteTargetFlag, ErrFileHasNoRSTs)
	}

	var requests []*beeremote.JobRequest
	for _, rstId := range entry.Remote.RSTIDs {
		client, ok := rstMap[rstId]
		if !ok {
			return nil, fmt.Errorf("remote storage target ID %d from file metadata does not exist in the configuration: %w", rstId, ErrFileHasNoRSTs)
		}
		cfg.SetRemoteStorageTarget(rstId)
		requests = append(requests, client.GetJobRequest(cfg))
	}
	return requests, nil
}

type mountPathInfo struct {
	Path   string
	Exists bool
	IsDir  bool
	IsGlob bool
}

func getMountPathInfo(mountPoint filesystem.Provider, path string) (mountPathInfo, error) {
	result := mountPathInfo{Path: path}
	pathInMount, err := mountPoint.GetRelativePathWithinMount(path)
	if err != nil {
		return result, err
	}
	result.Path = pathInMount

	info, err := mountPoint.Lstat(pathInMount)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
		result.IsGlob = filesystem.IsGlobPattern(path)
		return result, nil
	}
	result.Exists = true
	result.IsDir = info.IsDir()
	return result, nil
}
