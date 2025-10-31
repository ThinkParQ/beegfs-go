package rst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	doublestar "github.com/bmatcuk/doublestar/v4"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type AzureBlobClient struct {
	config      *flex.RemoteStorageTarget
	client      *azblob.Client
	endpointUrl string
	container   string
	mountPoint  filesystem.Provider
}

var _ Provider = &AzureBlobClient{}

// Well-known storage account and key for Azurite
const azuriteAccountName = "devstoreaccount1"
const azuriteAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func newAzure(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	azure := rstConfig.GetAzure()
	credentials, err := azblob.NewSharedKeyCredential(azure.GetAccountName(), azure.GetAccountKey())
	if err != nil {
		return nil, fmt.Errorf("failed to create shared credential: %w", err)
	}

	var endpointUrl string
	if azure.GetAzuriteUrl() != "" {
		azuriteName := azure.GetAccountName()
		if azuriteName == "" {
			azuriteName = azuriteAccountName
		}
		azuriteKey := azure.GetAccountKey()
		if azuriteKey == "" {
			azuriteKey = azuriteAccountKey
		}
		azuriteUrl := azure.GetAzuriteUrl()
		endpointUrl = fmt.Sprintf("%s/%s", strings.TrimRight(azuriteUrl, "/"), azuriteName)
		if credentials, err = azblob.NewSharedKeyCredential(azuriteName, azuriteKey); err != nil {
			return nil, fmt.Errorf("failed to create Azurite credential for testing: %w", err)
		}
	} else {
		endpointUrl = fmt.Sprintf("https://%s.blob.core.windows.net", azure.AccountName)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(endpointUrl, credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure client: %w", err)
	}

	return &AzureBlobClient{
		config:      rstConfig,
		client:      client,
		endpointUrl: endpointUrl,
		container:   azure.GetContainer(),
		mountPoint:  mountPoint,
	}, nil
}

func (r *AzureBlobClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	operation := flex.SyncJob_UPLOAD
	if cfg.Download {
		operation = flex.SyncJob_DOWNLOAD
	}
	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: cfg.RemoteStorageTarget,
		StubLocal:           cfg.StubLocal,
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
			Operation:  operation,
			Overwrite:  cfg.Overwrite,
			RemotePath: cfg.RemotePath,
			Flatten:    cfg.Flatten,
			LockedInfo: cfg.LockedInfo,
		}},
	}
}

func (r *AzureBlobClient) getJobRequestCfg(request *beeremote.JobRequest) *flex.JobRequestCfg {
	sync := request.GetSync()
	return &flex.JobRequestCfg{
		RemoteStorageTarget: r.config.Id,
		Path:                request.Path,
		RemotePath:          sync.RemotePath,
		Download:            sync.Operation == flex.SyncJob_DOWNLOAD,
		StubLocal:           request.StubLocal,
		Overwrite:           sync.Overwrite,
		Flatten:             sync.Flatten,
		Force:               request.Force,
		LockedInfo:          sync.LockedInfo,
	}
}

func (r *AzureBlobClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
	// identical logic for sync type and external ID as S3Client
	request := job.GetRequest()
	if !request.HasSync() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	if job.GetExternalId() != "" {
		return nil, ErrJobAlreadyHasExternalID
	}

	sync := request.GetSync()
	if sync.RemotePath == "" {
		if lastJob != nil {
			sync.RemotePath = lastJob.Request.GetSync().RemotePath
		} else {
			sync.RemotePath = r.SanitizeRemotePath(request.Path)
		}
	}

	// Prepare lock and state (reuse util and entry)
	var mappings *util.Mappings
	var writeLockSet bool
	defer func() {
		if writeLockSet {
			if mappings == nil {
				var mappingsErr error
				if mappings, mappingsErr = util.GetMappings(ctx); mappingsErr != nil {
					return
				}
			}

			if clearWriteLockErr := entry.ClearAccessFlags(ctx, mappings, request.Path, LockedAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	writeLockSet, err = r.prepareJobRequest(ctx, mappings, r.getJobRequestCfg(request), sync)
	if err != nil {
		return nil, err
	}
	job.SetExternalId(sync.LockedInfo.ExternalId)

	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		requests, err = r.generateSyncJobWorkRequest_Upload(job)
	case flex.SyncJob_DOWNLOAD:
		requests, err = r.generateSyncJobWorkRequest_Download(job)
	default:
		err = ErrUnsupportedOpForRST
	}
	return
}

// ExecuteJobBuilderRequest is not implemented and should never be called.
func (r *AzureBlobClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	return ErrUnsupportedOpForRST
}

func (r *AzureBlobClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	sync := request.GetSync()

	var err error
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		err = r.upload(ctx, request.Path, sync.RemotePath, request.ExternalId, part, sync.LockedInfo.Mtime.AsTime())
	case flex.SyncJob_DOWNLOAD:
		err = r.download(ctx, request.Path, sync.RemotePath, part)
	}
	if err != nil {
		return err
	}

	part.Completed = true
	return nil
}

func (r *AzureBlobClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	sync := job.GetRequest().GetSync()
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		return r.completeSyncWorkRequests_Upload(ctx, job, workResults, abort)
	case flex.SyncJob_DOWNLOAD:
		return r.completeSyncWorkRequests_Download(ctx, job, workResults, abort)
	}
	return ErrUnsupportedOpForRST
}

func (r *AzureBlobClient) GetConfig() *flex.RemoteStorageTarget {
	return proto.Clone(r.config).(*flex.RemoteStorageTarget)
}

func (r *AzureBlobClient) GetWalk(ctx context.Context, prefix string, chanSize int) (<-chan *WalkResponse, error) {
	prefix = r.SanitizeRemotePath(prefix)
	if _, err := filepath.Match(prefix, ""); err != nil {
		return nil, fmt.Errorf("invalid prefix %s: %w", prefix, err)
	}
	prefixWithoutPattern := StripGlobPattern(prefix)
	isKey := prefix == prefixWithoutPattern

	walkChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(walkChan)

		prefixWalk := func() bool {
			keysFound := false
			pager := r.client.NewListBlobsFlatPager(r.container, &container.ListBlobsFlatOptions{Prefix: &prefix})
			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						walkChan <- &WalkResponse{Err: fmt.Errorf("list blobs was cancelled: %w", err)}
					} else {
						walkChan <- &WalkResponse{Err: fmt.Errorf("list blobs failed: %w", err)}
					}
					return keysFound
				}

				for _, item := range page.Segment.BlobItems {
					if !isKey {
						if match, _ := doublestar.Match(prefix, *item.Name); !match {
							continue
						}
					}
					keysFound = true
					walkChan <- &WalkResponse{Path: *item.Name}
				}
			}
			return keysFound
		}

		if isKey {
			client := r.client.ServiceClient().NewContainerClient(r.container).NewBlobClient(prefix)
			_, err := client.GetProperties(ctx, nil)
			if err != nil {
				var respErr *azcore.ResponseError
				if errors.As(err, &respErr) && respErr.StatusCode == 404 {
					// Try walking as a prefix since there was no key. If not a valid prefix
					// fallback to the original error.
					if !prefixWalk() {
						walkChan <- &WalkResponse{Err: fmt.Errorf("key not found: %s", prefix)}
					}
				}
				walkChan <- &WalkResponse{Err: fmt.Errorf("query failed: %w", err)}
				return
			}

			walkChan <- &WalkResponse{Path: prefix}
			return
		} else {
			prefixWalk()
		}
	}()

	return walkChan, nil
}

func (r *AzureBlobClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, error) {
	return r.getBlobMetadata(ctx, cfg.RemotePath, cfg.Download)
}

func (r *AzureBlobClient) getBlobMetadata(ctx context.Context, key string, keyMustExist bool) (int64, time.Time, error) {
	if key == "" {
		if keyMustExist {
			return 0, time.Time{}, fmt.Errorf("unable to retrieve blob metadata! --remote-path must be specified")
		}
		return 0, time.Time{}, nil
	}

	client := r.client.ServiceClient().NewContainerClient(r.container).NewBlobClient(key)
	resp, err := client.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if !keyMustExist && errors.As(err, &respErr) && respErr.StatusCode == 404 {
			return 0, time.Time{}, nil
		}
		return 0, time.Time{}, err
	}

	beegfsMtime, ok := resp.Metadata["Beegfs_mtime"]
	if !ok {
		return *resp.ContentLength, *resp.LastModified, nil
	}

	mtimeEpoch, err := strconv.ParseInt(*beegfsMtime, 10, 64)
	mtime := time.Unix(mtimeEpoch, 0)
	if err != nil {
		return *resp.ContentLength, *resp.LastModified, fmt.Errorf("unable to parse remote blob's Beegfs_mtime")
	}
	return *resp.ContentLength, mtime, nil
}

func (r *AzureBlobClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	sync := cfg.GetLockedInfo()
	if !cfg.Download {
		segCount, _ := r.recommendedSegments(sync.Size)
		if segCount > 1 {

			// TODO: support multi-part upload

			return "", fmt.Errorf("multipart upload not supported: %w", ErrUnsupportedOpForRST)
		}
	}
	return "", nil
}

func (r *AzureBlobClient) generateSyncJobWorkRequest_Upload(job *beeremote.Job) ([]*flex.WorkRequest, error) {
	request := job.GetRequest()
	sync := request.GetSync()
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.Mtime)

	filemode := fs.FileMode(lockedInfo.Mode)
	if filemode.Type()&fs.ModeSymlink != 0 {
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/25
		// Support symbolic links.
		return nil, fmt.Errorf("unable to upload symlink: %w", ErrFileTypeUnsupported)
	}
	if !filemode.IsRegular() {
		return nil, fmt.Errorf("%w", ErrFileTypeUnsupported)
	}

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.Size)
	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.Size, segCount, partsPerSegment))
	return workRequests, nil
}

func (r *AzureBlobClient) generateSyncJobWorkRequest_Download(job *beeremote.Job) ([]*flex.WorkRequest, error) {
	request := job.GetRequest()
	sync := request.GetSync()
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.RemoteMtime)

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.RemoteSize)
	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.RemoteSize, segCount, partsPerSegment))
	return workRequests, nil
}

// prepareJobRequest ensures that sync.LockedInfo is full populated.
func (r *AzureBlobClient) prepareJobRequest(ctx context.Context, mappings *util.Mappings, cfg *flex.JobRequestCfg, sync *flex.SyncJob) (writeLockSet bool, err error) {
	lockedInfo := sync.LockedInfo
	if IsFileLocked(lockedInfo) && HasRemotePathInfo(lockedInfo) {
		return
	}

	if mappings == nil {
		if mappings, err = util.GetMappings(ctx); err != nil {
			err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, err.Error())
			return
		}
	}

	if !IsFileLocked(lockedInfo) {
		if lockedInfo, writeLockSet, _, err = GetLockedInfo(ctx, r.mountPoint, mappings, cfg, cfg.Path); err != nil {
			err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, fmt.Sprintf("failed to acquire lock: %s", err.Error()))
			return
		}
		cfg.SetLockedInfo(lockedInfo)
		sync.SetLockedInfo(lockedInfo)
	}

	if !HasRemotePathInfo(lockedInfo) {
		request := BuildJobRequest(ctx, r, r.mountPoint, mappings, cfg)
		status := request.GetGenerationStatus()
		if status != nil {
			err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, status.Message)
			return
		}

		if err = PrepareFileStateForWorkRequests(ctx, r, r.mountPoint, mappings, cfg); err != nil {
			if !errors.Is(err, ErrJobAlreadyComplete) && !errors.Is(err, ErrJobAlreadyOffloaded) {
				err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, fmt.Sprintf("failed to prepare file state: %s", err.Error()))
			}
			return
		}
		sync.SetRemotePath(cfg.RemotePath)
		sync.SetFlatten(cfg.Flatten)
		sync.SetOverwrite(cfg.Overwrite)
	}
	return
}

func (r *AzureBlobClient) SanitizeRemotePath(remotePath string) string {
	return strings.TrimLeft(remotePath, "/")
}

func (r *AzureBlobClient) recommendedSegments(fileSize int64) (int64, int32) {
	if fileSize <= r.config.Policies.FastStartMaxSize || r.config.Policies.FastStartMaxSize == 0 {
		return 1, 1
	} else if fileSize/4 < 5*1024*1024 {
		return 1, 1
	}
	return 4, 1
}

func (r *AzureBlobClient) upload(ctx context.Context, path, remotePath, uploadID string, part *flex.Work_Part, mtime time.Time) error {
	reader, sha256sum, err := r.mountPoint.ReadFilePart(path, part.OffsetStart, part.OffsetStop)
	if err != nil {
		return err
	}
	part.ChecksumSha256 = sha256sum

	var resp azblob.UploadStreamResponse
	if uploadID == "" {
		beegfsMtime := strconv.FormatInt(mtime.Unix(), 10)
		resp, err = r.client.UploadStream(ctx, r.container, remotePath, reader, &azblob.UploadStreamOptions{
			Metadata: map[string]*string{"Beegfs_mtime": &beegfsMtime},
		})
		if err != nil {
			return err
		}
	} else {

		//  TODO: multipart upload - see https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage

		return fmt.Errorf("multipart upload not supported: %w", ErrUnsupportedOpForRST)
	}

	part.EntityTag = string(*resp.ETag)
	return nil
}

func (r *AzureBlobClient) download(ctx context.Context, path, remotePath string, part *flex.Work_Part) error {
	file, err := r.mountPoint.WriteFilePart(path, part.OffsetStart, part.OffsetStop)
	if err != nil {
		return err
	}
	defer file.Close()

	// rangeHeader := fmt.Sprintf("bytes=%d-%d", part.OffsetStart, part.OffsetStop)
	resp, err := r.client.DownloadStream(ctx, r.container, remotePath, nil) //&azblob.DownloadStreamOptions{Range: &rangeHeader})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(file, resp.Body)
	return err
}

func (r *AzureBlobClient) completeSyncWorkRequests_Upload(ctx context.Context, job *beeremote.Job, work []*flex.Work, abort bool) error {
	request := job.GetRequest()
	sync := request.GetSync()

	stat, err := r.mountPoint.Lstat(request.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("unable to complete job requests: %w", err)
		} else if !abort {
			// Ignore errors when aborting.
			return fmt.Errorf("unable to set local file mtime as part of completing job requests: %w", err)
		}
	}
	mtime := stat.ModTime()
	job.SetStopMtime(timestamppb.New(mtime))

	if job.ExternalId != "" {
		// TODO:
		// if abort {
		// 	// When aborting there is no reason to check the mtime below and it may not have
		// 	// been set correctly anyway given the error check is skipped above.
		// 	return r.abortUpload(ctx, job.ExternalId, request.Path)
		// } else {
		// 	// TODO: https://github.com/thinkparq/gobee/issues/29
		// 	// There could be lots of parts. Look for ways to optimize this. Like if we could
		// 	// determine the total number of parts and make an appropriately sized slice up front.
		// 	// Or if we could just pass the RST method the unmodified map since it potentially has
		// 	// to iterate over the slice to convert it to the type it expects anyway. The drawback
		// 	// with the latter approach is the RST package would import a BeeRemote package and the
		// 	// goal has been to break this out into a standalone package to reuse for BeeSync.
		// 	partsToFinish := make([]*flex.Work_Part, 0)
		// 	for _, r := range workResults {
		// 		partsToFinish = append(partsToFinish, r.Parts...)
		// 	}
		// 	// If there was an error finishing the upload we should return that and not worry
		// 	// about checking if the file was modified.
		// 	if err := r.finishUpload(ctx, job.ExternalId, request.Path, partsToFinish); err != nil {
		// 		return err
		// 	}
		// }
	}
	// Skip checking the file was modified if we were told to abort since the mtime may not have
	// been set correctly anyway given the error check is skipped above.
	if !abort {
		start := job.GetStartMtime().AsTime()
		stop := job.GetStopMtime().AsTime()
		if !start.Equal(stop) {
			return fmt.Errorf("successfully completed all work requests but the file appears to have been modified (mtime at job start: %s / mtime at job completion: %s)",
				start.Format(time.RFC3339), stop.Format(time.RFC3339))
		}
	}

	if request.StubLocal {
		mappings, err := util.GetMappings(ctx)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}

		err = CreateOffloadedDataFile(ctx, r.mountPoint, mappings, request.Path, sync.RemotePath, request.RemoteStorageTarget, true)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		job.GetStatus().SetState(beeremote.Job_OFFLOADED)
	}
	return nil
}

func (r *AzureBlobClient) completeSyncWorkRequests_Download(ctx context.Context, job *beeremote.Job, work []*flex.Work, abort bool) error {
	request := job.GetRequest()
	sync := request.GetSync()

	_, mtime, err := r.getBlobMetadata(ctx, sync.RemotePath, false)
	if err != nil {
		return fmt.Errorf("unable to verify the remote object has not changed: %w", err)
	}
	job.SetStopMtime(timestamppb.New(mtime))

	// Skip checking the file was modified if we were told to abort since the mtime may not have
	// been set correctly anyway given the error check is skipped above.
	if !abort {
		start := job.GetStartMtime().AsTime()
		stop := job.GetStopMtime().AsTime()
		if !start.Equal(stop) {
			return fmt.Errorf("successfully completed all work requests but the remote file or object appears to have been modified (mtime at job start: %s / mtime at job completion: %s)",
				start.Format(time.RFC3339), stop.Format(time.RFC3339))
		}

		// Update the downloaded file's access and modification times so they accurately reflect the beegfs_mtime.
		absPath := filepath.Join(r.mountPoint.GetMountPath(), request.Path)
		if err := os.Chtimes(absPath, mtime, mtime); err != nil {
			return fmt.Errorf("failed to update download's mtime: %w", err)
		}

		// Clear offloaded data state when contents for a stub file were downloaded successfully.
		if !request.StubLocal && IsFileOffloaded(sync.LockedInfo) {
			mappings, err := util.GetMappings(ctx)
			if err != nil {
				return err
			}
			entry.SetDataState(ctx, mappings, request.Path, DataStateNone)
		}

	}
	return nil
}
