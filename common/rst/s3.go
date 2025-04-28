package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	doublestar "github.com/bmatcuk/doublestar/v4"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type S3Client struct {
	config *flex.RemoteStorageTarget
	// TODO: https://github.com/thinkparq/gobee/issues/28
	// Rework client into an `s3Provider` interface type.
	client     *s3.Client
	mountPoint filesystem.Provider
}

var _ Provider = &S3Client{}

func newS3(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {

	resolver := aws.EndpointResolverWithOptionsFunc(func(string, string, ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       rstConfig.GetS3().GetPartitionId(),
			URL:               rstConfig.GetS3().GetEndpointUrl(),
			SigningRegion:     rstConfig.GetS3().GetRegion(),
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := s3Config.LoadDefaultConfig(
		ctx,
		s3Config.WithRegion(rstConfig.GetS3().GetRegion()),
		s3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rstConfig.GetS3().GetAccessKey(), rstConfig.GetS3().GetSecretKey(), "")),
		s3Config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load config for RST client: %w", err)
	}

	r := &S3Client{
		config:     rstConfig,
		client:     s3.NewFromConfig(cfg),
		mountPoint: mountPoint,
	}

	return r, nil
}

func (r *S3Client) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	operation := flex.SyncJob_UPLOAD
	if cfg.Download {
		operation = flex.SyncJob_DOWNLOAD
	}

	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: cfg.RemoteStorageTarget,
		StubLocal:           cfg.StubLocal,
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation:  operation,
				Overwrite:  cfg.Overwrite,
				RemotePath: cfg.RemotePath,
				Flatten:    cfg.Flatten,
				LockedInfo: cfg.LockedInfo,
			},
		},
	}
}

func (r *S3Client) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, canRetry bool, err error) {
	request := job.GetRequest()
	if !request.HasSync() {
		return nil, false, ErrReqAndRSTTypeMismatch
	}
	if job.GetExternalId() != "" {
		return nil, false, ErrJobAlreadyHasExternalID
	}

	sync := request.GetSync()
	if sync.RemotePath == "" {
		if lastJob != nil {
			sync.SetRemotePath(lastJob.Request.GetSync().RemotePath)
		} else {
			sync.SetRemotePath(r.SanitizeRemotePath(request.Path))
		}
	}

	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		return r.generateSyncJobWorkRequest_Upload(ctx, job, availableWorkers)
	case flex.SyncJob_DOWNLOAD:
		return r.generateSyncJobWorkRequest_Download(ctx, job, availableWorkers)
	}
	return nil, false, ErrUnsupportedOpForRST
}

// ExecuteJobBuilderRequest is not implemented and should never be called.
func (r *S3Client) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	return ErrUnsupportedOpForRST
}

func (r *S3Client) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
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

func (r *S3Client) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}

	sync := request.GetSync()
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		return r.completeSyncWorkRequests_Upload(ctx, job, workResults, abort)
	case flex.SyncJob_DOWNLOAD:
		return r.completeSyncWorkRequests_Download(ctx, job, workResults, abort)
	default:
		return ErrUnsupportedOpForRST
	}
}

func (r *S3Client) GetConfig() *flex.RemoteStorageTarget {
	return proto.Clone(r.config).(*flex.RemoteStorageTarget)
}

// walkPrefix lists objects in an S3 bucket that match the specified prefix, returning results via
// the *WalkPrefixResponse channel.Results can also be filtered using file globbing in the pattern
// which includes double start for directory recursion.
func (r *S3Client) GetWalk(ctx context.Context, prefix string, chanSize int) (<-chan *WalkResponse, error) {
	prefix = r.SanitizeRemotePath(prefix)
	if _, err := filepath.Match(prefix, ""); err != nil {
		return nil, fmt.Errorf("invalid prefix %s: %w", prefix, err)
	}

	prefixWithoutPattern := StripGlobPattern(prefix)
	usePattern := prefix != prefixWithoutPattern

	walkChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(walkChan)
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			var output *s3.ListObjectsV2Output
			input := &s3.ListObjectsV2Input{
				Bucket: aws.String(r.config.GetS3().Bucket),
				Prefix: aws.String(prefixWithoutPattern),
			}

			objectPaginator := s3.NewListObjectsV2Paginator(r.client, input)
			for objectPaginator.HasMorePages() {
				output, err = objectPaginator.NextPage(ctx)
				if err != nil {
					var noBucket *types.NoSuchBucket
					if errors.As(err, &noBucket) {
						err := fmt.Errorf("s3 bucket %s does not exist: %s", r.config.GetS3().Bucket, noBucket.Error())
						walkChan <- &WalkResponse{Err: err, FatalErr: true}
					}
					return
				}

				for _, content := range output.Contents {
					if usePattern {
						if match, _ := doublestar.Match(prefix, *content.Key); !match {
							continue
						}
					}
					walkChan <- &WalkResponse{Path: *content.Key}
				}
			}
		}
	}()

	return walkChan, nil
}

func (r *S3Client) GetRemoteInfo(ctx context.Context, remotePath string, cfg *flex.JobRequestCfg, lockedInfo *flex.JobLockedInfo) (remoteSize int64, remoteMtime time.Time, externalId string, err error) {
	if remoteSize, remoteMtime, err = r.getObjectMetadata(ctx, remotePath, cfg.Download); err != nil {
		return
	}

	if !cfg.Download {
		segCount, _ := r.recommendedSegments(lockedInfo.Size)
		if segCount > 1 {
			externalId, err = r.createUpload(ctx, remotePath, lockedInfo.Mtime.AsTime())
		}
	}
	return
}

func (r *S3Client) SanitizeRemotePath(remotePath string) string {
	// Valid s3 prefixes do not start with a '/' (e.g. myfolder/*/subdir?[a-z])
	return strings.TrimLeft(remotePath, "/")
}

func (r *S3Client) generateSyncJobWorkRequest_Upload(ctx context.Context, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	sync := request.GetSync()

	err := r.setLockedInfo(ctx, request, sync)
	if err != nil {
		return nil, true, err
	}
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.Mtime)
	job.SetExternalId(lockedInfo.ExternalId)

	filemode := fs.FileMode(lockedInfo.Mode)
	if filemode.Type()&fs.ModeSymlink != 0 {
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/25
		// Support symbolic links.
		return nil, true, fmt.Errorf("unable to upload symlink: %w", rst.ErrFileTypeUnsupported)
	}
	if !filemode.IsRegular() {
		return nil, true, fmt.Errorf("%w", rst.ErrFileTypeUnsupported)
	}

	if IsFileAlreadySynced(lockedInfo) {
		if request.StubLocal {
			store, err := config.NodeStore(ctx)
			if err != nil {
				return nil, true, err
			}
			mappings, err := util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, true, err
			}
			if err := CreateOffloadedDataFile(ctx, r.mountPoint, store, mappings, request.Path, sync.RemotePath, request.RemoteStorageTarget, true); err != nil {
				return nil, true, fmt.Errorf("file is already synced but failed to create stub file: %w", err)
			}
			return nil, true, ErrJobAlreadyOffloaded
		}
		return nil, true, ErrJobAlreadyComplete
	}

	if IsFileOffloaded(lockedInfo) {
		if request.StubLocal {
			if IsFileOffloadedUrlCorrect(request.RemoteStorageTarget, sync.RemotePath, lockedInfo) {
				return nil, true, ErrJobAlreadyOffloaded
			}
			return nil, true, fmt.Errorf("failed to complete migration! File is already a stub")
		}
		return nil, true, fmt.Errorf("unable to upload stub file: %w", rst.ErrFileTypeUnsupported)
	}

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.Size)
	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.Size, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) generateSyncJobWorkRequest_Download(ctx context.Context, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	sync := request.GetSync()

	err := r.setLockedInfo(ctx, request, sync)
	if err != nil {
		return nil, true, err
	}
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.RemoteMtime)
	job.SetExternalId(lockedInfo.ExternalId)

	overwrite := sync.Overwrite
	if request.StubLocal {
		if IsFileExist(lockedInfo) {
			if IsFileOffloaded(lockedInfo) {
				if IsFileOffloadedUrlCorrect(request.RemoteStorageTarget, sync.RemotePath, lockedInfo) {
					return nil, true, ErrJobAlreadyOffloaded
				} else if !overwrite {
					return nil, true, fmt.Errorf("unable to create stub file: %w", ErrOffloadFileUrlMismatch)
				}
			} else if !overwrite {
				return nil, true, fmt.Errorf("unable to create stub file: %w", fs.ErrExist)
			}
		}

		store, err := config.NodeStore(ctx)
		if err != nil {
			return nil, true, err
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return nil, true, err
		}
		err = CreateOffloadedDataFile(ctx, r.mountPoint, store, mappings, request.Path, sync.RemotePath, request.RemoteStorageTarget, overwrite)
		if err != nil {
			return nil, true, err
		}
		return nil, true, ErrJobAlreadyOffloaded
	}

	if IsFileExist(lockedInfo) {
		if IsFileOffloaded(lockedInfo) {
			store, err := config.NodeStore(ctx)
			if err != nil {
				return nil, true, err
			}
			mappings, err := util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, true, err
			}
			if err := entry.ClearFileDataState(ctx, mappings, store, request.Path); err != nil {
				return nil, true, fmt.Errorf("unable to clear stub file flag: %w", err)
			}

			overwrite = true
		} else if IsFileAlreadySynced(lockedInfo) {
			return nil, true, ErrJobAlreadyComplete
		}
	}

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.RemoteSize)
	if !IsFileSizeMatched(lockedInfo) {
		err = r.mountPoint.CreatePreallocatedFile(request.Path, lockedInfo.RemoteSize, overwrite)
		if err != nil {
			return nil, false, err
		}
	}

	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.RemoteSize, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) completeSyncWorkRequests_Upload(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
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
		if abort {
			// When aborting there is no reason to check the mtime below and it may not have
			// been set correctly anyway given the error check is skipped above.
			return r.abortUpload(ctx, job.ExternalId, request.Path)
		} else {
			// TODO: https://github.com/thinkparq/gobee/issues/29
			// There could be lots of parts. Look for ways to optimize this. Like if we could
			// determine the total number of parts and make an appropriately sized slice up front.
			// Or if we could just pass the RST method the unmodified map since it potentially has
			// to iterate over the slice to convert it to the type it expects anyway. The drawback
			// with the latter approach is the RST package would import a BeeRemote package and the
			// goal has been to break this out into a standalone package to reuse for BeeSync.
			partsToFinish := make([]*flex.Work_Part, 0)
			for _, r := range workResults {
				partsToFinish = append(partsToFinish, r.Parts...)
			}
			// If there was an error finishing the upload we should return that and not worry
			// about checking if the file was modified.
			if err := r.finishUpload(ctx, job.ExternalId, request.Path, partsToFinish); err != nil {
				return err
			}
		}
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
		store, err := config.NodeStore(ctx)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}

		err = CreateOffloadedDataFile(ctx, r.mountPoint, store, mappings, request.Path, sync.RemotePath, request.RemoteStorageTarget, true)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		job.GetStatus().SetState(beeremote.Job_OFFLOADED)
	}
	return nil
}

func (r *S3Client) completeSyncWorkRequests_Download(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	sync := request.GetSync()

	mtime := sync.LockedInfo.RemoteMtime.AsTime()
	job.SetStopMtime(timestamppb.New(mtime))

	absPath := filepath.Join(r.mountPoint.GetMountPath(), request.Path)
	if err := os.Chtimes(absPath, mtime, mtime); err != nil {
		return fmt.Errorf("failed to update download's mtime: %w", err)
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
	return nil
}

func (r *S3Client) setLockedInfo(ctx context.Context, request *beeremote.JobRequest, sync *flex.SyncJob) error {
	if sync.LockedInfo != nil && sync.LockedInfo.Locked {
		return nil
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		return err
	}

	cfg := &flex.JobRequestCfg{
		RemoteStorageTarget: r.config.Id,
		Path:                request.Path,
		RemotePath:          sync.RemotePath,
		Download:            sync.Operation == flex.SyncJob_DOWNLOAD,
		StubLocal:           request.StubLocal,
		Overwrite:           sync.Overwrite,
		Flatten:             sync.Flatten,
		Force:               request.Force,
	}

	lockedInfo, _, err := GetLockedInfo(ctx, r.mountPoint, store, mappings, cfg, request.Path)
	if err != nil {
		return err
	}
	remoteSize, remoteMtime, externalId, err := r.GetRemoteInfo(ctx, sync.RemotePath, cfg, lockedInfo)
	if err != nil {
		return err
	}
	lockedInfo.SetRemoteSize(remoteSize)
	lockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))
	lockedInfo.SetExternalId(externalId)

	rstId := r.GetConfig().Id
	if err := PrepareJob(ctx, r.mountPoint, store, mappings, request.Path, rstId, sync.RemotePath, cfg, lockedInfo); err != nil {
		return err
	}
	sync.SetLockedInfo(lockedInfo)
	return nil
}

// getObjectMetadata returns the object's size in bytes, modification time if it exists.
func (r *S3Client) getObjectMetadata(ctx context.Context, key string, keyMustExist bool) (int64, time.Time, error) {

	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(key),
	}

	resp, err := r.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var apiErr smithy.APIError
		if !keyMustExist && errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return 0, time.Time{}, nil
			}
		}
		return 0, time.Time{}, err
	}

	beegfsMtime, ok := resp.Metadata["beegfs-mtime"]
	if !ok {
		return *resp.ContentLength, *resp.LastModified, nil
	}

	mtime, err := time.Parse(time.RFC3339, beegfsMtime)
	if err != nil {
		return *resp.ContentLength, *resp.LastModified, fmt.Errorf("unable to parse s3 object's beegfs-mtime metadata")
	}

	return *resp.ContentLength, mtime, nil
}

func (r *S3Client) createUpload(ctx context.Context, path string, mtime time.Time) (uploadID string, err error) {

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
		Metadata: map[string]string{"beegfs-mtime": mtime.Format(time.RFC3339)},
	}

	result, err := r.client.CreateMultipartUpload(ctx, createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (r *S3Client) abortUpload(ctx context.Context, uploadID string, path string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
	}

	_, err := r.client.AbortMultipartUpload(ctx, abortMultipartUploadInput)
	return err
}

// finishUpload will automatically sort parts by number if they are not already in order.
func (r *S3Client) finishUpload(ctx context.Context, uploadID string, path string, parts []*flex.Work_Part) error {

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber:     aws.Int32(part.PartNumber),
			ETag:           aws.String(part.EntityTag),
			ChecksumSHA256: aws.String(part.ChecksumSha256),
		}
	}

	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := r.client.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
	return err
}

// upload attempts to upload the specified part of the provided file path. If an upload ID is
// provided it will treat the provided part as one part in a larger multi-part upload. If the upload
// ID is empty then it will not perform a multi-part upload and just do PutObject, but this also
// requires the provided part number to be "1". When not performing a multi-part upload it still
// honors the provided offset start/stop range and does not check to verify this range covers the
// entirety of the specified file. If the upload is successful the part will be updated directly
// with the results (such as the etag), otherwise an error will be returned.
func (r *S3Client) upload(ctx context.Context, path string, remotePath string, uploadID string, part *flex.Work_Part, mtime time.Time) error {

	filePart, sha256sum, err := r.mountPoint.ReadFilePart(path, part.OffsetStart, part.OffsetStop)

	if err != nil {
		// S3 allows uploading empty (zero byte) files. Only allow this if the offset start/stop are
		// actually zero.
		if errors.Is(err, io.EOF) && part.OffsetStart == 0 && part.OffsetStop == 0 {
			filePart = bytes.NewReader([]byte{})
		} else {
			return err
		}
	}
	part.ChecksumSha256 = sha256sum

	if uploadID == "" {
		// This should catch most issues where the user intended to perform a multi-part upload, but
		// did not generate an upload ID first or if multiple parts were generated inadvertently.
		if part.PartNumber != 1 {
			return fmt.Errorf("only multi-part uploads can have a part number other than 1 (did you intend to create a multi-part upload first?)")
		}

		resp, err := r.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:         aws.String(r.config.GetS3().Bucket),
			Key:            aws.String(remotePath),
			Body:           filePart,
			ChecksumSHA256: aws.String(part.ChecksumSha256),
			// Could a local mtime match and allow for a continue/resume feature...
			//	- If there was a previous failure and the mtime still match then continue from the last byte write
			Metadata: map[string]string{"beegfs-mtime": mtime.Format(time.RFC3339)},
		})

		if err != nil {
			return err
		}
		part.EntityTag = *resp.ETag
		return nil
	}

	uploadPartReq := &s3.UploadPartInput{
		Bucket:         aws.String(r.config.GetS3().Bucket),
		Key:            aws.String(remotePath),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(part.PartNumber),
		Body:           filePart,
		ChecksumSHA256: aws.String(part.ChecksumSha256),
	}

	resp, err := r.client.UploadPart(ctx, uploadPartReq)
	if err != nil {
		return err
	}
	part.EntityTag = *resp.ETag
	return nil
}

func (r *S3Client) download(ctx context.Context, path string, remotePath string, part *flex.Work_Part) error {
	if part.OffsetStart == part.OffsetStop {
		// There are no bytes to write to the file. Most likely this is an empty file.
		return nil
	}

	filePart, err := r.mountPoint.WriteFilePart(path, part.OffsetStart, part.OffsetStop)
	if err != nil {
		return err
	}
	defer filePart.Close()

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(remotePath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", part.OffsetStart, part.OffsetStop)),
	}

	resp, err := r.client.GetObject(ctx, getObjectInput)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	copiedBytes, err := io.Copy(filePart, resp.Body)
	if err != nil {
		return err
	}
	if copiedBytes != part.OffsetStop-part.OffsetStart+1 {
		return fmt.Errorf("%w (expected: %d, actual: %d)", ErrPartialPartDownload, part.OffsetStop-part.OffsetStart+1, copiedBytes)
	}
	return nil
}

func (r *S3Client) recommendedSegments(fileSize int64) (int64, int32) {

	if fileSize <= r.config.Policies.FastStartMaxSize || r.config.Policies.FastStartMaxSize == 0 {
		return 1, 1
	} else if fileSize/4 < 5242880 {
		// Each part must be at least 5MB except for the last part which can be any size.
		// Regardless of the FastStartMaxSize ensure we don't try to use a multipart upload when it is not valid.
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
		return 1, 1
	}
	// TODO: https://github.com/thinkparq/gobee/issues/7
	// Arbitrary selection for now. We should be smarter and take into
	// consideration file size and number of workers for this RST type.
	return 4, 1
}
