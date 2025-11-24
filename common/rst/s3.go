package rst

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	doublestar "github.com/bmatcuk/doublestar/v4"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type S3StorageClass struct {
	retrievalTier types.Tier
	archival      bool
	retentionDays int32         // defines how long the retrieved object will be available in days.
	checkTime     time.Duration // defines retry time after initiating the restore.
	recheckTime   time.Duration // defines retry time when restore was previously initiated.
	autoRestore   bool          // defines whether archived objects should be permitted to be restored.
}

type S3Client struct {
	config *flex.RemoteStorageTarget
	// TODO: https://github.com/thinkparq/gobee/issues/28
	// Rework client into an `s3Provider` interface type.
	client                         *s3.Client
	mountPoint                     filesystem.Provider
	storageClasses                 map[types.StorageClass]S3StorageClass
	isListStartAfterKeySupported   *bool
	isListStartAfterKeySupportedMu sync.Mutex
}

var _ Provider = &S3Client{}

func newS3(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	s3Provider := rstConfig.GetS3()
	awsCfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithBaseEndpoint(s3Provider.GetEndpointUrl()),
		awsConfig.WithRegion(s3Provider.GetRegion()),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				s3Provider.GetAccessKey(),
				s3Provider.GetSecretKey(),
				"", // session token
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load config for RST client: %w", err)
	}

	// AWS recommends virtual-hosted style (bucketName.s3.amazonaws.com); path-style (/bucketName/)
	// was deprecated for new regions in 2020. So, check whether the provided endpoint url starts
	// with the bucket as part of the hostname. Otherwise, use the path-style.
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
	endpointUrl, err := url.Parse(s3Provider.GetEndpointUrl())
	if err != nil {
		return nil, fmt.Errorf("unable to parse s3 end-point: %w", err)
	}
	bucket := s3Provider.GetBucket()
	host := endpointUrl.Hostname()
	usePathStyle := !strings.HasPrefix(host, bucket+".")
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	})

	s3Client := &S3Client{
		config:                         rstConfig,
		client:                         client,
		mountPoint:                     mountPoint,
		storageClasses:                 make(map[types.StorageClass]S3StorageClass),
		isListStartAfterKeySupportedMu: sync.Mutex{},
	}

	for _, class := range s3Provider.StorageClass {
		name := types.StorageClass(class.GetName())
		if name == "" {
			return nil, fmt.Errorf("storage class must specify a valid storage class name")
		}

		archive := class.GetArchival()
		if archive == nil {
			return nil, fmt.Errorf("storage class, %s, is not archival. Currently all storage class definitions must be archival", name)
		}

		retrievalTier := types.Tier(archive.GetRetrievalTier())
		retentionsDays := archive.GetRetentionDays()
		if retentionsDays < 1 {
			return nil, fmt.Errorf("storage class, %s, has invalid retention days: %d", name, retentionsDays)
		}
		checkTime, err := time.ParseDuration(strings.ToLower(archive.GetCheckTime()))
		if err != nil {
			return nil, fmt.Errorf("storage class, %s, has invalid checkTime: %w", name, err)
		} else if checkTime < time.Duration(time.Second) {
			return nil, fmt.Errorf("storage class, %s, must specify checkTime >= '1s'", name)
		}
		recheckTime, err := time.ParseDuration(strings.ToLower(archive.GetRecheckTime()))
		if err != nil {
			return nil, fmt.Errorf("storage class, %s, has invalid recheckTime: %w", name, err)
		} else if recheckTime < time.Duration(time.Second) {
			return nil, fmt.Errorf("storage class, %s, must specify recheckTime >= '1s'", name)
		}

		s3Client.storageClasses[name] = S3StorageClass{
			retrievalTier: retrievalTier,
			archival:      true,
			retentionDays: retentionsDays,
			checkTime:     checkTime,
			recheckTime:   recheckTime,
			autoRestore:   archive.GetAutoRestore(),
		}
	}

	return s3Client, nil
}

func (s *S3Client) checkStartAfterSupport(ctx context.Context) error {
	s.isListStartAfterKeySupportedMu.Lock()
	defer s.isListStartAfterKeySupportedMu.Unlock()
	if s.isListStartAfterKeySupported != nil {
		return nil
	}

	input := &s3.ListObjectsV2Input{
		Bucket:     aws.String(s.config.GetS3().Bucket),
		StartAfter: aws.String(""),
		MaxKeys:    aws.Int32(0),
	}

	if _, err := s.client.ListObjectsV2(ctx, input); err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidArgument" && strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "startafter") {
			s.isListStartAfterKeySupported = new(bool)
			*s.isListStartAfterKeySupported = false
			return nil
		}
		return fmt.Errorf("unable to determine bucket's StartAfter option support: %w", err)
	}
	s.isListStartAfterKeySupported = new(bool)
	*s.isListStartAfterKeySupported = true
	return nil
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
		Priority:            cfg.GetPriority(),
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation:    operation,
				Overwrite:    cfg.Overwrite,
				RemotePath:   cfg.RemotePath,
				Flatten:      cfg.Flatten,
				LockedInfo:   cfg.LockedInfo,
				Metadata:     cfg.Metadata,
				Tagging:      cfg.Tagging,
				StorageClass: cfg.StorageClass,
				AllowRestore: cfg.AllowRestore,
			},
		},
		Update: cfg.Update,
	}
}

func (r *S3Client) getJobRequestCfg(request *beeremote.JobRequest) *flex.JobRequestCfg {
	sync := request.GetSync()
	return &flex.JobRequestCfg{
		RemoteStorageTarget: r.config.Id,
		Path:                request.Path,
		RemotePath:          sync.RemotePath,
		Download:            sync.Operation == flex.SyncJob_DOWNLOAD,
		StubLocal:           request.StubLocal,
		Overwrite:           sync.Overwrite,
		Flatten:             sync.Flatten,
		Priority:            &request.Priority,
		Force:               request.Force,
		LockedInfo:          sync.LockedInfo,
		Update:              request.Update,
		Metadata:            sync.Metadata,
		Tagging:             sync.Tagging,
		StorageClass:        sync.StorageClass,
		AllowRestore:        sync.AllowRestore,
	}
}

func (r *S3Client) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
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
			sync.SetRemotePath(lastJob.Request.GetSync().RemotePath)
		} else {
			sync.SetRemotePath(r.SanitizeRemotePath(request.Path))
		}
	}

	var writeLockSet bool
	defer func() {
		if err == nil || errors.Is(err, ErrJobAlreadyOffloaded) {
			return
		}

		if writeLockSet {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, request.Path, LockedAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	if writeLockSet, err = r.prepareJobRequest(ctx, r.getJobRequestCfg(request), sync); err != nil {
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
func (r *S3Client) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (string, error) {
	return "", ErrUnsupportedOpForRST
}

func (r *S3Client) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	if !request.HasSync() {
		return false, 0, ErrReqAndRSTTypeMismatch
	}

	sync := request.GetSync()
	lockedInfo := sync.GetLockedInfo()
	if sync.Operation == flex.SyncJob_DOWNLOAD && lockedInfo.IsArchived {

		_, _, archiveStatus, err := r.getObjectMetadata(ctx, sync.RemotePath, true)
		if err != nil {
			return false, 0, err
		}

		if archiveStatus != nil && archiveStatus.IsArchived && ((sync.AllowRestore != nil && sync.GetAllowRestore()) || (sync.AllowRestore == nil && archiveStatus.Info.autoRestore)) {
			if !archiveStatus.RestoreInProgress {
				restoreRequest := &types.RestoreRequest{
					Days: aws.Int32(archiveStatus.Info.retentionDays),
				}
				if archiveStatus.Info.retrievalTier != "" {
					restoreRequest.Tier = archiveStatus.Info.retrievalTier
				}

				restoreObjectInput := &s3.RestoreObjectInput{
					Bucket:         aws.String(r.config.GetS3().Bucket),
					Key:            aws.String(sync.RemotePath),
					RestoreRequest: restoreRequest,
				}

				// Multiple workers may attempt to restore the same object concurrently. In that
				// case, a RestoreAlreadyInProgress error can occur and should be ignored. If the
				// restore has already completed, subsequent requests will succeed with HTTP 200 OK.
				if _, err := r.client.RestoreObject(ctx, restoreObjectInput); err != nil {
					var apiErr smithy.APIError
					if !(errors.As(err, &apiErr) && apiErr.ErrorCode() == "RestoreAlreadyInProgress") {
						archiveStatus.RestoreInProgress = true
					} else {
						return false, 0, err
					}
				}
				return false, archiveStatus.Info.checkTime, nil
			}
			return false, archiveStatus.Info.recheckTime, nil
		}
	}

	return true, 0, nil
}

func (r *S3Client) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	sync := request.GetSync()

	var err error
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		err = r.upload(ctx, request.Path, sync.RemotePath, request.ExternalId, part, sync.LockedInfo.Mtime.AsTime(), sync.Metadata, sync.Tagging, sync.StorageClass)
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

const maxWalkPageSize = 1000

// GetWalk returns WalkResponse for each prefix match found. The prefix can include glob patterns.
// The resumeToken is returned with the WalkStoppedWithMoreError sentinel when maxKeys is
// exceeded and should be used to resume from the prior walk position. maxKeys must be greater
// than zero or -1 to walk all paths.
//
// Sort order of returned aws objects from ListObjectsV2
// (https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
//   - For general purpose buckets, ListObjectsV2 returns objects in lexicographical order based
//     on their key names. This is important since it allows resuming at any point in the walk.
//   - For directory buckets, ListObjectsV2 does not return objects in lexicographical order so
//     this bucket type must start at the beginning of a new page using the continuation token.
func (r *S3Client) GetWalk(ctx context.Context, prefix string, chanSize int, resumeToken string, maxKeys int) (<-chan *WalkResponse, error) {
	if maxKeys != -1 && maxKeys <= 0 {
		return nil, fmt.Errorf("maxKeys must be greater than zero or -1")
	}

	prefix = r.SanitizeRemotePath(prefix)
	if _, err := filepath.Match(prefix, ""); err != nil {
		return nil, fmt.Errorf("invalid prefix %s: %w", prefix, err)
	}
	prefixWithoutPattern := StripGlobPattern(prefix)
	isKey := prefix == prefixWithoutPattern

	startAfterKey, continuationToken, err := decodeResumeToken(resumeToken)
	if err != nil {
		return nil, err
	}

	// Check if ListObjectV2 StartAfter input is supported. This will only run once unless it fails
	// which would most likely be the result of an unavailable remote target.
	if r.isListStartAfterKeySupported == nil {
		if err := r.checkStartAfterSupport(ctx); err != nil {
			return nil, err
		}
	}

	walkChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(walkChan)

		prefixWalk := func() (keysFound bool) {
			// Size the request's MaxKeys to minimize requests. Sizing with respect to maxKeys
			// optimizes the response size and optimizes both general-purpose and directory bucket
			// types.
			maxKeysPerPage := maxKeys
			if maxKeysPerPage == -1 {
				maxKeysPerPage = maxWalkPageSize
			} else if maxKeysPerPage > maxWalkPageSize {
				pages := (maxKeys + maxWalkPageSize - 1) / maxWalkPageSize
				maxKeysPerPage = (maxKeys + pages - 1) / pages
			}

			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(r.config.GetS3().Bucket),
				Prefix:  aws.String(prefixWithoutPattern),
				MaxKeys: aws.Int32(int32(maxKeysPerPage)),
			}
			if r.isListStartAfterKeySupported != nil && *r.isListStartAfterKeySupported && startAfterKey != "" {
				input.StartAfter = aws.String(startAfterKey)
			} else if continuationToken != "" {
				input.ContinuationToken = aws.String(continuationToken)
			}

			var key string
			var lastKey string
			stopBeforeNextPage := false
			objectPaginator := s3.NewListObjectsV2Paginator(r.client, input)
			keysFound = objectPaginator.HasMorePages()
			for objectPaginator.HasMorePages() {
				if stopBeforeNextPage && continuationToken != "" {
					walkChan <- &WalkResponse{Err: WalkStoppedWithMoreError{resumeToken: encodeResumeToken("", continuationToken)}}
					return
				}

				output, err := objectPaginator.NextPage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						walkChan <- &WalkResponse{Err: fmt.Errorf("prefix walk was cancelled: %w", err)}
					} else {
						walkChan <- &WalkResponse{Err: fmt.Errorf("prefix walk failed: %w", err)}
					}
					return
				}

				for _, content := range output.Contents {
					key = *content.Key
					if !isKey {
						if match, _ := doublestar.Match(prefix, key); !match {
							continue
						}
					}

					if maxKeys == 0 {
						if r.isListStartAfterKeySupported != nil && *r.isListStartAfterKeySupported {
							walkChan <- &WalkResponse{Err: WalkStoppedWithMoreError{resumeToken: encodeResumeToken(lastKey, "")}}
							return
						}

						stopBeforeNextPage = true
						continuationToken = ""
						if output.IsTruncated != nil && *output.IsTruncated {
							continuationToken = *output.NextContinuationToken
						}
					}

					select {
					case <-ctx.Done():
						walkChan <- &WalkResponse{Err: fmt.Errorf("prefix walk was cancelled: %w", ctx.Err())}
						return
					default:
						walkChan <- &WalkResponse{Path: key}
					}
					lastKey = key
					maxKeys--
				}
			}
			return
		}

		if isKey {
			_, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(r.config.GetS3().Bucket),
				Key:    aws.String(prefix),
			})
			if err != nil {
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
					// Try walking as a prefix since there was no key. If not a valid prefix
					// fallback to the original error.
					if !prefixWalk() {
						walkChan <- &WalkResponse{Err: fmt.Errorf("key not found: %s", prefix)}
					}
				} else {
					walkChan <- &WalkResponse{Err: fmt.Errorf("query failed: %w", err)}
				}
				return
			}

			walkChan <- &WalkResponse{Path: prefix}
			return
		}

		prefixWalk()
	}()

	return walkChan, nil
}

func encodeResumeToken(startAfterKey string, continuationToken string) string {
	encodedNextKey := base64.StdEncoding.EncodeToString([]byte(startAfterKey))
	encodedContinuationToken := base64.StdEncoding.EncodeToString([]byte(continuationToken))
	return encodedNextKey + ":" + encodedContinuationToken
}

func decodeResumeToken(resumeToken string) (startAfterKey string, continuationToken string, err error) {
	if !strings.Contains(resumeToken, ":") {
		return "", "", nil
	}

	decode := func(encoded string) (string, error) {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return "", fmt.Errorf("unable to decode walk resume token: %w", err)
		}
		return string(data), err
	}
	parts := strings.Split(resumeToken, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("unable to decode walk resume token: %q", resumeToken)
	}

	if startAfterKey, err = decode(parts[0]); err != nil {
		return
	}
	continuationToken, err = decode(parts[1])
	return
}

func (r *S3Client) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, bool, bool, error) {
	remoteSize, remoteMtime, archiveStatus, err := r.getObjectMetadata(ctx, cfg.RemotePath, cfg.Download)
	if archiveStatus == nil {
		return remoteSize, remoteMtime, false, false, err
	}

	isArchived := (*archiveStatus).IsArchived
	isArchiveRestoreAllowed := (cfg.AllowRestore != nil && *cfg.AllowRestore) || (cfg.AllowRestore == nil && archiveStatus.Info.autoRestore)

	return remoteSize, remoteMtime, isArchived, isArchiveRestoreAllowed, err
}

func (r *S3Client) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	if !cfg.Download {
		segCount, _ := r.recommendedSegments(cfg.LockedInfo.Size)
		if segCount > 1 {
			return r.createUpload(ctx, cfg.RemotePath, cfg.LockedInfo.Mtime.AsTime(), cfg.Metadata, cfg.Tagging, cfg.StorageClass)
		}
	}
	return "", nil
}

func (r *S3Client) SanitizeRemotePath(remotePath string) string {
	// Valid s3 prefixes do not start with a '/' (e.g. myfolder/*/subdir?[a-z])
	return strings.TrimLeft(remotePath, "/")
}

func (r *S3Client) generateSyncJobWorkRequest_Upload(job *beeremote.Job) ([]*flex.WorkRequest, error) {
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

func (r *S3Client) generateSyncJobWorkRequest_Download(job *beeremote.Job) ([]*flex.WorkRequest, error) {
	request := job.GetRequest()
	sync := request.GetSync()
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.RemoteMtime)

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.RemoteSize)
	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.RemoteSize, segCount, partsPerSegment))
	return workRequests, nil
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
			return r.abortUpload(ctx, job.ExternalId, sync.RemotePath)
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
			if err := r.finishUpload(ctx, job.ExternalId, sync.RemotePath, partsToFinish); err != nil {
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

		if request.StubLocal {
			err = CreateOffloadedDataFile(ctx, r.mountPoint, request.Path, sync.RemotePath, request.RemoteStorageTarget, true)
			if err != nil {
				return fmt.Errorf("upload successful but failed to create stub file: %w", err)
			}
			job.GetStatus().SetState(beeremote.Job_OFFLOADED)
		}
	}

	return nil
}

func (r *S3Client) completeSyncWorkRequests_Download(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	sync := request.GetSync()

	_, mtime, _, err := r.getObjectMetadata(ctx, sync.RemotePath, false)
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

		// Update the downloaded file's access and modification times so they accurately reflect the beegfs-mtime.
		absPath := filepath.Join(r.mountPoint.GetMountPath(), request.Path)
		if err := os.Chtimes(absPath, mtime, mtime); err != nil {
			return fmt.Errorf("failed to update download's mtime: %w", err)
		}

		// Clear offloaded data state when contents for a stub file were downloaded successfully.
		if !request.StubLocal && IsFileOffloaded(sync.LockedInfo) {
			entry.SetFileDataState(ctx, request.Path, DataStateNone)
		}
	}

	return nil
}

// prepareJobRequest ensures that sync.LockedInfo is full populated.
func (r *S3Client) prepareJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, sync *flex.SyncJob) (writeLockSet bool, err error) {
	lockedInfo := sync.LockedInfo
	if IsFileLocked(lockedInfo) && HasRemotePathInfo(lockedInfo) {
		return
	}

	var entryInfoMsg msg.EntryInfo
	var ownerNode beegfs.Node
	var getLockedInfoCalled bool

	if !IsFileLocked(lockedInfo) {
		if lockedInfo, writeLockSet, _, entryInfoMsg, ownerNode, err = GetLockedInfo(ctx, r.mountPoint, cfg, cfg.Path, false); err != nil {
			err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, fmt.Sprintf("failed to acquire lock: %s", err.Error()))
			return
		}
		getLockedInfoCalled = true
		cfg.SetLockedInfo(lockedInfo)
		sync.SetLockedInfo(lockedInfo)
	}

	if !HasRemotePathInfo(lockedInfo) {
		request := BuildJobRequest(ctx, r, r.mountPoint, cfg)
		status := request.GetGenerationStatus()
		if status != nil {
			err = fmt.Errorf("%w: %s", ErrJobFailedPrecondition, status.Message)
			return
		}

		if !getLockedInfoCalled {
			if entryInfoMsg, ownerNode, err = entry.GetEntryAndOwnerFromPath(ctx, nil, cfg.Path); err != nil {
				err = fmt.Errorf("failed to get entry info: %w", err)
				return
			}
		}

		if err = PrepareFileStateForWorkRequests(ctx, r, r.mountPoint, entryInfoMsg, ownerNode, cfg); err != nil {
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

type s3ArchiveInfo struct {
	IsArchived        bool
	RestoreInProgress bool
	Info              S3StorageClass
}

// archiveStatus returns whether the resource is archived and is in the process of being restored in
// order to be accessible.
func (r *S3Client) archiveStatus(storageClass types.StorageClass, restoreMsg *string) *s3ArchiveInfo {

	var status *s3ArchiveInfo
	if class, ok := r.storageClasses[storageClass]; ok && class.archival {
		status = &s3ArchiveInfo{Info: r.storageClasses[storageClass]}
		if restoreMsg == nil {
			status.IsArchived = true
			status.RestoreInProgress = false
		} else if strings.Contains(*restoreMsg, `ongoing-request="false"`) {
			status.IsArchived = false
			status.RestoreInProgress = false
		} else {
			status.IsArchived = true
			status.RestoreInProgress = true
		}
	}

	return status
}

// getObjectMetadata returns the object's size in bytes, modification time if it exists.
func (r *S3Client) getObjectMetadata(ctx context.Context, key string, keyMustExist bool) (int64, time.Time, *s3ArchiveInfo, error) {
	if key == "" {
		if keyMustExist {
			return 0, time.Time{}, nil, fmt.Errorf("unable to retrieve object metadata! --remote-path must be specified")
		}
		return 0, time.Time{}, nil, nil
	}

	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(key),
	}

	resp, err := r.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return 0, time.Time{}, nil, os.ErrNotExist
			}
		}
		return 0, time.Time{}, nil, err
	}

	archivedStatus := r.archiveStatus(resp.StorageClass, resp.Restore)

	beegfsMtime, ok := resp.Metadata["beegfs-mtime"]
	if !ok {
		return *resp.ContentLength, *resp.LastModified, archivedStatus, nil
	}

	mtime, err := time.Parse(time.RFC3339, beegfsMtime)
	if err != nil {
		return *resp.ContentLength, *resp.LastModified, archivedStatus, fmt.Errorf("unable to parse remote object's beegfs-mtime")
	}

	return *resp.ContentLength, mtime, archivedStatus, nil
}

func (r *S3Client) createUpload(ctx context.Context, path string, mtime time.Time, metadata map[string]string, tagging *string, storageClass *string) (uploadID string, err error) {
	beegfsMtime := mtime.Format(time.RFC3339)
	if metadata == nil {
		metadata = map[string]string{"beegfs-mtime": beegfsMtime}
	} else if _, ok := metadata["beegfs-mtime"]; ok {
		return "", fmt.Errorf("'beegfs-mtime' is a reserved metadata key")
	} else {
		metadata["beegfs-mtime"] = beegfsMtime
	}

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
		Metadata: metadata,
		Tagging:  tagging,
	}
	if storageClass != nil && *storageClass != "" {
		createMultipartUploadInput.StorageClass = types.StorageClass(*storageClass)
	}

	result, err := r.client.CreateMultipartUpload(ctx, createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (r *S3Client) abortUpload(ctx context.Context, uploadID string, remotePath string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(remotePath),
	}

	_, err := r.client.AbortMultipartUpload(ctx, abortMultipartUploadInput)
	return err
}

// finishUpload will automatically sort parts by number if they are not already in order.
func (r *S3Client) finishUpload(ctx context.Context, uploadID string, remotePath string, parts []*flex.Work_Part) error {

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
		Key:      aws.String(remotePath),
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
func (r *S3Client) upload(
	ctx context.Context,
	path string,
	remotePath string,
	uploadID string,
	part *flex.Work_Part,
	mtime time.Time,
	metadata map[string]string,
	tagging *string,
	storageClass *string,
) error {

	filePart, sha256sum, err := r.mountPoint.ReadFilePart(path, part.OffsetStart, part.OffsetStop)

	if err != nil {
		// S3 allows uploading empty (zero byte) files. Only allow this if the offset start/stop are
		// actually zero and -1.
		if errors.Is(err, io.EOF) && part.OffsetStart == 0 && part.OffsetStop == -1 {
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

		beegfsMtime := mtime.Format(time.RFC3339)
		if metadata == nil {
			metadata = map[string]string{"beegfs-mtime": beegfsMtime}
		} else if _, ok := metadata["beegfs-mtime"]; ok {
			return fmt.Errorf("'beegfs-mtime' is a reserved metadata key")
		} else {
			metadata["beegfs-mtime"] = beegfsMtime
		}

		input := &s3.PutObjectInput{
			Bucket:         aws.String(r.config.GetS3().Bucket),
			Key:            aws.String(remotePath),
			Body:           filePart,
			ChecksumSHA256: aws.String(part.ChecksumSha256),
			// Could a local mtime match and allow for a continue/resume feature...
			//	- If there was a previous failure and the mtime still match then continue from the last byte write
			Metadata: metadata,
			Tagging:  tagging,
		}
		if storageClass != nil && *storageClass != "" {
			input.StorageClass = types.StorageClass(*storageClass)
		}

		resp, err := r.client.PutObject(ctx, input)

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
	if part.OffsetStop == -1 {
		if part.OffsetStart == 0 {
			// There are no bytes to write to the file (i.e., the file is empty).
			return nil
		}
		return fmt.Errorf("the offset stop is %d however the offset start is %d not 0 (this is likely a bug)", part.OffsetStop, part.OffsetStart)
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
