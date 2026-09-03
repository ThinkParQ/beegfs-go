package rst

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
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
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// s3ApiClient is the low-level s3 transport layer used by S3Client. Provider specific wrappers can
// customize SDK calls here without reimplementing the higher-level RST behavior.
type s3ApiClient interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjectsV2Pages(ctx context.Context, params *s3.ListObjectsV2Input, pageFn func(*s3.ListObjectsV2Output) (bool, error)) error
	RestoreObject(ctx context.Context, params *s3.RestoreObjectInput, optFns ...func(*s3.Options)) (*s3.RestoreObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// defaultS3ApiClient is the default s3ApiClient backed by the AWS SDK's s3 client.
type defaultS3ApiClient struct {
	client *s3.Client
}

var _ s3ApiClient = &defaultS3ApiClient{}

func (d *defaultS3ApiClient) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return d.client.ListObjectsV2(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) ListObjectsV2Pages(ctx context.Context, params *s3.ListObjectsV2Input, pageFn func(*s3.ListObjectsV2Output) (bool, error)) error {
	paginator := s3.NewListObjectsV2Paginator(d.client, params)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		cont, err := pageFn(output)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func (d *defaultS3ApiClient) RestoreObject(ctx context.Context, params *s3.RestoreObjectInput, optFns ...func(*s3.Options)) (*s3.RestoreObjectOutput, error) {
	return d.client.RestoreObject(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return d.client.HeadObject(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return d.client.CreateMultipartUpload(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return d.client.AbortMultipartUpload(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return d.client.CompleteMultipartUpload(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return d.client.PutObject(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return d.client.UploadPart(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return d.client.GetObject(ctx, params, optFns...)
}

func (d *defaultS3ApiClient) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return d.client.DeleteObject(ctx, params, optFns...)
}

type S3StorageClass struct {
	retrievalTier types.Tier
	archival      bool
	retentionDays int32         // defines how long the retrieved object will be available in days.
	checkTime     time.Duration // defines retry time after initiating the restore.
	recheckTime   time.Duration // defines retry time when restore was previously initiated.
	autoRestore   bool          // defines whether archived objects should be permitted to be restored.
}

// S3Client implements the shared Provider behavior for s3 compatible backends and uses s3ApiClient
// to perform the low-level s3 operations.
type S3Client struct {
	config *flex.RemoteStorageTarget
	// s3Config holds provider-specific S3 options used by this client. This allows providers such
	// as xtreemstore to reuse the S3 implementation while keeping their own top-level RST type.
	s3Config                       *flex.RemoteStorageTarget_S3
	apiClient                      s3ApiClient
	mountPoint                     filesystem.Provider
	storageClasses                 map[types.StorageClass]S3StorageClass
	isListStartAfterKeySupported   *bool
	isListStartAfterKeySupportedMu sync.Mutex
}

var _ Provider = &S3Client{}

func newS3(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	return newS3WithOptions(ctx, rstConfig, rstConfig.GetS3(), mountPoint)
}

type s3ProviderOption func(*s3ProviderBuildCfg)
type s3ProviderBuildCfg struct {
	apiClient func(base s3ApiClient) s3ApiClient
	s3Options []func(*s3.Options)
}

func defaultS3ProviderBuildCfg() s3ProviderBuildCfg {
	return s3ProviderBuildCfg{
		apiClient: func(base s3ApiClient) s3ApiClient { return base },
	}
}

func withS3ApiClient(fn func(s3ApiClient) s3ApiClient) s3ProviderOption {
	return func(cfg *s3ProviderBuildCfg) {
		if fn != nil {
			cfg.apiClient = fn
		}
	}
}

// newS3WithOptions constructs an S3Client. withS3ApiClient is applied before the client is
// created, so shared wrapper state can rely on apiClient already being populated.
func newS3WithOptions(ctx context.Context, rstConfig *flex.RemoteStorageTarget, s3Config *flex.RemoteStorageTarget_S3, mountPoint filesystem.Provider, opts ...s3ProviderOption) (Provider, error) {
	if s3Config == nil {
		return nil, fmt.Errorf("s3 configuration must be specified")
	}

	buildCfg := defaultS3ProviderBuildCfg()
	for _, opt := range opts {
		if opt != nil {
			opt(&buildCfg)
		}
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithBaseEndpoint(s3Config.GetEndpointUrl()),
		awsConfig.WithRegion(s3Config.GetRegion()),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				s3Config.GetAccessKey(),
				s3Config.GetSecretKey(),
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
	endpointUrl, err := url.Parse(s3Config.GetEndpointUrl())
	if err != nil {
		return nil, fmt.Errorf("unable to parse s3 end-point: %w", err)
	}
	bucket := s3Config.GetBucket()
	host := endpointUrl.Hostname()
	usePathStyle := !strings.HasPrefix(host, bucket+".")
	awsClient := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		for _, optFn := range buildCfg.s3Options {
			optFn(o)
		}
	})

	apiClient := buildCfg.apiClient(&defaultS3ApiClient{client: awsClient})
	if apiClient == nil {
		return nil, fmt.Errorf("s3 api client wrapper returned nil")
	}
	s3Client := &S3Client{
		config:                         rstConfig,
		s3Config:                       s3Config,
		apiClient:                      apiClient,
		mountPoint:                     mountPoint,
		storageClasses:                 make(map[types.StorageClass]S3StorageClass),
		isListStartAfterKeySupportedMu: sync.Mutex{},
	}

	for _, class := range s3Config.StorageClass {
		name := types.StorageClass(class.GetName())
		if name == "" {
			return nil, fmt.Errorf("storage class must specify a valid storage class name")
		}

		archive := class.GetArchival()
		if archive == nil {
			return nil, fmt.Errorf("storage class, %s, is not archival. Currently all storage class definitions must be archival", name)
		}

		retrievalTier := types.Tier(archive.GetRetrievalTier())
		retentionDays := archive.GetRetentionDays()
		if retentionDays == 0 {
			retentionDays = 1
		} else if retentionDays < 1 {
			return nil, fmt.Errorf("storage class, %s, has invalid retention days: %d", name, retentionDays)
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
			retentionDays: retentionDays,
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
		Bucket:     aws.String(s.s3Config.Bucket),
		StartAfter: aws.String("-"),
		MaxKeys:    aws.Int32(0),
	}

	if _, err := s.apiClient.ListObjectsV2(ctx, input); err != nil {
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok && apiErr.ErrorCode() == "InvalidArgument" && strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "startafter") {
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
		RestorePolicy:       cfg.RestorePolicy,
		CooldownSecs:        cfg.CooldownSecs,
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
		RestorePolicy:       request.RestorePolicy,
		CooldownSecs:        request.CooldownSecs,
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

func (r *S3Client) GenerateWorkRequests(workCtx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
	request := job.GetRequest()
	if !request.HasSync() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	if job.GetExternalId() != "" {
		return nil, ErrJobAlreadyHasExternalID
	}

	ctx, cancel, _ := WithCancellationDelay(workCtx, time.Minute)
	defer cancel()

	undoAppliedPlan := noopUndo
	lockAcquired := true
	defer func() {
		if err == nil {
			return
		}

		if !IsErrJobTerminalSentinel(err) {
			if undoErr := undoAppliedPlan(ctx); undoErr != nil {
				err = fmt.Errorf("%w: failed to undo changes: %w", err, undoErr)
			} else {
				err = fmt.Errorf("%w: %w", ErrJobFailedPrecondition, err)
			}
		}

		if lockAcquired && !errors.Is(err, ErrJobAlreadyOffloaded) {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, request.Path, beegfs.LockedContentAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	sync := request.GetSync()
	if sync.RemotePath == "" {
		if lastJob != nil {
			sync.SetRemotePath(lastJob.Request.GetSync().RemotePath)
		} else {
			sync.SetRemotePath(r.SanitizeRemotePath(request.Path))
		}
	}

	// Reject a caller-supplied key that isn't already in provider-normal form rather than
	// silently rewriting it: the object would land under a key the caller never asked for,
	// and a mismatch here is what made slash-prefixed keys unreachable by any walk.
	if r.SanitizeRemotePath(sync.RemotePath) != sync.RemotePath {
		err = fmt.Errorf("invalid remote path %q: s3 keys must not begin with '/'", sync.RemotePath)
		return
	}

	if !IsFileLocked(sync.LockedInfo) {
		// The file access lock was not previously acquired which means the file state information
		// has not been determine and by extension, work request in unprepared.
		if _, undoAppliedPlan, lockAcquired, err = r.prepareJobRequest(ctx, request, sync); err != nil {
			return
		}
	}

	job.SetExternalId(sync.LockedInfo.ExternalId)

	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		requests, err = r.generateSyncJobWorkRequest_Upload(job, availableWorkers)
	case flex.SyncJob_DOWNLOAD:
		requests, err = r.generateSyncJobWorkRequest_Download(job, availableWorkers)
	default:
		err = ErrUnsupportedOpForRST
	}
	return
}

// prepareJobRequest acquires the file access lock (if it isn't already held), plans and applies
// any local file state changes needed for the sync operation, updates the file's RST
// configuration if requested, and generates an external ID for the job. It is only called the
// first time GenerateWorkRequests runs for a given job; callers should skip it once
// sync.LockedInfo indicates the lock was already acquired by an earlier call.
func (r *S3Client) prepareJobRequest(ctx context.Context, request *beeremote.JobRequest, sync *flex.SyncJob) (planApplied bool, undoAppliedPlan undoFn, lockAcquired bool, err error) {
	undoAppliedPlan = noopUndo
	cfg := r.getJobRequestCfg(request)

	var pathState *PathState
	pathState, err = r.getLockedInfo(ctx, cfg)
	lockAcquired = pathState != nil && IsFileLocked(pathState.LockedInfo) && pathState.LockAcquired
	if err != nil {
		return
	}
	sync.SetLockedInfo(pathState.LockedInfo)
	cfg.SetLockedInfo(pathState.LockedInfo)

	if !FileExists(pathState.LockedInfo) {
		err = os.ErrNotExist
		return
	}

	if !IsFileLocked(pathState.LockedInfo) || (!pathState.LockAcquired && !IsFileOffloaded(pathState.LockedInfo)) {
		err = fmt.Errorf("failed to acquire the write lock")
		return
	}

	var applyPlan applyPlanFn
	if applyPlan, err = PlanFileStateForWorkRequests(r.mountPoint, cfg); err != nil {
		return
	}

	planApplied, undoAppliedPlan, err = applyPlan(ctx, pathState)
	if err != nil {
		return
	}

	var externalId string
	if externalId, err = r.GenerateExternalId(ctx, cfg); err != nil {
		return
	}
	sync.LockedInfo.SetExternalId(externalId)
	return
}

// ExecuteJobBuilderRequest is not implemented and should never be called.
func (r *S3Client) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (bool, error) {
	return false, ErrUnsupportedOpForRST
}

func (r *S3Client) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	if !request.HasSync() {
		return false, 0, ErrReqAndRSTTypeMismatch
	}
	if shutdownCtx.Err() != nil {
		return false, 0, nil
	}

	sync := request.GetSync()
	lockedInfo := sync.GetLockedInfo()
	if sync.Operation == flex.SyncJob_DOWNLOAD && lockedInfo.IsArchived {

		_, _, archiveStatus, err := r.getObjectMetadata(workCtx, sync.RemotePath, true)
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
					Bucket:         aws.String(r.s3Config.Bucket),
					Key:            aws.String(sync.RemotePath),
					RestoreRequest: restoreRequest,
				}

				// Multiple workers may attempt to restore the same object concurrently. In that
				// case, a RestoreAlreadyInProgress error can occur and should be ignored. If the
				// restore has already completed, subsequent requests will succeed with HTTP 200 OK.
				if _, err := r.apiClient.RestoreObject(workCtx, restoreObjectInput); err != nil {
					if apiErr, ok := errors.AsType[smithy.APIError](err); !ok || apiErr.ErrorCode() != "RestoreAlreadyInProgress" {
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

func (r *S3Client) ExecuteWorkRequestPart(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest, part *flex.Work_Part) *SchedulingResult {
	if !request.HasSync() {
		return &SchedulingResult{Err: ErrReqAndRSTTypeMismatch}
	}
	sync := request.GetSync()

	var err error
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		err = r.upload(workCtx, request.Path, sync.RemotePath, request.ExternalId, part, sync.LockedInfo.Mtime.AsTime(), sync.Metadata, sync.Tagging, sync.StorageClass)
	case flex.SyncJob_DOWNLOAD:
		err = r.download(workCtx, request.Path, sync.RemotePath, part)
	}
	if err != nil {
		return &SchedulingResult{Err: err}
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

// maxListPageSize is the maximum number of objects returned by a single
// ListObjectsV2 request. Using the maximum page size minimizes the number of
// billable LIST requests required when walking objects in S3.
const maxListPageSize = 1000

// GetWalk streams StreamPathResult entries for each object whose key matches the prefix; glob
// patterns in the prefix are supported. Provide resumeToken to continue a previous walk (empty
// string starts fresh).
func (r *S3Client) GetWalk(ctx context.Context, prefix string, chanSize int, resumeToken string) (walk <-chan *filesystem.StreamPathResult, stopWalk func(), err error) {
	prefix = r.SanitizeRemotePath(prefix)
	prefixWithoutPattern := filesystem.StripGlobPattern(prefix)
	isKey := prefix == prefixWithoutPattern
	// All s3 api calls must use unescapedPrefixWithoutPattern to avoid sending escaped path
	// characters that are otherwise handled by doublestar.
	unescapedPrefixWithoutPattern := filesystem.Unescape(prefixWithoutPattern)

	resumeTokenInfo, err := decodeResumeToken(resumeToken)
	if err != nil {
		return nil, func() {}, err
	}

	// Check if ListObjectV2 StartAfter input is supported. This will only run once unless it fails
	// which would most likely be the result of an unavailable remote target.
	if r.isListStartAfterKeySupported == nil {
		if err := r.checkStartAfterSupport(ctx); err != nil {
			return nil, func() {}, err
		}
	}

	stopWalkCh := make(chan struct{}, 1)
	stopWalk = func() {
		select {
		case stopWalkCh <- struct{}{}:
		default:
		}
	}

	walkChan := make(chan *filesystem.StreamPathResult, chanSize)
	send := func(path string, err error) bool {
		result := &filesystem.StreamPathResult{
			Path:        path,
			ResumeToken: resumeToken,
			Err:         err,
		}

		if ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return false
			case <-stopWalkCh:
				return false
			case walkChan <- result:
				return true
			}
		} else {
			return false
		}
	}

	go func() {
		defer close(walkChan)

		prefixWalk := func() (keysFound bool) {
			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(r.s3Config.Bucket),
				Prefix:  aws.String(unescapedPrefixWithoutPattern),
				MaxKeys: aws.Int32(int32(maxListPageSize)),
			}
			if r.isListStartAfterKeySupported != nil && *r.isListStartAfterKeySupported && resumeTokenInfo.StartAfter != "" {
				input.StartAfter = aws.String(resumeTokenInfo.StartAfter)
			} else if resumeTokenInfo.ContinuationToken != "" {
				input.ContinuationToken = aws.String(resumeTokenInfo.ContinuationToken)
			}

			continuationFindStart := false
			if resumeTokenInfo.ContinuationStartKey != "" {
				continuationFindStart = true
			}

			lastKeySent := resumeTokenInfo.StartAfter
			var key string
			pageFn := func(output *s3.ListObjectsV2Output) (bool, error) {
				keysFound = true

				// When resuming with s3ResumeToken ContinuationToken and ContinuationStartKey,
				// search for ContinuationStartKey on the page. If it does not exist and there's a
				// key that's lexically greater than start with it; otherwise, try again on the next
				// page.
				if continuationFindStart {
					filteredContents := output.Contents[:0]
					nextGreaterKeyIndex := -1
					for index, content := range output.Contents {
						key := *content.Key
						if key == resumeTokenInfo.ContinuationStartKey {
							filteredContents = append(filteredContents, output.Contents[index:]...)
							break
						}
						if nextGreaterKeyIndex == -1 && key > resumeTokenInfo.ContinuationStartKey {
							nextGreaterKeyIndex = index
						}
					}

					if len(filteredContents) == 0 {
						if nextGreaterKeyIndex == -1 {
							// There were no greater keys on the current page. So check the next page.
							return true, nil
						}
						continuationFindStart = false
						filteredContents = append(filteredContents, output.Contents[nextGreaterKeyIndex:]...)
					}
					continuationFindStart = false
					output.Contents = filteredContents
				}

				for _, content := range output.Contents {
					key = aws.ToString(content.Key)
					if !isKey {
						if match := doublestar.MatchUnvalidated(prefix, key); !match {
							continue
						}
					}

					// Update resumeToken
					var rt s3ResumeToken
					if r.isListStartAfterKeySupported != nil && *r.isListStartAfterKeySupported {
						rt = s3ResumeToken{StartAfter: lastKeySent}
					} else {
						rt = s3ResumeToken{ContinuationToken: aws.ToString(output.ContinuationToken), ContinuationStartKey: key}
					}

					var encodeErr error
					if resumeToken, encodeErr = rt.encode(); encodeErr != nil {
						send("", encodeErr)
						return false, nil
					}

					if !send(key, nil) {
						return false, nil
					}
					lastKeySent = key
				}

				return true, nil
			}

			if err := r.apiClient.ListObjectsV2Pages(ctx, input, pageFn); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					send("", fmt.Errorf("prefix walk was cancelled: %w", err))
				} else {
					send("", fmt.Errorf("prefix walk failed: %w", err))
				}
				return
			}
			return
		}

		if isKey {
			if _, err := r.headObject(ctx, unescapedPrefixWithoutPattern); err != nil {
				if apiErr, ok := errors.AsType[smithy.APIError](err); ok && apiErr.ErrorCode() == "NotFound" {
					// Try walking as a prefix since there was no key. If not a valid prefix
					// fallback to the original error.
					if !prefixWalk() {
						send("", fmt.Errorf("key not found: %s", unescapedPrefixWithoutPattern))
					}
				} else {
					send("", fmt.Errorf("query failed: %w", err))
				}
				return
			}

			send(unescapedPrefixWithoutPattern, nil)
			return
		}

		prefixWalk()
	}()

	return walkChan, stopWalk, nil
}

// s3ResumeToken holds pagination state so a walk can be resumed. When the list-object api supports
// starting after a specific key then StartAfter will be populated; otherwise, ContinuationToken and
// ContinuationStartKey will be.
type s3ResumeToken struct {
	StartAfter           string
	ContinuationToken    string
	ContinuationStartKey string
}

func (r s3ResumeToken) encode() (string, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(r); err != nil {
		return "", fmt.Errorf("failed to encode s3 resume token: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes()), nil
}

func decodeResumeToken(s string) (s3ResumeToken, error) {
	if s == "" {
		return s3ResumeToken{}, nil
	}

	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s3ResumeToken{}, fmt.Errorf("failed to decode s3 resume token: %w", err)
	}

	var token s3ResumeToken
	reader := bytes.NewReader(raw)
	decoder := gob.NewDecoder(reader)
	if err = decoder.Decode(&token); err != nil {
		return token, fmt.Errorf("failed to decode s3 resume token: %w", err)
	}
	return token, nil
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
		segCount, parts := r.recommendedSegments(cfg.LockedInfo.Size, 0)
		if segCount*int64(parts) > 1 {
			return r.createUpload(ctx, cfg.RemotePath, cfg.LockedInfo.Mtime.AsTime(), cfg.Metadata, cfg.Tagging, cfg.StorageClass)
		}
	}
	return "", nil
}

// ReleaseExternalId aborts the multipart upload GenerateExternalId created. Anything else (a single
// segment upload or a download) never reserved remote state, in which case the externalId is empty
// and there is nothing to release.
func (r *S3Client) ReleaseExternalId(ctx context.Context, cfg *flex.JobRequestCfg, externalId string) error {
	if externalId == "" {
		return nil
	}
	// S3 AbortMultipartUpload is idempotent: aborting an upload id that is already gone is not an
	// error, so a repeated release is safe.
	return r.abortUpload(ctx, externalId, cfg.GetRemotePath())
}

func (r *S3Client) SanitizeRemotePath(remotePath string) string {
	// Valid s3 prefixes do not start with a '/' (e.g. myfolder/*/subdir?[a-z])
	return strings.TrimLeft(remotePath, "/")
}

func (r *S3Client) generateSyncJobWorkRequest_Upload(job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, error) {
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

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.Size, availableWorkers)
	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.Size, segCount, partsPerSegment))
	return workRequests, nil
}

func (r *S3Client) generateSyncJobWorkRequest_Download(job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, error) {
	request := job.GetRequest()
	sync := request.GetSync()
	lockedInfo := sync.LockedInfo
	job.SetStartMtime(lockedInfo.RemoteMtime)

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.RemoteSize, availableWorkers)
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
			err = CreateOffloadedDataFile(ctx, r.mountPoint, request.Path, sync.RemotePath, request.RemoteStorageTarget, true, restorePolicyToDataState(request.GetRestorePolicy()))
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
			if err := entry.SetFileDataState(ctx, request.Path, beegfs.DataStateAvailable); err != nil {
				return fmt.Errorf("unable to clear offloaded data state: %w", err)
			}
		}
	}

	return nil
}

func (r *S3Client) getLockedInfo(ctx context.Context, cfg *flex.JobRequestCfg) (*PathState, error) {
	pathState, err := GetPathState(ctx, r.mountPoint, cfg.Path, PathStateWithLock)
	if err != nil {
		return &pathState, fmt.Errorf("failed to get path state information: %w", err)
	}

	remoteSize, remoteMtime, isArchived, isArchiveRestoreAllowed, err := r.GetRemotePathInfo(ctx, cfg)
	if err != nil && (cfg.Download || !errors.Is(err, os.ErrNotExist)) {
		return &pathState, fmt.Errorf("unable to retrieve remote path information: %w", err)
	}
	if cfg.Download && isArchived && !isArchiveRestoreAllowed {
		return &pathState, fmt.Errorf("remote object is archived and restore is not permitted; rerun with --%s to continue", AllowRestoreFlag)
	}

	if !errors.Is(err, os.ErrNotExist) {
		pathState.LockedInfo.SetRemoteSize(remoteSize)
		pathState.LockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))
		pathState.LockedInfo.SetIsArchived(isArchived)
	}
	return &pathState, nil
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

func (r *S3Client) headObject(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(r.s3Config.Bucket),
		Key:    aws.String(key),
	}
	return r.apiClient.HeadObject(ctx, input)
}

// getObjectMetadata returns the object's size in bytes, modification time if it exists.
func (r *S3Client) getObjectMetadata(ctx context.Context, key string, keyMustExist bool) (int64, time.Time, *s3ArchiveInfo, error) {
	if key == "" {
		if keyMustExist {
			return 0, time.Time{}, nil, fmt.Errorf("unable to retrieve object metadata! --%s must be specified", RemotePathFlag)
		}
		return 0, time.Time{}, nil, nil
	}

	resp, err := r.headObject(ctx, key)
	if err != nil {
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
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
		Bucket:   aws.String(r.s3Config.Bucket),
		Key:      aws.String(path),
		Metadata: metadata,
		Tagging:  tagging,
		// Every part is uploaded with a SHA256 checksum and CompleteMultipartUpload sends those
		// checksums back, so the algorithm has to be declared when the upload is created.
		// Otherwise providers reject the completion with InvalidPart because they never recorded a
		// checksum for any part.
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}
	if storageClass != nil && *storageClass != "" {
		createMultipartUploadInput.StorageClass = types.StorageClass(*storageClass)
	}

	result, err := r.apiClient.CreateMultipartUpload(ctx, createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (r *S3Client) abortUpload(ctx context.Context, uploadID string, remotePath string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   aws.String(r.s3Config.Bucket),
		Key:      aws.String(remotePath),
	}

	_, err := r.apiClient.AbortMultipartUpload(ctx, abortMultipartUploadInput)
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
		Bucket:   aws.String(r.s3Config.Bucket),
		Key:      aws.String(remotePath),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := r.apiClient.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
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
			Bucket:         aws.String(r.s3Config.Bucket),
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

		resp, err := r.apiClient.PutObject(ctx, input)

		if err != nil {
			return err
		}
		part.EntityTag = *resp.ETag
		return nil
	}

	uploadPartReq := &s3.UploadPartInput{
		Bucket:         aws.String(r.s3Config.Bucket),
		Key:            aws.String(remotePath),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(part.PartNumber),
		Body:           filePart,
		ChecksumSHA256: aws.String(part.ChecksumSha256),
	}

	resp, err := r.apiClient.UploadPart(ctx, uploadPartReq)
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
		Bucket: aws.String(r.s3Config.Bucket),
		Key:    aws.String(remotePath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", part.OffsetStart, part.OffsetStop)),
	}

	resp, err := r.apiClient.GetObject(ctx, getObjectInput)
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

const (

	// TODO: Consider whether the following constants should be exposed to the user configuration.

	// targetWorkRequestSegmentPartSize limits part size which behaves as a checkpoint. So when a
	// shutdown or crash occurs, the request will resume from the last completed part. The target
	// size will grow if maxWorkRequestSegments would be exceeded. 100MB is AWS's recommended size
	// before splitting into parts.
	targetWorkRequestSegmentPartSize = 100 * 1024 * 1024
	// 10000 is the maximum number of multipart upload parts in accordance with AWS's
	// recommendation and is commonly listed as the maximum for s3 cloud object storage.
	maxWorkRequestSegments    = 10000
	minWorkRequestSegmentSize = 5 * 1024 * 1024
)

// recommendedSegments determines how to split a transfer of fileSize bytes into work request
// segments, and how many parts each segment is broken into. Segments are the unit of parallelism
// (each is handed to a worker), while parts are the unit of resumption (a segment restarts from its
// last completed part). It returns (1, 1) when segmentation is disabled (FastStartMaxSize <= 0) or
// the file is small enough to fast start. Note that a file at or below FastStartMaxSize is sent as a
// single part, so crossing that threshold by one byte jumps straight to multiple segments and parts.
//
// Segment count aims for FastStartMaxSize bytes per segment, but is bounded by
// minWorkRequestSegmentSize per segment, by availableWorkers (when > 0), and by
// maxWorkRequestSegments. Part count aims for targetWorkRequestSegmentPartSize bytes per part, but
// is bounded so segments*parts stays within maxWorkRequestSegments, which means parts grow larger
// than the target for very large transfers.
//
// For example, with FastStartMaxSize of 1GiB:
//
//	fileSize  availableWorkers  segments  parts   bytes/segment  bytes/part
//	100MiB    any               1         1       100MiB         100MiB
//	1GiB      any               1         1       1GiB           1GiB
//	1GiB+1    unlimited or 8    2         6       512MiB         ~85MiB
//	4GiB      unlimited or 8    4         11      1GiB           ~93MiB
//	100GiB    unlimited         100       11      1GiB           ~93MiB
//	100GiB    8                 8         128     12.5GiB        100MiB
//	1TiB      unlimited         1024      9       1GiB           ~114MiB
//	1TiB      8                 8         1250    128GiB         ~105MiB
//	10TiB     unlimited         10000     1       ~1GiB          ~1GiB
//	10TiB     8                 8         1250    1.25TiB        1GiB
//
// The last two rows show the maxWorkRequestSegments ceiling forcing parts well above the target
// size, which coarsens resumption granularity.
func (r *S3Client) recommendedSegments(fileSize int64, availableWorkers int) (segments int64, parts int32) {
	segments, parts = 1, 1
	fastStartMaxSize := r.config.Policies.FastStartMaxSize
	if fastStartMaxSize <= 0 || fileSize <= fastStartMaxSize {
		return
	}

	// Determine work request segment count.
	targetSegments := (fileSize + fastStartMaxSize - 1) / fastStartMaxSize
	maxSegmentsBySize := fileSize / minWorkRequestSegmentSize
	maxSegmentsByWorker := int64(maxWorkRequestSegments)
	if availableWorkers > 0 {
		maxSegmentsByWorker = int64(availableWorkers)
	}
	segments = max(1, min(targetSegments, maxSegmentsBySize, maxSegmentsByWorker, maxWorkRequestSegments))

	// Determine work request segment part count.
	bytesPerSegment := fileSize / segments
	maxParts := maxWorkRequestSegments / segments
	targetParts := (bytesPerSegment + targetWorkRequestSegmentPartSize - 1) / targetWorkRequestSegmentPartSize
	parts = int32(max(1, min(targetParts, maxParts)))

	// TODO: https://github.com/thinkparq/gobee/issues/7
	// Arbitrary selection for now. We should be smarter and take into
	// consideration the number of workers for this RST type.
	return
}
