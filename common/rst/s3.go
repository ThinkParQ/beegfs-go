package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

type S3Client struct {
	config *flex.RemoteStorageTarget
	// TODO: https://github.com/ThinkParQ/gobee/issues/28
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

	rst := &S3Client{
		config:     rstConfig,
		client:     s3.NewFromConfig(cfg),
		mountPoint: mountPoint,
	}

	return rst, nil
}

func (rst *S3Client) GetConfig() *flex.RemoteStorageTarget {
	return proto.Clone(rst.config).(*flex.RemoteStorageTarget)
}

func (rst *S3Client) GenerateWorkRequests(ctx context.Context, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {

	if job.ExternalId != "" {
		return nil, false, ErrJobAlreadyHasExternalID
	}

	var fileSize int64
	var externalID string
	var segCount int64
	var partsPerSegment int32

	if job.Request.GetSync() == nil {
		return nil, false, ErrReqAndRSTTypeMismatch
	}
	switch job.Request.GetSync().Operation {
	case flex.SyncJob_UPLOAD:
		// The most likely reason for an stat error is the path wasn't found because the request is
		// for a file in BeeGFS that doesn't exist or was removed after the request was submitted.
		// Less likely there was some transient network/other issue preventing communication to
		// BeeGFS that could be retried (generally the client should retry most issues anyway).
		// Until there is a reason to add more complex error handling just treat all stat errors as
		// fatal.
		stat, err := rst.mountPoint.Stat(job.Request.GetPath())
		if err != nil {
			return nil, false, err
		}
		fileSize = stat.Size()
		segCount, partsPerSegment = rst.recommendedSegments(fileSize)
		if segCount > 1 {
			externalID, err = rst.CreateUpload(ctx, job.Request.Path)
			if err != nil {
				return nil, true, fmt.Errorf("error creating multipart upload: %w", err)
			}
			job.ExternalId = externalID
		}
	case flex.SyncJob_DOWNLOAD:
		var err error
		srcKey := job.Request.Path
		if job.Request.GetSync().RemotePath != "" {
			srcKey = job.Request.GetSync().RemotePath
		}
		fileSize, err = rst.GetObjectSize(ctx, srcKey)
		if err != nil {
			return nil, false, err
		}
		segCount, partsPerSegment = rst.recommendedSegments(fileSize)
		err = rst.mountPoint.CreatePreallocatedFile(job.Request.Path, fileSize, job.Request.GetSync().Overwrite)
		if err != nil {
			return nil, false, err
		}
	default:
		return nil, false, ErrUnsupportedOpForRST
	}

	workRequests := RecreateWorkRequests(job, generateSegments(fileSize, segCount, partsPerSegment))
	return workRequests, true, nil
}

// ExecuteWorkRequestPart() attempts to carry out the specified part of the provided requests.
// The part is updated directly with the results.
func (rst *S3Client) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {

	var err error
	switch request.GetSync().Operation {
	case flex.SyncJob_UPLOAD:
		err = rst.Upload(ctx, request.Path, request.ExternalId, part)
	case flex.SyncJob_DOWNLOAD:
		srcKey := request.GetPath()
		if request.GetSync().RemotePath != "" {
			srcKey = request.GetSync().RemotePath
		}
		err = rst.Download(ctx, srcKey, request.Path, part)
	default:
		return fmt.Errorf("operation is not supported by this RST type")
	}

	if err != nil {
		return err
	}

	part.Completed = true
	return nil
}

func (rst *S3Client) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {

	if job.Request.GetSync() == nil {
		return ErrReqAndRSTTypeMismatch
	}
	switch job.Request.GetSync().Operation {
	case flex.SyncJob_UPLOAD:
		if job.ExternalId != "" {
			if abort {
				return rst.AbortUpload(ctx, job.ExternalId, job.Request.Path)
			}
			// TODO: https://github.com/ThinkParQ/gobee/issues/29
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
			return rst.FinishUpload(ctx, job.ExternalId, job.Request.Path, partsToFinish)
		}
		return nil
	case flex.SyncJob_DOWNLOAD:
		return nil
	default:
		return ErrUnsupportedOpForRST
	}
}

func (rst *S3Client) recommendedSegments(fileSize int64) (int64, int32) {

	if fileSize <= rst.config.Policies.FastStartMaxSize || rst.config.Policies.FastStartMaxSize == 0 {
		return 1, 1
	} else if fileSize/4 < 5242880 {
		// Each part must be at least 5MB except for the last part which can be any size.
		// Regardless of the FastStartMaxSize ensure we don't try to use a multipart upload when it is not valid.
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
		return 1, 1
	}
	// TODO: https://github.com/ThinkParQ/gobee/issues/7
	// Arbitrary selection for now. We should be smarter and take into
	// consideration file size and number of workers for this RST type.
	return 4, 1
}

func (rst *S3Client) GetObjectSize(ctx context.Context, key string) (int64, error) {

	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(rst.config.GetS3().Bucket),
		Key:    aws.String(key),
	}
	resp, err := rst.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func (rst *S3Client) CreateUpload(ctx context.Context, path string) (uploadID string, err error) {

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rst.config.GetS3().Bucket),
		Key:    aws.String(path),
	}

	result, err := rst.client.CreateMultipartUpload(ctx, createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (rst *S3Client) AbortUpload(ctx context.Context, uploadID string, path string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   aws.String(rst.config.GetS3().Bucket),
		Key:      aws.String(path),
	}

	_, err := rst.client.AbortMultipartUpload(ctx, abortMultipartUploadInput)
	return err
}

// FinishUpload will automatically sort parts by number if they are not already in order.
func (rst *S3Client) FinishUpload(ctx context.Context, uploadID string, path string, parts []*flex.Work_Part) error {

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
		Bucket:   aws.String(rst.config.GetS3().Bucket),
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := rst.client.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
	return err
}

// Upload attempts to upload the specified part of the provided file path. If an upload ID is
// provided it will treat the provided part as one part in a larger multi-part upload. If the upload
// ID is empty then it will not perform a multi-part upload and just do PutObject, but this also
// requires the provided part number to be "1". When not performing a multi-part upload it still
// honors the provided offset start/stop range and does not check to verify this range covers the
// entirety of the specified file. If the upload is successful the part will be updated directly
// with the results (such as the etag), otherwise an error will be returned.
func (rst *S3Client) Upload(ctx context.Context, path string, uploadID string, part *flex.Work_Part) error {

	filePart, sha256sum, err := rst.mountPoint.ReadFilePart(path, part.OffsetStart, part.OffsetStop)

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
		resp, err := rst.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:         aws.String(rst.config.GetS3().Bucket),
			Key:            aws.String(path),
			Body:           filePart,
			ChecksumSHA256: aws.String(part.ChecksumSha256),
		})

		if err != nil {
			return err
		}
		part.EntityTag = *resp.ETag
		return nil
	}

	uploadPartReq := &s3.UploadPartInput{
		Bucket:         aws.String(rst.config.GetS3().Bucket),
		Key:            aws.String(path),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(part.PartNumber),
		Body:           filePart,
		ChecksumSHA256: aws.String(part.ChecksumSha256),
	}

	resp, err := rst.client.UploadPart(ctx, uploadPartReq)
	if err != nil {
		return err
	}
	part.EntityTag = *resp.ETag
	return nil
}

func (rst *S3Client) Download(ctx context.Context, sourceKey string, destPath string, part *flex.Work_Part) error {
	filePart, err := rst.mountPoint.WriteFilePart(destPath, part.OffsetStart, part.OffsetStop)
	if err != nil {
		return err
	}
	defer filePart.Close()

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(rst.config.GetS3().Bucket),
		Key:    aws.String(sourceKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", part.OffsetStart, part.OffsetStop)),
	}

	resp, err := rst.client.GetObject(ctx, getObjectInput)
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
