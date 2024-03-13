package rst

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

type S3Client struct {
	config *flex.RemoteStorageTarget
	client *s3.Client
}

var _ Client = &S3Client{}

func newS3(rstConfig *flex.RemoteStorageTarget) (Client, error) {

	resolver := aws.EndpointResolverWithOptionsFunc(func(string, string, ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       rstConfig.GetS3().GetPartitionId(),
			URL:               rstConfig.GetS3().GetEndpointUrl(),
			SigningRegion:     rstConfig.GetS3().GetRegion(),
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := s3Config.LoadDefaultConfig(
		context.TODO(),
		s3Config.WithRegion(rstConfig.GetS3().GetRegion()),
		s3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rstConfig.GetS3().GetAccessKey(), rstConfig.GetS3().GetSecretKey(), "")),
		s3Config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load config for RST client: %w", err)
	}

	rst := &S3Client{
		config: rstConfig,
		client: s3.NewFromConfig(cfg),
	}

	return rst, nil
}

func (rst *S3Client) GetType() Type {
	return S3
}

func (rst *S3Client) RecommendedSegments(fileSize int64) (int64, int32) {
	if fileSize <= rst.config.Policies.FastStartMaxSize || rst.config.Policies.FastStartMaxSize == 0 {
		return 1, 1
	}
	// TODO: Arbitrary selection for now. We should be smarter and take into
	// consideration file size and number of workers for this RST type.
	return 4, 1
}

func (rst *S3Client) CreateUpload(path string) (uploadID string, err error) {

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rst.config.GetS3().GetBucket()),
		// TODO: Transform path as needed based on policies.
		Key: aws.String(path),
	}

	result, err := rst.client.CreateMultipartUpload(context.TODO(), createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (rst *S3Client) AbortUpload(uploadID string, path string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   &rst.config.GetS3().Bucket,
		Key:      aws.String(path),
	}

	_, err := rst.client.AbortMultipartUpload(context.TODO(), abortMultipartUploadInput)
	return err
}

func (rst *S3Client) FinishUpload(uploadID string, path string, parts []*flex.WorkResponse_Part) error {

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber: &part.PartNumber,
			ETag:       &part.EntityTag,
		}
	}

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &rst.config.GetS3().Bucket,
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := rst.client.CompleteMultipartUpload(context.TODO(), completeMultipartUploadInput)
	return err
}

// TODO: This is a WIP used for initial testing. For example it doesn't take
// into account what offset in the file should be uploaded. This should probably
// also accept a context.
func (rst *S3Client) UploadPart(uploadID string, part int32, path string, offsetStart int64, offsetStop int64) (string, error) {

	// TODO: If uploadID == nil support uploading without a multipart upload.
	// This way we don't have to worry about completing a multipart upload
	// if BeeSync was asked to just upload using a single part.

	file, err := filesystem.MountPoint.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	uploadPartReq := &s3.UploadPartInput{
		Bucket:     &rst.config.GetS3().Bucket,
		Key:        aws.String(path),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(part),
		Body:       file,
	}

	uploadPartResp, err := rst.client.UploadPart(context.TODO(), uploadPartReq)
	if err != nil {
		return "", err
	}
	return *uploadPartResp.ETag, nil
}

func (rst *S3Client) DownloadPart(path string, offsetStart int64, offsetStop int64) error {
	// TODO implement. This should probably also accept a context.

	return nil
}

func (rst *S3Client) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.WorkResponse_Part) error {

	var err error
	switch request.GetSync().Operation {
	case flex.SyncJob_UPLOAD:
		// We don't check if a multi-part ID is included with the request. We could check the
		// segment to see if there are multiple parts, but we still wouldn't know if there were
		// other requests scheduled to other nodes for this Job. In any case we should never get a
		// request that needs a multi-part ID unless there is a bug in JobMgr, which should be
		// fairly obvious because there would be multiple versions of the same object with different
		// parts of the file.
		part.EntityTag, err = rst.UploadPart(request.ExternalId, part.PartNumber, request.Path, part.OffsetStart, part.OffsetStop)
	case flex.SyncJob_DOWNLOAD:
		err = rst.DownloadPart(request.Path, part.OffsetStart, part.OffsetStop)
	default:
		return fmt.Errorf("operation is not supported by this RST type")
	}

	if err != nil {
		return err
	}

	part.Completed = true
	return nil
}
