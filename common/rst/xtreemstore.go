package rst

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type xtreemstoreS3Provider struct {
	Provider
	s3ApiClient
}

var _ Provider = &xtreemstoreS3Provider{}

// newXtreemstore initializes an xtreemstore provider by reusing the S3 client implementation.
func newXtreemstore(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	xtreemstore := rstConfig.GetXtreemstore()
	if xtreemstore == nil || xtreemstore.GetS3() == nil {
		return nil, fmt.Errorf("xtreemstore configuration must include s3 settings")
	}

	wrapper := &xtreemstoreS3Provider{}
	s3Client, err := newS3WithOptions(ctx, rstConfig, xtreemstore.GetS3(), mountPoint,
		withS3ApiClient(func(base s3ApiClient) s3ApiClient {
			wrapper.s3ApiClient = base
			return wrapper
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize xtreemstore provider: %w", err)
	}

	wrapper.Provider = s3Client
	return wrapper, nil
}

func (x *xtreemstoreS3Provider) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	// Update xtreemstore head-object api request headers to include storage details.
	optFns = append(optFns, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, smithyhttp.AddHeaderValue("x-amz-meta-xts-request-storage-details", "true"))
	})

	return x.s3ApiClient.HeadObject(ctx, in, optFns...)
}

func (x *xtreemstoreS3Provider) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (includeInBulk bool) {
	return false
}

func (x *xtreemstoreS3Provider) BuildBulkRequest(ctx context.Context) (submitBulkRequest SubmitBulkRequestFn, appendBulkRequestCfg AppendBulkRequestCfgFn, err error) {
	return nil, nil, nil
}
