package rst

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file checks and verifies S3 backends (see backend.go). An xtreemstore backend embeds an S3
// part, so it uses the same code.
//
// Facts about S3 and the AWS SDK that verify relies on:
//   - ListBuckets is the only S3 request that names no bucket. So it is the one request a backend,
//     which has no bucket, can send.
//   - Many sites give keys access to their own buckets only. Such a key gets AccessDenied for
//     ListBuckets. The store only answers AccessDenied after it found the key and checked the
//     request's signature. So AccessDenied proves that the endpoint and the credentials work.
//   - A store answers InvalidAccessKeyId for an unknown key, and SignatureDoesNotMatch for a wrong
//     secret key.
//   - The SDK retries a failed request up to three times by default. A verify sets one attempt, so
//     an unreachable endpoint fails at once instead of after the backoff.
//   - The SDK reports a request it sent but got no answer to as a smithyhttp.RequestSendError, such
//     as on a DNS, connection or TLS error. Any other error without an answer from the store comes
//     from the client's own settings, before anything was sent.
//   - The SDK needs a region to sign a request, even for a store that ignores regions. A backend
//     must set one (see s3type.go), so verify never depends on the host's AWS environment for it.

// s3VerifyCredentialErrors are the error codes with which a store rejects a backend's keys.
var s3VerifyCredentialErrors = []string{"InvalidAccessKeyId", "SignatureDoesNotMatch"}

// verifyS3Backend sends one ListBuckets request with the endpoint, region and keys of s3Config,
// and reads the answer:
//   - A list, even an empty one, passes.
//   - AccessDenied passes, with a note that the keys may not list buckets.
//   - InvalidAccessKeyId or SignatureDoesNotMatch fails as rejected credentials.
//   - Any other answer of the store fails with the store's error.
//   - No answer, such as a DNS, connection or TLS error, fails as an unreachable endpoint.
//   - A request the client could not build fails with the SDK's error.
func verifyS3Backend(ctx context.Context, s3Config *flex.RemoteStorageTarget_S3) (string, error) {
	client, err := newS3AWSClient(ctx, s3Config, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	if err != nil {
		return "", err
	}
	_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{MaxBuckets: aws.Int32(1)})
	if err == nil {
		return "", nil
	}
	var apiErr smithy.APIError
	var sendErr *smithyhttp.RequestSendError
	switch {
	case errors.As(err, &sendErr):
		return "", fmt.Errorf("endpoint %s is unreachable: %w", s3Config.GetEndpointUrl(), err)
	case !errors.As(err, &apiErr):
		return "", fmt.Errorf("unable to send a request: %w", err)
	}
	switch code := apiErr.ErrorCode(); {
	case code == "AccessDenied":
		return "the store accepted the credentials, but they may not list buckets", nil
	case slices.Contains(s3VerifyCredentialErrors, code):
		return "", fmt.Errorf("the store rejected the credentials: %s: %s", code, apiErr.ErrorMessage())
	default:
		return "", fmt.Errorf("the store refused the request: %w", err)
	}
}
