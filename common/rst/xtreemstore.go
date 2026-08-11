package rst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type xtreemstoreS3Provider struct {
	Provider
	s3ApiClient
	mountPoint filesystem.Provider
}

var _ Provider = &xtreemstoreS3Provider{}

type xtreemstoreS3BulkOperation byte

const (
	xtreemstoreS3BulkOperationUnknown xtreemstoreS3BulkOperation = iota
	xtreemstoreS3BulkOperationRetrieve
)

func (o xtreemstoreS3BulkOperation) String() string {
	switch o {
	case xtreemstoreS3BulkOperationRetrieve:
		return "bulk-retrieve"
	default:
		return "unknown"
	}
}

func parseBulkOperation(operation string) xtreemstoreS3BulkOperation {
	switch operation {
	case "bulk-retrieve":
		return xtreemstoreS3BulkOperationRetrieve
	default:
		return xtreemstoreS3BulkOperationUnknown
	}
}

// newXtreemstore initializes an xtreemstore provider by reusing the S3 client implementation.
func newXtreemstore(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	xtreemstore := rstConfig.GetXtreemstore()
	if xtreemstore == nil || xtreemstore.GetS3() == nil {
		return nil, fmt.Errorf("xtreemstore configuration must include s3 settings")
	}

	wrapper := &xtreemstoreS3Provider{
		mountPoint: mountPoint,
	}
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
		options.APIOptions = append(options.APIOptions, smithyhttp.AddHeaderValue("x-amz-optional-object-attributes", "RestoreStatus"))
	})

	return x.s3ApiClient.HeadObject(ctx, in, optFns...)
}

func (x *xtreemstoreS3Provider) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
	defer func() {
		if err != nil {
			request := job.GetRequest()
			if request.HasBulkInfo() {
				bulkInfo := request.GetBulkInfo()
				operation := parseBulkOperation(bulkInfo.Operation)

				switch operation {
				case xtreemstoreS3BulkOperationRetrieve:
					if bulkErr := xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); bulkErr != nil {
						err = fmt.Errorf("%w: failed to mark bulk request complete: %w", err, bulkErr)
					}
				default:
					err = fmt.Errorf("%w: unknown xtreemstore bulk operation %q, unable to mark request complete: %w", err, bulkInfo.Operation, ErrUnsupportedOpForRST)
				}
			}
		}
	}()

	requests, err = x.Provider.GenerateWorkRequests(ctx, lastJob, job, availableWorkers)
	return
}

func (x *xtreemstoreS3Provider) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) (err error) {
	defer func() {
		if err != nil {
			if request.HasBulkInfo() {
				bulkInfo := request.GetBulkInfo()
				operation := parseBulkOperation(bulkInfo.Operation)

				switch operation {
				case xtreemstoreS3BulkOperationRetrieve:
					// It's safe to mark the same request as complete for multiple work requests.
					if bulkErr := xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); bulkErr != nil {
						err = fmt.Errorf("%w: failed to mark bulk request complete: %w", err, bulkErr)
					}
				default:
					err = fmt.Errorf("%w: unknown xtreemstore bulk operation %q, unable to mark request complete: %w", err, bulkInfo.Operation, ErrUnsupportedOpForRST)
				}
			}
		}
	}()

	err = x.Provider.ExecuteWorkRequestPart(ctx, request, part)
	return
}

func (x *xtreemstoreS3Provider) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	if request.HasBulkInfo() {

		defer func() {
			if err != nil {
				bulkInfo := request.GetBulkInfo()
				operation := parseBulkOperation(bulkInfo.Operation)

				switch operation {
				case xtreemstoreS3BulkOperationRetrieve:
					// It's safe to mark the same request as complete for multiple work requests.
					if bulkErr := xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); bulkErr != nil {
						err = fmt.Errorf("%w: failed to mark bulk request complete: %w", err, bulkErr)
					}
				default:
					// err already explains the operation is unsupported; there's nothing to mark complete.
				}
			}
		}()

		if !request.HasSync() {
			return false, 0, ErrReqAndRSTTypeMismatch
		}

		bulkInfo := request.GetBulkInfo()
		operation := parseBulkOperation(bulkInfo.Operation)

		switch operation {
		case xtreemstoreS3BulkOperationRetrieve:
			if bulkErr := xtreemstoreS3BulkRetrieveError(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); bulkErr != nil {
				// Bulk operation requests must be ready before they are sent, so either an error occurred
				// or the bulk request was aborted. For a bulk retrieve operation, the resource was
				// retrieved but removed from the tape buffer before the download.
				err = fmt.Errorf("bulk %s operation failed: %w", bulkInfo.Operation, bulkErr)
			} else {
				ready = true
			}
		default:
			err = fmt.Errorf("failed to determine bulk request readiness for operation %q: %w", bulkInfo.Operation, ErrUnsupportedOpForRST)
		}
	} else {
		ready, delay, err = x.Provider.IsWorkRequestReady(ctx, request)
	}

	return
}

func (x *xtreemstoreS3Provider) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}

	var bulkErr error
	if request.HasBulkInfo() {
		bulkInfo := request.GetBulkInfo()
		switch parseBulkOperation(bulkInfo.Operation) {
		case xtreemstoreS3BulkOperationRetrieve:
			if err := xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); err != nil {
				bulkErr = fmt.Errorf("failed to mark bulk request complete: %w", err)
			}
		default:
			bulkErr = fmt.Errorf("failed to mark bulk request complete for operation %q: %w", bulkInfo.Operation, ErrUnsupportedOpForRST)
		}
	}

	err := x.Provider.CompleteWorkRequests(ctx, job, workResults, abort)
	return errors.Join(err, bulkErr)
}

func (x *xtreemstoreS3Provider) IncludeRequestInBulkOperation(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	if !request.HasSync() {
		return
	}

	sync := request.GetSync()
	lockedInfo := sync.GetLockedInfo()
	if lockedInfo == nil {
		return
	}

	if lockedInfo.IsArchived {
		include = true
		operation = xtreemstoreS3BulkOperationRetrieve.String()
	}
	return
}

// ExcludeRequestFromBulkOperation marks a bulk operation request as complete.
func (x *xtreemstoreS3Provider) ExcludeRequestFromBulkOperation(ctx context.Context, request *beeremote.JobRequest, reason error) error {
	if !request.HasBulkInfo() {
		return fmt.Errorf("cannot cancel request for path %q: request has no bulk operation info", request.GetPath())
	}

	bulkInfo := request.GetBulkInfo()
	switch parseBulkOperation(bulkInfo.Operation) {
	case xtreemstoreS3BulkOperationRetrieve:
		manager := &xtreemstoreS3BulkRetrieveManager{
			rstId:          x.GetConfig().GetId(),
			mountPath:      x.mountPoint.GetMountPath(),
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
		}

		return manager.CancelRequest(ctx, bulkInfo.JobIndex, reason)
	default:
		return fmt.Errorf("failed to cancel bulk request for operation %q: %w", bulkInfo.Operation, ErrUnsupportedOpForRST)
	}
}

func (x *xtreemstoreS3Provider) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	switch parseBulkOperation(operation) {
	case xtreemstoreS3BulkOperationRetrieve:
		manager := x.newXtreemstoreS3BulkRetrieveManager(stateMountPath, operation)
		if err := manager.openState(); err != nil {
			manager.closeState()
			return nil, fmt.Errorf("failed to open bulk operation: %w", err)
		}

		return manager, nil
	default:
		return nil, ErrUnsupportedOpForRST
	}
}

func (x *xtreemstoreS3Provider) newXtreemstoreS3BulkRetrieveManager(stateMountPath string, operation string) *xtreemstoreS3BulkRetrieveManager {
	return &xtreemstoreS3BulkRetrieveManager{
		s3ApiClient:    x,
		rstId:          x.GetConfig().GetId(),
		bucket:         x.GetConfig().GetXtreemstore().S3.Bucket,
		mountPath:      x.mountPoint.GetMountPath(),
		stateMountPath: stateMountPath,
		operation:      operation,
		state:          &xtreemstoreS3BulkRetrieveManagerState{},
	}
}
