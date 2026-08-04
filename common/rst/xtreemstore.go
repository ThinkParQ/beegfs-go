package rst

import (
	"context"
	"errors"
	"fmt"
	"os"
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

type xtreemstoreS3BulkRequestStatus byte

const (
	xtreemstoreS3BulkRequestInitialized xtreemstoreS3BulkRequestStatus = iota
	xtreemstoreS3BulkRequestSent
	xtreemstoreS3BulkRequestComplete
	xtreemstoreS3BulkRequestCompleteAck
)

func (s xtreemstoreS3BulkRequestStatus) Bytes() []byte {
	return []byte{byte(s)}
}

type xtreemstoreS3BulkStatuses struct {
	jobStatuses []byte
	jobCount    int64
	offset      int64 // this will correspond the xtreemstoreS3BulkRetrieveManager.state.ActiveJobStart at the time retrieved
}

func (s *xtreemstoreS3BulkStatuses) Get(jobIndex int64) (status xtreemstoreS3BulkRequestStatus, err error) {
	if jobIndex < s.offset {
		err = fmt.Errorf("invalid index for active session")
		return
	}

	statusesJobIndex := jobIndex - s.offset
	if statusesJobIndex >= int64(len(s.jobStatuses)) {
		err = fmt.Errorf("invalid index for active session")
		return
	}
	return xtreemstoreS3BulkRequestStatus(s.jobStatuses[statusesJobIndex]), nil
}

func (s *xtreemstoreS3BulkStatuses) All() []xtreemstoreS3BulkRequestStatus {
	statuses := make([]xtreemstoreS3BulkRequestStatus, s.jobCount)
	for jobIndex := range s.jobCount {
		// Ignore status error since the jobIndex is valid.
		status, _ := s.Get(jobIndex)
		statuses[jobIndex] = status
	}

	return statuses
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

func (x *xtreemstoreS3Provider) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	if !request.HasSync() {
		return false, 0, ErrReqAndRSTTypeMismatch
	}

	if request.HasBulkInfo() {
		bulkInfo := request.GetBulkInfo()
		if bulkErr := x.xtreemstoreS3BulkError(bulkInfo); bulkErr != nil {
			// Bulk operation requests must be ready before they are sent, so either an error occurred
			// or the bulk request was aborted. For a bulk retrieve operation, the resource was
			// retrieved but removed from the tape buffer before the download.
			err = fmt.Errorf("bulk %s operation failed: %w", bulkInfo.Operation, bulkErr)
		} else {
			ready = true
		}
		return
	}

	if ready, delay, err = x.Provider.IsWorkRequestReady(ctx, request); err != nil {
		return
	}

	// // Option 3: Fail if the bulk request was not ready
	// //  - Verifies the work is in fact ready but requires an api call.
	// if !ready && request.HasBulkInfo() {
	// 	// Bulk operation requests must be ready before they are sent, so either an error occurred
	// 	// or the bulk request was aborted. For a bulk retrieve operation, the resource was
	// 	// retrieved but removed from the tape buffer before the download.
	// 	bulkInfo := request.GetBulkInfo()
	// 	err = fmt.Errorf("bulk %s operation failed: %w", bulkInfo.Operation, x.xtreemstoreS3BulkError(bulkInfo))
	// }

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
		if err := x.xtreemstoreS3BulkMarkRequestComplete(bulkInfo); err != nil {
			bulkErr = fmt.Errorf("failed to mark bulk request complete: %w", err)
		}
	}

	err := x.Provider.CompleteWorkRequests(ctx, job, workResults, abort)
	return errors.Join(err, bulkErr)
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

// xtreemstoreS3BulkMarkRequestComplete marks a request sent by a bulk operation as complete.
func (x *xtreemstoreS3Provider) xtreemstoreS3BulkMarkRequestComplete(bulkInfo *flex.BulkJobRequestInfo) (err error) {
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          x.GetConfig().GetId(),
		mountPath:      x.mountPoint.GetMountPath(),
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}
	return manager.MarkComplete(bulkInfo.JobIndex)
}

// ExcludeRequestFromBulkOperation marks a bulk operation request as complete.
func (x *xtreemstoreS3Provider) ExcludeRequestFromBulkOperation(ctx context.Context, request *beeremote.JobRequest, reason error) error {
	if !request.HasBulkInfo() {
		return fmt.Errorf("cannot cancel request for path %q: request has no bulk operation info", request.GetPath())
	}
	bulkInfo := request.GetBulkInfo()
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          x.GetConfig().GetId(),
		mountPath:      x.mountPoint.GetMountPath(),
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}

	return manager.CancelRequest(ctx, bulkInfo.JobIndex, reason)
}

// xtreemstoreS3BulkError retrieves any bulk operation errors. If no errors were found then nil will
// be returned.
func (x *xtreemstoreS3Provider) xtreemstoreS3BulkError(bulkInfo *flex.BulkJobRequestInfo) error {
	m := &xtreemstoreS3BulkRetrieveManager{
		rstId:          x.GetConfig().GetId(),
		mountPath:      x.mountPoint.GetMountPath(),
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}

	message, err := os.ReadFile(m.getErrorsPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("unable to retrieve bulk operation error message for %q: %w", bulkInfo.Operation, err)
	}
	return fmt.Errorf("%s", message)
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
