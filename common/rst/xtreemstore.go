package rst

import (
	"context"
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
		err = x.resolveBulkRequest(job.GetRequest().GetBulkInfo(), xtreemstoreS3BulkRequestReceived, err)
	}()

	requests, err = x.Provider.GenerateWorkRequests(ctx, lastJob, job, availableWorkers)
	return
}

func (x *xtreemstoreS3Provider) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) (err error) {
	defer func() {
		err = x.resolveBulkRequest(request.GetBulkInfo(), xtreemstoreS3BulkRequestUnchanged, err)
	}()

	err = x.Provider.ExecuteWorkRequestPart(ctx, request, part)
	return
}

func (x *xtreemstoreS3Provider) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	if !request.HasSync() {
		return false, 0, ErrReqAndRSTTypeMismatch
	}

	bulkInfo := request.GetBulkInfo()
	if bulkInfo == nil {
		return x.Provider.IsWorkRequestReady(ctx, request)
	}

	defer func() {
		err = x.resolveBulkRequest(bulkInfo, xtreemstoreS3BulkRequestUnchanged, err)
	}()

	switch parseBulkOperation(bulkInfo.Operation) {
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

	return
}

func (x *xtreemstoreS3Provider) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) (err error) {
	request := job.GetRequest()
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}

	defer func() {
		err = x.resolveBulkRequest(job.GetRequest().GetBulkInfo(), xtreemstoreS3BulkRequestComplete, err)
	}()

	err = x.Provider.CompleteWorkRequests(ctx, job, workResults, abort)
	return
}

// ResolveBulkRequest marks the request complete from the bulk operation's perspective. It is the
// only thing that releases a request whose job was never created or will never run: the operation's
// batch is not finished (and its retrieve-session not released) until every request in it reaches a
// terminal status, so a request left behind here stalls the owning builder job indefinitely.
func (x *xtreemstoreS3Provider) ResolveBulkRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return x.resolveBulkRequest(request.GetBulkInfo(), xtreemstoreS3BulkRequestComplete, nil)
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

	if sync.Operation == flex.SyncJob_DOWNLOAD && lockedInfo.IsArchived {
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

type xtreemstoreS3BulkRequestStatus byte

const (
	// Request has been added to bulk operation.
	xtreemstoreS3BulkRequestAdded xtreemstoreS3BulkRequestStatus = iota
	// Request has been sent from the bulk operation and is waiting for GenerateWorkRequests to
	// acknowledge by marking it xtreemstoreS3BulkRequestReceived.
	xtreemstoreS3BulkRequestSent
	// Request has been received by GenerateWorkRequests.
	xtreemstoreS3BulkRequestReceived
	// Request has been completed from the perspective of the bulk operation but has not been
	// acknowledged by the bulk operation yet.
	xtreemstoreS3BulkRequestComplete
	// Request has been completed and bulk operation has acknowledge the completion.
	xtreemstoreS3BulkRequestCompleteAcked
	// xtreemstoreS3BulkRequestUnchanged is not a real status and is never persisted. It is only used
	// as a resolveBulkRequest on-success argument, to say the request is still in flight and a later
	// stage is responsible for resolving it.
	xtreemstoreS3BulkRequestUnchanged = 255
)

func (s xtreemstoreS3BulkRequestStatus) String() string {
	switch s {
	case xtreemstoreS3BulkRequestAdded:
		return "added"
	case xtreemstoreS3BulkRequestSent:
		return "sent"
	case xtreemstoreS3BulkRequestReceived:
		return "received"
	case xtreemstoreS3BulkRequestComplete:
		return "complete"
	case xtreemstoreS3BulkRequestCompleteAcked:
		return "complete-acked"
	case xtreemstoreS3BulkRequestUnchanged:
		return "unchanged"
	default:
		return "unknown"
	}
}

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

func (x *xtreemstoreS3Provider) resolveBulkRequest(bulkInfo *flex.BulkJobRequestInfo, onSuccess xtreemstoreS3BulkRequestStatus, opErr error) error {
	if bulkInfo == nil {
		return opErr
	}

	switch parseBulkOperation(bulkInfo.Operation) {
	case xtreemstoreS3BulkOperationRetrieve:
	default:
		return appendError(opErr, fmt.Errorf("unknown xtreemstore bulk operation %q, unable to resolve request: %w", bulkInfo.Operation, ErrUnsupportedOpForRST))
	}

	outcome := onSuccess
	if opErr != nil {
		outcome = xtreemstoreS3BulkRequestComplete
	}

	var bulkErr error
	switch outcome {
	case xtreemstoreS3BulkRequestUnchanged:
	case xtreemstoreS3BulkRequestReceived:
		bulkErr = xtreemstoreS3BulkRetrieveMarkReceived(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath())
	case xtreemstoreS3BulkRequestComplete:
		// It's safe to mark the same request complete more than once.
		bulkErr = xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath())
	default:
		bulkErr = fmt.Errorf("unexpected on-success bulk request status %q for bulk operation %q (this is probably a bug)", outcome, bulkInfo.Operation)
	}
	if bulkErr != nil {
		return appendError(opErr, fmt.Errorf("failed to mark bulk request %s: %w", outcome, bulkErr))
	}

	return opErr
}
