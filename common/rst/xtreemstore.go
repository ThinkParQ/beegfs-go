package rst

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type BulkOperation struct {
	RetryDelay time.Duration // defines the retry delay when the bulk operation has not started yet.
	PollDelay  time.Duration // defines the polling delay after the bulk operation has started.
}

type xtreemstoreS3Provider struct {
	Provider
	s3ApiClient
	bulkOperationCfgs map[flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation]*BulkOperation
	mountPoint        filesystem.Provider
}

var _ Provider = &xtreemstoreS3Provider{}

// parseBulkOperation maps the operation name carried with a request back to the xtreemstore bulk
// operation it names. Names are the protobuf enum's, so the operations xtreemstore can be configured
// with are exactly the ones it knows how to run. An unrecognized name resolves to UNKNOWN, which
// every caller rejects as an unsupported operation.
func parseBulkOperation(operation string) flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation {
	if value, ok := flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation_value[operation]; ok {
		return flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation(value)
	}
	return flex.RemoteStorageTarget_XtreemStore_BulkOperation_UNKNOWN
}

// newXtreemstore initializes an xtreemstore provider by reusing the S3 client implementation.
func newXtreemstore(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	if !rstConfig.HasXtreemstore() {
		return nil, ErrConfigRSTTypeIsUnknown
	}
	xtreemstore := rstConfig.GetXtreemstore()

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
	wrapper.bulkOperationCfgs = make(map[flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation]*BulkOperation)
	for _, operation := range xtreemstore.BulkOperations {
		name := operation.GetOperation().String()
		if operation.GetOperation() == flex.RemoteStorageTarget_XtreemStore_BulkOperation_UNKNOWN {
			return nil, fmt.Errorf("bulk operation must specify a valid operation")
		}
		if _, exists := wrapper.bulkOperationCfgs[operation.GetOperation()]; exists {
			return nil, fmt.Errorf("bulk operation, %s, is configured more than once", name)
		}

		cfg := &BulkOperation{
			RetryDelay: DefaultRetryDelay,
			PollDelay:  DefaultPollDelay,
		}

		if operation.RetryDelay != nil {
			retryDelay, err := time.ParseDuration(strings.ToLower(operation.GetRetryDelay()))
			if err != nil {
				return nil, fmt.Errorf("bulk operation, %s, has invalid retryDelay: %w", name, err)
			} else if retryDelay < time.Duration(time.Second) {
				return nil, fmt.Errorf("bulk operation, %s, must specify retryDelay >= '1s'", name)
			}
			cfg.RetryDelay = retryDelay
		}

		if operation.PollDelay != nil {
			pollDelay, err := time.ParseDuration(strings.ToLower(operation.GetPollDelay()))
			if err != nil {
				return nil, fmt.Errorf("bulk operation, %s, has invalid pollDelay: %w", name, err)
			} else if pollDelay < time.Duration(time.Second) {
				return nil, fmt.Errorf("bulk operation, %s, must specify pollDelay >= '1s'", name)
			}
			cfg.PollDelay = pollDelay
		}

		wrapper.bulkOperationCfgs[operation.GetOperation()] = cfg
	}
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

	// TODO: Move this functionality into work.go.
	defer func() {
		// The request is not marked received here even though this is where remote takes ownership
		// of it. This runs while the job is still being created, before remote has committed it, so
		// a crash in that window would leave a received request that no job will ever resolve and a
		// batch that can never complete. The builder marks it received once its submission returns,
		// by which point the job is durable. Until then it stays sent, which the operation replays.
		err = x.resolveBulkRequest(job.GetRequest().GetBulkInfo(), xtreemstoreS3BulkRequestUnchanged, err)
	}()

	requests, err = x.Provider.GenerateWorkRequests(ctx, lastJob, job, availableWorkers)
	return
}

func (x *xtreemstoreS3Provider) ExecuteWorkRequestPart(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest, part *flex.Work_Part) (result *SchedulingResult) {
	defer func() {
		var opErr error
		if result != nil {
			opErr = result.Err
		}
		// resolveBulkRequest returns opErr, optionally with its own failure appended, so its result
		// replaces the error the part ended with.
		if err := x.resolveBulkRequest(request.GetBulkInfo(), xtreemstoreS3BulkRequestUnchanged, opErr); err != nil {
			if result == nil {
				result = &SchedulingResult{}
			}
			// Err and Reschedule are mutually exclusive and the failure wins.
			result.Reschedule = false
			result.Err = err
		}
	}()

	result = x.Provider.ExecuteWorkRequestPart(shutdownCtx, workCtx, request, part)
	return
}

func (x *xtreemstoreS3Provider) IsWorkRequestReady(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	if !request.HasSync() {
		return false, 0, ErrReqAndRSTTypeMismatch
	}

	if shutdownCtx.Err() != nil {
		return false, 0, nil
	}

	bulkInfo := request.GetBulkInfo()
	if bulkInfo == nil {
		return x.Provider.IsWorkRequestReady(shutdownCtx, workCtx, request)
	}

	defer func() {
		err = x.resolveBulkRequest(bulkInfo, xtreemstoreS3BulkRequestUnchanged, err)
	}()

	switch parseBulkOperation(bulkInfo.Operation) {
	case flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE:
		if bulkErr := xtreemstoreS3BulkRetrieveError(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath()); bulkErr != nil {
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
		if _, ok := x.bulkOperationCfgs[flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE]; ok {
			include = true
			operation = flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()
		}
	}

	return
}

func (x *xtreemstoreS3Provider) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	switch parseBulkOperation(operation) {
	case flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE:
		if _, ok := x.bulkOperationCfgs[flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE]; !ok {
			return nil, ErrUnsupportedOpForRST
		}
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
	cfg := x.bulkOperationCfgs[flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE]
	return &xtreemstoreS3BulkRetrieveManager{
		s3ApiClient:           x,
		rstId:                 x.GetConfig().GetId(),
		bucket:                x.GetConfig().GetXtreemstore().S3.Bucket,
		mountPath:             x.mountPoint.GetMountPath(),
		stateMountPath:        stateMountPath,
		operation:             operation,
		state:                 &xtreemstoreS3BulkRetrieveManagerState{},
		batchPollDelay:        cfg.PollDelay,
		sessionBusyRetryDelay: cfg.RetryDelay,
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
	case flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE:
	default:
		return appendErrors(opErr, fmt.Errorf("unknown xtreemstore bulk operation %q, unable to resolve request: %w", bulkInfo.Operation, ErrUnsupportedOpForRST))
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
		return appendErrors(opErr, fmt.Errorf("failed to mark bulk request %s: %w", outcome, bulkErr))
	}

	return opErr
}
