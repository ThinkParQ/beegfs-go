package rst

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

const (
	// xtreemstoreS3BulkRecordLen is the width of one status file record. A record holds everything
	// known about one job index: the request's status, the ID of the job reserved for it, and where
	// that request's in-mount and remote paths live in the record file. Keeping them in one record
	// means one durable append per request rather than one per field. It also makes it impossible
	// for a request to have a status without a job, or a job without its paths.
	xtreemstoreS3BulkRecordLen = 1 + JobIdLen + 8 + 2 + 2
	// xtreemstoreS3BulkJobIdOffset is where the job ID starts within a record.
	xtreemstoreS3BulkJobIdOffset = 1
	// xtreemstoreS3BulkPathsOffsetOffset is where the byte offset of the request's paths in the
	// record file starts, as a big endian int64. The in-mount path starts at that offset, and the
	// remote path directly follows it.
	xtreemstoreS3BulkPathsOffsetOffset = xtreemstoreS3BulkJobIdOffset + JobIdLen
	// xtreemstoreS3BulkInMountPathLenOffset is where the in-mount path's length in bytes starts, as
	// a big endian uint16. The in-mount path is the request's path inside the BeeGFS mount.
	xtreemstoreS3BulkInMountPathLenOffset = xtreemstoreS3BulkPathsOffsetOffset + 8
	// xtreemstoreS3BulkRemotePathLenOffset is where the remote path's length in bytes starts, as a
	// big endian uint16.
	xtreemstoreS3BulkRemotePathLenOffset = xtreemstoreS3BulkInMountPathLenOffset + 2
	// xtreemstoreS3BulkMaxPathLen is the longest in-mount or remote path a record can describe, set
	// by the width of the length fields. It is far above both PATH_MAX and the 1024 byte S3 key
	// limit, so a path that exceeds it is rejected rather than silently truncated.
	xtreemstoreS3BulkMaxPathLen = math.MaxUint16

	// xtreemstoreS3BulkStatusFileName is the file, within the state directory, holding one
	// xtreemstoreS3BulkRecordLen record per jobIndex: the request's status byte, the ID of the job
	// reserved for it, and the offset and lengths of its paths in the record file. An
	// operation's AddRequest only ever appends, and it assigns jobIndex from includedJobs while
	// holding the manager lock. So record N always belongs to index N, and any index can be
	// addressed at N*xtreemstoreS3BulkRecordLen without reading what precedes it. The size of this
	// file is also what includedJobs is recovered from.
	//
	// This file is the operation's index. Nothing in the record file can be located without it.
	xtreemstoreS3BulkStatusFileName = "status"
	// xtreemstoreS3BulkRecordFileName is the file, within the state directory, holding the paths of
	// every request the operation took, in the order they were added. Each request contributes its
	// in-mount path and then its remote path, back to back with no separator.
	//
	// A path is located only by the offset and lengths in its status record. No separator is
	// needed, and none could be trusted: an S3 object key may contain any byte, including a newline.
	//
	// The file can hold bytes belonging to no record, left by paths whose status record never
	// followed them. Those bytes are unreachable, because nothing reads this file except through a
	// status record.
	xtreemstoreS3BulkRecordFileName = "record"
	// xtreemstoreS3BulkErrorsFileName is the file, within the state directory, holding the reason
	// the operation was cancelled. It is empty while the operation is healthy, and its absence
	// means the operation's state was destroyed.
	xtreemstoreS3BulkErrorsFileName = "errors"
	// xtreemstoreS3BulkManagerFileName is the file, within the state directory, holding the state
	// an operation's manager needs to resume after a restart. Every operation has one, but each
	// operation defines its contents: for a retrieve it is the active retrieve-session and the job
	// index range that session covers (xtreemstoreS3BulkRetrieveManagerState).
	xtreemstoreS3BulkManagerFileName = "manager.json"
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
	// stateRoot is where bulk operations keep their state, relative to the mount. Requests carry the
	// path of their operation's state, and it is only honored when it lies under this root.
	stateRoot string
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
func newXtreemstore(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) (Provider, error) {
	if !rstConfig.HasXtreemstore() {
		return nil, ErrConfigRSTTypeIsUnknown
	}
	xtreemstore := rstConfig.GetXtreemstore()

	wrapper := &xtreemstoreS3Provider{
		mountPoint: mountPoint,
		stateRoot:  stateRoot,
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
		if bulkErr := xtreemstoreS3BulkRetrieveError(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath(), x.stateRoot); bulkErr != nil {
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
		stateRoot:             x.stateRoot,
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
	// Remote has durably recorded a job for the request, so the job now owns it and resolves it.
	xtreemstoreS3BulkRequestAccepted
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
	case xtreemstoreS3BulkRequestAccepted:
		return "accepted"
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

// xtreemstoreS3BulkInfo is what the status file records for one request: how far the request has
// got, the ID of the job the bulk operation reserved for it, and where the request's in-mount and
// remote paths live in the record file.
//
// pathsOffset and the two lengths locate the paths but do not read them. They cost nothing to
// decode because they arrive in the same bytes as the status and the job ID, and a caller that only
// needs a status never touches the record file.
type xtreemstoreS3BulkInfo struct {
	status         xtreemstoreS3BulkRequestStatus
	jobId          string
	jobIndex       int64
	pathsOffset    int64
	inMountPathLen int
	remotePathLen  int
}

// remotePathOffset is where the remote path starts in the record file. It directly follows the
// in-mount path.
func (i xtreemstoreS3BulkInfo) remotePathOffset() int64 {
	return i.pathsOffset + int64(i.inMountPathLen)
}

// pathsEnd is the record file offset just past the remote path, where the next request's paths
// may start.
func (i xtreemstoreS3BulkInfo) pathsEnd() int64 {
	return i.remotePathOffset() + int64(i.remotePathLen)
}

// xtreemstoreS3BulkInfos is a range of status file records, read in one go and left as the bytes
// that were read. Nothing is decoded until Get asks for a record, because a caller only ever asks
// about the keys of the batch it is working on, which is a fraction of the range.
//
// offset is the index of the first record held. It is the xtreemstoreS3BulkRetrieveManager's
// state.SessionJobStart at the time the records were read, so a job index is only meaningful
// against the session the range was read from.
type xtreemstoreS3BulkInfos struct {
	records []byte
	offset  int64
}

// jobCount is how many records the range holds.
func (s *xtreemstoreS3BulkInfos) jobCount() int64 {
	return int64(len(s.records)) / xtreemstoreS3BulkRecordLen
}

// Get decodes the record for jobIndex. It reports an error when the index is outside the range that
// was read, or when what stands where the job ID belongs is not a UUID.
func (s *xtreemstoreS3BulkInfos) Get(jobIndex int64) (info xtreemstoreS3BulkInfo, err error) {
	if jobIndex < s.offset || jobIndex >= s.offset+s.jobCount() {
		err = fmt.Errorf("job index %d is outside the records read for this session (%d up to %d)", jobIndex, s.offset, s.offset+s.jobCount())
		return
	}

	record := s.records[(jobIndex-s.offset)*xtreemstoreS3BulkRecordLen:]
	jobId := string(record[xtreemstoreS3BulkJobIdOffset : xtreemstoreS3BulkJobIdOffset+JobIdLen])
	if err = uuid.Validate(jobId); err != nil {
		// A record that is not a job ID means the file no longer lines up with the indexes, so
		// every later record is suspect too. Returning it would claim some other path's job.
		err = fmt.Errorf("the job ID recorded for index %d is not a valid UUID (%q): %w", jobIndex, jobId, err)
		return
	}

	info.jobIndex = jobIndex
	info.status = xtreemstoreS3BulkRequestStatus(record[0])
	info.jobId = jobId
	info.pathsOffset = int64(binary.BigEndian.Uint64(record[xtreemstoreS3BulkPathsOffsetOffset:]))
	info.inMountPathLen = int(binary.BigEndian.Uint16(record[xtreemstoreS3BulkInMountPathLenOffset:]))
	info.remotePathLen = int(binary.BigEndian.Uint16(record[xtreemstoreS3BulkRemotePathLenOffset:]))
	return
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
	case xtreemstoreS3BulkRequestComplete:
		// It's safe to mark the same request complete more than once.
		bulkErr = xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo, x.GetConfig().GetId(), x.mountPoint.GetMountPath(), x.stateRoot)
	default:
		bulkErr = fmt.Errorf("unexpected on-success bulk request status %q for bulk operation %q (this is probably a bug)", outcome, bulkInfo.Operation)
	}
	if bulkErr != nil {
		return appendErrors(opErr, fmt.Errorf("failed to mark bulk request %s: %w", outcome, bulkErr))
	}

	return opErr
}
