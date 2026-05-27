package rst

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

const (
	XTS_SYSTEM                     = ".xts-system"
	XTS_SYSTEM_ERRORS              = XTS_SYSTEM + "/errors"
	XTS_SYSTEM_RETRIEVE_SESSION    = XTS_SYSTEM + "/retrieve-session.json"
	XTS_SYSTEM_RETRIEVE_BATCH_LIST = XTS_SYSTEM + "/retrieve-batch-list.json"
	XTS_SYSTEM_RETRIEVE_BATCH_FMT  = XTS_SYSTEM + "/retrieve-batch-%d.json"
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
)

func (s xtreemstoreS3BulkRequestStatus) Bytes() []byte { return []byte{byte(s)} }

var (
	ErrActiveRetrieveSessionAlreadyExists = errors.New("active retrieve-session already exists")
)

type xtreemstoreS3BulkRetrieveManager struct {
	s3ApiClient
	bucket       string
	operation    string
	statePath    string
	state        *xtreemstoreS3BulkRetrieveManagerState
	includedJobs int64
	walkCh       chan *filesystem.StreamPathResult
	statusHandle *os.File
	recordHandle *os.File
}

var _ clientBulkOperation = &xtreemstoreS3BulkRetrieveManager{}

type xtreemstoreS3BulkRetrieveSessionInfo struct {
	Active     bool      `json:"active"`
	RetrieveId string    `json:"retrieve-id"`
	Started    time.Time `json:"started"`
}

type xtreemstoreS3BulkRetrieveManagerState struct {
	ActiveRetrieveId string `json:"active-retrieve-id"`
	ActiveJobStart   int64  `json:"active-job-start"`
	ActiveJobEnd     int64  `json:"active-job-end"`
}

type xtreemstoreS3BulkRetrieveBatchInfo struct {
	Number  int64 `json:"number"`
	Objects int64 `json:"objects"`
	Size    int64 `json:"size"`
}

type xtreemstoreS3BulkRetrieveRequest struct {
	Ids            []string `json:"ids,omitempty"`
	BucketRetrieve bool     `json:"bucket-retrieve,omitempty"`
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

	// // Option 1: Assume bulk operation sent when ready.
	// //  - This option assumes the work is ready based on the existence of the job request and requires no additional api calls.
	// //    The job will fail if an error has occurred.
	// if request.HasBulkInfo() {
	// 	ready = true
	// 	return
	// }

	// Option 2: Also assumes bulk operation was sent when ready. However, it checks for errors before proceeding.
	//  - Requires a stat call but prevents the work from starting if there was a bulk operation error.
	if request.HasBulkInfo() {
		// Bulk operation requests must be ready before they are sent, so either an error occurred
		// or the bulk request was aborted. For a bulk retrieve operation, the resource was
		// retrieved but removed from the tape buffer before the download.
		bulkInfo := request.GetBulkInfo()
		if bulkErr := x.xtreemstoreS3BulkError(bulkInfo); bulkErr != nil {
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

func (x *xtreemstoreS3Provider) newXtreemstoreS3BulkRetrieveManager(stateMountPath string, operation string) (*xtreemstoreS3BulkRetrieveManager, chan *filesystem.StreamPathResult) {
	walkCh := make(chan *filesystem.StreamPathResult, 128)
	statePath := path.Join(x.mountPoint.GetMountPath(), stateMountPath, operation)
	return &xtreemstoreS3BulkRetrieveManager{
		s3ApiClient: x,
		bucket:      x.GetConfig().GetXtreemstore().S3.Bucket,
		operation:   operation,
		statePath:   statePath,
		state:       &xtreemstoreS3BulkRetrieveManagerState{},
		walkCh:      walkCh,
	}, walkCh
}

// xtreemstoreS3BulkMarkRequestComplete marks a request sent by a bulk operation as complete.
func (x *xtreemstoreS3Provider) xtreemstoreS3BulkMarkRequestComplete(bulkInfo *flex.BulkJobRequestInfo) (err error) {
	statePath := path.Join(x.mountPoint.GetMountPath(), bulkInfo.StateMountPath, bulkInfo.Operation)
	manager := &xtreemstoreS3BulkRetrieveManager{
		operation: bulkInfo.Operation,
		statePath: statePath,
	}
	return manager.MarkComplete(bulkInfo.JobIndex)
}

// xtreemstoreS3BulkError retrieves any bulk operation errors. If no errors were found then nil will
// be returned.
func (x *xtreemstoreS3Provider) xtreemstoreS3BulkError(bulkInfo *flex.BulkJobRequestInfo) error {
	statePath := path.Join(x.mountPoint.GetMountPath(), bulkInfo.StateMountPath, bulkInfo.Operation)
	m := &xtreemstoreS3BulkRetrieveManager{
		operation: bulkInfo.Operation,
		statePath: statePath,
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

func (x *xtreemstoreS3Provider) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
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
		manager, _ := x.newXtreemstoreS3BulkRetrieveManager(stateMountPath, operation)
		if err := manager.openState(); err != nil {
			manager.closeState()
			return nil, fmt.Errorf("failed to open bulk operation: %w", err)
		}
		return manager, nil
	default:
		return nil, ErrUnsupportedOpForRST
	}
}

func (m *xtreemstoreS3BulkRetrieveManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	if request.GetBulkInfo().JobIndex != m.includedJobs {
		return fmt.Errorf("unexpected request bulkInfo.JobIndex")
	}

	if _, err = m.statusHandle.Write(xtreemstoreS3BulkRequestInitialized.Bytes()); err != nil {
		return
	}

	remotePath := request.GetSync().GetRemotePath()
	if m.includedJobs == 0 {
		_, err = m.recordHandle.WriteString(remotePath)
	} else {
		_, err = m.recordHandle.WriteString("\n" + remotePath)
	}

	m.includedJobs++
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) Execute(ctx context.Context) (walkCh <-chan *filesystem.StreamPathResult, getResults BulkRequestWaitForResultFn, err error) {
	// // TESTING ONLY, TEMPORARY:
	// // Replace manager.Execute(ctx) with manager.ExecuteTest(ctx) to exercise the bulk-request
	// // plumbing without an xtreemstore retrieve-session.
	// getResults = func() (bool, time.Duration, error) {
	// 	defer close(m.WalkCh)
	// 	return m.ExecuteTest(ctx)
	// }

	getResults = func() (bool, time.Duration, error) {
		defer m.closeState()
		defer close(m.walkCh)
		return m.execute(ctx)
	}
	return m.walkCh, getResults, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Resume(ctx context.Context) (walkCh <-chan *filesystem.StreamPathResult, wait BulkWaitFn, err error) {
	wait = func() error {
		defer m.closeState()
		defer close(m.walkCh)
		return m.resume(ctx)
	}
	return m.walkCh, wait, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *filesystem.StreamPathResult, wait BulkWaitFn, err error) {

	wait = func() error {
		defer m.closeState()
		defer close(m.walkCh)
		if reason != nil {
			if err := m.CancelRequests(ctx, reason, m.walkCh); err != nil {
				return fmt.Errorf("failed to cancel all bulk operation job requests: %w", err)
			}
		} else {
			statuses, err := m.getAllStatuses()
			if err != nil {
				return fmt.Errorf("failed to verify the bulk operation job requests: %w", err)
			}
			for _, status := range statuses.All() {
				_ = status // TODO:
				switch status {
				case xtreemstoreS3BulkRequestInitialized:
				case xtreemstoreS3BulkRequestSent:
				case xtreemstoreS3BulkRequestComplete:
				default:
				}
			}
		}

		sessionInfo, err := m.getSessionInfo(ctx)
		if err != nil {
			return fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)
		}

		if !sessionInfo.Active || sessionInfo.RetrieveId != m.state.ActiveRetrieveId {
			if err := m.deleteState(); err != nil {
				return fmt.Errorf("failed to remove retrieve-session state file: %w", err)
			}
			return nil
		}

		batchInfos, err := m.getSessionBatchInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to get retrieve-session batch info: %w", err)
		}

		for _, batchInfo := range batchInfos {
			if err := m.deleteSessionBatch(ctx, batchInfo); err != nil {
				return fmt.Errorf("failed to delete retrieve-session batch: %w", err)
			}
		}

		if err := m.destroyRetrieveSession(ctx); err != nil {
			return fmt.Errorf("failed to deactivate retrieve-session: %w", err)
		}

		if err := m.deleteState(); err != nil {
			return fmt.Errorf("failed to remove retrieve-session state file: %w", err)
		}

		return nil
	}

	return m.walkCh, wait, nil
}

// CancelRequests sends the key for each object that has not been retrieved.
func (m *xtreemstoreS3BulkRetrieveManager) CancelRequests(ctx context.Context, reason error, walkCh chan<- *filesystem.StreamPathResult) error {
	records, err := m.getRecordsFromActiveStart()
	if err != nil {
		return fmt.Errorf("unable to determine records to cancel: %w", err)
	}
	statuses, err := m.getStatusesFromActiveStart()
	if err != nil {
		return fmt.Errorf("unable to determine records to cancel: %w", err)
	}

	cancelErr := &RequestCancelError{Reason: reason}
	for jobIndex, record := range records {
		activeJobIndex := m.state.ActiveJobStart + int64(jobIndex)
		if status, err := statuses.Get(activeJobIndex); err != nil {
			return fmt.Errorf("unable to send failed job request: %w", err)
		} else {
			switch status {
			case xtreemstoreS3BulkRequestInitialized:
				walkCh <- &filesystem.StreamPathResult{Path: record, Err: cancelErr}
				if err := m.MarkSent(activeJobIndex); err != nil {
					return fmt.Errorf("failed to mark bulk job request as sent: %w", err)
				}
			case xtreemstoreS3BulkRequestSent:
			case xtreemstoreS3BulkRequestComplete:
			default:
				return fmt.Errorf("unknown record status. Record: %s, Status: %v", record, status)
			}
		}
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) deleteState() error {
	var errs []error
	errs = append(errs, os.Remove(m.getStatusPath()))
	errs = append(errs, os.Remove(m.getRecordPath()))
	errs = append(errs, os.Remove(m.getErrorsPath()))
	errs = append(errs, os.Remove(m.getManagerPath()))
	return errors.Join(errs...)
}

func (m *xtreemstoreS3BulkRetrieveManager) execute(ctx context.Context) (reschedule bool, delay time.Duration, err error) {
	rescheduleOperation := func() (bool, time.Duration, error) {
		rescheduleDelay := 5 * time.Second // TODO: This should be based on config...
		return true, rescheduleDelay, nil
	}

	if ready, err := m.ensureSessionActive(ctx); err != nil {
		return false, 0, err
	} else if !ready {
		return rescheduleOperation()
	}

	if allBatchesComplete, err := m.processSessionBatches(ctx); err != nil {
		return false, 0, err
	} else if !allBatchesComplete {
		return rescheduleOperation()
	}

	if err = m.destroyRetrieveSession(ctx); err != nil {
		err = fmt.Errorf("retrieve-session completed successfully, but the active session could not be destroyed and manual intervention is required: %w", err)
	}

	// TODO: Should all the state be destroyed here?
	//  - Batches are complete and active session is destroyed
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) resume(ctx context.Context) error {
	sessionInfo, err := m.getSessionInfo(ctx)
	if err != nil {
		return fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)

	}
	if !(sessionInfo.Active && sessionInfo.RetrieveId == m.state.ActiveRetrieveId) {
		return nil
	}

	if allBatchesComplete, err := m.processSessionBatches(ctx); err != nil {
		return err
	} else if !allBatchesComplete {
		return nil
	}

	if err := m.destroyRetrieveSession(ctx); err != nil {
		return fmt.Errorf("retrieve-session completed successfully, but the active session could not be destroyed and manual intervention is required: %w", err)
	}
	return nil
}

// executeTest is a temporary manual test helper that bypasses the xtreemstore retrieve-session and
// emits all initialized bulk requests as ready immediately.
func (m *xtreemstoreS3BulkRetrieveManager) executeTest(ctx context.Context) (reschedule bool, delay time.Duration, err error) {
	records, err := m.getRecords(0, -1)
	if err != nil {
		return false, 0, fmt.Errorf("failed to load bulk request records: %w", err)
	}

	statuses, err := m.getAllStatuses()
	if err != nil {
		return false, 0, fmt.Errorf("failed to load bulk request statuses: %w", err)
	}

	for jobIndex, record := range records {
		status, err := statuses.Get(int64(jobIndex))
		if err != nil {
			return false, 0, fmt.Errorf("failed to determine status for key %q: %w", record, err)
		}

		switch status {
		case xtreemstoreS3BulkRequestInitialized:
			select {
			case <-ctx.Done():
				return false, 0, ctx.Err()
			case m.walkCh <- &filesystem.StreamPathResult{Path: record}:
			}

			if err := m.MarkSent(int64(jobIndex)); err != nil {
				return false, 0, fmt.Errorf("failed to mark bulk job request as sent: %w", err)
			}
		case xtreemstoreS3BulkRequestSent:
		case xtreemstoreS3BulkRequestComplete:
		default:
			return false, 0, fmt.Errorf("unknown record status. Record: %s, Status: %v", record, status)
		}
	}

	return false, 0, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) ensureSessionActive(ctx context.Context) (ready bool, err error) {
	sessionInfo, sessionInfoErr := m.getSessionInfo(ctx)
	if sessionInfoErr != nil {
		err = fmt.Errorf("unable to determine whether retrieve-session is active: %w", sessionInfoErr)
		return
	}

	if sessionInfo.Active {
		ready = sessionInfo.RetrieveId == m.state.ActiveRetrieveId
	} else if startSessionErr := m.startSession(ctx); startSessionErr != nil {
		if !errors.Is(startSessionErr, ErrActiveRetrieveSessionAlreadyExists) {
			err = fmt.Errorf("failed to start retrieve-session: %w", startSessionErr)
		}
	} else {
		ready = true
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatches(ctx context.Context) (complete bool, err error) {
	var activeRecordMap map[string]int64
	var activeStatuses *xtreemstoreS3BulkStatuses
	var batchInfos []xtreemstoreS3BulkRetrieveBatchInfo
	if activeRecordMap, err = m.getActiveRecordsMap(); err != nil {
		return false, fmt.Errorf("failed to get record mappings for the active retrieve-session: %w", err)
	}
	if activeStatuses, err = m.getActiveSessionStatuses(); err != nil {
		return false, fmt.Errorf("failed to get request statuses for active retrieve-session: %w", err)
	}
	if batchInfos, err = m.getSessionBatchInfo(ctx); err != nil {
		return false, fmt.Errorf("failed to load retrieve-session batch info: %w", err)
	}

	for _, batchInfo := range batchInfos {
		if complete, err = m.processSessionBatch(ctx, activeRecordMap, activeStatuses, batchInfo); err != nil {
			return false, err
		} else if !complete {
			return false, nil
		}
	}

	return true, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatch(ctx context.Context, activeRecordMap map[string]int64, activeStatuses *xtreemstoreS3BulkStatuses, batchInfo xtreemstoreS3BulkRetrieveBatchInfo) (bool, error) {
	keys, err := m.getSessionBatchKeys(ctx, batchInfo)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve batch keys: %w", err)
	}

	batchComplete := true
	for _, key := range keys {
		if complete, err := m.processSessionBatchKey(ctx, activeRecordMap, activeStatuses, key); err != nil {
			return false, err
		} else if !complete {
			batchComplete = false
		}
	}

	if !batchComplete {
		return false, nil
	}

	if err = m.deleteSessionBatch(ctx, batchInfo); err != nil {
		return false, fmt.Errorf("failed to delete retrieve-session batch: %w", err)
	}
	return true, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatchKey(ctx context.Context, activeRecordMap map[string]int64, activeStatuses *xtreemstoreS3BulkStatuses, key string) (bool, error) {
	jobIndex, ok := activeRecordMap[key]
	if !ok {
		return false, fmt.Errorf("unable to determine status for key: %s", key)
	}

	jobStatus, err := activeStatuses.Get(jobIndex)
	if err != nil {
		return false, fmt.Errorf("unable to determine status: %w", err)
	}

	switch jobStatus {
	case xtreemstoreS3BulkRequestInitialized:
		if ready, err := m.isObjectReadyForDownload(ctx, key); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				m.walkCh <- &filesystem.StreamPathResult{
					Path: key,
					Err:  fmt.Errorf("object no longer exists"),
				}
				if err := m.MarkComplete(jobIndex); err != nil {
					return false, fmt.Errorf("failed to mark bulk job request as complete: %w", err)
				}
				return true, nil
			}
			return false, fmt.Errorf("failed to determine restore state: %w", err)
		} else if ready {
			m.walkCh <- &filesystem.StreamPathResult{Path: key}
			if err := m.MarkSent(jobIndex); err != nil {
				return false, fmt.Errorf("failed to mark bulk job request as complete: %w", err)
			}
		}
		return false, nil
	case xtreemstoreS3BulkRequestSent:
		return false, nil
	case xtreemstoreS3BulkRequestComplete:
		return true, nil
	default:
		m.walkCh <- &filesystem.StreamPathResult{
			Path: key,
			Err:  fmt.Errorf("unknown job request status"),
		}
		if err := m.MarkComplete(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete: %w", err)
		}
		return true, nil
	}
}

/*
func (m *xtreemstoreS3BulkRetrieveManager) addRequests(requests []*beeremote.JobRequest) (err error) {
	defer func() {
		if saveErr := m.saveManagerState(); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to update manager state: %w", saveErr))
		}
	}()

	var fStatus *os.File
	if fStatus, err = os.OpenFile(m.getStatusPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600)); err != nil {
		return err
	}
	defer func() {
		if closeErr := fStatus.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	var fRecord *os.File
	if fRecord, err = os.OpenFile(m.getRecordPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600)); err != nil {
		return err
	}
	defer func() {
		if closeErr := fRecord.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	for _, request := range requests {
		if err = m.insertRequest(fStatus, fRecord, request); err != nil {
			return err
		}
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) insertRequest(fStatus *os.File, fRecord *os.File, request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	if request.GetBulkInfo().JobIndex != m.includedJobs {
		return fmt.Errorf("unexpected request bulkInfo.JobIndex")
	}

	if _, err = fStatus.Write(xtreemstoreS3BulkRequestInitialized.Bytes()); err != nil {
		return
	}

	remotePath := request.GetSync().GetRemotePath()
	if m.includedJobs == 0 {
		_, err = fRecord.WriteString(remotePath)
	} else {
		_, err = fRecord.WriteString("\n" + remotePath)
	}

	m.includedJobs++
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) insertRequest(request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	if request.GetBulkInfo().JobIndex != m.includedJobs {
		return fmt.Errorf("unexpected request bulkInfo.JobIndex")
	}

	if _, err = m.statusHandle.Write(xtreemstoreS3BulkRequestInitialized.Bytes()); err != nil {
		return
	}

	remotePath := request.GetSync().GetRemotePath()
	if m.includedJobs == 0 {
		_, err = m.recordHandle.WriteString(remotePath)
	} else {
		_, err = m.recordHandle.WriteString("\n" + remotePath)
	}

	m.includedJobs++
	return
}
*/

func (m *xtreemstoreS3BulkRetrieveManager) loadManagerState() error {
	f, err := os.OpenFile(m.getManagerPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// includedJobs is reconstructed from the status file, so a missing manager state file does
		// not prevent recovery of appended bulk requests.
		*m.state = xtreemstoreS3BulkRetrieveManagerState{}
	} else {
		defer f.Close()

		if err := json.NewDecoder(f).Decode(m.state); err != nil {
			return err
		}
	}

	statusInfo, err := os.Stat(m.getStatusPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m.includedJobs = 0
		return nil
	}
	m.includedJobs = statusInfo.Size()

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) saveManagerState() (err error) {
	var f *os.File
	if f, err = os.OpenFile(m.getManagerPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0600)); err != nil {
		return
	}

	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	err = json.NewEncoder(f).Encode(m.state)
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) openState() (err error) {
	if err = m.loadManagerState(); err != nil {
		err = fmt.Errorf("failed to load manager state: %w", err)
		return
	}

	if err = os.MkdirAll(m.statePath, 0o700); err != nil {
		return
	}

	if m.statusHandle, err = os.OpenFile(m.getStatusPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600)); err != nil {
		return
	}
	// statusHandle is already assigned, so a failure here will be cleaned up by the caller via closeState().
	if m.recordHandle, err = os.OpenFile(m.getRecordPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600)); err != nil {
		return
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) closeState() (err error) {
	if m.statusHandle != nil {
		err = errors.Join(err, m.statusHandle.Close())
	}
	if m.recordHandle != nil {
		err = errors.Join(err, m.recordHandle.Close())
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkComplete(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestComplete, jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkSent(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestSent, jobIndex)
}
func (m *xtreemstoreS3BulkRetrieveManager) markJobStatus(status xtreemstoreS3BulkRequestStatus, jobIndex int64) error {
	f, err := os.OpenFile(m.getStatusPath(), os.O_WRONLY, os.FileMode(0600))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	_, err = f.WriteAt(status.Bytes(), jobIndex)
	return err
}

func (m *xtreemstoreS3BulkRetrieveManager) getStatusPath() string {
	return path.Join(m.statePath, "status")
}

func (m *xtreemstoreS3BulkRetrieveManager) getRecordPath() string {
	return path.Join(m.statePath, "record")
}

func (m *xtreemstoreS3BulkRetrieveManager) getErrorsPath() string {
	return path.Join(m.statePath, "errors")
}

func (m *xtreemstoreS3BulkRetrieveManager) getManagerPath() string {
	return path.Join(m.statePath, "manager.json")
}

func (m *xtreemstoreS3BulkRetrieveManager) getSessionInfo(ctx context.Context) (*xtreemstoreS3BulkRetrieveSessionInfo, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(XTS_SYSTEM_RETRIEVE_SESSION),
	}

	resp, err := m.s3ApiClient.GetObject(ctx, getObjectInput)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
			return &xtreemstoreS3BulkRetrieveSessionInfo{}, nil
		}
		return nil, err
	}
	defer resp.Body.Close()

	info := &xtreemstoreS3BulkRetrieveSessionInfo{}
	if err := json.NewDecoder(resp.Body).Decode(info); err != nil {
		return nil, err
	}
	return info, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) getSessionBatchInfo(ctx context.Context) ([]xtreemstoreS3BulkRetrieveBatchInfo, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(XTS_SYSTEM_RETRIEVE_BATCH_LIST),
	}

	resp, err := m.s3ApiClient.GetObject(ctx, getObjectInput)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var info []xtreemstoreS3BulkRetrieveBatchInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode retrieve batch info: %w", err)
	}

	return info, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) getSessionBatchKeys(ctx context.Context, batchInfo xtreemstoreS3BulkRetrieveBatchInfo) ([]string, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(fmt.Sprintf(XTS_SYSTEM_RETRIEVE_BATCH_FMT, batchInfo.Number)),
	}

	resp, err := m.s3ApiClient.GetObject(ctx, getObjectInput)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var keys []string
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("decode retrieve batch keys for batch %d: %w", batchInfo.Number, err)
	}

	return keys, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) deleteSessionBatch(ctx context.Context, batchInfo xtreemstoreS3BulkRetrieveBatchInfo) error {
	_, err := m.s3ApiClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(fmt.Sprintf(XTS_SYSTEM_RETRIEVE_BATCH_FMT, batchInfo.Number)),
	})
	if err != nil {
		return fmt.Errorf("delete retrieve batch %d: %w", batchInfo.Number, err)
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) isObjectReadyForDownload(ctx context.Context, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	}
	resp, err := m.s3ApiClient.HeadObject(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
			return false, os.ErrNotExist
		}
		return false, fmt.Errorf("head object for key %q: %w", key, err)
	}

	if resp.Restore == nil {
		return true, nil
	}

	return strings.Contains(*resp.Restore, `ongoing-request="false"`), nil
}

func (m *xtreemstoreS3BulkRetrieveManager) startSession(ctx context.Context) (err error) {
	if m.includedJobs == 0 {
		return fmt.Errorf("retrieve-session requires at least one key")
	}

	previousState := *m.state
	cleanupCreatedSession := func(reason error) error {
		*m.state = previousState
		if cleanupErr := m.destroyRetrieveSession(ctx); cleanupErr != nil {
			return fmt.Errorf("retrieve-session was created but local ownership state could not be persisted, cleanup also failed, and manual intervention is required: %w", errors.Join(reason, cleanupErr))
		}
		return reason
	}

	activeJobStart := m.state.ActiveJobEnd
	activeJobEnd := m.includedJobs
	keys, err := m.getRecords(activeJobStart, activeJobEnd)
	if err != nil {
		return err
	}

	retrieveSessionJson, err := json.Marshal(&xtreemstoreS3BulkRetrieveRequest{Ids: keys})
	if err != nil {
		return fmt.Errorf("failed to marshal retrieve-session request: %w", err)
	}

	_, err = m.s3ApiClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(XTS_SYSTEM_RETRIEVE_SESSION),
		Body:        bytes.NewReader(retrieveSessionJson),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusConflict {
			*m.state = previousState
			return ErrActiveRetrieveSessionAlreadyExists
		}

		return fmt.Errorf("failed to start retrieve-session: %w", err)
	}

	sessionInfo, err := m.getSessionInfo(ctx)
	if err != nil {
		return cleanupCreatedSession(fmt.Errorf("failed to recover retrieve-session ownership state after creation: %w", err))
	}

	m.state.ActiveJobStart = activeJobStart
	m.state.ActiveJobEnd = activeJobEnd
	m.state.ActiveRetrieveId = sessionInfo.RetrieveId

	if err = m.saveManagerState(); err != nil {
		return cleanupCreatedSession(fmt.Errorf("failed to store retrieve-session ownership state after creation: %w", err))
	}

	return nil
}

// destroyRetrieveSession deletes the active retrieve session. This must only be called after an
// active session has been confirmed to belong to the manager.
func (m *xtreemstoreS3BulkRetrieveManager) destroyRetrieveSession(ctx context.Context) error {
	_, err := m.s3ApiClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(XTS_SYSTEM_RETRIEVE_SESSION),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
			return nil
		}
		return fmt.Errorf("destroy retrieve session: %w", err)
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) getActiveRecordsMap() (map[string]int64, error) {
	return m.getRecordsMap(m.state.ActiveJobStart, m.state.ActiveJobEnd)
}

// func (m *xtreemstoreS3BulkRetrieveManager) getRecordsMapFromActiveStart() (map[string]int64, error) {
// 	return m.getRecordsMap(m.state.ActiveJobStart, -1)
// }

func (m *xtreemstoreS3BulkRetrieveManager) getActiveRecords() ([]string, error) {
	return m.getRecords(m.state.ActiveJobStart, m.state.ActiveJobEnd)
}

func (m *xtreemstoreS3BulkRetrieveManager) getRecordsFromActiveStart() ([]string, error) {
	return m.getRecords(m.state.ActiveJobStart, -1)
}

func (m *xtreemstoreS3BulkRetrieveManager) getActiveSessionStatuses() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(m.state.ActiveJobStart, m.state.ActiveJobEnd)
}

func (m *xtreemstoreS3BulkRetrieveManager) getStatusesFromActiveStart() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(m.state.ActiveJobStart, -1)
}

func (m *xtreemstoreS3BulkRetrieveManager) getAllStatuses() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(0, -1)
}

// getRecordsMap returns a mapping of record paths to job indexes for the specified range. Set end
// to -1 to get all mappings beginning from the start index.
func (m *xtreemstoreS3BulkRetrieveManager) getRecordsMap(start int64, end int64) (map[string]int64, error) {
	if end == -1 {
		end = m.includedJobs
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", start, end)
	}

	keyMap := map[string]int64{}
	if start == end {
		return keyMap, nil
	}

	f, err := os.OpenFile(m.getRecordPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for index := int64(0); scanner.Scan(); index++ {
		if index < start {
			continue
		}
		if index >= end {
			break
		}
		keyMap[scanner.Text()] = index
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read bulk entry keys file: %w", err)
	}

	return keyMap, nil
}

// getRecords returns a range of record paths. Set end to -1 to get all records beginning with start.
func (m *xtreemstoreS3BulkRetrieveManager) getRecords(start int64, end int64) ([]string, error) {
	if end == -1 {
		end = m.includedJobs
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", start, end)
	}
	if start == end {
		return []string{}, nil
	}

	f, err := os.OpenFile(m.getRecordPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	keys := make([]string, 0, max(0, int(end-start)))
	scanner := bufio.NewScanner(f)
	for index := int64(0); scanner.Scan(); index++ {
		if index < start {
			continue
		}
		if index >= end {
			break
		}
		keys = append(keys, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read queued records: %w", err)
	}

	return keys, nil
}

// getStatuses returns statuses. Set end to -1 to get all statues beginning with start.
func (m *xtreemstoreS3BulkRetrieveManager) getStatuses(start int64, end int64) (*xtreemstoreS3BulkStatuses, error) {
	if end == -1 {
		end = m.includedJobs
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", start, end)
	}

	statuses := &xtreemstoreS3BulkStatuses{offset: start}
	if start == end {
		return statuses, nil
	}

	f, err := os.OpenFile(m.getStatusPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	statuses.jobCount = end - start
	statuses.jobStatuses = make([]byte, statuses.jobCount)
	if n, err := f.ReadAt(statuses.jobStatuses, int64(statuses.offset)); err != nil {
		return nil, fmt.Errorf("failed to read bulk entry state file: %w", err)
	} else if n != len(statuses.jobStatuses) {
		return nil, fmt.Errorf("invalid bulk entry state")
	}

	return statuses, nil
}
