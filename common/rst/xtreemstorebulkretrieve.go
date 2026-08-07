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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

const (
	XTS_SYSTEM                     = ".xts-system"
	XTS_SYSTEM_ERRORS              = XTS_SYSTEM + "/errors"
	XTS_SYSTEM_RETRIEVE_SESSION    = XTS_SYSTEM + "/retrieve-session.json"
	XTS_SYSTEM_RETRIEVE_BATCH_LIST = XTS_SYSTEM + "/retrieve-batch-list.json"
	XTS_SYSTEM_RETRIEVE_BATCH_FMT  = XTS_SYSTEM + "/retrieve-batch-%d.json"

	reschedulePoolingDelay = 1 * time.Minute // This should be configurable
	rescheduleMaxDelay     = 5 * time.Minute // This should be configurable
)

var (
	ErrActiveRetrieveSessionAlreadyExists = errors.New("active retrieve-session already exists")
)

type xtreemstoreS3BulkRetrieveManager struct {
	s3ApiClient
	rstId          uint32
	bucket         string
	operation      string
	mountPath      string
	stateMountPath string
	state          *xtreemstoreS3BulkRetrieveManagerState
	includedJobs   int64
	statusHandle   *os.File
	recordHandle   *os.File
}

var _ clientBulkOperation = &xtreemstoreS3BulkRetrieveManager{}

type xtreemstoreS3BulkRetrieveManagerState struct {
	SessionRetrieveId string `json:"active-retrieve-id"`
	SessionJobStart   int64  `json:"active-job-start"`
	SessionJobEnd     int64  `json:"active-job-end"`
}

type xtreemstoreS3BulkRetrieveSessionInfo struct {
	Active     bool      `json:"active"`
	RetrieveId string    `json:"retrieve-id"`
	Started    time.Time `json:"started"`
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

// xtreemstoreS3BulkRetrieveMarkComplete marks a request sent by a bulk operation as complete.
func xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string) error {
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}
	return manager.MarkComplete(bulkInfo.JobIndex)
}

// xtreemstoreS3BulkRetrieveError retrieves any bulk operation errors. If no errors were found then
// nil will be returned.
func xtreemstoreS3BulkRetrieveError(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string) error {
	m := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
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

func (m *xtreemstoreS3BulkRetrieveManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	request.GetBulkInfo().SetJobIndex(m.includedJobs)

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

func (m *xtreemstoreS3BulkRetrieveManager) CancelRequest(ctx context.Context, jobIndex int64, reason error) error {
	return m.MarkComplete(jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) Execute(ctx context.Context) (walkCh <-chan *BulkStreamPathResult, getResults BulkExecuteResultFn, err error) {
	var reschedule bool
	var delay time.Duration
	var executeErr error

	executeWalkCh := make(chan *BulkStreamPathResult, 128)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		defer close(executeWalkCh)
		reschedule, delay, executeErr = m.execute(ctx, executeWalkCh)
	})

	getResults = func() *SchedulingResult {
		wg.Wait()
		return &SchedulingResult{
			Reschedule: reschedule,
			Delay:      delay,
			Err:        executeErr,
		}
	}
	return executeWalkCh, getResults, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error) {
	cancelWalkCh := make(chan *BulkStreamPathResult, 128)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(cancelWalkCh)
		if reason != nil {
			if err := m.CancelRequests(ctx, reason, cancelWalkCh); err != nil {
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
				case xtreemstoreS3BulkRequestCompleteAck:
				default:
				}
			}
		}

		sessionInfo, err := m.getSessionInfo(ctx)
		if err != nil {
			return fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)
		}

		if !sessionInfo.Active || sessionInfo.RetrieveId != m.state.SessionRetrieveId {
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
	})

	return cancelWalkCh, g.Wait, nil
}

// CancelRequests sends the key for each object that has not been retrieved.
func (m *xtreemstoreS3BulkRetrieveManager) CancelRequests(ctx context.Context, reason error, walkCh chan<- *BulkStreamPathResult) error {
	records, err := m.getRecordsFromActiveStart()
	if err != nil {
		return fmt.Errorf("unable to determine records to cancel: %w", err)
	}
	statuses, err := m.getStatusesFromActiveStart()
	if err != nil {
		return fmt.Errorf("unable to determine records to cancel: %w", err)
	}

	cancelErr := &RequestCancelError{Reason: reason}
	for index, record := range records {
		jobIndex := m.state.SessionJobStart + int64(index)
		if status, err := statuses.Get(jobIndex); err != nil {
			return fmt.Errorf("unable to send failed job request. Record: %s, Status: %v: %w", record, status, err)
		} else {
			switch status {
			case xtreemstoreS3BulkRequestInitialized:
				walkCh <- &BulkStreamPathResult{
					Path:  record,
					RstId: m.rstId,
					Err:   cancelErr,
					BulkInfo: &flex.BulkJobRequestInfo{
						StateMountPath: m.stateMountPath,
						Operation:      m.operation,
						JobIndex:       jobIndex,
					},
				}
				if err := m.MarkComplete(jobIndex); err != nil {
					return fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", record, status, err)
				}
			case xtreemstoreS3BulkRequestSent, xtreemstoreS3BulkRequestComplete, xtreemstoreS3BulkRequestCompleteAck:
			default:
				walkCh <- &BulkStreamPathResult{
					Path:  record,
					RstId: m.rstId,
					Err:   fmt.Errorf("unknown record status: %w", cancelErr),
					BulkInfo: &flex.BulkJobRequestInfo{
						StateMountPath: m.stateMountPath,
						Operation:      m.operation,
						JobIndex:       jobIndex,
					},
				}
				if err := m.MarkComplete(jobIndex); err != nil {
					return fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", record, status, err)
				}
			}
		}
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Close(ctx context.Context) error {
	return m.closeState()
}

func (m *xtreemstoreS3BulkRetrieveManager) deleteState() error {
	var errs []error
	errs = append(errs, removeIfExists(m.getStatusPath()))
	errs = append(errs, removeIfExists(m.getRecordPath()))
	errs = append(errs, removeIfExists(m.getErrorsPath()))
	errs = append(errs, removeIfExists(m.getManagerPath()))
	return errors.Join(errs...)
}

// removeIfExists removes the file at path, returning nil if it does not exist since the state
// files aren't guaranteed to have been created yet when deleteState() is called.
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) execute(ctx context.Context, walkCh chan<- *BulkStreamPathResult) (reschedule bool, delay time.Duration, err error) {
	for {
		if ready, err := m.ensureSessionActive(ctx); err != nil {
			return false, 0, err
		} else if !ready {
			return true, rescheduleMaxDelay, nil
		}

		if batchesComplete, err := m.processSessionBatches(ctx, walkCh); err != nil {
			return false, 0, err
		} else if !batchesComplete {
			return true, reschedulePoolingDelay, nil
		}

		if err = m.destroyRetrieveSession(ctx); err != nil {
			return false, 0, fmt.Errorf("retrieve-session completed successfully, but the active session could not be destroyed and manual intervention is required: %w", err)
		}

		if m.includedJobs == m.state.SessionJobEnd {
			// no more requests were add
			break
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
		ready = sessionInfo.RetrieveId == m.state.SessionRetrieveId
	} else if startSessionErr := m.startSession(ctx); startSessionErr != nil {
		if !errors.Is(startSessionErr, ErrActiveRetrieveSessionAlreadyExists) {
			err = fmt.Errorf("failed to start retrieve-session: %w", startSessionErr)
		}
	} else {
		ready = true
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatches(ctx context.Context, walkCh chan<- *BulkStreamPathResult) (success bool, err error) {
	batchInfos, err := m.getSessionBatchInfo(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to load retrieve-session batch info: %w", err)
	}

	defer func() {
		m.saveManagerState()
	}()

	for _, batchInfo := range batchInfos {
		if batchComplete, err := m.processSessionBatch(ctx, walkCh, batchInfo); err != nil || !batchComplete {
			return false, err
		}
	}
	return true, nil
}

// processSessionBatch processes the retrieve-session batch and returns whether it completed.
func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatch(
	ctx context.Context,
	walkCh chan<- *BulkStreamPathResult,
	batchInfo xtreemstoreS3BulkRetrieveBatchInfo,
) (allComplete bool, err error) {
	activeRecordMap, err := m.getActiveRecordsMap()
	if err != nil {
		return false, fmt.Errorf("failed to get record mappings for the active retrieve-session: %w", err)
	}

	var activeStatuses *xtreemstoreS3BulkStatuses
	if activeStatuses, err = m.getActiveSessionStatuses(); err != nil {
		return false, fmt.Errorf("failed to get request statuses for active retrieve-session: %w", err)
	}

	var keys []string
	if keys, err = m.getSessionBatchKeys(ctx, batchInfo); err != nil {
		return false, fmt.Errorf("failed to retrieve batch keys: %w", err)
	}

	allComplete = true
	for _, key := range keys {
		jobIndex, ok := activeRecordMap[key]
		if !ok {
			return false, fmt.Errorf("unable to determine status for key: %s", key)
		}
		status, statusErr := activeStatuses.Get(jobIndex)
		if statusErr != nil {
			return false, fmt.Errorf("unable to determine status: %w", statusErr)
		}

		if done, err := m.processSessionBatchKey(ctx, walkCh, key, jobIndex, status); err != nil {
			return false, err
		} else if !done {
			allComplete = false
		}
	}

	if allComplete {
		if err = m.deleteSessionBatch(ctx, batchInfo); err != nil {
			err = fmt.Errorf("failed to delete retrieve-session batch: %w", err)
		}
	}
	return
}

// processSessionBatchKey advances one key's bulk-retrieve state machine a step and reports whether
// it has reached a terminal (Complete/CompleteAck) state. Reporting per-key rather than mutating a
// shared flag from inside the switch means a batch can never be marked complete just because
// whichever key happened to be checked last was already done.
func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatchKey(
	ctx context.Context,
	walkCh chan<- *BulkStreamPathResult,
	key string,
	jobIndex int64,
	status xtreemstoreS3BulkRequestStatus,
) (done bool, err error) {
	result := &BulkStreamPathResult{
		Path:     key,
		RstId:    m.rstId,
		BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
	}

	switch status {
	case xtreemstoreS3BulkRequestInitialized:
		ready, err := m.isObjectReadyForDownload(ctx, key)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return false, fmt.Errorf("failed to determine restore state. Record: %s, Status: %v: %w", key, status, err)
			}
			result.Err = &RequestCancelError{Reason: fmt.Errorf("object no longer exists")}
			walkCh <- result
			if err := m.MarkCompleteAck(jobIndex); err != nil {
				return false, fmt.Errorf("remote object no longer exists but failed to mark bulk job request as complete. Record: %s, Status: %v", key, status)
			}
			return true, nil
		}
		if !ready {
			return false, nil
		}
		walkCh <- result
		if err := m.MarkSent(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", key, status, err)
		}
		return false, nil
	case xtreemstoreS3BulkRequestSent:
		return false, nil
	case xtreemstoreS3BulkRequestComplete:
		if err := m.MarkCompleteAck(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete and acknowledged. Record: %s, Status: %v", key, status)
		}
		return true, nil
	case xtreemstoreS3BulkRequestCompleteAck:
		return true, nil
	default:
		result.Err = fmt.Errorf("unexpected record status. Record: %s, Status: %v", key, status)
		walkCh <- result
		if err := m.MarkCompleteAck(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", key, status, err)
		}
		return true, nil
	}
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

	switch resp.StorageClass {
	case types.StorageClassStandard:
		return true, nil
	case types.StorageClassGlacier:
		return resp.Restore != nil && strings.Contains(*resp.Restore, `ongoing-request="false"`), nil
	default:
		return false, fmt.Errorf("unexpected storage class, %s", resp.StorageClass)
	}

}

func (m *xtreemstoreS3BulkRetrieveManager) loadManagerState() error {
	f, err := os.OpenFile(m.getManagerPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		*m.state = xtreemstoreS3BulkRetrieveManagerState{}
	} else {
		defer f.Close()
		if err := json.NewDecoder(f).Decode(m.state); err != nil {
			return err
		}
	}

	// includedJobs is reconstructed from the status file rather than persisted in manager.json, so
	// it stays correct even when manager.json doesn't exist (or predates the status file). It is the
	// sole source of truth for the next JobIndex to assign, so this must run on every load.
	if statusInfo, err := os.Stat(m.getStatusPath()); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m.includedJobs = 0
	} else {
		m.includedJobs = statusInfo.Size()
	}

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

	if err = os.MkdirAll(m.getStateMountPath(), 0o700); err != nil {
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

func (m *xtreemstoreS3BulkRetrieveManager) MarkCompleteAck(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestCompleteAck, jobIndex)
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

func (m *xtreemstoreS3BulkRetrieveManager) getStateMountPath() string {
	return path.Join(m.mountPath, m.stateMountPath, m.operation)
}

func (m *xtreemstoreS3BulkRetrieveManager) getStatusPath() string {
	return path.Join(m.getStateMountPath(), "status")
}

func (m *xtreemstoreS3BulkRetrieveManager) getRecordPath() string {
	return path.Join(m.getStateMountPath(), "record")
}

func (m *xtreemstoreS3BulkRetrieveManager) getErrorsPath() string {
	return path.Join(m.getStateMountPath(), "errors")
}

func (m *xtreemstoreS3BulkRetrieveManager) getManagerPath() string {
	return path.Join(m.getStateMountPath(), "manager.json")
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

	activeJobStart := m.state.SessionJobEnd
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

	m.state.SessionJobStart = activeJobStart
	m.state.SessionJobEnd = activeJobEnd
	m.state.SessionRetrieveId = sessionInfo.RetrieveId

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
	return m.getRecordsMap(m.state.SessionJobStart, m.state.SessionJobEnd)
}

// func (m *xtreemstoreS3BulkRetrieveManager) getRecordsMapFromActiveStart() (map[string]int64, error) {
// 	return m.getRecordsMap(m.state.ActiveJobStart, -1)
// }

func (m *xtreemstoreS3BulkRetrieveManager) getRecordsFromActiveStart() ([]string, error) {
	return m.getRecords(m.state.SessionJobStart, -1)
}

func (m *xtreemstoreS3BulkRetrieveManager) getActiveSessionStatuses() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(m.state.SessionJobStart, m.state.SessionJobEnd)
}

func (m *xtreemstoreS3BulkRetrieveManager) getStatusesFromActiveStart() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(m.state.SessionJobStart, -1)
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
