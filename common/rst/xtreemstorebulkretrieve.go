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
	ErrBulkOperationDestroyed             = errors.New("the bulk operation that staged this request no longer exists (resubmit the job to retrieve the object again)")
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
	// statusAppendHandle is maintained when the manager is open and should only be used for
	// persistent appending new statuses. Do not use this to update statuses; use statusUpdateHandle
	// instead.
	statusAppendHandle *os.File
	// statusUpdateHandle is maintained when the manager is open and should only be used for
	// persistent status updates. Do not use to append new statuses; use statusAppendHandle instead.
	statusUpdateHandle *os.File
	// recordHandle is maintained when the manager is open and is used to append new records. It is
	// imperative that records are only added and never changed for the bulk operation's lifecycle.
	recordHandle *os.File
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

// xtreemstoreS3BulkRetrieveMarkReceived marks a request sent by a bulk operation as received.
func xtreemstoreS3BulkRetrieveMarkReceived(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string) error {
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}
	return manager.MarkReceived(bulkInfo.JobIndex)
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

// xtreemstoreS3BulkRetrieveError reports why a request belonging to a bulk operation cannot proceed,
// or nil when there is nothing to report.
func xtreemstoreS3BulkRetrieveError(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string) error {
	m := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}

	message, err := os.ReadFile(m.getErrorsPath())
	if err == nil {
		if len(message) > 0 {
			return fmt.Errorf("%s", message)
		}
		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unable to retrieve bulk operation error message for %q: %w", bulkInfo.Operation, err)
	}

	if _, err := os.Stat(m.getStatusPath()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrBulkOperationDestroyed
		}
		return fmt.Errorf("unable to determine whether bulk operation %q still exists: %w", bulkInfo.Operation, err)
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	request.GetBulkInfo().SetJobIndex(m.includedJobs)

	if _, err = m.statusAppendHandle.Write(xtreemstoreS3BulkRequestAdded.Bytes()); err != nil {
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

// Cancel releases any xtreemstore-side resources reserved for this bulk operation (the active
// retrieve-session and its batches, if any) and records reason to the errors file so any request
// that already passed IsWorkRequestReady can see why the operation was cancelled. It deliberately
// does not resolve individual jobIndexes or delete local state:
//   - Requests still Added were never handed off anywhere else, so nothing downstream needs to hear
//     about them; they're simply dropped.
//   - Requests already Sent/Received depend on their own Job's normal lifecycle
//     (IsWorkRequestReady, ExecuteWorkRequestPart, or CompleteWorkRequests) to resolve, and that Job
//     may still be in flight. Deleting local state out from under it here would make its own
//     mark-complete calls fail (or worse, collide with a future reuse of these files). Local state is
//     only removed via Destroy, once the owning builder job itself is torn down for good.
//   - Requests already Complete/CompleteAck are already terminal and are left as-is.
//
// wait returns nil once reason has been recorded and any active retrieve-session has been released;
// it only returns an error if one of those steps itself fails, so a clean cancel (including one where
// the session had already completed and self-destroyed) is not reported as a failure.
func (m *xtreemstoreS3BulkRetrieveManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error) {
	cancelWalkCh := make(chan *BulkStreamPathResult)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(cancelWalkCh)

		if err := m.recordError(reason); err != nil {
			return fmt.Errorf("failed to record cancellation reason: %w", err)
		}

		sessionInfo, err := m.getSessionInfo(ctx)
		if err != nil {
			return fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)
		}

		if sessionInfo != nil && sessionInfo.Active && sessionInfo.RetrieveId == m.state.SessionRetrieveId {
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
		}

		return nil
	})

	return cancelWalkCh, g.Wait, nil
}

// recordError writes reason to the operation's shared errors file, overwriting any previous content.
func (m *xtreemstoreS3BulkRetrieveManager) recordError(reason error) error {
	if err := os.WriteFile(m.getErrorsPath(), []byte(reason.Error()), 0o600); err != nil {
		return fmt.Errorf("failed to write bulk-retrieve errors file: %w", err)
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Close(ctx context.Context) error {
	if err := m.closeState(); err != nil {
		return fmt.Errorf("failed to close bulk-retrieve manager: %w", err)
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Destroy(ctx context.Context) error {
	if err := m.deleteState(); err != nil {
		return fmt.Errorf("failed to delete bulk-retrieve state: %w", err)
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) deleteState() (err error) {
	err = appendError(err, removeIfExists(m.getStatusPath()))
	err = appendError(err, removeIfExists(m.getRecordPath()))
	err = appendError(err, removeIfExists(m.getErrorsPath()))
	err = appendError(err, removeIfExists(m.getManagerPath()))
	err = appendError(err, removeIfExists(persistentTmpPath(m.getManagerPath())))
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) execute(ctx context.Context, walkCh chan<- *BulkStreamPathResult) (reschedule bool, delay time.Duration, err error) {
	defer func() {
		err = appendError(err, m.saveManagerState())
	}()

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
			return false, 0, fmt.Errorf("retrieve-session completed successfully but the active session could not be destroyed and manual intervention is required: %w", err)
		}
		if err = m.saveManagerState(); err != nil {
			return false, 0, fmt.Errorf("retrieve-session was completed and destroyed successfully but the state could not be saved: %w", err)
		}

		if m.includedJobs == m.state.SessionJobEnd {
			// No more requests were added since the previous session was started.
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

	if sessionInfo != nil && sessionInfo.Active {
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

	for _, batchInfo := range batchInfos {
		batchComplete, batchErr := m.processSessionBatch(ctx, walkCh, batchInfo)
		if batchErr != nil || !batchComplete {
			return false, batchErr
		}
		if deleteErr := m.deleteSessionBatch(ctx, batchInfo); deleteErr != nil {
			return false, fmt.Errorf("failed to delete retrieve-session batch: %w", deleteErr)
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

	defer func() {
		err = appendError(err, m.saveManagerState())
	}()

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

	return
}

// sendBulkResult delivers result on walkCh, giving up if ctx is cancelled. The ctx case is required:
// the consumer in buildercontroller stops draining as soon as its context is cancelled, so an
// unconditional send would strand this producer forever once the channel buffer fills, and the
// manager's own getResults() (which waits on that producer) would deadlock the whole builder job.
func sendBulkResult(ctx context.Context, walkCh chan<- *BulkStreamPathResult, result *BulkStreamPathResult) error {
	select {
	case walkCh <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processSessionBatchKey advances one key's bulk-retrieve state machine a step and reports whether
// it has reached a terminal (Complete/CompleteAck) state.
func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatchKey(
	ctx context.Context,
	walkCh chan<- *BulkStreamPathResult,
	key string,
	jobIndex int64,
	status xtreemstoreS3BulkRequestStatus,
) (terminal bool, err error) {
	switch status {
	case xtreemstoreS3BulkRequestAdded, xtreemstoreS3BulkRequestSent:
		// A request that is xtreemstoreS3BulkRequestSent means that remote never received the the
		// job request as the result of a sync worker crash; otherwise, the status would already be
		// xtreemstoreS3BulkRequestReceived.
		if ready, readyErr := m.isObjectReadyForDownload(ctx, key); readyErr != nil {
			if !errors.Is(readyErr, os.ErrNotExist) {
				err = fmt.Errorf("failed to determine restore state. Record: %s, Status: %v: %w", key, status, readyErr)
				return
			}
			result := &BulkStreamPathResult{
				Path:     key,
				RstId:    m.rstId,
				BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
				Err:      &RequestCancelError{Reason: fmt.Errorf("object no longer exists")},
			}
			if err = sendBulkResult(ctx, walkCh, result); err != nil {
				return
			}
			if err := m.MarkCompleteAck(jobIndex); err != nil {
				return false, fmt.Errorf("remote object no longer exists but failed to mark bulk job request as complete. Record: %s, Status: %v", key, status)
			}

			terminal = true
		} else if ready {
			result := &BulkStreamPathResult{
				Path:     key,
				RstId:    m.rstId,
				BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
			}
			if err = sendBulkResult(ctx, walkCh, result); err != nil {
				return
			}
			if err := m.MarkSent(jobIndex); err != nil {
				return false, fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", key, status, err)
			}
		}
	case xtreemstoreS3BulkRequestReceived:
	case xtreemstoreS3BulkRequestComplete:
		if err := m.MarkCompleteAck(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete and acknowledged. Record: %s, Status: %v", key, status)
		}
		terminal = true
	case xtreemstoreS3BulkRequestCompleteAcked:
		terminal = true
	default:
		result := &BulkStreamPathResult{
			Path:     key,
			RstId:    m.rstId,
			BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
			Err:      fmt.Errorf("unexpected record status. Record: %s, Status: %v", key, status),
		}
		if err = sendBulkResult(ctx, walkCh, result); err != nil {
			return
		}
		if err := m.MarkCompleteAck(jobIndex); err != nil {
			return false, fmt.Errorf("failed to mark bulk job request as complete. Record: %s, Status: %v: %w", key, status, err)
		}
		terminal = true
	}

	return
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
		// xtreemstore retrieve process is not complete for the object since it has not be
		// converted into standard storage class and is therefore not available yet.
		return false, nil
	default:
		return false, fmt.Errorf("unexpected storage class, %s", resp.StorageClass)
	}
}

func (m *xtreemstoreS3BulkRetrieveManager) loadManagerState() error {
	*m.state = xtreemstoreS3BulkRetrieveManagerState{}

	data, err := os.ReadFile(m.getManagerPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err := json.Unmarshal(data, m.state); err != nil {
		return err
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

func (m *xtreemstoreS3BulkRetrieveManager) saveManagerState() error {
	return m.writeManagerState(m.state)
}

// createManagerFile creates a persistent file for operation's manager state. An existing file
// is left untouched.
func (m *xtreemstoreS3BulkRetrieveManager) createManagerFile() error {
	if _, err := os.Stat(m.getManagerPath()); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return m.writeManagerState(&xtreemstoreS3BulkRetrieveManagerState{})
}

func (m *xtreemstoreS3BulkRetrieveManager) writeManagerState(state *xtreemstoreS3BulkRetrieveManagerState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to encode manager state: %w", err)
	}

	return writePersistentFile(m.getManagerPath(), data, 0600)
}

func (m *xtreemstoreS3BulkRetrieveManager) openState() (err error) {
	if err := os.MkdirAll(m.getStateMountPath(), 0o700); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	if err := m.createManagerFile(); err != nil {
		return fmt.Errorf("failed to create manager state file: %w", err)
	}

	if err := m.loadManagerState(); err != nil {
		return fmt.Errorf("failed to load manager state: %w", err)
	}

	defer func() {
		if err != nil {
			m.closeState()
		}
	}()

	if m.statusAppendHandle, err = m.openStatusFileForAppend(); err != nil {
		err = fmt.Errorf("failed to open status append file: %w", err)
	} else if m.statusUpdateHandle, err = m.openStatusFileForUpdate(); err != nil {
		err = fmt.Errorf("failed to open status update file: %w", err)
	} else if m.recordHandle, err = m.openRecordFileForAppend(); err != nil {
		err = fmt.Errorf("failed to open record file: %w", err)
	} else if err = m.createErrorsFile(); err != nil {
		err = fmt.Errorf("failed to create errors file: %w", err)
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) closeState() (err error) {
	if m.recordHandle != nil {
		err = appendError(err, m.recordHandle.Close())
		m.recordHandle = nil
	}

	if m.statusUpdateHandle != nil {
		err = appendError(err, m.statusUpdateHandle.Close())
		m.statusUpdateHandle = nil
	}

	if m.statusAppendHandle != nil {
		err = appendError(err, m.statusAppendHandle.Close())
		m.statusAppendHandle = nil
	}

	return err
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkSent(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestSent, jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkReceived(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestReceived, jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkComplete(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestComplete, jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkCompleteAck(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestCompleteAcked, jobIndex)
}

func (m *xtreemstoreS3BulkRetrieveManager) markJobStatus(status xtreemstoreS3BulkRequestStatus, jobIndex int64) (err error) {
	f := m.statusUpdateHandle
	if f == nil {
		if f, err = m.openStatusFileForUpdate(); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// The operation's state has been destroyed, so there is nothing left to record.
				// Only Destroy deletes state, and it runs once every request the builder sent has
				// reached a terminal bulk status, so a job that outlives its builder (a FAILED
				// download being cancelled or retried) has no operation left to report to. Treating
				// this as an error would make such a job impossible to resolve, since cancelling or
				// retrying it resolves its bulk request first.
				return nil
			}
			return
		}
		defer f.Close()
	}

	_, err = f.WriteAt(status.Bytes(), jobIndex)
	return err
}

func (m *xtreemstoreS3BulkRetrieveManager) openStatusFileForAppend() (*os.File, error) {
	return openFileForAppend(m.getStatusPath())
}

func (m *xtreemstoreS3BulkRetrieveManager) openStatusFileForUpdate() (*os.File, error) {
	return openFileForUpdate(m.getStatusPath())
}

// createErrorsFile ensures the operation's errors file exists so its absence unambiguously means the
// operation's state was destroyed. It must not truncate or fail when the file is already there: a
// reason recorded by a previous Cancel has to survive the builder reopening state.
func (m *xtreemstoreS3BulkRetrieveManager) createErrorsFile() error {
	return touchFile(m.getErrorsPath())
}

func (m *xtreemstoreS3BulkRetrieveManager) openRecordFileForAppend() (*os.File, error) {
	return openFileForAppend(m.getRecordPath())
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
			return nil, nil
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
			return fmt.Errorf("retrieve-session was created but local ownership state could not be persisted, cleanup also failed, and manual intervention is required: %w; %w", reason, cleanupErr)
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
	if err != nil || sessionInfo == nil {
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
		if !(errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey")) {
			return fmt.Errorf("unable to destroy retrieve session: %w", err)
		}
	}

	m.state.SessionRetrieveId = ""
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) getActiveRecordsMap() (map[string]int64, error) {
	return m.getRecordsMap(m.state.SessionJobStart, m.state.SessionJobEnd)
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

func (m *xtreemstoreS3BulkRetrieveManager) getActiveSessionStatuses() (*xtreemstoreS3BulkStatuses, error) {
	return m.getStatuses(m.state.SessionJobStart, m.state.SessionJobEnd)
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
