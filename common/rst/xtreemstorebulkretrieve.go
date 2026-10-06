package rst

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
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

	DefaultPollDelay  = 15 * time.Second
	DefaultRetryDelay = 45 * time.Second

	// restoreProbeWorkerMultiplier scales GOMAXPROCS to set how many restore-readiness checks are
	// done concurrently.
	restoreProbeWorkerMultiplier = 4.0
)

// xtreemstoreS3BulkRetrieveBatchEntry is one key of a retrieve-session batch, resolved to the bulk
// job request behind it and, once probeBatchReadiness has run, to whether the object is off tape.
type xtreemstoreS3BulkRetrieveBatchEntry struct {
	key      string
	jobIndex int64
	// info is the status file record for jobIndex: the request's status and the job reserved for
	// it. Both are read once for the whole batch, so the job ID is already in hand when a result
	// for this entry has to carry it.
	info xtreemstoreS3BulkInfo
	// ready and readyErr are isObjectReadyForDownload's result for key, and are only meaningful
	// when needsRestoreProbe reports the entry's status depends on them.
	ready    bool
	readyErr error
}

// needsRestoreProbe reports whether advancing this entry depends on the object's restore state. An
// entry whose request is already with remote does not: its status moves on remote's word, not the
// object's, so probing it would spend a HEAD to learn nothing.
func (e *xtreemstoreS3BulkRetrieveBatchEntry) needsRestoreProbe() bool {
	return e.info.status == xtreemstoreS3BulkRequestAdded || e.info.status == xtreemstoreS3BulkRequestSent
}

var (
	ErrActiveRetrieveSessionAlreadyExists = errors.New("active retrieve-session already exists")
	ErrBulkOperationDestroyed             = errors.New("the bulk operation that staged this request no longer exists (resubmit the job to retrieve the object again)")
)

type xtreemstoreS3BulkRetrieveManager struct {
	s3ApiClient
	rstId     uint32
	bucket    string
	operation string
	mountPath string
	// stateRoot is the configured state root, which stateMountPath must lie under. See openStateRoot.
	stateRoot      string
	stateMountPath string
	// stateDir is the operation's state directory, held open from openState until closeState. Every
	// state file is reached through it rather than by path, for the reasons openStateRoot gives.
	stateDir     *os.Root
	state        *xtreemstoreS3BulkRetrieveManagerState
	includedJobs int64
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
	// recordBytes is the size of the record file, which is where the next path will land. The
	// manager holds the only handle that appends to that file and AddRequest is serialized by the
	// caller, so this tracks the file without re-reading its size on every request. openState sets
	// it, after trimming any orphaned bytes a crash left behind.
	recordBytes           int64
	batchPollDelay        time.Duration
	sessionBusyRetryDelay time.Duration
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
func xtreemstoreS3BulkRetrieveMarkReceived(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string, stateRoot string) error {
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateRoot:      stateRoot,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}
	return manager.MarkReceived(bulkInfo.JobIndex)
}

// xtreemstoreS3BulkRetrieveMarkComplete marks a request sent by a bulk operation as complete.
func xtreemstoreS3BulkRetrieveMarkComplete(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string, stateRoot string) error {
	manager := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateRoot:      stateRoot,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}
	return manager.MarkComplete(bulkInfo.JobIndex)
}

// xtreemstoreS3BulkRetrieveError reports why a request belonging to a bulk operation cannot proceed,
// or nil when there is nothing to report.
func xtreemstoreS3BulkRetrieveError(bulkInfo *flex.BulkJobRequestInfo, rstId uint32, mountPath string, stateRoot string) error {
	m := &xtreemstoreS3BulkRetrieveManager{
		rstId:          rstId,
		mountPath:      mountPath,
		stateRoot:      stateRoot,
		stateMountPath: bulkInfo.StateMountPath,
		operation:      bulkInfo.Operation,
	}

	dir, err := m.openStateDir(false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrBulkOperationDestroyed
		}
		return fmt.Errorf("unable to open the state of bulk operation %q: %w", bulkInfo.Operation, err)
	}
	defer dir.Close()

	message, err := dir.ReadFile(xtreemstoreS3BulkErrorsFileName)
	if err == nil {
		if len(message) > 0 {
			return fmt.Errorf("%s", message)
		}
		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unable to retrieve bulk operation error message for %q: %w", bulkInfo.Operation, err)
	}

	if _, err := dir.Stat(xtreemstoreS3BulkStatusFileName); err != nil {
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
	if !request.HasReserveJobId() {
		return fmt.Errorf("missing request reserveJobId")
	}

	remotePath := request.GetSync().GetRemotePath()
	if len(remotePath) > xtreemstoreS3BulkMaxPathLen {
		return fmt.Errorf("remote path is %d bytes which exceeds the %d byte maximum a bulk request record can describe", len(remotePath), xtreemstoreS3BulkMaxPathLen)
	}

	request.GetBulkInfo().SetJobIndex(m.includedJobs)

	// The path is written first and the status record second, because the status record is what
	// makes the request exist: it carries the index, the job and the pointer to the path. Nothing
	// reads the record file except through a status record, so bytes written here and not followed
	// by one are unreachable, and openState trims them off the end.
	//
	// The reverse order would commit a request whose path may never arrive. Its index would be
	// counted, its record would name a job the builder goes on to discard, and every later path
	// would sit one position out of step with the status record that points at it.
	pathOffset := m.recordBytes
	written, writeErr := m.recordHandle.WriteString(remotePath + xtreemstoreS3BulkPathTerminator)
	// A short write reports the bytes it did commit alongside the error, and those bytes are on
	// disk. The counter has to move past them or the next request would record an offset pointing
	// into this partial path.
	m.recordBytes += int64(written)
	if writeErr != nil {
		return writeErr
	}

	// The status, the reserved job and the path pointer go out as one record, so a torn append can
	// only leave a partial record at the end of the file, which openState truncates away. Writing
	// them separately would let a crash land between them and leave an index that is missing one of
	// the three, after which every later record would be read against the wrong index.
	var record [xtreemstoreS3BulkRecordLen]byte
	record[0] = byte(xtreemstoreS3BulkRequestAdded)
	copy(record[xtreemstoreS3BulkJobIdOffset:], request.GetReserveJobId())
	binary.BigEndian.PutUint64(record[xtreemstoreS3BulkPathOffsetOffset:], uint64(pathOffset))
	binary.BigEndian.PutUint16(record[xtreemstoreS3BulkPathLenOffset:], uint16(len(remotePath)))
	if _, err = m.statusAppendHandle.Write(record[:]); err != nil {
		return
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

	getResults = func() *BulkExecuteResult {
		wg.Wait()
		return &BulkExecuteResult{
			Reschedule: reschedule,
			Delay:      delay,
			Err:        executeErr,
		}
	}
	return executeWalkCh, getResults, nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error) {
	cancelWalkCh := make(chan *BulkStreamPathResult)

	ctx, cancel, checkpoint := WithCancellationDelay(ctx, checkpointGrace)
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer cancel()
		defer close(cancelWalkCh)

		if reason == nil || reason.Error() == "" {
			reason = errors.New("bulk operation that staged this request was cancelled")
		} else {
			reason = fmt.Errorf("bulk operation that staged this request was cancelled: %w", reason)
		}

		if err := m.recordError(reason); err != nil {
			return fmt.Errorf("failed to record cancellation reason: %w", err)
		}

		recordsMap, err := m.getRecordsMap(m.state.SessionJobStart, -1)
		if err != nil {
			return fmt.Errorf("failed to get record mappings for the active retrieve-session: %w", err)
		}

		activeInfos, err := m.getBulkInfos(m.state.SessionJobStart, -1)
		if err != nil {
			return fmt.Errorf("failed to get request records for active retrieve-session: %w", err)
		}

		for key, jobIndex := range recordsMap {
			checkpoint(checkpointGrace)
			info, infoErr := activeInfos.Get(jobIndex)
			if infoErr != nil {
				return fmt.Errorf("unable to determine status: %w", infoErr)
			}

			status := info.status
			if status == xtreemstoreS3BulkRequestComplete || status == xtreemstoreS3BulkRequestCompleteAcked {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case cancelWalkCh <- &BulkStreamPathResult{
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: m.stateMountPath,
					Operation:      m.operation,
					JobIndex:       jobIndex,
				},
				RstId:         m.rstId,
				Path:          key,
				ReservedJobId: info.jobId,
				Err:           &RequestCancelError{Reason: reason},
			}:
			}
		}

		checkpoint(checkpointGrace)
		sessionInfo, err := m.getSessionInfo(ctx)
		if err != nil {
			return fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)
		}

		if sessionInfo != nil && sessionInfo.Active && sessionInfo.RetrieveId == m.state.SessionRetrieveId {
			checkpoint(checkpointGrace)
			batchInfos, err := m.getSessionBatchInfo(ctx)
			if err != nil {
				return fmt.Errorf("failed to get retrieve-session batch info: %w", err)
			}

			for _, batchInfo := range batchInfos {
				checkpoint(checkpointGrace)
				if err := m.deleteSessionBatch(ctx, batchInfo); err != nil {
					return fmt.Errorf("failed to delete retrieve-session batch: %w", err)
				}
			}

			checkpoint(checkpointGrace)
			if err := m.destroyRetrieveSession(ctx); err != nil {
				return fmt.Errorf("failed to deactivate retrieve-session: %w", err)
			}
		}

		return nil
	})

	return cancelWalkCh, g.Wait, nil
}

// recordError records reason as the operation's cancellation reason which
// xtreemstoreS3BulkRetrieveError returns. Only the first recorded error will be returned.
func (m *xtreemstoreS3BulkRetrieveManager) recordError(reason error) error {
	message := "the bulk operation that staged this request was cancelled for an unspecified reason"
	if reason != nil && reason.Error() != "" {
		message = reason.Error()
	}

	if m.stateDir == nil {
		return errBulkStateNotOpen
	}
	if err := m.stateDir.WriteFile(xtreemstoreS3BulkErrorsFileName, []byte(message), stateFilePerm); err != nil {
		return fmt.Errorf("failed to write errors file for bulk operation: %w", err)
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Close(ctx context.Context) error {
	if err := m.closeState(); err != nil {
		return fmt.Errorf("failed to close bulk manager: %w", err)
	}
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) Destroy(ctx context.Context) error {
	if err := m.deleteState(); err != nil {
		return fmt.Errorf("failed to destroy bulk manager: %w", err)
	}
	return nil
}

// deleteState removes the operation's state files. Destroy may run whether or not the manager is
// open, so a closed manager opens the state directory for the duration. A directory that is already
// gone has nothing left to delete.
func (m *xtreemstoreS3BulkRetrieveManager) deleteState() error {
	dir := m.stateDir
	if dir == nil {
		var err error
		if dir, err = m.openStateDir(false); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		defer dir.Close()
	}

	return appendErrors(
		removeIfExists(dir, xtreemstoreS3BulkStatusFileName),
		removeIfExists(dir, xtreemstoreS3BulkRecordFileName),
		removeIfExists(dir, xtreemstoreS3BulkErrorsFileName),
		removeIfExists(dir, xtreemstoreS3BulkManagerFileName),
		removeIfExists(dir, persistentTmpPath(xtreemstoreS3BulkManagerFileName)),
	)
}

func (m *xtreemstoreS3BulkRetrieveManager) execute(ctx context.Context, walkCh chan<- *BulkStreamPathResult) (reschedule bool, delay time.Duration, err error) {
	defer func() {
		err = appendErrors(err, m.saveManagerState())
	}()

	for {
		if ready, err := m.ensureSessionActive(ctx); err != nil {
			return false, 0, err
		} else if !ready {
			return true, m.sessionBusyRetryDelay, nil
		}

		if batchesComplete, err := m.processSessionBatches(ctx, walkCh); err != nil {
			return false, 0, err
		} else if !batchesComplete {
			return true, m.batchPollDelay, nil
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

	var activeInfos *xtreemstoreS3BulkInfos
	if activeInfos, err = m.getActiveSessionBulkInfos(); err != nil {
		return false, fmt.Errorf("failed to get request records for active retrieve-session: %w", err)
	}

	var keys []string
	if keys, err = m.getSessionBatchKeys(ctx, batchInfo); err != nil {
		return false, fmt.Errorf("failed to retrieve batch keys: %w", err)
	}

	defer func() {
		err = appendErrors(err, m.saveManagerState())
	}()

	entries := make([]xtreemstoreS3BulkRetrieveBatchEntry, 0, len(keys))
	for _, key := range keys {
		jobIndex, ok := activeRecordMap[key]
		if !ok {
			return false, fmt.Errorf("unable to determine status for key: %s", key)
		}
		info, infoErr := activeInfos.Get(jobIndex)
		if infoErr != nil {
			return false, fmt.Errorf("unable to determine status: %w", infoErr)
		}
		entries = append(entries, xtreemstoreS3BulkRetrieveBatchEntry{key: key, jobIndex: jobIndex, info: info})
	}

	m.probeBatchReadiness(ctx, entries)

	allComplete = true
	for _, entry := range entries {
		if done, err := m.processSessionBatchKey(ctx, walkCh, entry); err != nil {
			return false, err
		} else if !done {
			allComplete = false
		}
	}

	return
}

// probeBatchReadiness populates the restore state of list of entries.
func (m *xtreemstoreS3BulkRetrieveManager) probeBatchReadiness(ctx context.Context, entries []xtreemstoreS3BulkRetrieveBatchEntry) {

	// TODO: This is currently a brute approach to checking all objects. However, this is not ideal
	// if xtreemstore can guarantee the batch is restored in order. If so, then a logarithmic
	// sampling approach would probably be better.

	g := &errgroup.Group{}
	g.SetLimit(max(1, int(restoreProbeWorkerMultiplier*float32(runtime.GOMAXPROCS(0)))))
	for i := range entries {
		entry := &entries[i]
		if entry.needsRestoreProbe() {
			g.Go(func() error {
				entry.ready, entry.readyErr = m.isObjectReadyForDownload(ctx, entry.key)
				return nil
			})
		}
	}
	g.Wait()
}

// processSessionBatchKey checks the key's state and updates it's states when it's changed.
func (m *xtreemstoreS3BulkRetrieveManager) processSessionBatchKey(
	ctx context.Context,
	walkCh chan<- *BulkStreamPathResult,
	entry xtreemstoreS3BulkRetrieveBatchEntry,
) (terminal bool, err error) {
	key, jobIndex, status, reservedJobId := entry.key, entry.jobIndex, entry.info.status, entry.info.jobId
	sendBulkResult := func(result *BulkStreamPathResult) error {
		// Every result carries the job the operation reserved for this request, including the ones
		// reporting that the request will never run: the reserved job is where that outcome is
		// recorded, and a result that left it out would strand the job waiting to be claimed.
		result.ReservedJobId = reservedJobId

		select {
		case walkCh <- result:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	switch status {
	case xtreemstoreS3BulkRequestAdded, xtreemstoreS3BulkRequestSent:
		// A request that is xtreemstoreS3BulkRequestSent means that remote never received the the
		// job request as the result of a sync worker crash; otherwise, the status would already be
		// xtreemstoreS3BulkRequestReceived.
		if ready, readyErr := entry.ready, entry.readyErr; readyErr != nil {
			if !errors.Is(readyErr, os.ErrNotExist) {
				err = fmt.Errorf("failed to determine restore state. Record: %s, Status: %v: %w", key, status, readyErr)
				return
			}
			result := &BulkStreamPathResult{
				Path:          key,
				RstId:         m.rstId,
				ReservedJobId: reservedJobId,
				BulkInfo:      &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
				Err:           &RequestCancelError{Reason: fmt.Errorf("object no longer exists")},
			}
			if err = sendBulkResult(result); err != nil {
				return
			}
			if err := m.MarkCompleteAck(jobIndex); err != nil {
				return false, fmt.Errorf("remote object no longer exists but failed to mark bulk job request as complete. Record: %s, Status: %v", key, status)
			}

			terminal = true
		} else if ready {
			result := &BulkStreamPathResult{
				Path:          key,
				RstId:         m.rstId,
				ReservedJobId: reservedJobId,
				BulkInfo:      &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
			}
			if err = sendBulkResult(result); err != nil {
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
			Path:          key,
			RstId:         m.rstId,
			ReservedJobId: reservedJobId,
			BulkInfo:      &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
			Err:           fmt.Errorf("unexpected record status. Record: %s, Status: %v", key, status),
		}
		if err = sendBulkResult(result); err != nil {
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

	data, err := m.stateDir.ReadFile(xtreemstoreS3BulkManagerFileName)
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
	if statusInfo, err := m.stateDir.Stat(xtreemstoreS3BulkStatusFileName); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m.includedJobs = 0
	} else {
		m.includedJobs = statusInfo.Size() / xtreemstoreS3BulkRecordLen
	}

	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) saveManagerState() error {
	return m.writeManagerState(m.state)
}

// createManagerFile creates a persistent file for operation's manager state. An existing file
// is left untouched.
func (m *xtreemstoreS3BulkRetrieveManager) createManagerFile() error {
	if _, err := m.stateDir.Stat(xtreemstoreS3BulkManagerFileName); err == nil {
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

	if m.stateDir == nil {
		return errBulkStateNotOpen
	}
	return writePersistentFile(m.stateDir, xtreemstoreS3BulkManagerFileName, data)
}

func (m *xtreemstoreS3BulkRetrieveManager) openState() (err error) {
	if m.stateDir, err = m.openStateDir(true); err != nil {
		return fmt.Errorf("failed to open state directory: %w", err)
	}

	defer func() {
		if err != nil {
			m.closeState()
		}
	}()

	if err = m.createManagerFile(); err != nil {
		return fmt.Errorf("failed to create manager state file: %w", err)
	}

	if err = m.loadManagerState(); err != nil {
		return fmt.Errorf("failed to load manager state: %w", err)
	}

	if m.statusAppendHandle, err = m.openStatusFileForAppend(); err != nil {
		err = fmt.Errorf("failed to open status append file: %w", err)
	} else if m.statusUpdateHandle, err = m.openStatusFileForUpdate(); err != nil {
		err = fmt.Errorf("failed to open status update file: %w", err)
	} else if m.recordHandle, err = m.openRecordFileForAppend(); err != nil {
		err = fmt.Errorf("failed to open record file: %w", err)
	} else if err = m.truncatePartialStatusRecord(); err != nil {
		err = fmt.Errorf("failed to reconcile status file: %w", err)
	} else if err = m.truncateOrphanedRecordBytes(); err != nil {
		err = fmt.Errorf("failed to reconcile record file: %w", err)
	} else if err = m.createErrorsFile(); err != nil {
		err = fmt.Errorf("failed to create errors file: %w", err)
	}
	return
}

func (m *xtreemstoreS3BulkRetrieveManager) closeState() (err error) {
	if m.recordHandle != nil {
		err = appendErrors(err, m.recordHandle.Close())
		m.recordHandle = nil
	}

	if m.statusUpdateHandle != nil {
		err = appendErrors(err, m.statusUpdateHandle.Close())
		m.statusUpdateHandle = nil
	}

	if m.statusAppendHandle != nil {
		err = appendErrors(err, m.statusAppendHandle.Close())
		m.statusAppendHandle = nil
	}

	if m.stateDir != nil {
		err = appendErrors(err, m.stateDir.Close())
		m.stateDir = nil
	}

	return err
}

func (m *xtreemstoreS3BulkRetrieveManager) MarkSent(jobIndex int64) error {
	return m.markJobStatus(xtreemstoreS3BulkRequestSent, jobIndex)
}

// UpdateBulkRequest records the request reaching a job, or being released because it never will.
//
// A submitted request is only marked received once remote has durably recorded its job. Marking it
// any earlier is what strands a request when sync or remote dies mid-submission: a received request
// is the job's to resolve, so one with no job behind it is never released and its batch never
// completes. Left at sent instead, the request is simply replayed by the next execute.
//
// A failed request is marked complete because the operation only needs to stop waiting on it; why
// it will never run is the job's story to tell, not the batch's.
func (m *xtreemstoreS3BulkRetrieveManager) UpdateBulkRequest(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
	jobIndex := request.GetBulkInfo().GetJobIndex()
	switch state {
	case BulkRequestSubmitted:
		return m.MarkReceived(jobIndex)
	case BulkRequestFailed:
		// It's safe to mark the same request complete more than once.
		return m.MarkComplete(jobIndex)
	default:
		// Never silently ignore a state this operation does not know about: a dropped transition is
		// indistinguishable from one that never happened, and the batch waits on it forever.
		return fmt.Errorf("unable to record %s for bulk retrieve request %d: %w", state, jobIndex, ErrUnsupportedOpForRST)
	}
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
		// A manager built only to report a request's outcome is never opened, so it reaches the
		// state directory for this one write.
		var dir *os.Root
		if dir, err = m.openStateDir(false); err == nil {
			defer dir.Close()
			f, err = openFileForUpdate(dir, xtreemstoreS3BulkStatusFileName)
		}
		if err != nil {
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

	_, err = f.WriteAt(status.Bytes(), jobIndex*xtreemstoreS3BulkRecordLen)
	return err
}

func (m *xtreemstoreS3BulkRetrieveManager) openStatusFileForAppend() (*os.File, error) {
	return openFileForAppend(m.stateDir, xtreemstoreS3BulkStatusFileName)
}

func (m *xtreemstoreS3BulkRetrieveManager) openStatusFileForUpdate() (*os.File, error) {
	return openFileForUpdate(m.stateDir, xtreemstoreS3BulkStatusFileName)
}

// createErrorsFile ensures the operation's errors file exists so its absence unambiguously means the
// operation's state was destroyed. It must not truncate or fail when the file is already there: a
// reason recorded by a previous Cancel has to survive the builder reopening state.
func (m *xtreemstoreS3BulkRetrieveManager) createErrorsFile() error {
	return touchFile(m.stateDir, xtreemstoreS3BulkErrorsFileName)
}

func (m *xtreemstoreS3BulkRetrieveManager) openRecordFileForAppend() (*os.File, error) {
	return openFileForAppend(m.stateDir, xtreemstoreS3BulkRecordFileName)
}

// errBulkStateNotOpen is returned by the operations that need the state directory a manager holds
// open between openState and closeState, when they are called outside that window.
var errBulkStateNotOpen = errors.New("the bulk operation's state is not open (this is probably a bug)")

// openStateDir opens the operation's state directory through the checked state root. See
// openStateRoot for what is checked and why.
func (m *xtreemstoreS3BulkRetrieveManager) openStateDir(create bool) (*os.Root, error) {
	return openStateDir(m.mountPath, m.stateRoot, m.stateMountPath, create)
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
//
// The mapping is keyed by path because the caller starts from an object key that a retrieve-session
// batch returned and needs the job index behind it. Two requests for the same path in one operation
// would collide, which is a property of the operation, not of this read.
func (m *xtreemstoreS3BulkRetrieveManager) getRecordsMap(start int64, end int64) (map[string]int64, error) {
	paths, err := m.readRecordPaths(start, end)
	if err != nil {
		return nil, err
	}

	keyMap := make(map[string]int64, len(paths))
	for i, path := range paths {
		keyMap[path] = start + int64(i)
	}
	return keyMap, nil
}

// getRecords returns a range of record paths. Set end to -1 to get all records beginning with start.
func (m *xtreemstoreS3BulkRetrieveManager) getRecords(start int64, end int64) ([]string, error) {
	return m.readRecordPaths(start, end)
}

// readRecordPaths returns the remote paths for the job indexes from start up to but not including
// end, in index order. Set end to -1 to read every path from start on.
//
// It costs two sized reads whatever the range: one for the status records, which carry each path's
// offset and length, and one for the span of the record file those records point at. Neither read
// touches what precedes the range, which is what a scan of the record file cannot avoid, because
// there the index of a path is its line number.
//
// Paths are only ever appended, so offsets rise with the index and a contiguous range of indexes
// occupies a contiguous span of bytes. The span can also contain bytes belonging to no record, from
// a path whose status record never followed it; those are never sliced because every path is taken
// at its own offset for its own length.
func (m *xtreemstoreS3BulkRetrieveManager) readRecordPaths(start int64, end int64) ([]string, error) {
	if end == -1 {
		end = m.includedJobs
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", start, end)
	}
	if start == end {
		return []string{}, nil
	}

	infos, err := m.getBulkInfos(start, end)
	if err != nil {
		return nil, err
	}

	located := make([]xtreemstoreS3BulkInfo, 0, end-start)
	for index := start; index < end; index++ {
		info, err := infos.Get(index)
		if err != nil {
			return nil, err
		}
		if previous := len(located) - 1; previous >= 0 {
			// Offsets that do not advance mean two records claim the same bytes, so neither path
			// can be trusted. Returning one would hand the caller some other request's path.
			if minimum := located[previous].pathOffset + int64(located[previous].pathLen); info.pathOffset < minimum {
				return nil, fmt.Errorf("the record for index %d starts at offset %d which overlaps the path recorded for index %d", index, info.pathOffset, index-1)
			}
		}
		located = append(located, info)
	}

	first := located[0]
	last := located[len(located)-1]
	span := make([]byte, last.pathOffset+int64(last.pathLen)-first.pathOffset)

	if m.stateDir == nil {
		return nil, errBulkStateNotOpen
	}
	f, err := m.stateDir.Open(xtreemstoreS3BulkRecordFileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.ReadAt(span, first.pathOffset); err != nil {
		return nil, fmt.Errorf("failed to read the record file for job indexes %d up to %d: %w", start, end, err)
	}

	paths := make([]string, 0, len(located))
	for _, info := range located {
		at := info.pathOffset - first.pathOffset
		paths = append(paths, string(span[at:at+int64(info.pathLen)]))
	}
	return paths, nil
}

// truncatePartialStatusRecord drops a trailing record that was not written in full. Each record is
// appended in one write, so an interrupted append is the only way the file ends mid-record, and the
// request it belonged to was never counted by includedJobs. Cutting it keeps record N addressable
// at N*xtreemstoreS3BulkRecordLen, which every reader relies on. It runs after
// loadManagerState has set includedJobs from the size of this file.
func (m *xtreemstoreS3BulkRetrieveManager) truncatePartialStatusRecord() error {
	whole := m.includedJobs * xtreemstoreS3BulkRecordLen
	info, err := m.stateDir.Stat(xtreemstoreS3BulkStatusFileName)
	if err != nil {
		return err
	}

	if info.Size() == whole {
		return nil
	}
	return truncateStateFile(m.stateDir, xtreemstoreS3BulkStatusFileName, whole)
}

// truncateOrphanedRecordBytes cuts the record file back to the end of the last path a status record
// points at, and sets recordBytes to that size.
//
// AddRequest writes the path first and the status record second, so a crash between the two leaves
// path bytes that no record will ever point at. They sit at the end of the file, where the next
// append would otherwise land after them and grow the file on every retry. Cutting them is not what
// makes them safe: a record's own offset and length are what locate a path, so unreachable bytes
// stay unreachable either way.
//
// It must run after truncatePartialStatusRecord, because it reads the last status record and that
// record has to be a whole one.
//
// Only a trailing orphan is removed. A Write that returns bytes together with an error, from ENOSPC
// or EIO, leaves an orphan mid-file once the process carries on and appends the next path after it.
// The invariant readers rely on is the weaker one: every byte of a live path is covered by exactly
// one status record, and any byte no record covers is unreachable.
func (m *xtreemstoreS3BulkRetrieveManager) truncateOrphanedRecordBytes() error {
	live := int64(0)
	if m.includedJobs > 0 {
		last := m.includedJobs - 1
		infos, err := m.getBulkInfos(last, m.includedJobs)
		if err != nil {
			return fmt.Errorf("failed to read the last status record: %w", err)
		}
		info, err := infos.Get(last)
		if err != nil {
			return fmt.Errorf("failed to decode the last status record: %w", err)
		}
		live = info.pathOffset + int64(info.pathLen) + int64(len(xtreemstoreS3BulkPathTerminator))
	}

	stat, err := m.stateDir.Stat(xtreemstoreS3BulkRecordFileName)
	if err != nil {
		return err
	}

	// A record file shorter than what the last record describes means the two files disagree in a
	// way this scheme says cannot happen. Truncating to live would grow the file with zeroes and
	// hand out a garbage path, so refuse to open instead.
	if stat.Size() < live {
		return fmt.Errorf("record file is %d bytes but the status record for index %d describes a path ending at %d", stat.Size(), m.includedJobs-1, live)
	}

	if stat.Size() != live {
		if err := truncateStateFile(m.stateDir, xtreemstoreS3BulkRecordFileName, live); err != nil {
			return err
		}
	}

	// Set from the truncated size rather than by seeking the handle. A freshly opened O_APPEND
	// descriptor reports a current offset of zero even though its writes land at the end, and the
	// open handle follows the truncation because O_APPEND seeks to the end as part of every write.
	m.recordBytes = live
	return nil
}

func (m *xtreemstoreS3BulkRetrieveManager) getActiveSessionBulkInfos() (*xtreemstoreS3BulkInfos, error) {
	return m.getBulkInfos(m.state.SessionJobStart, m.state.SessionJobEnd)
}

// getBulkInfos reads the status file records for the job indexes from start up to but not including
// end. Set end to -1 to read every record from start on.
//
// The whole range is read in one call and handed back undecoded, because a caller works through a
// batch and only asks about the indexes that batch holds. Each record carries the status and the
// reserved job together, so both come out of this one read (see xtreemstoreS3BulkInfos.Get).
func (m *xtreemstoreS3BulkRetrieveManager) getBulkInfos(start int64, end int64) (*xtreemstoreS3BulkInfos, error) {
	if end == -1 {
		end = m.includedJobs
	}
	if start < 0 || end < start || end > m.includedJobs {
		return nil, fmt.Errorf("invalid record range: start=%d end=%d, this operation has %d requests", start, end, m.includedJobs)
	}

	infos := &xtreemstoreS3BulkInfos{offset: start}
	if start == end {
		return infos, nil
	}

	if m.stateDir == nil {
		return nil, errBulkStateNotOpen
	}
	f, err := m.stateDir.Open(xtreemstoreS3BulkStatusFileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	infos.records = make([]byte, (end-start)*xtreemstoreS3BulkRecordLen)
	if n, err := f.ReadAt(infos.records, start*xtreemstoreS3BulkRecordLen); err != nil {
		return nil, fmt.Errorf("failed to read bulk entry state file: %w", err)
	} else if n != len(infos.records) {
		return nil, fmt.Errorf("invalid bulk entry state")
	}

	return infos, nil
}
