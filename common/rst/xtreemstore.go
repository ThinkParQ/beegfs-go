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
	"sort"
	"strconv"
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

const (
	xtreemstoreS3BulkRequestInitialized byte = iota
	xtreemstoreS3BulkRequestComplete
)

const xtreemstoreS3BulkRetrieveBatchFileVersion = 1

var (
	ErrActiveRetrieveSessionAlreadyExists = errors.New("active retrieve-session already exists")
)

type xtreemstoreS3BulkRetrieveManager struct {
	s3ApiClient    s3ApiClient
	bucket         string
	operation      string
	stateMountPath string
	state          *xtreemstoreS3BulkRetrieveManagerState
}

type xtreemstoreS3BulkRetrieveSessionInfo struct {
	Active     bool      `json:"active"`
	RetrieveId string    `json:"retrieve-id"`
	Started    time.Time `json:"started"`
}

type xtreemstoreS3BulkRetrieveManagerState struct {
	IncludedJobs     int64  `json:"included-jobs"`
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

type xtreemstoreS3BulkRetrieveBatchFile struct {
	Version             int `json:"version"`
	BatchInfo           xtreemstoreS3BulkRetrieveBatchInfo
	ActiveRetrieveId    string   `json:"active-retrieve-id"`
	ActiveJobStart      int64    `json:"active-job-start"`
	ActiveJobEnd        int64    `json:"active-job-end"` // exclusive
	ActiveJobReferences []uint64 `json:"active-job-references"`
}

type activeSessionStatuses struct {
	jobStatuses []byte
	jobCount    uint64
	offset      uint64 // this will correspond the xtreemstoreS3BulkRetrieveManager.state.ActiveJobStart at the time retrieved
}

func (s *activeSessionStatuses) IsComplete(jobIndex uint64) (bool, error) {
	if jobIndex < s.offset {
		return false, fmt.Errorf("invalid index for active session")
	}
	statusesJobIndex := jobIndex - s.offset
	if statusesJobIndex >= uint64(len(s.jobStatuses)) {
		return false, fmt.Errorf("invalid index for active session")
	}
	return s.jobStatuses[statusesJobIndex] == xtreemstoreS3BulkRequestComplete, nil
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

func newXtreemstoreS3BulkRetrieveManager(s3ApiClient s3ApiClient, bucket string, stateMountPath string, operation string) *xtreemstoreS3BulkRetrieveManager {
	stateMountPath = path.Join(stateMountPath, operation)
	return &xtreemstoreS3BulkRetrieveManager{
		s3ApiClient:    s3ApiClient,
		bucket:         bucket,
		operation:      operation,
		stateMountPath: stateMountPath,
		state:          &xtreemstoreS3BulkRetrieveManagerState{},
	}
}

func xtreemstoreS3BulkMarkRequestComplete(mountPath string, bulkInfo *flex.BulkJobRequestInfo) error {
	stateMountPath := path.Join(mountPath, bulkInfo.StateMountPath, bulkInfo.Operation)
	manager := &xtreemstoreS3BulkRetrieveManager{
		operation:      bulkInfo.Operation,
		stateMountPath: stateMountPath,
	}
	f, err := os.OpenFile(manager.getStatusPath(), os.O_WRONLY, os.FileMode(0600))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteAt([]byte{xtreemstoreS3BulkRequestComplete}, bulkInfo.JobIndex)
	return err

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

	ready, delay, err = x.Provider.IsWorkRequestReady(ctx, request)
	if ready {
		if request.HasBulkInfo() {
			err = xtreemstoreS3BulkMarkRequestComplete(x.mountPoint.GetMountPath(), request.GetBulkInfo())
			if err != nil {
				return false, 0, fmt.Errorf("failed to mark bulk request complete: %w", err)
			}
		}
	}

	return
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

func (x *xtreemstoreS3Provider) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest) (reschedule bool, delay time.Duration, err error) {
	bucket := x.GetConfig().GetXtreemstore().S3.Bucket
	path := path.Join(x.mountPoint.GetMountPath(), stateMountPath)

	switch parseBulkOperation(operation) {
	case xtreemstoreS3BulkOperationRetrieve:
		manager := newXtreemstoreS3BulkRetrieveManager(x.s3ApiClient, bucket, path, operation)
		if err = manager.LoadManagerState(); err != nil {
			return false, 0, fmt.Errorf("failed to load manager state: %w", err)
		}
		if err := manager.AddRequests(requests); err != nil {
			return false, 0, err
		}
		return manager.Execute(ctx)
	default:
		return false, 0, ErrUnsupportedOpForRST
	}
}

func (x *xtreemstoreS3Provider) CompleteBulkRequest(ctx context.Context, stateMountPath string, operation string) error {
	return x.deleteBulkStateFiles(stateMountPath, operation)
}

func (x *xtreemstoreS3Provider) CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error) error {
	bucket := x.GetConfig().GetXtreemstore().S3.Bucket
	path := path.Join(x.mountPoint.GetMountPath(), stateMountPath)

	switch parseBulkOperation(operation) {
	case xtreemstoreS3BulkOperationRetrieve:
		manager := newXtreemstoreS3BulkRetrieveManager(x.s3ApiClient, bucket, path, operation)
		if err := manager.LoadManagerState(); err != nil {
			return fmt.Errorf("failed to load manager state: %w", err)
		}

		var errs []error

		// TODO: Verify whether deleting the batch files are desired. Is it enough just to destroy the session?
		paths, err := manager.getSortedBatchReferencePaths()
		if err != nil {
			errs = append(errs, err)
		} else {
			errs = append(errs, manager.removeBatchReferenceFiles(paths))
		}

		errs = append(errs, manager.destroyRetrieveSession(ctx))
		errs = append(errs, x.deleteBulkStateFiles(stateMountPath, operation))

		return errors.Join(errs...)
	default:
		return ErrUnsupportedOpForRST
	}
}

func (x *xtreemstoreS3BulkRetrieveManager) Execute(ctx context.Context) (reschedule bool, delay time.Duration, err error) {
	sessionInfo, err := x.getSessionInfo(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("unable to determine whether retrieve-session is active: %w", err)
	}

	if sessionInfo.Active {
		if sessionInfo.RetrieveId != x.state.ActiveRetrieveId {
			// Another session is active so reschedule
			return true, 5 * time.Second, nil
		}
	} else {
		if err = x.StartSession(ctx); err != nil {
			if errors.Is(err, ErrActiveRetrieveSessionAlreadyExists) {
				return true, 5 * time.Second, nil
			}
			return false, 0, fmt.Errorf("failed to start retrieve-session: %w", err)
		}
	}

	batchReferencePaths, err := x.ensureBatchReferenceFiles(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("failed to create batch reference files for the active retrieve-session: %w", err)
	}

	statuses, err := x.getActiveSessionStatuses()
	if err != nil {
		return false, 0, fmt.Errorf("failed to get bulk retrieve entry states: %w", err)
	}

	// Review the remaining batches removing any that are complete. Reschedule the work request if a
	// batch is still incomplete.
	for _, path := range batchReferencePaths {
		complete, err := x.checkBulkBatch(path, statuses)
		if err != nil {
			return false, 0, fmt.Errorf("failed to create bulk batch reference files: %w", err)
		}

		if complete {
			if err = os.Remove(path); err != nil {
				return false, 0, fmt.Errorf("failed to remove batch file! Path: %q, Error: %q", path, err)
			}
		} else {
			return true, 5 * time.Second, nil
		}
	}

	if err = x.destroyRetrieveSession(ctx); err != nil {
		return false, 0, fmt.Errorf("retrieve-session completed successfully, but the active session could not be destroyed and manual intervention is required: %w", err)
	}
	return false, 0, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) AddRequests(requests []*beeremote.JobRequest) error {
	if err := os.MkdirAll(x.stateMountPath, os.FileMode(0700)); err != nil {
		return err
	}

	queueStatusPath := x.getStatusPath()
	if info, err := os.Stat(queueStatusPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		x.state.IncludedJobs = 0
	} else {
		x.state.IncludedJobs = info.Size()
	}

	status, err := os.OpenFile(queueStatusPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		return err
	}
	defer status.Close()

	record, err := os.OpenFile(x.getRecordPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		return err
	}
	defer record.Close()

	for _, request := range requests {
		if err = x.insertRequest(status, record, request); err != nil {
			return err
		}
	}
	return nil
}

func (x *xtreemstoreS3BulkRetrieveManager) LoadManagerState() error {
	f, err := os.OpenFile(x.getManagerPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(x.state)
}

func (x *xtreemstoreS3BulkRetrieveManager) saveManagerState() error {
	f, err := os.OpenFile(x.getManagerPath(), os.O_WRONLY|os.O_CREATE, os.FileMode(0600))
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(x.state)
}

func (x *xtreemstoreS3BulkRetrieveManager) getStatusPath() string {
	return path.Join(x.stateMountPath, "status")
}

func (x *xtreemstoreS3BulkRetrieveManager) getRecordPath() string {
	return path.Join(x.stateMountPath, "record")
}

func (x *xtreemstoreS3BulkRetrieveManager) getManagerPath() string {
	return path.Join(x.stateMountPath, "manager.json")
}

func (x *xtreemstoreS3BulkRetrieveManager) getBatchPath(number int64) string {
	return path.Join(x.stateMountPath, fmt.Sprintf("batch.%d", number))
}

func (x *xtreemstoreS3BulkRetrieveManager) insertRequest(status *os.File, record *os.File, request *beeremote.JobRequest) (err error) {
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if !request.HasBulkInfo() {
		return fmt.Errorf("missing request bulkInfo")
	}
	if request.GetBulkInfo().JobIndex != x.state.IncludedJobs {
		return fmt.Errorf("unexpected request bulkInfo.JobIndex")
	}

	if _, err = status.Write([]byte{xtreemstoreS3BulkRequestInitialized}); err != nil {
		return
	}

	remotePath := request.GetSync().GetRemotePath()
	if x.state.IncludedJobs == 0 {
		_, err = record.WriteString(remotePath)
	} else {
		_, err = record.WriteString("\n" + remotePath)
	}

	x.state.IncludedJobs++
	return
}

func (x *xtreemstoreS3Provider) deleteBulkStateFiles(stateMountPath string, operation string) error {
	statePath := path.Join(x.mountPoint.GetMountPath(), stateMountPath, operation)
	if err := os.RemoveAll(statePath); err != nil {
		return fmt.Errorf("delete bulk state files: %w", err)
	}

	return nil
}

func (x *xtreemstoreS3BulkRetrieveManager) getSessionInfo(ctx context.Context) (*xtreemstoreS3BulkRetrieveSessionInfo, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(x.bucket),
		Key:    aws.String(XTS_SYSTEM_RETRIEVE_SESSION),
	}

	resp, err := x.s3ApiClient.GetObject(ctx, getObjectInput)
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

func (x *xtreemstoreS3BulkRetrieveManager) getSessionBatchInfo(ctx context.Context) ([]xtreemstoreS3BulkRetrieveBatchInfo, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(x.bucket),
		Key:    aws.String(XTS_SYSTEM_RETRIEVE_BATCH_LIST),
	}

	resp, err := x.s3ApiClient.GetObject(ctx, getObjectInput)
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

func (x *xtreemstoreS3BulkRetrieveManager) getSessionBatchKeys(ctx context.Context, batchInfo xtreemstoreS3BulkRetrieveBatchInfo) ([]string, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(x.bucket),
		Key:    aws.String(fmt.Sprintf(XTS_SYSTEM_RETRIEVE_BATCH_FMT, batchInfo.Number)),
	}

	resp, err := x.s3ApiClient.GetObject(ctx, getObjectInput)
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

func (x *xtreemstoreS3BulkRetrieveManager) createBatchReferenceFiles(ctx context.Context, batchInfos []xtreemstoreS3BulkRetrieveBatchInfo) ([]string, error) {
	keyMap, err := x.getActiveRecordsMap()
	if err != nil {
		return nil, err
	}

	paths := []string{}
	for _, batchInfo := range batchInfos {
		keys, err := x.getSessionBatchKeys(ctx, batchInfo)
		if err != nil {
			return nil, err
		}
		if len(keys) != int(batchInfo.Objects) {
			return nil, fmt.Errorf("key count does not match the expected number of objects")
		}

		references := make([]uint64, 0, len(keys))
		for _, key := range keys {
			if index, ok := keyMap[key]; ok {
				references = append(references, uint64(index))
			} else {
				return nil, fmt.Errorf("key was not found in the key map")
			}
		}

		path := x.getBatchPath(batchInfo.Number)
		paths = append(paths, path)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0600))
		if err != nil {
			return nil, err
		}
		batchFile := &xtreemstoreS3BulkRetrieveBatchFile{
			Version:             xtreemstoreS3BulkRetrieveBatchFileVersion,
			BatchInfo:           batchInfo,
			ActiveRetrieveId:    x.state.ActiveRetrieveId,
			ActiveJobStart:      x.state.ActiveJobStart,
			ActiveJobEnd:        x.state.ActiveJobEnd,
			ActiveJobReferences: references,
		}
		if err := json.NewEncoder(f).Encode(batchFile); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}

	return paths, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) ensureBatchReferenceFiles(ctx context.Context) ([]string, error) {
	batchInfos, err := x.getSessionBatchInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load retrieve-session batch info: %w", err)
	}

	paths, err := x.getSortedBatchReferencePaths()
	if err != nil {
		return nil, err
	}

	if err := x.validateBatchReferenceFiles(paths, batchInfos); err == nil {
		return paths, nil
	} else if len(paths) > 0 {
		if removeErr := x.removeBatchReferenceFiles(paths); removeErr != nil {
			return nil, fmt.Errorf("failed to reset invalid batch reference files: %w", errors.Join(err, removeErr))
		}
	}

	return x.createBatchReferenceFiles(ctx, batchInfos)
}

func (x *xtreemstoreS3BulkRetrieveManager) StartSession(ctx context.Context) error {
	if x.state.IncludedJobs == 0 {
		return fmt.Errorf("retrieve-session requires at least one key")
	}

	previousState := *x.state
	cleanupCreatedSession := func(reason error) error {
		*x.state = previousState
		if cleanupErr := x.destroyRetrieveSession(ctx); cleanupErr != nil {
			return fmt.Errorf("retrieve-session was created but local ownership state could not be persisted, cleanup also failed, and manual intervention is required: %w", errors.Join(reason, cleanupErr))
		}
		return reason
	}

	x.state.ActiveJobStart = x.state.ActiveJobEnd
	x.state.ActiveJobEnd = x.state.IncludedJobs
	keys, err := x.getActiveRecords()
	if err != nil {
		return err
	}

	retrieveSessionJson, err := json.Marshal(&xtreemstoreS3BulkRetrieveRequest{Ids: keys})
	if err != nil {
		return fmt.Errorf("failed to marshal retrieve-session request: %w", err)
	}

	_, err = x.s3ApiClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(x.bucket),
		Key:         aws.String(XTS_SYSTEM_RETRIEVE_SESSION),
		Body:        bytes.NewReader(retrieveSessionJson),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusConflict {
			*x.state = previousState
			return ErrActiveRetrieveSessionAlreadyExists
		}

		return fmt.Errorf("failed to start retrieve-session: %w", err)
	}

	sessionInfo, err := x.getSessionInfo(ctx)
	if err != nil {
		return cleanupCreatedSession(fmt.Errorf("failed to recover retrieve-session ownership state after creation: %w", err))
	}
	x.state.ActiveRetrieveId = sessionInfo.RetrieveId

	if err = x.saveManagerState(); err != nil {
		return cleanupCreatedSession(fmt.Errorf("failed to store retrieve-session ownership state after creation: %w", err))
	}
	return nil
}

func (x *xtreemstoreS3BulkRetrieveManager) destroyRetrieveSession(ctx context.Context) error {
	_, err := x.s3ApiClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(x.bucket),
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

func (x *xtreemstoreS3BulkRetrieveManager) getActiveRecordsMap() (map[string]int64, error) {
	f, err := os.OpenFile(x.getRecordPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if x.state.ActiveJobStart < 0 || x.state.ActiveJobEnd < x.state.ActiveJobStart {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", x.state.ActiveJobStart, x.state.ActiveJobEnd)
	}

	keyMap := make(map[string]int64)
	scanner := bufio.NewScanner(f)
	for index := int64(0); scanner.Scan(); index++ {
		if index < x.state.ActiveJobStart {
			continue
		}
		if index >= x.state.ActiveJobEnd {
			break
		}
		keyMap[scanner.Text()] = index
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read bulk entry keys file: %w", err)
	}

	return keyMap, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) getActiveRecords() ([]string, error) {
	f, err := os.OpenFile(x.getRecordPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if x.state.ActiveJobStart < 0 || x.state.ActiveJobEnd < x.state.ActiveJobStart {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", x.state.ActiveJobStart, x.state.ActiveJobEnd)
	}

	keys := make([]string, 0, max(0, int(x.state.ActiveJobEnd-x.state.ActiveJobStart)))
	scanner := bufio.NewScanner(f)
	for index := int64(0); scanner.Scan(); index++ {
		if index < x.state.ActiveJobStart {
			continue
		}
		if index >= x.state.ActiveJobEnd {
			break
		}
		keys = append(keys, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read queued records: %w", err)
	}

	return keys, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) loadBatchReferenceFile(path string) (*xtreemstoreS3BulkRetrieveBatchFile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, fmt.Errorf("failed to open batch reference file %q: %w", path, err)
	}
	defer f.Close()

	batchFile := &xtreemstoreS3BulkRetrieveBatchFile{}
	if err := json.NewDecoder(f).Decode(batchFile); err != nil {
		return nil, fmt.Errorf("failed to decode batch reference file %q: %w", path, err)
	}
	if batchFile.Version != xtreemstoreS3BulkRetrieveBatchFileVersion {
		return nil, fmt.Errorf("invalid batch reference file %q: unsupported version %d", path, batchFile.Version)
	}

	return batchFile, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) validateLoadedBatchReferenceFile(path string, batchFile *xtreemstoreS3BulkRetrieveBatchFile) error {
	if batchFile.ActiveRetrieveId != x.state.ActiveRetrieveId {
		return fmt.Errorf("invalid batch reference file %q: retrieve-session mismatch", path)
	}
	if batchFile.ActiveJobStart != x.state.ActiveJobStart || batchFile.ActiveJobEnd != x.state.ActiveJobEnd {
		return fmt.Errorf("invalid batch reference file %q: active job range mismatch", path)
	}

	return nil
}

func (x *xtreemstoreS3BulkRetrieveManager) validateBatchReferenceFiles(paths []string, batchInfos []xtreemstoreS3BulkRetrieveBatchInfo) error {
	if len(paths) != len(batchInfos) {
		return fmt.Errorf("unexpected batch reference file count")
	}

	expectedByNumber := make(map[int64]xtreemstoreS3BulkRetrieveBatchInfo, len(batchInfos))
	for _, batchInfo := range batchInfos {
		expectedByNumber[batchInfo.Number] = batchInfo
	}

	seen := make(map[int64]struct{}, len(paths))
	for _, path := range paths {
		batchFile, err := x.loadBatchReferenceFile(path)
		if err != nil {
			return err
		}
		if err := x.validateLoadedBatchReferenceFile(path, batchFile); err != nil {
			return err
		}

		expectedBatchInfo, ok := expectedByNumber[batchFile.BatchInfo.Number]
		if !ok {
			return fmt.Errorf("invalid batch reference file %q: unexpected batch number %d", path, batchFile.BatchInfo.Number)
		}
		if batchFile.BatchInfo != expectedBatchInfo {
			return fmt.Errorf("invalid batch reference file %q: batch metadata mismatch", path)
		}
		if _, exists := seen[batchFile.BatchInfo.Number]; exists {
			return fmt.Errorf("invalid batch reference file %q: duplicate batch number %d", path, batchFile.BatchInfo.Number)
		}
		seen[batchFile.BatchInfo.Number] = struct{}{}
	}

	return nil
}

func (x *xtreemstoreS3BulkRetrieveManager) removeBatchReferenceFiles(paths []string) error {
	errs := make([]error, 0, len(paths))
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove batch reference file %q: %w", path, err))
		}
	}

	return errors.Join(errs...)
}

func (x *xtreemstoreS3BulkRetrieveManager) getSortedBatchReferencePaths() ([]string, error) {
	prefix := "batch."
	entries, err := os.ReadDir(x.stateMountPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to list batch reference files: %w", err)
	}

	type batchReference struct {
		number int
		path   string
	}

	batches := []batchReference{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasPrefix(filename, prefix) {
			continue
		}

		suffix := strings.TrimPrefix(filename, prefix)
		batchNumber, err := strconv.Atoi(suffix)
		if err != nil {
			continue
		}

		batches = append(batches, batchReference{
			number: batchNumber,
			path:   path.Join(x.stateMountPath, filename),
		})
	}

	sort.Slice(batches, func(i, j int) bool {
		return batches[i].number < batches[j].number
	})

	paths := make([]string, 0, len(batches))
	for _, batch := range batches {
		paths = append(paths, batch.path)
	}
	return paths, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) getActiveSessionStatuses() (*activeSessionStatuses, error) {
	if x.state.ActiveJobStart < 0 || x.state.ActiveJobEnd < x.state.ActiveJobStart {
		return nil, fmt.Errorf("invalid active record range: start=%d end=%d", x.state.ActiveJobStart, x.state.ActiveJobEnd)
	}

	activeStatuses := &activeSessionStatuses{offset: uint64(x.state.ActiveJobStart)}
	if x.state.ActiveJobEnd == x.state.ActiveJobStart {
		return activeStatuses, nil
	}

	f, err := os.OpenFile(x.getStatusPath(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	activeStatuses.jobCount = uint64(x.state.ActiveJobEnd - x.state.ActiveJobStart)
	activeStatuses.jobStatuses = make([]byte, activeStatuses.jobCount)
	if n, err := f.ReadAt(activeStatuses.jobStatuses, int64(activeStatuses.offset)); err != nil {
		return nil, fmt.Errorf("failed to read bulk entry state file: %w", err)
	} else if n != len(activeStatuses.jobStatuses) {
		return nil, fmt.Errorf("invalid bulk entry state")
	}

	return activeStatuses, nil
}

func (x *xtreemstoreS3BulkRetrieveManager) checkBulkBatch(path string, activeStatuses *activeSessionStatuses) (bool, error) {
	batchFile, err := x.loadBatchReferenceFile(path)
	if err != nil {
		return false, err
	}
	if err := x.validateLoadedBatchReferenceFile(path, batchFile); err != nil {
		return false, err
	}

	for _, index := range batchFile.ActiveJobReferences {
		if isComplete, err := activeStatuses.IsComplete(index); err != nil {
			return false, err
		} else if !isComplete {
			return false, nil
		}
	}

	return true, nil
}
