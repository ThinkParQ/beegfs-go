package rst

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// fakeS3ApiClient is a minimal, hand-rolled s3ApiClient scoped to exactly what
// TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete needs to drive the real (non-executeTest)
// execute() path: a single retrieve-session containing one batch with every id from the PUT body,
// and objects that always report ready-for-download -- this test exercises session/batch/reschedule
// bookkeeping, not tape-restore timing. A single instance must be shared across every
// xtreemstoreS3BulkRetrieveManager the test constructs (mirroring a real S3 bucket's state
// persisting across manager reloads), rather than one fake per manager.
type fakeS3ApiClient struct {
	mu     sync.Mutex
	active bool
	id     string
	ids    []string
}

var _ s3ApiClient = &fakeS3ApiClient{}

func fakeNotFoundErr() error {
	return &smithy.GenericAPIError{Code: "NoSuchKey", Message: "key not found"}
}

func fakeConflictErr() error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusConflict}},
		Err:      &smithy.GenericAPIError{Code: "ActiveRetrieveSessionAlreadyExists"},
	}
}

func fakeJSONBody(v any) io.ReadCloser {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return io.NopCloser(bytes.NewReader(b))
}

func (f *fakeS3ApiClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch aws.ToString(params.Key) {
	case XTS_SYSTEM_RETRIEVE_SESSION:
		if !f.active {
			return nil, fakeNotFoundErr()
		}
		return &s3.GetObjectOutput{Body: fakeJSONBody(xtreemstoreS3BulkRetrieveSessionInfo{
			Active:     true,
			RetrieveId: f.id,
			Started:    time.Now(),
		})}, nil
	case XTS_SYSTEM_RETRIEVE_BATCH_LIST:
		if !f.active {
			return nil, fakeNotFoundErr()
		}
		return &s3.GetObjectOutput{Body: fakeJSONBody([]xtreemstoreS3BulkRetrieveBatchInfo{
			{Number: 0, Objects: int64(len(f.ids)), Size: 0},
		})}, nil
	case fmt.Sprintf(XTS_SYSTEM_RETRIEVE_BATCH_FMT, 0):
		if !f.active {
			return nil, fakeNotFoundErr()
		}
		return &s3.GetObjectOutput{Body: fakeJSONBody(f.ids)}, nil
	default:
		return nil, fakeNotFoundErr()
	}
}

func (f *fakeS3ApiClient) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if aws.ToString(params.Key) != XTS_SYSTEM_RETRIEVE_SESSION {
		return nil, fmt.Errorf("fakeS3ApiClient: unexpected PutObject key %q", aws.ToString(params.Key))
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active {
		return nil, fakeConflictErr()
	}

	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	var req xtreemstoreS3BulkRetrieveRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	f.active = true
	f.id = "test-retrieve-id"
	f.ids = req.Ids
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3ApiClient) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if aws.ToString(params.Key) == XTS_SYSTEM_RETRIEVE_SESSION {
		f.active = false
		f.id = ""
		f.ids = nil
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3ApiClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	// Always ready: this test exercises reschedule/completion bookkeeping, not restore timing.
	return &s3.HeadObjectOutput{StorageClass: types.StorageClassStandard}, nil
}

func (f *fakeS3ApiClient) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: ListObjectsV2 not used by this test")
}

func (f *fakeS3ApiClient) ListObjectsV2Pages(ctx context.Context, params *s3.ListObjectsV2Input, pageFn func(*s3.ListObjectsV2Output) (bool, error)) error {
	return fmt.Errorf("fakeS3ApiClient: ListObjectsV2Pages not used by this test")
}

func (f *fakeS3ApiClient) RestoreObject(ctx context.Context, params *s3.RestoreObjectInput, optFns ...func(*s3.Options)) (*s3.RestoreObjectOutput, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: RestoreObject not used by this test")
}

func (f *fakeS3ApiClient) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: CreateMultipartUpload not used by this test")
}

func (f *fakeS3ApiClient) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: AbortMultipartUpload not used by this test")
}

func (f *fakeS3ApiClient) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: CompleteMultipartUpload not used by this test")
}

func (f *fakeS3ApiClient) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, fmt.Errorf("fakeS3ApiClient: UploadPart not used by this test")
}

// TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete reproduces the reported issue in
// isolation: after every dispatched record is marked complete (exactly as CompleteWorkRequests ->
// xtreemstoreS3BulkMarkRequestComplete does for each individual sub-job), a fresh Execute() pass
// (as happens on every builder-job reschedule) should stop asking to reschedule.
func TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete(t *testing.T) {
	tmpDir := t.TempDir()
	const stateMountPath = testStateMountPath
	var operation = flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()

	// Shared across every manager instance the test constructs, mirroring how a real S3 bucket's
	// retrieve-session state persists across manager reloads.
	fake := &fakeS3ApiClient{}

	newManager := func(t *testing.T) *xtreemstoreS3BulkRetrieveManager {
		m := &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    fake,
			rstId:          1,
			mountPath:      tmpDir,
			stateRoot:      DefaultStateRoot,
			stateMountPath: stateMountPath,
			operation:      operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
		require.NoError(t, m.openState())
		return m
	}

	m := newManager(t)
	for _, p := range []string{"/a", "/b", "/c"} {
		// A request is only absorbed once a job has been reserved for it, so AddRequest requires
		// the reserved job ID.
		reservedJobId := uuid.NewString()
		req := &beeremote.JobRequest{
			Type:         &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{RemotePath: p}},
			BulkInfo:     &flex.BulkJobRequestInfo{},
			ReserveJobId: &reservedJobId,
		}
		require.NoError(t, m.AddRequest(context.Background(), req))
	}
	require.NoError(t, m.closeState())

	// First Execute pass: every record is Added, so all 3 get dispatched. Dispatch does not change
	// a record's status.
	m = newManager(t)
	walkCh, getResults, err := m.Execute(context.Background())
	require.NoError(t, err)

	var dispatched []*flex.BulkJobRequestInfo
	for r := range walkCh {
		require.NoError(t, r.Err)
		dispatched = append(dispatched, r.BulkInfo)
	}
	result := getResults()
	require.NoError(t, result.Err)
	assert.True(t, result.Reschedule, "should reschedule while records are still Added")
	require.Len(t, dispatched, 3)
	require.NoError(t, m.closeState())

	// Simulate every dispatched sub-job completing successfully, exactly like
	// xtreemstoreS3BulkMarkRequestComplete does when CompleteWorkRequests is called.
	for _, bulkInfo := range dispatched {
		completeManager := &xtreemstoreS3BulkRetrieveManager{
			mountPath:      tmpDir,
			stateRoot:      DefaultStateRoot,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
		}
		err = completeManager.MarkComplete(bulkInfo.JobIndex)
		require.NoError(t, err)
	}

	// Second Execute pass (fresh manager instance, exactly as happens on a real builder-job
	// reschedule): every record is now Complete, so this should NOT ask to reschedule again.
	m = newManager(t)
	walkCh2, getResults2, err := m.Execute(context.Background())
	require.NoError(t, err)
	for r := range walkCh2 {
		t.Fatalf("expected no further dispatch once everything is complete, got: %+v", r)
	}
	result2 := getResults2()
	require.NoError(t, result2.Err)
	assert.False(t, result2.Reschedule, "bulk operation should stop rescheduling once every record is marked complete")
}

// TestProcessSessionBatchKeyAcceptedStillWaits ensures a record that has reached
// xtreemstoreS3BulkRequestAccepted (remote durably recorded a Job for it) is
// still treated as in-progress, not as an unexpected status. Falling through to the default case
// here would force-MarkCompleteAck a record whose real download may still be running in
// ExecuteWorkRequestPart, and report a bogus "unexpected record status" error for it.
func TestProcessSessionBatchKeyAcceptedStillWaits(t *testing.T) {
	m := &xtreemstoreS3BulkRetrieveManager{rstId: 1}
	walkCh := make(chan *BulkStreamPathResult, 1)

	entry := &xtreemstoreS3BulkRetrieveBatchEntry{remotePath: "/a", info: &xtreemstoreS3BulkInfo{jobIndex: 0, status: xtreemstoreS3BulkRequestAccepted}}
	done, err := m.processSessionBatchKey(context.Background(), walkCh, entry)
	require.NoError(t, err)
	assert.False(t, done, "an Accepted record is still in flight and must not be reported done")

	select {
	case r := <-walkCh:
		t.Fatalf("expected no result to be sent for an Accepted record, got: %+v", r)
	default:
	}
}

// perKeyHeadObjectClient answers HeadObject from a per-key table and records every key it was
// asked about, so a concurrent probe can be checked for both correctness and wasted calls.
type perKeyHeadObjectClient struct {
	s3ApiClient
	outputs map[string]*s3.HeadObjectOutput
	errs    map[string]error

	mu          sync.Mutex
	head        []string
	inFlight    int
	maxInFlight int
}

func (c *perKeyHeadObjectClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(params.Key)
	c.mu.Lock()
	c.head = append(c.head, key)
	c.inFlight++
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
	c.mu.Unlock()
	// Hold the call open briefly so probes genuinely overlap; a serial implementation would never
	// see maxInFlight above 1.
	time.Sleep(2 * time.Millisecond)
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
	if err, ok := c.errs[key]; ok {
		return nil, err
	}
	return c.outputs[key], nil
}

func (c *perKeyHeadObjectClient) headedKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.head...)
	sort.Strings(out)
	return out
}

// TestProbeBatchReadiness covers the concurrent restore-readiness probe: every entry must come back
// with its own object's result (not a neighbour's), entries whose status no longer depends on the
// object must not be probed at all, and a failed probe must be recorded on its entry rather than
// aborting the probes of unrelated keys.
func TestProbeBatchReadiness(t *testing.T) {
	client := &perKeyHeadObjectClient{
		outputs: map[string]*s3.HeadObjectOutput{
			"ready": {StorageClass: types.StorageClassStandard},
			"cold":  {StorageClass: types.StorageClassGlacier},
		},
		errs: map[string]error{
			"gone":   fakeNotFoundErr(),
			"broken": fmt.Errorf("connection reset"),
		},
	}
	m := &xtreemstoreS3BulkRetrieveManager{s3ApiClient: client, bucket: "test-bucket"}

	entries := []*xtreemstoreS3BulkRetrieveBatchEntry{
		{remotePath: "ready", info: &xtreemstoreS3BulkInfo{jobIndex: 0, status: xtreemstoreS3BulkRequestAdded}},
		{remotePath: "cold", info: &xtreemstoreS3BulkInfo{jobIndex: 1, status: xtreemstoreS3BulkRequestAdded}},
		{remotePath: "accepted", info: &xtreemstoreS3BulkInfo{jobIndex: 2, status: xtreemstoreS3BulkRequestAccepted}},
		{remotePath: "complete", info: &xtreemstoreS3BulkInfo{jobIndex: 3, status: xtreemstoreS3BulkRequestComplete}},
		{remotePath: "acked", info: &xtreemstoreS3BulkInfo{jobIndex: 4, status: xtreemstoreS3BulkRequestCompleteAcked}},
		{remotePath: "gone", info: &xtreemstoreS3BulkInfo{jobIndex: 6, status: xtreemstoreS3BulkRequestAdded}},
		{remotePath: "broken", info: &xtreemstoreS3BulkInfo{jobIndex: 7, status: xtreemstoreS3BulkRequestAdded}},
	}

	m.probeBatchReadiness(context.Background(), entries)

	// Only the statuses that turn on the object's restore state are worth a HEAD.
	assert.Equal(t, []string{"broken", "cold", "gone", "ready"}, client.headedKeys(),
		"an entry whose request is already with remote must not be probed")
	assert.Greater(t, client.maxInFlight, 1, "probes should overlap, not run one at a time")

	byKey := map[string]*xtreemstoreS3BulkRetrieveBatchEntry{}
	for _, e := range entries {
		byKey[e.remotePath] = e
	}
	assert.True(t, byKey["ready"].ready)
	assert.NoError(t, byKey["ready"].readyErr)
	assert.False(t, byKey["cold"].ready)
	assert.NoError(t, byKey["cold"].readyErr)
	assert.ErrorIs(t, byKey["gone"].readyErr, os.ErrNotExist,
		"a vanished object must still reach processSessionBatchKey as os.ErrNotExist so it is cancelled")
	assert.ErrorContains(t, byKey["broken"].readyErr, "head object for key",
		"a failed probe is recorded on its own entry, not propagated")
	assert.NoError(t, byKey["accepted"].readyErr)
	assert.False(t, byKey["accepted"].ready)
}

// stubHeadObjectClient fakes only HeadObject, the sole s3ApiClient method isObjectReadyForDownload
// calls.
type stubHeadObjectClient struct {
	s3ApiClient
	output *s3.HeadObjectOutput
	err    error
}

func (s *stubHeadObjectClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return s.output, s.err
}

// TestIsObjectReadyForDownload covers every storage-class/error branch of isObjectReadyForDownload,
// including the "unexpected storage class" default case that TestBulkRetrieveExecuteStops... does
// not exercise (that test only ever sees StorageClassStandard).
func TestIsObjectReadyForDownload(t *testing.T) {
	tests := []struct {
		name            string
		output          *s3.HeadObjectOutput
		err             error
		wantReady       bool
		wantErrIs       error
		wantErrContains string
	}{
		{
			name:      "standard storage class is ready",
			output:    &s3.HeadObjectOutput{StorageClass: types.StorageClassStandard},
			wantReady: true,
		},
		{
			// Unlike plain S3, xtreemstore signals a finished retrieve by flipping the storage class
			// to STANDARD (see XTS_S3_Headers_and_Efficient_Retrieve), so a still-GLACIER object is
			// not ready even when it carries a completed-restore marker.
			name:      "glacier object with completed restore marker is still not ready",
			output:    &s3.HeadObjectOutput{StorageClass: types.StorageClassGlacier, Restore: aws.String(`ongoing-request="false", expiry-date="Fri, 01 Jan 2027 00:00:00 GMT"`)},
			wantReady: false,
		},
		{
			name:      "glacier object with restore in progress is not ready",
			output:    &s3.HeadObjectOutput{StorageClass: types.StorageClassGlacier, Restore: aws.String(`ongoing-request="true"`)},
			wantReady: false,
		},
		{
			name:      "glacier object with no restore requested yet is not ready",
			output:    &s3.HeadObjectOutput{StorageClass: types.StorageClassGlacier},
			wantReady: false,
		},
		{
			name:      "missing object maps to os.ErrNotExist",
			err:       fakeNotFoundErr(),
			wantReady: false,
			wantErrIs: os.ErrNotExist,
		},
		{
			name:            "unrecognized storage class is an error",
			output:          &s3.HeadObjectOutput{StorageClass: types.StorageClass("UNKNOWN")},
			wantReady:       false,
			wantErrContains: "unexpected storage class",
		},
		{
			name:            "generic API error is wrapped, not treated as not-exist",
			err:             fmt.Errorf("connection reset"),
			wantReady:       false,
			wantErrContains: "head object for key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &xtreemstoreS3BulkRetrieveManager{
				s3ApiClient: &stubHeadObjectClient{output: tt.output, err: tt.err},
				bucket:      "test-bucket",
			}
			ready, err := m.isObjectReadyForDownload(context.Background(), "some/key")
			assert.Equal(t, tt.wantReady, ready)
			switch {
			case tt.wantErrIs != nil:
				assert.ErrorIs(t, err, tt.wantErrIs)
			case tt.wantErrContains != "":
				assert.ErrorContains(t, err, tt.wantErrContains)
			default:
				assert.NoError(t, err)
			}
		})
	}
}

// The errors file is the sole signal that separates a live bulk operation from a destroyed one, so
// openState must create it and Destroy must be the only thing that takes it away.
func TestBulkRetrieveErrorsFileTracksOperationLifetime(t *testing.T) {
	tmpDir := t.TempDir()
	bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: testStateMountPath, Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}

	newManager := func() *xtreemstoreS3BulkRetrieveManager {
		return &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    &fakeS3ApiClient{},
			rstId:          1,
			mountPath:      tmpDir,
			stateRoot:      DefaultStateRoot,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
	}
	readinessErr := func() error {
		return xtreemstoreS3BulkRetrieveError(bulkInfo, 1, tmpDir, DefaultStateRoot)
	}

	// Before the operation exists at all there is nothing staged, so requests must be refused.
	assert.ErrorIs(t, readinessErr(), ErrBulkOperationDestroyed)

	m := newManager()
	require.NoError(t, m.openState())
	errorsPath := path.Join(m.mountPath, m.stateMountPath, xtreemstoreS3BulkErrorsFileName)

	contents, err := os.ReadFile(errorsPath)
	require.NoError(t, err, "openState must create the errors file")
	assert.Empty(t, contents, "a live operation with nothing to report has an empty errors file")
	assert.NoError(t, readinessErr(), "an empty errors file means the operation is healthy")

	// Reopening must not discard a reason already recorded, since the builder reopens state on every
	// reschedule and a cancelled operation has to stay cancelled.
	require.NoError(t, m.recordError(errors.New("tape buffer eviction")))
	require.NoError(t, m.closeState())
	m = newManager()
	require.NoError(t, m.openState())
	assert.ErrorContains(t, readinessErr(), "tape buffer eviction")

	require.NoError(t, m.closeState())
	require.NoError(t, m.Destroy(context.Background()))

	_, err = os.Stat(errorsPath)
	assert.ErrorIs(t, err, os.ErrNotExist, "Destroy must remove the errors file")
	assert.ErrorIs(t, readinessErr(), ErrBulkOperationDestroyed)
}

// Each status record carries the request's status byte and the job reserved for it, addressed by
// offset, so record N must always belong to index N. Nothing separates the records, which makes the
// file's length the only thing keeping the indexes lined up: a partial trailing record has to be
// cut, and updating a status must not disturb the job ID beside it.
func TestBulkRetrieveStatusRecordHoldsReservedJobPerIndex(t *testing.T) {
	tmpDir := t.TempDir()
	bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: testStateMountPath, Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}

	newManager := func() *xtreemstoreS3BulkRetrieveManager {
		return &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    &fakeS3ApiClient{},
			rstId:          1,
			mountPath:      tmpDir,
			stateRoot:      DefaultStateRoot,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
	}

	addRequest := func(m *xtreemstoreS3BulkRetrieveManager, remotePath string, jobId string) error {
		return m.AddRequest(context.Background(), beeremote.JobRequest_builder{
			Path:                remotePath,
			RemoteStorageTarget: 1,
			Sync:                flex.SyncJob_builder{Operation: flex.SyncJob_DOWNLOAD, RemotePath: remotePath}.Build(),
			BulkInfo:            &flex.BulkJobRequestInfo{StateMountPath: bulkInfo.StateMountPath, Operation: bulkInfo.Operation},
			ReserveJobId:        &jobId,
		}.Build())
	}

	// getJobId reads one record the way a caller that only wants the reserved job does: it takes
	// the range holding that one index and decodes it.
	getJobId := func(m *xtreemstoreS3BulkRetrieveManager, jobIndex int64) (string, error) {
		infos, err := m.getBulkInfos(jobIndex, jobIndex+1)
		if err != nil {
			return "", err
		}
		info, err := infos.Get(jobIndex)
		return info.jobId, err
	}

	m := newManager()
	require.NoError(t, m.openState())

	jobIds := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	for i, jobId := range jobIds {
		require.NoError(t, addRequest(m, fmt.Sprintf("/objects/%d", i), jobId))
	}

	for i, jobId := range jobIds {
		got, err := getJobId(m, int64(i))
		require.NoError(t, err)
		assert.Equal(t, jobId, got, "index %d must return the job reserved for it", i)
	}

	_, err := getJobId(m, int64(len(jobIds)))
	assert.Error(t, err, "an index beyond the requests added has no reserved job")

	// Updating a status rewrites only the first byte of the record, so the job ID beside it and
	// every record after it must be left alone.
	require.NoError(t, m.MarkAccepted(1))
	infos, err := m.getBulkInfos(0, int64(len(jobIds)))
	require.NoError(t, err)
	wantStatuses := []xtreemstoreS3BulkRequestStatus{
		xtreemstoreS3BulkRequestAdded,
		xtreemstoreS3BulkRequestAccepted,
		xtreemstoreS3BulkRequestAdded,
	}
	for i, jobId := range jobIds {
		info, err := infos.Get(int64(i))
		require.NoError(t, err)
		assert.Equal(t, wantStatuses[i], info.status, "record %d must report the status it was left with", i)
		assert.Equal(t, jobId, info.jobId, "a status update must not disturb the job ID in record %d", i)
	}

	// A reschedule reopens the operation, so the records have to survive losing the handle.
	require.NoError(t, m.closeState())
	m = newManager()
	require.NoError(t, m.openState())
	assert.Equal(t, int64(len(jobIds)), m.includedJobs, "includedJobs counts records, not bytes")
	got, err := getJobId(m, 2)
	require.NoError(t, err)
	assert.Equal(t, jobIds[2], got)
	statusPath := path.Join(m.mountPath, m.stateMountPath, xtreemstoreS3BulkStatusFileName)
	require.NoError(t, m.closeState())

	// A record is appended in one write, so an interrupted append is the only way the file ends
	// mid-record. Opening the operation must cut the partial record rather than shift every index.
	f, err := os.OpenFile(statusPath, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = f.WriteString("\x00" + uuid.NewString()[:10])
	require.NoError(t, f.Close())
	require.NoError(t, err)

	m = newManager()
	require.NoError(t, m.openState(), "a partial trailing record is cut rather than treated as corruption")
	assert.Equal(t, int64(len(jobIds)), m.includedJobs)
	info, err := os.Stat(statusPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(jobIds)*xtreemstoreS3BulkRecordLen), info.Size())
	got, err = getJobId(m, 2)
	require.NoError(t, err)
	assert.Equal(t, jobIds[2], got, "cutting the partial record must not disturb the ones before it")
	require.NoError(t, m.closeState())
}

// The builder, sync and remote all write a request's status byte, and nothing orders their writes.
// Remote can finish a job and mark its request complete before the builder hears back from the
// submission that created that job. The builder's late accepted must not undo the completion, or
// the record is left waiting on a job that no longer exists and its batch never completes. So a
// status may only move forward, and a write for an index past the last record must fail rather
// than grow the file and misalign every record appended after it.
func TestBulkRetrieveStatusOnlyMovesForward(t *testing.T) {
	tmpDir := t.TempDir()
	newManager, addRequest := bulkRetrieveTestManager(t, tmpDir)

	m := newManager()
	require.NoError(t, m.openState())
	defer func() { require.NoError(t, m.closeState()) }()
	for i := range 3 {
		require.NoError(t, addRequest(m, fmt.Sprintf("/objects/%d", i)))
	}

	// Remote and sync report a request's outcome through a manager that is never opened, so the
	// write takes a fresh handle instead of the opened manager's.
	oneShot := &xtreemstoreS3BulkRetrieveManager{
		mountPath:      tmpDir,
		stateRoot:      DefaultStateRoot,
		stateMountPath: m.stateMountPath,
		operation:      m.operation,
	}
	// The builder reports through the opened manager it registered for the operation.
	builderReports := func(jobIndex int64, state BulkRequestState) error {
		request := beeremote.JobRequest_builder{
			BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: m.stateMountPath, Operation: m.operation, JobIndex: jobIndex},
		}.Build()
		return m.UpdateBulkRequest(context.Background(), request, state)
	}
	statusOf := func(jobIndex int64) xtreemstoreS3BulkRequestStatus {
		t.Helper()
		infos, err := m.getBulkInfos(jobIndex, jobIndex+1)
		require.NoError(t, err)
		info, err := infos.Get(jobIndex)
		require.NoError(t, err)
		return info.status
	}

	// Record 0 takes the normal path, so the guard must still let every forward move through.
	require.NoError(t, builderReports(0, BulkRequestAccepted))
	assert.Equal(t, xtreemstoreS3BulkRequestAccepted, statusOf(0))
	require.NoError(t, oneShot.MarkComplete(0))
	assert.Equal(t, xtreemstoreS3BulkRequestComplete, statusOf(0))
	require.NoError(t, m.MarkCompleteAck(0))
	assert.Equal(t, xtreemstoreS3BulkRequestCompleteAcked, statusOf(0))

	// Record 1 is the race: the job completes before the builder records the submission.
	require.NoError(t, oneShot.MarkComplete(1))
	require.NoError(t, builderReports(1, BulkRequestAccepted), "a late update is skipped, not refused")
	assert.Equal(t, xtreemstoreS3BulkRequestComplete, statusOf(1), "a late accepted must not undo the completion")

	// A completion that arrives after the operation acknowledged it must not reopen it either.
	require.NoError(t, oneShot.MarkComplete(0))
	assert.Equal(t, xtreemstoreS3BulkRequestCompleteAcked, statusOf(0))

	// Repeating a status is a no-op, which is what makes marking a request complete twice safe.
	require.NoError(t, oneShot.MarkComplete(1))
	assert.Equal(t, xtreemstoreS3BulkRequestComplete, statusOf(1))

	assert.Equal(t, xtreemstoreS3BulkRequestAdded, statusOf(2), "updates to other records must leave this one alone")

	// A request that was not delivered may still hold a live reservation, so it stays added for
	// the next execute to replay or a cancel to release.
	require.NoError(t, builderReports(2, BulkRequestNotDelivered))
	assert.Equal(t, xtreemstoreS3BulkRequestAdded, statusOf(2), "a request that was not delivered must stay replayable")

	statusPath := path.Join(tmpDir, m.stateMountPath, xtreemstoreS3BulkStatusFileName)
	before, err := os.Stat(statusPath)
	require.NoError(t, err)
	for _, writer := range []*xtreemstoreS3BulkRetrieveManager{m, oneShot} {
		assert.Error(t, writer.MarkComplete(3), "index 3 has no record")
	}
	after, err := os.Stat(statusPath)
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size(), "a write past the last record must not grow the file")
}

// bulkRetrieveTestManager builds a manager against tmpDir together with helpers for adding requests
// and reading them back, which the record-file tests all need.
func bulkRetrieveTestManager(t *testing.T, tmpDir string) (
	newManager func() *xtreemstoreS3BulkRetrieveManager,
	addRequest func(m *xtreemstoreS3BulkRetrieveManager, remotePath string) error,
) {
	t.Helper()
	operation := flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()

	newManager = func() *xtreemstoreS3BulkRetrieveManager {
		return &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    &fakeS3ApiClient{},
			rstId:          1,
			mountPath:      tmpDir,
			stateRoot:      DefaultStateRoot,
			stateMountPath: testStateMountPath,
			operation:      operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
	}

	addRequest = func(m *xtreemstoreS3BulkRetrieveManager, remotePath string) error {
		jobId := uuid.NewString()
		return m.AddRequest(context.Background(), beeremote.JobRequest_builder{
			Path:                bulkRetrieveTestInMountPath(remotePath),
			RemoteStorageTarget: 1,
			Sync:                flex.SyncJob_builder{Operation: flex.SyncJob_DOWNLOAD, RemotePath: remotePath}.Build(),
			BulkInfo:            &flex.BulkJobRequestInfo{StateMountPath: testStateMountPath, Operation: operation},
			ReserveJobId:        &jobId,
		}.Build())
	}
	return
}

// bulkRetrieveTestInMountPath is the in-mount path addRequest records for remotePath. It differs
// from the remote path so a reader that returns one path where the other belongs is caught.
func bulkRetrieveTestInMountPath(remotePath string) string {
	return "/mnt" + remotePath
}

// TestBulkRetrieveRecordPathsAreLocatedByOffset checks that a path is returned by the offset and
// length its status record carries, not by counting newlines in the record file. A remote path that
// contains a newline is the case that tells the two apart: an S3 object key may hold one, and a
// reader that splits on newlines would return it as two records and shift every index after it.
func TestBulkRetrieveRecordPathsAreLocatedByOffset(t *testing.T) {
	newManager, addRequest := bulkRetrieveTestManager(t, t.TempDir())

	paths := []string{
		"/objects/plain",
		"/objects/with\nnewline",
		"/objects/with\n\ntwo",
		"/objects/trailing\n",
		"/objects/last",
	}

	m := newManager()
	require.NoError(t, m.openState())
	for _, p := range paths {
		require.NoError(t, addRequest(m, p))
	}
	require.NoError(t, m.closeState())

	m = newManager()
	require.NoError(t, m.openState())
	defer m.closeState()

	require.Equal(t, int64(len(paths)), m.includedJobs, "every path must be counted once, whatever bytes it holds")

	got, err := m.readRecordPaths(0, -1)
	require.NoError(t, err)
	assert.Equal(t, paths, got, "each path must come back exactly as it went in")

	// A tail read is the common case: it must not depend on anything that precedes it.
	got, err = m.readRecordPaths(3, -1)
	require.NoError(t, err)
	assert.Equal(t, paths[3:], got)

	// The span carries both paths of every record, and each record knows its own index.
	span, err := m.readRecordSpan(0, -1)
	require.NoError(t, err)
	require.Len(t, span.recordInfos, len(paths))
	for i, info := range span.recordInfos {
		assert.Equal(t, int64(i), info.jobIndex, "record %d must report the index it was added at", i)
		assert.Equal(t, bulkRetrieveTestInMountPath(paths[i]), span.inMountPath(info), "in-mount path of record %d", i)
		assert.Equal(t, paths[i], span.remotePath(info), "remote path of record %d", i)
	}
}

// TestBulkRetrieveRecoversFromInterruptedAdd covers the crash window AddRequest leaves open. The
// path is written before the status record, so a crash between them leaves path bytes that no
// record points at. Opening the operation must trim them and carry on at the next index.
func TestBulkRetrieveRecoversFromInterruptedAdd(t *testing.T) {
	for _, tc := range []struct {
		name    string
		orphan  string
		comment string
	}{
		{name: "whole path", orphan: "/objects/never-recorded\n"},
		{name: "torn path", orphan: "/objects/half-writ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newManager, addRequest := bulkRetrieveTestManager(t, t.TempDir())
			before := []string{"/objects/0", "/objects/1"}

			m := newManager()
			require.NoError(t, m.openState())
			for _, p := range before {
				require.NoError(t, addRequest(m, p))
			}
			recordPath := path.Join(m.mountPath, m.stateMountPath, xtreemstoreS3BulkRecordFileName)
			require.NoError(t, m.closeState())

			livePath, err := os.Stat(recordPath)
			require.NoError(t, err)
			liveBytes := livePath.Size()

			// Stand in for a crash after the path write and before the status record append.
			f, err := os.OpenFile(recordPath, os.O_WRONLY|os.O_APPEND, 0600)
			require.NoError(t, err)
			_, err = f.WriteString(tc.orphan)
			require.NoError(t, f.Close())
			require.NoError(t, err)

			m = newManager()
			require.NoError(t, m.openState(), "bytes no record points at are trimmed, not treated as corruption")
			defer m.closeState()

			assert.Equal(t, int64(len(before)), m.includedJobs, "the interrupted request was never counted")
			trimmed, err := os.Stat(recordPath)
			require.NoError(t, err)
			assert.Equal(t, liveBytes, trimmed.Size(), "the record file is cut back to the end of the last recorded path")
			assert.Equal(t, liveBytes, m.recordBytes, "the next append lands where the file now ends")

			got, err := m.readRecordPaths(0, -1)
			require.NoError(t, err)
			assert.Equal(t, before, got, "the requests added before the interruption are untouched")

			// The index the interrupted request would have taken is still free.
			require.NoError(t, addRequest(m, "/objects/2"))
			got, err = m.readRecordPaths(0, -1)
			require.NoError(t, err)
			assert.Equal(t, append(append([]string{}, before...), "/objects/2"), got)
		})
	}
}

// TestBulkRetrieveRefusesTruncatedRecordFile checks that a record file too short for what the last
// status record describes stops the operation opening. Truncating to the described size would pad
// the file with zeroes and hand out a path that was never written, so this has to fail loudly.
func TestBulkRetrieveRefusesTruncatedRecordFile(t *testing.T) {
	newManager, addRequest := bulkRetrieveTestManager(t, t.TempDir())

	m := newManager()
	require.NoError(t, m.openState())
	require.NoError(t, addRequest(m, "/objects/0"))
	require.NoError(t, addRequest(m, "/objects/1"))
	recordPath := path.Join(m.mountPath, m.stateMountPath, xtreemstoreS3BulkRecordFileName)
	require.NoError(t, m.closeState())

	stat, err := os.Stat(recordPath)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(recordPath, stat.Size()-4))

	m = newManager()
	err = m.openState()
	require.Error(t, err, "a record file shorter than its last record describes is not something to repair")
	assert.Contains(t, err.Error(), "record file is")
}

// TestBulkRetrieveRejectsOversizedPath checks the guard on the record's length field. A path longer
// than the field can hold would be stored with a truncated length and read back as a different
// path, so it is refused before anything is written.
func TestBulkRetrieveRejectsOversizedPath(t *testing.T) {
	newManager, addRequest := bulkRetrieveTestManager(t, t.TempDir())

	m := newManager()
	require.NoError(t, m.openState())
	defer m.closeState()

	require.NoError(t, addRequest(m, "/objects/0"))
	recordBytes := m.recordBytes

	err := addRequest(m, "/"+strings.Repeat("x", xtreemstoreS3BulkMaxPathLen))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")

	assert.Equal(t, int64(1), m.includedJobs, "a refused request must not take an index")
	assert.Equal(t, recordBytes, m.recordBytes, "a refused request must not write to the record file")

	got, err := m.readRecordPaths(0, -1)
	require.NoError(t, err)
	assert.Equal(t, []string{"/objects/0"}, got)
}

// testStateMountPath is the state mount path the bulk retrieve tests keep an operation's state in.
// It has to lie under this build's layout directory, because the manager refuses any other path.
const testStateMountPath = DefaultStateRoot + "/" + stateLayoutDir + "/state"

// fakeUnavailableErr is what the SDK returns once its own retries of an HTTP 503 run out.
func fakeUnavailableErr() error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}},
		Err:      &smithy.GenericAPIError{Code: "ServiceUnavailable"},
	}
}

func fakeForbiddenErr() error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
		Err:      &smithy.GenericAPIError{Code: "AccessDenied"},
	}
}

// flakyS3ApiClient is a fakeS3ApiClient whose HeadObject fails with an HTTP 503 until failHeads
// calls have failed.
type flakyS3ApiClient struct {
	*fakeS3ApiClient
	failHeads atomic.Int64
}

func (f *flakyS3ApiClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.failHeads.Add(-1) >= 0 {
		return nil, fakeUnavailableErr()
	}
	return f.fakeS3ApiClient.HeadObject(ctx, params, optFns...)
}

// bulkRetrieveExecuteRound opens a manager, runs one Execute round and closes it again. It returns
// the round's result and the in-mount paths the round sent.
func bulkRetrieveExecuteRound(t *testing.T, newManager func() *xtreemstoreS3BulkRetrieveManager) (*BulkExecuteResult, []string, *xtreemstoreS3BulkRetrieveManagerState) {
	t.Helper()
	m := newManager()
	require.NoError(t, m.openState())
	walkCh, getResults, err := m.Execute(context.Background())
	require.NoError(t, err)
	var sent []string
	for r := range walkCh {
		require.NoError(t, r.Err)
		sent = append(sent, r.InMountPath)
	}
	result := getResults()
	state := *m.state
	require.NoError(t, m.closeState())
	return result, sent, &state
}

// TestBulkRetrieveRetriesTransientS3Errors checks that a retryable S3 error while polling only
// delays the operation. Returning it in BulkExecuteResult.Err would cancel the operation and fail
// every reserved job.
func TestBulkRetrieveRetriesTransientS3Errors(t *testing.T) {
	tmpDir := t.TempDir()
	client := &flakyS3ApiClient{fakeS3ApiClient: &fakeS3ApiClient{}}
	client.failHeads.Store(1)
	newManagerBase, addRequest := bulkRetrieveTestManager(t, tmpDir)
	newManager := func() *xtreemstoreS3BulkRetrieveManager {
		m := newManagerBase()
		m.s3ApiClient = client
		m.batchPollDelay = DefaultPollDelay
		return m
	}

	m := newManager()
	require.NoError(t, m.openState())
	for _, p := range []string{"/a", "/b", "/c"} {
		require.NoError(t, addRequest(m, p))
	}
	require.NoError(t, m.closeState())

	result, sent, state := bulkRetrieveExecuteRound(t, newManager)
	require.NoError(t, result.Err, "a retryable error must not fail the operation")
	assert.True(t, result.Reschedule)
	assert.Equal(t, DefaultPollDelay, result.Delay)
	// The round stops at the entry whose probe failed. Entries before it may already be sent. The
	// next round sends them again, which is safe because each carries its reserved job ID.
	assert.Less(t, len(sent), 3, "the round stops at the failed probe")
	assert.Equal(t, int64(1), state.TransientErrors)
	assert.Contains(t, state.LastTransientError, "ServiceUnavailable")

	result, sent, state = bulkRetrieveExecuteRound(t, newManager)
	require.NoError(t, result.Err)
	assert.ElementsMatch(t, []string{"/mnt/a", "/mnt/b", "/mnt/c"}, sent)
	assert.Zero(t, state.TransientErrors, "a successful round ends the run")
	assert.True(t, state.TransientErrorsSince.IsZero())
	assert.True(t, state.LastTransientErrorAt.IsZero())
	assert.Empty(t, state.LastTransientError)
}

// TestBulkRetrieveFailsOnceTransientWindowIsExhausted checks that retryable errors stop being
// retried once they have lasted transientS3ErrorWindow, however many rounds that took. It also checks
// that a gap longer than the window, as after a long service stop, starts the clock again.
func TestBulkRetrieveFailsOnceTransientWindowIsExhausted(t *testing.T) {
	tmpDir := t.TempDir()
	client := &flakyS3ApiClient{fakeS3ApiClient: &fakeS3ApiClient{}}
	client.failHeads.Store(100)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newManagerBase, addRequest := bulkRetrieveTestManager(t, tmpDir)
	newManager := func() *xtreemstoreS3BulkRetrieveManager {
		m := newManagerBase()
		m.s3ApiClient = client
		m.now = func() time.Time { return now }
		return m
	}

	m := newManager()
	require.NoError(t, m.openState())
	require.NoError(t, addRequest(m, "/a"))
	require.NoError(t, m.closeState())

	// Many rounds in quick succession stay inside the window. Rounds come this fast while the
	// builder job's walk is still adding requests.
	for range 10 {
		result, _, _ := bulkRetrieveExecuteRound(t, newManager)
		require.NoError(t, result.Err)
		require.True(t, result.Reschedule)
		now = now.Add(time.Second)
	}

	// A gap longer than the window starts a new run instead of failing the operation.
	now = now.Add(transientS3ErrorWindow + time.Minute)
	result, _, state := bulkRetrieveExecuteRound(t, newManager)
	require.NoError(t, result.Err, "a gap longer than the window must start a new run")
	assert.Equal(t, int64(1), state.TransientErrors)
	assert.Equal(t, now, state.TransientErrorsSince)

	// Rounds spaced well inside the window add up until the run lasts the whole window.
	now = now.Add(transientS3ErrorWindow / 2)
	result, _, _ = bulkRetrieveExecuteRound(t, newManager)
	require.NoError(t, result.Err)

	now = now.Add(transientS3ErrorWindow / 2)
	result, _, state = bulkRetrieveExecuteRound(t, newManager)
	require.Error(t, result.Err, "the run has lasted the whole window")
	assert.False(t, result.Reschedule)
	var responseErr *smithyhttp.ResponseError
	require.ErrorAs(t, result.Err, &responseErr, "the S3 error must stay reachable for the caller")
	assert.Equal(t, http.StatusServiceUnavailable, responseErr.HTTPStatusCode())
	assert.Equal(t, int64(3), state.TransientErrors)
}

func TestIsRetryableS3Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"HTTP 503", fakeUnavailableErr(), true},
		{"throttling", &smithy.GenericAPIError{Code: "SlowDown"}, true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"wrapped HTTP 503", fmt.Errorf("failed to load retrieve-session batch info: %w", fakeUnavailableErr()), true},
		{"access denied", fakeForbiddenErr(), false},
		{"missing key", fakeNotFoundErr(), false},
		{"cancelled context", context.Canceled, false},
		{"expired context", fmt.Errorf("head object: %w", context.DeadlineExceeded), false},
		{"local error", errors.New("failed to write manager state"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableS3Error(tt.err))
		})
	}
}

func TestObjectCancelReason(t *testing.T) {
	tests := []struct {
		name       string
		readyErr   error
		wantCancel bool
	}{
		{"missing object", os.ErrNotExist, true},
		{"unexpected storage class", fmt.Errorf("%w, DEEP_ARCHIVE", errUnexpectedStorageClass), true},
		{"access denied on the key", fmt.Errorf("head object for key %q: %w", "k", fakeForbiddenErr()), true},
		{"HTTP 503 belongs to the operation", fmt.Errorf("head object for key %q: %w", "k", fakeUnavailableErr()), false},
		{"connection reset belongs to the operation", errors.New("connection reset"), false},
		{"cancelled context belongs to the operation", fmt.Errorf("head object for key %q: %w", "k", context.Canceled), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantCancel, objectCancelReason(tt.readyErr) != nil)
		})
	}
}
