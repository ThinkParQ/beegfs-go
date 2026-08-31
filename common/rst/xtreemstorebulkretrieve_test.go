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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	const stateMountPath = "state"
	var operation = flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()

	// Shared across every manager instance the test constructs, mirroring how a real S3 bucket's
	// retrieve-session state persists across manager reloads.
	fake := &fakeS3ApiClient{}

	newManager := func(t *testing.T) *xtreemstoreS3BulkRetrieveManager {
		m := &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    fake,
			rstId:          1,
			mountPath:      tmpDir,
			stateMountPath: stateMountPath,
			operation:      operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
		require.NoError(t, m.openState())
		return m
	}

	m := newManager(t)
	for _, p := range []string{"/a", "/b", "/c"} {
		req := &beeremote.JobRequest{
			Type:     &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{RemotePath: p}},
			BulkInfo: &flex.BulkJobRequestInfo{},
		}
		require.NoError(t, m.AddRequest(context.Background(), req))
	}
	require.NoError(t, m.closeState())

	// First Execute pass: everything is Initialized, so all 3 get dispatched and marked Sent.
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
	assert.True(t, result.Reschedule, "should reschedule while records are still Sent")
	require.Len(t, dispatched, 3)
	require.NoError(t, m.closeState())

	// Simulate every dispatched sub-job completing successfully, exactly like
	// xtreemstoreS3BulkMarkRequestComplete does when CompleteWorkRequests is called.
	for _, bulkInfo := range dispatched {
		completeManager := &xtreemstoreS3BulkRetrieveManager{
			mountPath:      tmpDir,
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

// TestProcessSessionBatchKeyReceivedStillWaits ensures a record that has reached
// xtreemstoreS3BulkRequestReceived (GenerateWorkRequests successfully created a Job for it) is
// still treated as in-progress, not as an unexpected status. Falling through to the default case
// here would force-MarkCompleteAck a record whose real download may still be running in
// ExecuteWorkRequestPart, and report a bogus "unexpected record status" error for it.
func TestProcessSessionBatchKeyReceivedStillWaits(t *testing.T) {
	m := &xtreemstoreS3BulkRetrieveManager{rstId: 1}
	walkCh := make(chan *BulkStreamPathResult, 1)

	entry := xtreemstoreS3BulkRetrieveBatchEntry{key: "/a", jobIndex: 0, status: xtreemstoreS3BulkRequestReceived}
	done, err := m.processSessionBatchKey(context.Background(), walkCh, entry)
	require.NoError(t, err)
	assert.False(t, done, "a Received record is still in flight and must not be reported done")

	select {
	case r := <-walkCh:
		t.Fatalf("expected no result to be sent for a Received record, got: %+v", r)
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
			"ready":     {StorageClass: types.StorageClassStandard},
			"cold":      {StorageClass: types.StorageClassGlacier},
			"sent-cold": {StorageClass: types.StorageClassGlacier},
		},
		errs: map[string]error{
			"gone":   fakeNotFoundErr(),
			"broken": fmt.Errorf("connection reset"),
		},
	}
	m := &xtreemstoreS3BulkRetrieveManager{s3ApiClient: client, bucket: "test-bucket"}

	entries := []xtreemstoreS3BulkRetrieveBatchEntry{
		{key: "ready", jobIndex: 0, status: xtreemstoreS3BulkRequestAdded},
		{key: "cold", jobIndex: 1, status: xtreemstoreS3BulkRequestAdded},
		{key: "received", jobIndex: 2, status: xtreemstoreS3BulkRequestReceived},
		{key: "complete", jobIndex: 3, status: xtreemstoreS3BulkRequestComplete},
		{key: "acked", jobIndex: 4, status: xtreemstoreS3BulkRequestCompleteAcked},
		{key: "sent-cold", jobIndex: 5, status: xtreemstoreS3BulkRequestSent},
		{key: "gone", jobIndex: 6, status: xtreemstoreS3BulkRequestAdded},
		{key: "broken", jobIndex: 7, status: xtreemstoreS3BulkRequestAdded},
	}

	m.probeBatchReadiness(context.Background(), entries)

	// Only the statuses that turn on the object's restore state are worth a HEAD.
	assert.Equal(t, []string{"broken", "cold", "gone", "ready", "sent-cold"}, client.headedKeys(),
		"an entry whose request is already with remote must not be probed")
	assert.Greater(t, client.maxInFlight, 1, "probes should overlap, not run one at a time")

	byKey := map[string]xtreemstoreS3BulkRetrieveBatchEntry{}
	for _, e := range entries {
		byKey[e.key] = e
	}
	assert.True(t, byKey["ready"].ready)
	assert.NoError(t, byKey["ready"].readyErr)
	assert.False(t, byKey["cold"].ready)
	assert.NoError(t, byKey["cold"].readyErr)
	assert.False(t, byKey["sent-cold"].ready)
	assert.NoError(t, byKey["sent-cold"].readyErr)
	assert.ErrorIs(t, byKey["gone"].readyErr, os.ErrNotExist,
		"a vanished object must still reach processSessionBatchKey as os.ErrNotExist so it is cancelled")
	assert.ErrorContains(t, byKey["broken"].readyErr, "head object for key",
		"a failed probe is recorded on its own entry, not propagated")
	assert.NoError(t, byKey["received"].readyErr)
	assert.False(t, byKey["received"].ready)
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
	bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}

	newManager := func() *xtreemstoreS3BulkRetrieveManager {
		return &xtreemstoreS3BulkRetrieveManager{
			s3ApiClient:    &fakeS3ApiClient{},
			rstId:          1,
			mountPath:      tmpDir,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
	}
	readinessErr := func() error {
		return xtreemstoreS3BulkRetrieveError(bulkInfo, 1, tmpDir)
	}

	// Before the operation exists at all there is nothing staged, so requests must be refused.
	assert.ErrorIs(t, readinessErr(), ErrBulkOperationDestroyed)

	m := newManager()
	require.NoError(t, m.openState())
	errorsPath := m.getErrorsPath()

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
