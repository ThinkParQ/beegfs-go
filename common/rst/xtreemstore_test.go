package rst

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	return &s3.HeadObjectOutput{}, nil
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
// isolation: after every dispatched bulk-retrieve record is marked complete (exactly as
// CompleteWorkRequests -> xtreemstoreS3BulkMarkRequestComplete does for each individual sub-job),
// a fresh Execute() pass (as happens on every builder-job reschedule) should stop asking to
// reschedule.
func TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete(t *testing.T) {
	tmpDir := t.TempDir()
	const stateMountPath = "state"
	const operation = "bulk-retrieve"

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
