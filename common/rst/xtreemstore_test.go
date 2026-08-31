package rst

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

func TestIncludeRequestInBulkOperation(t *testing.T) {
	x := &xtreemstoreS3Provider{
		bulkOperationCfgs: map[flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation]*BulkOperation{
			flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE: {},
		},
	}

	tests := []struct {
		name          string
		request       *beeremote.JobRequest
		wantInclude   bool
		wantOperation string
	}{
		{
			name:        "non-sync request is never included",
			request:     &beeremote.JobRequest{Type: &beeremote.JobRequest_Mock{Mock: &flex.MockJob{}}},
			wantInclude: false,
		},
		{
			name:        "sync request without locked info is never included",
			request:     &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}}},
			wantInclude: false,
		},
		{
			name: "download that is not archived is not included",
			request: &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_DOWNLOAD,
				LockedInfo: &flex.JobLockedInfo{IsArchived: false},
			}}},
			wantInclude: false,
		},
		{
			// Efficient-retrieve exists to restore archived objects for download. An upload to an
			// already-archived key must not be queued behind a tape restore of data it overwrites.
			name: "archived upload is not included",
			request: &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_UPLOAD,
				LockedInfo: &flex.JobLockedInfo{IsArchived: true},
			}}},
			wantInclude: false,
		},
		{
			name: "archived download is included in the efficient-retrieve operation",
			request: &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_DOWNLOAD,
				LockedInfo: &flex.JobLockedInfo{IsArchived: true},
			}}},
			wantInclude:   true,
			wantOperation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			include, operation := x.IncludeRequestInBulkOperation(context.Background(), tt.request)
			assert.Equal(t, tt.wantInclude, include)
			assert.Equal(t, tt.wantOperation, operation)
		})
	}

	// An operation the target was not configured with must not absorb requests. They fall back to
	// the per-object restore the embedded S3 provider performs.
	t.Run("archived download is not included when the operation is not configured", func(t *testing.T) {
		unconfigured := &xtreemstoreS3Provider{}
		include, operation := unconfigured.IncludeRequestInBulkOperation(context.Background(),
			&beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_DOWNLOAD,
				LockedInfo: &flex.JobLockedInfo{IsArchived: true},
			}}})
		assert.False(t, include)
		assert.Equal(t, "", operation)
	})
}

func TestNewXtreemstoreBulkOperationConfig(t *testing.T) {
	newRST := func(bulkOperations ...*flex.RemoteStorageTarget_XtreemStore_BulkOperation) *flex.RemoteStorageTarget {
		return &flex.RemoteStorageTarget{
			Id:       1,
			Policies: &flex.RemoteStorageTarget_Policies{},
			Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{
				S3:             &flex.RemoteStorageTarget_S3{EndpointUrl: "https://xtreemstore:9000", Bucket: "bucket"},
				BulkOperations: bulkOperations,
			}},
		}
	}
	efficientRetrieve := flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE

	t.Run("delays default when not specified", func(t *testing.T) {
		provider, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{Operation: efficientRetrieve},
		), stubMountPoint{mountPath: t.TempDir()})
		require.NoError(t, err)

		cfg := provider.(*xtreemstoreS3Provider).bulkOperationCfgs[efficientRetrieve]
		require.NotNil(t, cfg)
		assert.Equal(t, DefaultRetryDelay, cfg.RetryDelay)
		assert.Equal(t, DefaultPollDelay, cfg.PollDelay)
	})

	t.Run("configured delays are used", func(t *testing.T) {
		provider, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{
				Operation:  efficientRetrieve,
				RetryDelay: proto.String("2m"),
				PollDelay:  proto.String("30s"),
			},
		), stubMountPoint{mountPath: t.TempDir()})
		require.NoError(t, err)

		cfg := provider.(*xtreemstoreS3Provider).bulkOperationCfgs[efficientRetrieve]
		require.NotNil(t, cfg)
		assert.Equal(t, 2*time.Minute, cfg.RetryDelay)
		assert.Equal(t, 30*time.Second, cfg.PollDelay)
	})

	// An operation is only ever looked up by the value naming it, so an entry left at UNKNOWN (which
	// is what an omitted operation decodes to) would silently disable what it meant to enable.
	t.Run("an operation that was not specified is rejected", func(t *testing.T) {
		_, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{RetryDelay: proto.String("2m")},
		), stubMountPoint{mountPath: t.TempDir()})
		assert.ErrorContains(t, err, "must specify a valid operation")
	})

	t.Run("the same operation configured twice is rejected", func(t *testing.T) {
		_, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{Operation: efficientRetrieve, PollDelay: proto.String("30s")},
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{Operation: efficientRetrieve, PollDelay: proto.String("45s")},
		), stubMountPoint{mountPath: t.TempDir()})
		assert.ErrorContains(t, err, "configured more than once")
	})

	t.Run("delays below the minimum are rejected", func(t *testing.T) {
		_, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{Operation: efficientRetrieve, PollDelay: proto.String("100ms")},
		), stubMountPoint{mountPath: t.TempDir()})
		assert.ErrorContains(t, err, "pollDelay >= '1s'")
	})

	t.Run("a delay that is not a duration is rejected", func(t *testing.T) {
		_, err := newXtreemstore(context.Background(), newRST(
			&flex.RemoteStorageTarget_XtreemStore_BulkOperation{Operation: efficientRetrieve, RetryDelay: proto.String("soon")},
		), stubMountPoint{mountPath: t.TempDir()})
		assert.ErrorContains(t, err, "invalid retryDelay")
	})
}

func TestXtreemstoreProviderIsWorkRequestReady(t *testing.T) {
	t.Run("non-sync request is rejected", func(t *testing.T) {
		x := &xtreemstoreS3Provider{}
		ready, _, err := x.IsWorkRequestReady(context.Background(), context.Background(), &flex.WorkRequest{Type: &flex.WorkRequest_Mock{Mock: &flex.MockJob{}}})
		assert.False(t, ready)
		assert.ErrorIs(t, err, ErrReqAndRSTTypeMismatch)
	})

	t.Run("bulk request with no recorded error is ready without consulting the Provider", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}
		statusDir := path.Join(mountPath, bulkInfo.StateMountPath)
		require.NoError(t, os.MkdirAll(statusDir, 0o700))
		require.NoError(t, os.WriteFile(path.Join(statusDir, "status"), xtreemstoreS3BulkRequestReceived.Bytes(), 0o600))

		request := &flex.WorkRequest{
			Type:     &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}
		ready, _, err := x.IsWorkRequestReady(context.Background(), context.Background(), request)
		require.NoError(t, err)
		assert.True(t, ready)
		mockProvider.AssertNotCalled(t, "IsWorkRequestReady", mock.Anything)
	})

	t.Run("bulk request with a recorded error is not ready and surfaces the error", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}
		errDir := path.Join(mountPath, bulkInfo.StateMountPath)
		require.NoError(t, os.MkdirAll(errDir, 0o700))
		require.NoError(t, os.WriteFile(path.Join(errDir, "errors"), []byte("object no longer exists"), 0o600))

		request := &flex.WorkRequest{
			Type:     &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}
		ready, _, err := x.IsWorkRequestReady(context.Background(), context.Background(), request)
		assert.False(t, ready)
		assert.ErrorContains(t, err, "object no longer exists")
	})

	// Destroy removes the errors file along with the rest of the operation's state, so a request
	// whose job outlived its builder finds no recorded reason to refuse it. It must still be refused:
	// the retrieve-session that staged its object has been released, so reporting it ready would let
	// the download run against whatever the object has decayed to.
	t.Run("bulk request whose operation was destroyed is not ready", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		// The state directory survives but every file in it is gone, as Destroy leaves it.
		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String()}
		require.NoError(t, os.MkdirAll(path.Join(mountPath, bulkInfo.StateMountPath), 0o700))

		request := &flex.WorkRequest{
			Type:     &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}
		ready, _, err := x.IsWorkRequestReady(context.Background(), context.Background(), request)
		assert.False(t, ready)
		assert.ErrorIs(t, err, ErrBulkOperationDestroyed)
	})

	t.Run("non-bulk request delegates entirely to the embedded Provider", func(t *testing.T) {
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{Provider: mockProvider}

		request := &flex.WorkRequest{Type: &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}}}
		mockProvider.On("IsWorkRequestReady", request).Return(true, 5*time.Second, nil)

		ready, delay, err := x.IsWorkRequestReady(context.Background(), context.Background(), request)
		require.NoError(t, err)
		assert.True(t, ready)
		assert.Equal(t, 5*time.Second, delay)
		mockProvider.AssertExpectations(t)
	})
}

// completedWorkResults returns a minimal terminal-success work result. CompleteWorkRequests only
// resolves a job whose work results reached a terminal success state unless the caller explicitly
// aborts, so tests exercising the non-abort path must supply one.
func completedWorkResults() []*flex.Work {
	return []*flex.Work{
		flex.Work_builder{
			Status: flex.Work_Status_builder{State: flex.Work_COMPLETED}.Build(),
		}.Build(),
	}
}

func TestXtreemstoreProviderCompleteWorkRequests(t *testing.T) {
	t.Run("non-sync request is rejected", func(t *testing.T) {
		x := &xtreemstoreS3Provider{}
		job := &beeremote.Job{Request: &beeremote.JobRequest{Type: &beeremote.JobRequest_Mock{Mock: &flex.MockJob{}}}}
		err := x.CompleteWorkRequests(context.Background(), job, nil, false)
		assert.ErrorIs(t, err, ErrReqAndRSTTypeMismatch)
	})

	t.Run("bulk request marks the job complete and still delegates to the Provider", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String(), JobIndex: 0}
		statusDir := path.Join(mountPath, bulkInfo.StateMountPath)
		require.NoError(t, os.MkdirAll(statusDir, 0o700))
		require.NoError(t, os.WriteFile(path.Join(statusDir, "status"), xtreemstoreS3BulkRequestAdded.Bytes(), 0o600))

		job := &beeremote.Job{Request: &beeremote.JobRequest{
			Type:     &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}}
		mockProvider.On("CompleteWorkRequests", job, mock.Anything, false).Return(nil)

		err := x.CompleteWorkRequests(context.Background(), job, completedWorkResults(), false)
		require.NoError(t, err)
		mockProvider.AssertExpectations(t)

		status, err := os.ReadFile(path.Join(statusDir, "status"))
		require.NoError(t, err)
		assert.Equal(t, xtreemstoreS3BulkRequestComplete.Bytes(), status)
	})

	t.Run("a failure marking the bulk job complete is joined with the Provider's own error", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		// The status path is a directory, so opening it for writing fails with something other
		// than ErrNotExist and is reported rather than tolerated.
		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String(), JobIndex: 0}
		statusDir := path.Join(mountPath, bulkInfo.StateMountPath)
		require.NoError(t, os.MkdirAll(path.Join(statusDir, "status"), 0o700))

		job := &beeremote.Job{Request: &beeremote.JobRequest{
			Type:     &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}}
		mockProvider.On("CompleteWorkRequests", job, mock.Anything, false).Return(assert.AnError)

		err := x.CompleteWorkRequests(context.Background(), job, completedWorkResults(), false)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to mark bulk request complete")
		assert.ErrorIs(t, err, assert.AnError)
	})

	// A job can outlive the bulk operation that spawned it: once every request the builder sent has
	// reached a terminal bulk status the builder job completes and destroys the operation's state,
	// but a request whose own job ended up FAILED is still live on remote. Cancelling or retrying
	// that job resolves its bulk request first, so if the missing state were an error the job could
	// never be resolved at all.
	t.Run("resolving a request whose bulk operation state was destroyed succeeds", func(t *testing.T) {
		mountPath := t.TempDir()
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{
			Provider:   mockProvider,
			mountPoint: stubMountPoint{mountPath: mountPath},
		}
		mockProvider.On("GetConfig").Return(&flex.RemoteStorageTarget{Id: 1})

		// No status file exists: the owning builder job already destroyed the operation's state.
		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String(), JobIndex: 0}
		job := &beeremote.Job{Request: &beeremote.JobRequest{
			Type:     &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}}

		// Aborting a FAILED job is what a cancel does, and it must be allowed to succeed.
		mockProvider.On("CompleteWorkRequests", job, mock.Anything, true).Return(nil)
		require.NoError(t, x.CompleteWorkRequests(context.Background(), job, nil, true))

		// Regenerating work requests is what a retry does, and it marks the request received.
		mockProvider.On("GenerateWorkRequests", job, 1).Return([]*flex.WorkRequest{}, nil, nil)
		_, err := x.GenerateWorkRequests(context.Background(), nil, job, 1)
		require.NoError(t, err)

		// BulkRequestFailed is the release path for a request whose job never ran at all.
		manager := &xtreemstoreS3BulkRetrieveManager{
			rstId:          1,
			mountPath:      mountPath,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
		}
		require.NoError(t, manager.UpdateBulkRequest(context.Background(), job.GetRequest(), BulkRequestFailed))

		mockProvider.AssertExpectations(t)
	})
}
