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
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// stubMountPoint fakes a filesystem.Provider that points at a real on-disk directory, since
// xtreemstoreS3BulkRetrieveManager reads/writes its state files with the os package directly
// rather than through the Provider interface.
type stubMountPoint struct {
	filesystem.Provider
	mountPath string
}

func (s stubMountPoint) GetMountPath() string {
	return s.mountPath
}

func TestIncludeRequestInBulkOperation(t *testing.T) {
	x := &xtreemstoreS3Provider{}

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
			name:        "sync request that is not archived is not included",
			request:     &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{LockedInfo: &flex.JobLockedInfo{IsArchived: false}}}},
			wantInclude: false,
		},
		{
			name:          "archived sync request is included in the bulk-retrieve operation",
			request:       &beeremote.JobRequest{Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{LockedInfo: &flex.JobLockedInfo{IsArchived: true}}}},
			wantInclude:   true,
			wantOperation: "bulk-retrieve",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			include, operation := x.IncludeRequestInBulkOperation(context.Background(), tt.request)
			assert.Equal(t, tt.wantInclude, include)
			assert.Equal(t, tt.wantOperation, operation)
		})
	}
}

func TestXtreemstoreProviderIsWorkRequestReady(t *testing.T) {
	t.Run("non-sync request is rejected", func(t *testing.T) {
		x := &xtreemstoreS3Provider{}
		ready, _, err := x.IsWorkRequestReady(context.Background(), &flex.WorkRequest{Type: &flex.WorkRequest_Mock{Mock: &flex.MockJob{}}})
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

		request := &flex.WorkRequest{
			Type:     &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: "bulk-retrieve"},
		}
		ready, _, err := x.IsWorkRequestReady(context.Background(), request)
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

		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: "bulk-retrieve"}
		errDir := path.Join(mountPath, bulkInfo.StateMountPath, bulkInfo.Operation)
		require.NoError(t, os.MkdirAll(errDir, 0o700))
		require.NoError(t, os.WriteFile(path.Join(errDir, "errors"), []byte("object no longer exists"), 0o600))

		request := &flex.WorkRequest{
			Type:     &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}},
			BulkInfo: bulkInfo,
		}
		ready, _, err := x.IsWorkRequestReady(context.Background(), request)
		assert.False(t, ready)
		assert.ErrorContains(t, err, "object no longer exists")
	})

	t.Run("non-bulk request delegates entirely to the embedded Provider", func(t *testing.T) {
		mockProvider := &MockClient{}
		x := &xtreemstoreS3Provider{Provider: mockProvider}

		request := &flex.WorkRequest{Type: &flex.WorkRequest_Sync{Sync: &flex.SyncJob{}}}
		mockProvider.On("IsWorkRequestReady", request).Return(true, 5*time.Second, nil)

		ready, delay, err := x.IsWorkRequestReady(context.Background(), request)
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

		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: "bulk-retrieve", JobIndex: 0}
		statusDir := path.Join(mountPath, bulkInfo.StateMountPath, bulkInfo.Operation)
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

		// No status file was ever created for this bulk operation, so marking it complete fails.
		bulkInfo := &flex.BulkJobRequestInfo{StateMountPath: "state", Operation: "bulk-retrieve", JobIndex: 0}
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
}
