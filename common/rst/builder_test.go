package rst

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// trackingBulkOperation is a clientBulkOperation stand-in that records whether Cancel was invoked
// and waited on, so tests can assert CompleteWorkRequests' abort path drives cancellation through
// to completion.
type trackingBulkOperation struct {
	cancelCalled  bool
	cancelReason  error
	waitCalled    bool
	destroyCalled bool
}

func (t *trackingBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return nil
}

func (t *trackingBulkOperation) UpdateBulkRequest(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
	return nil
}

func (t *trackingBulkOperation) Execute(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
	walkCh := make(chan *BulkStreamPathResult)
	close(walkCh)
	return walkCh, func() *BulkExecuteResult { return &BulkExecuteResult{} }, nil
}

func (t *trackingBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
	t.cancelCalled = true
	t.cancelReason = reason

	// The operation closes its own walk channel, as xtreemstoreS3BulkRetrieveManager.Cancel does
	// from the goroutine that feeds it. cancelBulkOperation drains the walk to the end before it
	// calls wait, so a fake that only closed the channel from wait would never be drained. This
	// operation has no requests to release, so the walk is empty.
	walkCh := make(chan *BulkStreamPathResult)
	close(walkCh)

	return walkCh, func() error {
		t.waitCalled = true
		return nil
	}, nil
}

func (t *trackingBulkOperation) Close(ctx context.Context) error {
	return nil
}

func (t *trackingBulkOperation) Destroy(ctx context.Context) error {
	t.destroyCalled = true
	return nil
}

// TestNewBulkOperationRegistryReopensSavedBulkOperations asserts a registry recovers the operations an
// earlier attempt of the builder job persisted, including ones that already failed permanently, since
// that state is what keeps an interrupted job from restarting or abandoning its bulk operations.
func TestNewBulkOperationRegistryReopensSavedBulkOperations(t *testing.T) {
	mountPath := t.TempDir()
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 2, Operation: "archive", Failed: true, Errors: []string{"resume failed"}})

	mockRST := &MockClient{}
	// Both operations are reopened, the failed one included: without a provider handle it could never
	// be cancelled or have its state deleted.
	mockRST.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/job-1/1/retrieve", "retrieve").Return(&trackingBulkOperation{}, nil).Once()
	mockRST.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/job-1/2/archive", "archive").Return(&trackingBulkOperation{}, nil).Once()

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath})
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)

	managers := registry.GetManagersSnapshot()
	require.Len(t, managers, 2)
	require.Contains(t, managers, "1-retrieve")
	require.Contains(t, managers, "2-archive")
	assert.False(t, managers["1-retrieve"].IsFailed())
	assert.NoError(t, managers["1-retrieve"].GetErrors())
	assert.True(t, managers["2-archive"].IsFailed(), "a permanently failed operation must not be resumed")
	require.Error(t, managers["2-archive"].GetErrors())
	assert.Contains(t, managers["2-archive"].GetErrors().Error(), "resume failed")
	mockRST.AssertExpectations(t)
}

// TestBulkOperationRegistry_GetFailedOperationErrors asserts a permanently failed operation is
// reported with its reason, and that healthy operations contribute nothing. This is what the builder
// job folds into its result: the paths a failed operation absorbed never become jobs, so without it
// they would be dropped and the job would still report success.
func TestBulkOperationRegistry_GetFailedOperationErrors(t *testing.T) {
	mountPath := t.TempDir()
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{
		RstId: 2, Operation: "archive", Failed: true, Errors: []string{"retrieve-session expired"},
	})

	mockRST := &MockClient{}
	mockRST.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&trackingBulkOperation{}, nil)

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath})
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)

	failedErr := registry.GetFailedOperationErrors()
	require.Error(t, failedErr)
	assert.Contains(t, failedErr.Error(), "2-archive")
	assert.Contains(t, failedErr.Error(), "retrieve-session expired")
	assert.NotContains(t, failedErr.Error(), "1-retrieve", "a healthy operation must not be reported as failed")
}

// TestBulkOperationRegistry_GetFailedOperationErrorsWhenNoneFailed asserts a job whose operations all
// succeeded reports no error, so a healthy builder job isn't cancelled by this check.
func TestBulkOperationRegistry_GetFailedOperationErrorsWhenNoneFailed(t *testing.T) {
	mountPath := t.TempDir()
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})

	mockRST := &MockClient{}
	mockRST.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&trackingBulkOperation{}, nil)

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath})
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)

	assert.NoError(t, registry.GetFailedOperationErrors())
}

// TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations asserts a builder job that never started
// a bulk operation doesn't fail just because it has no state on the mount.
func TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations(t *testing.T) {
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{}, stubMountPoint{mountPath: t.TempDir()})
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)
	assert.Empty(t, registry.GetManagersSnapshot())
}

func TestCompleteWorkRequestsAbortCancelsAllStartedBulkOperations(t *testing.T) {
	mountPath := t.TempDir()
	mockRST := &MockClient{}
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath})

	job := beeremote.Job_builder{
		Id: "builder-job",
		Request: beeremote.JobRequest_builder{
			Path:                "/test/builder",
			RemoteStorageTarget: JobBuilderRstId,
			Builder:             flex.BuilderJob_builder{}.Build(),
		}.Build(),
	}.Build()

	tracker := &trackingBulkOperation{}
	mockRST.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/builder-job/1/retrieve", "retrieve").Return(tracker, nil).Once()

	// The operation is recovered from the mount, which is the only record of it when the sync node
	// that started it crashed before reporting it back to remote.
	manager := saveTestBulkOperationEntry(t, mountPath, job.GetId(), &bulkOperationEntry{RstId: 1, Operation: "retrieve"})

	// An aborted builder job has no reserved requests to release here: the operation reports those
	// through its cancel walk, and this one has none.
	cancelRequest := func(path string, jobId string) error {
		t.Fatalf("no request may be cancelled for an operation with an empty cancel walk (path %q, job %q)", path, jobId)
		return nil
	}

	err := client.CompleteJobBuilderRequest(context.Background(), job, nil, cancelRequest, true)
	require.NoError(t, err)
	require.True(t, tracker.cancelCalled)
	require.True(t, tracker.waitCalled)
	require.ErrorContains(t, tracker.cancelReason, "builder job was aborted")
	require.True(t, tracker.destroyCalled)
	require.NoFileExists(t, manager.getEntryPath(), "a destroyed operation must not be left on the mount")
	mockRST.AssertExpectations(t)
}

func newDownloadPathsFn(t *testing.T, fs filesystem.Provider, cfg *flex.JobRequestCfg) requestPathResolverFn {
	t.Helper()
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, fs)
	getPaths, err := client.getPathsFn(cfg)
	require.NoError(t, err)
	return getPaths
}

// TestGetPathsFnResolvesWithoutTouchingTheFilesystem verifies path resolution is pure. Creating the
// download's parent directory belongs to the file creation in PlanFileStateForWorkRequests, which
// learns the directory is missing for free, so the resolver must not create directories itself.
func TestGetPathsFnResolvesWithoutTouchingTheFilesystem(t *testing.T) {
	rfs := newRecordingFS(t)
	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})

	for _, key := range []string{
		"/bucket/prefix/a/1", "/bucket/prefix/a/2", "/bucket/prefix/b/1",
	} {
		inMountPath, remotePath := getPaths(key)
		assert.Equal(t, key, remotePath)
		assert.Equal(t, "/mnt/dest/prefix"+key[len("/bucket/prefix"):], inMountPath)
	}

	created, _ := rfs.calls()
	assert.Empty(t, created, "the resolver must not create directories")
}

// TestGetPathsFnStatsThePathOnlyOnce verifies the Lstat is hoisted out of the walk. It used to run
// once per walked object.
func TestGetPathsFnStatsThePathOnlyOnce(t *testing.T) {
	rfs := &countingLstatFS{recordingFS: newRecordingFS(t)}
	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})

	for i := range 5 {
		getPaths(fmt.Sprintf("/bucket/prefix/a/%d", i))
	}
	assert.Equal(t, 1, rfs.lstats)
}

type countingLstatFS struct {
	*recordingFS
	lstats int
}

func (f *countingLstatFS) Lstat(path string) (os.FileInfo, error) {
	f.lstats++
	return f.recordingFS.Lstat(path)
}

// TestGetPathsFnToleratesMissingDestination verifies an Lstat that reports ErrNotExist does not
// abort the resolver. Downloading a single object to a name that does not exist yet is a supported
// flow, and GetDownloadInMountPath treats a destination that is not a directory as the file to
// write.
func TestGetPathsFnToleratesMissingDestination(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.lstatErr = os.ErrNotExist

	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest/newfile",
		RemotePath: "/bucket/prefix/key",
	})

	inMountPath, remotePath := getPaths("/bucket/prefix/key")
	assert.Equal(t, "/mnt/dest/newfile", inMountPath)
	assert.Equal(t, "/bucket/prefix/key", remotePath)
}

// TestGetPathsFnFailsOnUnreadableDestination verifies errors other than "does not exist" still
// abort, and do so when the resolver is built rather than once per object.
func TestGetPathsFnFailsOnUnreadableDestination(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.lstatErr = os.ErrPermission
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, rfs)

	getPaths, err := client.getPathsFn(&flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})
	require.ErrorIs(t, err, os.ErrPermission)
	assert.Nil(t, getPaths)
}

// TestGetPathsFnLocalWalkAndUploadResolvers covers the two resolvers that never consult the
// filesystem: a stub download walks local paths, and an upload walks the mount directly.
func TestGetPathsFnLocalWalkAndUploadResolvers(t *testing.T) {
	rfs := newRecordingFS(t)

	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{Download: true, Path: "/mnt/dest"})
	inMountPath, remotePath := getPaths("/mnt/dest/file")
	assert.Equal(t, "/mnt/dest/file", inMountPath)
	assert.Empty(t, remotePath)

	getPaths = newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{Path: "/mnt/src"})
	inMountPath, remotePath = getPaths("/mnt/src/file")
	assert.Equal(t, "/mnt/src/file", inMountPath)
	assert.Equal(t, "/mnt/src/file", remotePath)
}
