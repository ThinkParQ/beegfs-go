package rst

import (
	"context"
	"fmt"
	"os"
	"path"
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
	// cancelWalk is what Cancel reports, each as the operation's walk path for one request.
	cancelWalk    []*BulkStreamPathResult
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
	// calls wait, so a fake that only closed the channel from wait would never be drained.
	walkCh := make(chan *BulkStreamPathResult, len(t.cancelWalk))
	for _, result := range t.cancelWalk {
		walkCh <- result
	}
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
	saveTestBulkOperationEntry(t, mountPath, testBuilderJobId, &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	saveTestBulkOperationEntry(t, mountPath, testBuilderJobId, &bulkOperationEntry{RstId: 2, Operation: "archive", Failed: true, Errors: []string{"resume failed"}})

	mockRST := &MockClient{}
	// Both operations are reopened, the failed one included: without a provider handle it could never
	// be cancelled or have its state deleted.
	mockRST.On("OpenBulkOperation", mock.Anything, path.Join(stateLayoutMountPath(DefaultStateRoot), bulkManagerPath, testBuilderJobId, "1", "retrieve"), "retrieve").Return(&trackingBulkOperation{}, nil).Once()
	mockRST.On("OpenBulkOperation", mock.Anything, path.Join(stateLayoutMountPath(DefaultStateRoot), bulkManagerPath, testBuilderJobId, "2", "archive"), "archive").Return(&trackingBulkOperation{}, nil).Once()

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), testBuilderJobId)
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
	saveTestBulkOperationEntry(t, mountPath, testBuilderJobId, &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	saveTestBulkOperationEntry(t, mountPath, testBuilderJobId, &bulkOperationEntry{
		RstId: 2, Operation: "archive", Failed: true, Errors: []string{"retrieve-session expired"},
	})

	mockRST := &MockClient{}
	mockRST.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&trackingBulkOperation{}, nil)

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), testBuilderJobId)
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
	saveTestBulkOperationEntry(t, mountPath, testBuilderJobId, &bulkOperationEntry{RstId: 1, Operation: "retrieve"})

	mockRST := &MockClient{}
	mockRST.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&trackingBulkOperation{}, nil)

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), testBuilderJobId)
	require.NoError(t, err)

	assert.NoError(t, registry.GetFailedOperationErrors())
}

// TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations asserts a builder job that never started
// a bulk operation doesn't fail just because it has no state on the mount.
func TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations(t *testing.T) {
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{}, stubMountPoint{mountPath: t.TempDir()}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), testBuilderJobId)
	require.NoError(t, err)
	assert.Empty(t, registry.GetManagersSnapshot())
}

func TestCompleteWorkRequestsAbortCancelsAllStartedBulkOperations(t *testing.T) {
	mountPath := t.TempDir()
	mockRST := &MockClient{}
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)

	job := beeremote.Job_builder{
		Id: testBuilderJobId,
		Request: beeremote.JobRequest_builder{
			Path:                "/test/builder",
			RemoteStorageTarget: JobBuilderRstId,
			Builder: flex.BuilderJob_builder{
				Cfg: flex.JobRequestCfg_builder{Path: "/test/builder", RemoteStorageTarget: 1}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	tracker := &trackingBulkOperation{}
	mockRST.On("OpenBulkOperation", mock.Anything, path.Join(stateLayoutMountPath(DefaultStateRoot), bulkManagerPath, testBuilderJobId, "1", "retrieve"), "retrieve").Return(tracker, nil).Once()

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
	require.NoFileExists(t, entryFilePath(manager), "a destroyed operation must not be left on the mount")
	mockRST.AssertExpectations(t)
}

// missingPathMount reports every path as missing, like a download destination that does not exist
// yet. getPathsFn stats the destination to decide how a key maps into it.
type missingPathMount struct {
	stubMountPoint
}

func (m missingPathMount) Lstat(path string) (os.FileInfo, error) {
	return nil, os.ErrNotExist
}

// TestCompleteJobBuilderRequestCancelsReservationsByPathInMount pins how a cancelled builder job
// releases the jobs its bulk operation reserved. The cancel walk reports each request by its walk
// path, which for a download that walks the remote target is the object key. Remote keys reserved
// jobs by the path in the mount, so cancelling by the key would find nothing and strand them.
func TestCompleteJobBuilderRequestCancelsReservationsByPathInMount(t *testing.T) {
	mountPath := t.TempDir()
	mockRST := &MockClient{}
	mountPoint := missingPathMount{stubMountPoint{mountPath: mountPath}}
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, mountPoint, DefaultStateRoot)

	// A pull of the prefix data/ into /restore, which walks the remote target.
	job := beeremote.Job_builder{
		Id: testBuilderJobId,
		Request: beeremote.JobRequest_builder{
			Path:                "/restore",
			RemoteStorageTarget: JobBuilderRstId,
			Builder: flex.BuilderJob_builder{
				Cfg: flex.JobRequestCfg_builder{
					Path:                "/restore",
					RemoteStorageTarget: 1,
					RemotePath:          "data/",
					Download:            true,
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	tracker := &trackingBulkOperation{cancelWalk: []*BulkStreamPathResult{
		{InMountPath: "/restore/data/a", RemotePath: "data/a", ReservedJobId: "reserved-a", RstId: 1},
	}}
	mockRST.On("OpenBulkOperation", mock.Anything, mock.Anything, "retrieve").Return(tracker, nil).Once()
	saveTestBulkOperationEntry(t, mountPath, job.GetId(), &bulkOperationEntry{RstId: 1, Operation: "retrieve"})

	cancelled := map[string]string{}
	cancelRequest := func(path string, jobId string) error {
		cancelled[path] = jobId
		return nil
	}

	err := client.CompleteJobBuilderRequest(context.Background(), job, nil, cancelRequest, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"/restore/data/a": "reserved-a"}, cancelled,
		"the reserved job must be cancelled by the in-mount path the operation recorded, not by the object key")
	require.True(t, tracker.destroyCalled)
}

// ctxCheckingBulkOperation fails every call once its context is done.
//
// This is what keeps TestCompleteWorkRequestsFinishesBulkTeardownAfterCancel honest. The teardown
// it exercises is otherwise all os calls and mocks that ignore their context, so with a
// context-indifferent fake the test would pass whether or not CompleteWorkRequests held a grace
// period -- which is precisely the regression it exists to catch.
type ctxCheckingBulkOperation struct {
	destroyCalls int
	closeCalls   int
}

func (f *ctxCheckingBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return ctx.Err()
}

func (f *ctxCheckingBulkOperation) UpdateBulkRequest(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
	return ctx.Err()
}

func (f *ctxCheckingBulkOperation) Execute(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
	return nil, nil, ctx.Err()
}

func (f *ctxCheckingBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
	return nil, nil, ctx.Err()
}

func (f *ctxCheckingBulkOperation) Close(ctx context.Context) error {
	f.closeCalls++
	return ctx.Err()
}

func (f *ctxCheckingBulkOperation) Destroy(ctx context.Context) error {
	f.destroyCalls++
	return ctx.Err()
}

// TestCompleteWorkRequestsFinishesBulkTeardownAfterCancel pins the grace period
// CompleteWorkRequests holds (common/rst/builder.go).
//
// WHY A UNIT TEST. The behavioural suite cannot reach this: the grace is armed only while a bulk
// job is completing or being cancelled, which is a sub-second window, so a SIGTERM aimed at it from
// outside lands before or after it essentially every time.
//
// WHAT WOULD FAIL WITHOUT THE GRACE. CompleteWorkRequests is handed the manager's context, which is
// already cancelled by the time a shutdown reaches it. Passing that context straight through means
// clientBulkOperation.Destroy is called with a done context and returns immediately, leaving the
// operation's state directory and its entry on the mount -- and a leftover retrieve-session entry
// blocks every subsequent bulk retrieve for the whole bucket. The grace is what lets the teardown
// outlive the cancellation, so asserting the mount is CLEAN afterwards is the same as asserting the
// grace held.
func TestCompleteWorkRequestsFinishesBulkTeardownAfterCancel(t *testing.T) {
	const jobId = testBuilderJobId
	// The registry and its managers persist entries with plain os calls under
	// mountPoint.GetMountPath(), so the stub needs a real directory to report. A provider reporting
	// "" would put that state at a relative path outside the test's control.
	mountPath := t.TempDir()

	bulkOp := &ctxCheckingBulkOperation{}
	rstClient := &MockClient{}
	rstClient.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(bulkOp, nil)

	// An entry an earlier pass of this builder job would have written, so the registry rebuilds a
	// manager for it rather than starting empty (an empty registry returns before the teardown).
	saveTestBulkOperationEntry(t, mountPath, jobId, &bulkOperationEntry{
		RstId:     1,
		Operation: "EFFICIENT_RETRIEVE",
	})
	require.Len(t, readTestBulkOperationEntries(t, mountPath, jobId), 1,
		"precondition: the seeded entry must be on the mount before the teardown runs")

	client := NewJobBuilderClient(
		context.Background(),
		map[uint32]Provider{1: rstClient},
		stubMountPoint{mountPath: mountPath},
		DefaultStateRoot,
	)

	// Cancelled BEFORE the call, which is the shutdown ordering: Manager.Stop cancels the manager
	// context and only then waits for the work still running under it.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, ctx.Err(), "precondition: the context must already be cancelled")

	job := beeremote.Job_builder{
		Id: jobId,
		Request: beeremote.JobRequest_builder{
			Path:                "/test/builder",
			RemoteStorageTarget: JobBuilderRstId,
			Builder: flex.BuilderJob_builder{
				Cfg: flex.JobRequestCfg_builder{Path: "/test/builder", RemoteStorageTarget: 1}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	workResults := []*flex.Work{{Status: &flex.Work_Status{State: flex.Work_COMPLETED}}}

	// The job completed and is not aborted, so nothing is cancelled and the cancel walk never runs.
	cancelRequest := func(path string, jobId string) error {
		t.Fatalf("no request may be cancelled when the builder job completed (path %q, job %q)", path, jobId)
		return nil
	}

	err := client.CompleteJobBuilderRequest(ctx, job, workResults, cancelRequest, false)
	require.NoError(t, err, "the teardown must complete despite the cancelled parent context")

	require.Positive(t, bulkOp.destroyCalls,
		"Destroy was never called, so the cancelled context stopped the teardown before it started")
	require.Empty(t, readTestBulkOperationEntries(t, mountPath, jobId),
		"the bulk operation entry is still on the mount, so the teardown did not finish")

	// Nothing may be left behind under the job's directory either: a surviving state directory is
	// what a later builder job would find and try to reopen.
	_, statErr := os.Stat(path.Join(mountPath, path.Join(stateLayoutMountPath(DefaultStateRoot), bulkOperationEntriesPath(jobId))))
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"the job's bulk state directory survived the teardown")
}

func newDownloadPathsFn(t *testing.T, fs filesystem.Provider, cfg *flex.JobRequestCfg) requestPathResolverFn {
	t.Helper()
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, fs, DefaultStateRoot)
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
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, rfs, DefaultStateRoot)

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

// testBuilderJobId is the builder job the tests run as. It has to be a UUID because the registry
// refuses any other ID before joining it into a state path.
const testBuilderJobId = "6f1c2b9e-3d4a-4c5b-8e7f-0a1b2c3d4e5f"

// The builder job ID arrives in the work request and is joined into state paths, so anything that is
// not a UUID is refused before the mount is touched.
func TestNewBulkOperationRegistryRefusesNonUUIDJobId(t *testing.T) {
	mountPath := t.TempDir()
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)

	for _, jobId := range []string{"", "job-1", "../other", testBuilderJobId + "/../other"} {
		_, err := client.newBulkOperationRegistry(context.Background(), jobId)
		assert.ErrorContains(t, err, "is not a valid UUID", "job ID %q must be refused", jobId)
	}
	assert.NoDirExists(t, path.Join(mountPath, DefaultStateRoot), "a refused job ID must not create any state")
}
