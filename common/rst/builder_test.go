package rst

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
	walkCh := make(chan *BulkStreamPathResult)
	return walkCh, func() error {
		t.cancelCalled = true
		t.cancelReason = reason
		t.waitCalled = true
		close(walkCh)
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

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
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

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST, 2: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
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

	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)

	assert.NoError(t, registry.GetFailedOperationErrors())
}

// TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations asserts a builder job that never started
// a bulk operation doesn't fail just because it has no state on the mount.
func TestNewBulkOperationRegistryWithoutSavedStateHasNoOperations(t *testing.T) {
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{}, stubMountPoint{mountPath: t.TempDir()}, DefaultStateRoot)
	registry, err := client.newBulkOperationRegistry(context.Background(), "job-1")
	require.NoError(t, err)
	assert.Empty(t, registry.GetManagersSnapshot())
}

func TestCompleteWorkRequestsAbortCancelsAllStartedBulkOperations(t *testing.T) {
	mountPath := t.TempDir()
	mockRST := &MockClient{}
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, stubMountPoint{mountPath: mountPath}, DefaultStateRoot)

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

	err := client.CompleteWorkRequests(context.Background(), job, nil, true)
	require.NoError(t, err)
	require.True(t, tracker.cancelCalled)
	require.True(t, tracker.waitCalled)
	require.ErrorContains(t, tracker.cancelReason, "builder job was aborted")
	require.True(t, tracker.destroyCalled)
	require.NoFileExists(t, manager.getEntryPath(), "a destroyed operation must not be left on the mount")
	mockRST.AssertExpectations(t)
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
	const jobId = "grace-job-1"
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

	job := &beeremote.Job{Id: jobId}
	workResults := []*flex.Work{{Status: &flex.Work_Status{State: flex.Work_COMPLETED}}}

	err := client.CompleteWorkRequests(ctx, job, workResults, false)
	require.NoError(t, err, "the teardown must complete despite the cancelled parent context")

	require.Positive(t, bulkOp.destroyCalls,
		"Destroy was never called, so the cancelled context stopped the teardown before it started")
	require.Empty(t, readTestBulkOperationEntries(t, mountPath, jobId),
		"the bulk operation entry is still on the mount, so the teardown did not finish")

	// Nothing may be left behind under the job's directory either: a surviving state directory is
	// what a later builder job would find and try to reopen.
	_, statErr := os.Stat(path.Join(mountPath, bulkOperationMountPath(DefaultStateRoot, jobId)))
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"the job's bulk state directory survived the teardown")
}
