package rst

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestRequestBuildController_WalkSourceProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 2)
	walkCh <- &filesystem.StreamPathResult{Path: "/a"}
	walkCh <- &filesystem.StreamPathResult{Path: "/b"}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)
	require.NoError(t, controller.WaitForWalkSources())

	result, resumeToken := controller.GetResults()
	assert.Empty(t, resumeToken)
	assert.False(t, result.Reschedule)
	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_WalkSourceSetsResumeTokenAndReschedules(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "resume-token"}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)
	require.NoError(t, controller.WaitForWalkSources())

	result, resumeToken := controller.GetResults()
	assert.Equal(t, "resume-token", resumeToken)
	assert.True(t, result.Reschedule)
}

func TestRequestBuildController_WalkSourceRejectsConflictingResumeTokens(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.resumeToken = "existing-token"

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "new-token"}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)
	err := controller.WaitForWalkSources()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting walk resume tokens")
}

func TestRequestBuildController_WalkSourceReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkErr := fmt.Errorf("walk failed")
	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Err: walkErr}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)
	err := controller.WaitForWalkSources()
	require.ErrorIs(t, err, walkErr)
}

// TestRequestBuildController_WalkSourceConvertsRequestCancelErrorToFailedPrecondition asserts that a
// RequestCancelError on the walk result does not fail the builder job. Instead it is submitted as a
// FAILED_PRECONDITION request carrying the cancellation reason.
func TestRequestBuildController_WalkSourceConvertsRequestCancelErrorToFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Path: "/a", Err: &RequestCancelError{Reason: errors.New("cancelled")}}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)
	require.NoError(t, controller.WaitForWalkSources())

	requests := drainRequests(jobSubmissionCh)
	require.Len(t, requests, 1)
	assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, requests[0].GetGenerationStatus().GetState())
	assert.Equal(t, "cancelled", requests[0].GetGenerationStatus().GetMessage())
}

func TestRequestBuildController_ExecuteBulkOperationProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{
		Path:     "/bulk-a",
		RstId:    1,
		BulkInfo: &flex.BulkJobRequestInfo{Operation: "retrieve"},
	}
	close(bulkCh)

	manager := newTestBulkManager("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
	}, nil)
	controller.ExecuteBulkOperation(manager)
	require.NoError(t, controller.WaitForBulkOperations())

	result, resumeToken := controller.GetResults()
	assert.Empty(t, resumeToken)
	assert.False(t, result.Reschedule)
	assert.Equal(t, []string{"/bulk-a"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_ExecuteBulkOperationReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkErr := fmt.Errorf("bulk walk failed")
	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{Err: walkErr}
	close(bulkCh)

	manager := newTestBulkManager("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
	}, nil)
	controller.ExecuteBulkOperation(manager)

	err := controller.WaitForBulkOperations()
	require.ErrorIs(t, err, walkErr)
}

func TestRequestBuildController_ExecuteBulkOperationNoopWhenManagerAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		t.Fatal("Execute should not be called for an already-failed manager")
		return nil, nil, nil
	}, nil)
	manager.SetFailed()

	controller.ExecuteBulkOperation(manager)
	require.NoError(t, controller.WaitForBulkOperations())
}

// TestRequestBuildController_ExecuteBulkOperationExecuteErrorCancelsAndSurfacesReason covers the case
// where a registered bulk manager fails before it ever produces a walk channel (e.g. it could not be
// opened). ExecuteBulkOperation must fall back to cancelling the manager with that error as the
// reason rather than silently dropping it. A well-behaved clientBulkOperation forwards the reason
// for any paths it can't complete via its own Cancel walk channel, so the failure still surfaces
// through WaitForBulkOperations.
func TestRequestBuildController_ExecuteBulkOperationExecuteErrorCancelsAndSurfacesReason(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	openErr := fmt.Errorf("failed to open bulk operation")
	manager := newTestBulkManager("mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			return nil, nil, openErr
		},
		func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult, 1)
			walkCh <- &BulkStreamPathResult{Err: reason}
			close(walkCh)
			return walkCh, func() error { return nil }, nil
		},
	)
	controller.ExecuteBulkOperation(manager)

	err := controller.WaitForBulkOperations()
	require.ErrorIs(t, err, openErr)
}

// TestRequestBuildController_ExecuteBulkOperationMergesRescheduleAcrossManagers asserts that when
// multiple bulk managers report a reschedule, the merged result keeps the smallest delay while the
// error from the manager that produced it is preserved on the merged result.
func TestRequestBuildController_ExecuteBulkOperationMergesRescheduleAcrossManagers(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	boomErr := fmt.Errorf("boom")
	emptyBulkExecuteFn := func(delay time.Duration, err error) BulkExecuteFn {
		return func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *SchedulingResult {
				return &SchedulingResult{Reschedule: true, Delay: delay, Err: err}
			}, nil
		}
	}
	// A result.Err from getResult() triggers an automatic cancel of the manager, so every manager
	// here needs a Cancel implementation even though the test isn't exercising cancellation itself.
	noopCancel := func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		walkCh := make(chan *BulkStreamPathResult)
		close(walkCh)
		return walkCh, func() error { return nil }, nil
	}

	slowManager := newTestBulkManager("slow", emptyBulkExecuteFn(5*time.Second, nil), noopCancel)
	fastManager := newTestBulkManager("fast", emptyBulkExecuteFn(2*time.Second, boomErr), noopCancel)
	controller.ExecuteBulkOperation(slowManager)
	controller.ExecuteBulkOperation(fastManager)

	require.NoError(t, controller.WaitForBulkOperations())

	result, _ := controller.GetResults()
	assert.True(t, result.Reschedule)
	assert.Equal(t, 2*time.Second, result.Delay)
	assert.ErrorIs(t, result.Err, boomErr)
}

func TestRequestBuildController_CancelBulkOperationNoopWhenManagerAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager("mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		t.Fatal("Cancel should not be called for an already-failed manager")
		return nil, nil, nil
	})
	manager.SetFailed()

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))
	require.NoError(t, controller.WaitForBulkOperations())
}

func TestRequestBuildController_CancelBulkOperationSetsManagerFailedWhenCancelErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	cancelErr := fmt.Errorf("cannot cancel")
	manager := newTestBulkManager("mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		return nil, nil, cancelErr
	})

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))

	assert.True(t, manager.IsFailed())
	require.Error(t, manager.GetErrors())
	assert.Contains(t, manager.GetErrors().Error(), cancelErr.Error())
	// Cancel failed before any walk goroutine was spawned, so there's nothing left to wait for.
	require.NoError(t, controller.WaitForBulkOperations())
}

func TestRequestBuildController_CancelBulkOperationSetsManagerFailedOnWaitError(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	waitErr := fmt.Errorf("cancel wait failed")
	manager := newTestBulkManager("mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		walkCh := make(chan *BulkStreamPathResult)
		close(walkCh)
		return walkCh, func() error { return waitErr }, nil
	})

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))
	require.NoError(t, controller.WaitForBulkOperations())

	assert.True(t, manager.IsFailed())
	require.Error(t, manager.GetErrors())
	assert.Contains(t, manager.GetErrors().Error(), waitErr.Error())
}

func TestRequestBuildController_WaitForWalkSourcesReturnsImmediatelyWhenNoSourceWalk(t *testing.T) {
	controller := &requestBuildController{}
	require.NoError(t, controller.WaitForWalkSources())
}

func TestRequestBuildController_WaitForBulkOperationsReturnsImmediatelyWhenNoBulkWalk(t *testing.T) {
	controller := &requestBuildController{}
	require.NoError(t, controller.WaitForBulkOperations())
}

// TestRequestBuildController_PathProcessingConcurrencyIsBounded asserts that maxWorkersCh actually
// bounds how many paths are processed concurrently: with a single worker slot, a second path must
// not start processing until the first releases its slot.
func TestRequestBuildController_PathProcessingConcurrencyIsBounded(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.maxWorkersCh = make(chan struct{}, 1)

	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var mu sync.Mutex
	var maxInFlight, inFlight int

	baseGetPathState := controller.requestBuilder.getPathState
	controller.requestBuilder.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()

		started <- struct{}{}
		<-release

		mu.Lock()
		inFlight--
		mu.Unlock()
		return baseGetPathState(ctx, mountPoint, inMountPath, mode)
	}

	walkCh := make(chan *filesystem.StreamPathResult, 3)
	walkCh <- &filesystem.StreamPathResult{Path: "/a"}
	walkCh <- &filesystem.StreamPathResult{Path: "/b"}
	walkCh <- &filesystem.StreamPathResult{Path: "/c"}
	close(walkCh)

	controller.WalkSourceGenerator(testWalkChGenerator(walkCh), "", 0)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first path never started processing")
	}

	select {
	case <-started:
		t.Fatal("a second path started processing before the first released its worker slot")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	require.NoError(t, controller.WaitForWalkSources())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, maxInFlight)
	assert.Equal(t, 0, inFlight)
}

// testWalkChGenerator returns a nextWalkChGenerator that always hands back walkCh, for tests that
// only need a single walk channel and never expect nextWalkCh to be called with a follow-up resume
// token.
func testWalkChGenerator(walkCh <-chan *filesystem.StreamPathResult) nextWalkChGenerator {
	return func(resumeToken string) (<-chan *filesystem.StreamPathResult, error) {
		return walkCh, nil
	}
}

// submittedPaths closes and drains jobSubmissionCh, returning the path of every submitted request.
// Callers must only invoke this once no further sends can occur, e.g. after
// requestBuildController.WaitForWalkSources()/WaitForBulkOperations().
func submittedPaths(jobSubmissionCh chan *beeremote.JobRequest) []string {
	var paths []string
	for _, req := range drainRequests(jobSubmissionCh) {
		paths = append(paths, req.GetPath())
	}
	return paths
}

// drainRequests closes and drains jobSubmissionCh, returning every submitted request. Callers must
// only invoke this once no further sends can occur.
func drainRequests(jobSubmissionCh chan *beeremote.JobRequest) []*beeremote.JobRequest {
	close(jobSubmissionCh)
	var requests []*beeremote.JobRequest
	for req := range jobSubmissionCh {
		requests = append(requests, req)
	}
	return requests
}

func newTestRequestBuildController(ctx context.Context, jobSubmissionCh chan *beeremote.JobRequest) *requestBuildController {
	client := NewJobBuilderClient(ctx, map[uint32]Provider{1: &MockClient{}}, filesystem.NewMockFS())
	cfg := &flex.JobRequestCfg{RemoteStorageTarget: 1}
	controller := client.newRequestBuildController(ctx, cfg, jobSubmissionCh, func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
		return false, nil
	}, nil)

	controller.requestBuilder.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
		return PathState{
			LockedInfo:   &flex.JobLockedInfo{},
			LockAcquired: true,
			EntryInfo:    &entry.GetEntryCombinedInfo{},
			RstCfg: msg.RemoteStorageTarget{
				RSTIDs: []uint32{1},
			},
		}, nil
	}
	controller.requestBuilder.planFileState = func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
		return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
	}
	controller.requestBuilder.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
		return nil
	}

	return controller
}

// stubBulkOperation lets tests inject Execute/Cancel behavior directly into a *bulkOperationManager
// without going through a real clientBulkOperation implementation.
type stubBulkOperation struct {
	executeFn BulkExecuteFn
	cancelFn  BulkCancelFn
}

func (s *stubBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return nil
}

func (s *stubBulkOperation) Execute(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
	return s.executeFn(ctx)
}

func (s *stubBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
	return s.cancelFn(ctx, reason)
}

func (s *stubBulkOperation) Close(ctx context.Context) error {
	return nil
}

func (m *stubBulkOperation) Destroy(ctx context.Context) error {
	return nil
}

// newTestBulkManager builds a *bulkOperationManager backed by a stub clientBulkOperation, so tests
// can inject Execute/Cancel behavior without a real RST client. executeFn/cancelFn may be nil if the
// test never exercises that method.
func newTestBulkManager(operation string, executeFn BulkExecuteFn, cancelFn BulkCancelFn) *bulkOperationManager {
	return &bulkOperationManager{
		clientBulkOperation: &stubBulkOperation{executeFn: executeFn, cancelFn: cancelFn},
		operation:           operation,
		errors:              new(string),
		failed:              new(bool),
	}
}
