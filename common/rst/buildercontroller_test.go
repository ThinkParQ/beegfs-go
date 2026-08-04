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

func TestRequestBuildController_AddSourceProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 2)
	walkCh <- &filesystem.StreamPathResult{Path: "/a"}
	walkCh <- &filesystem.StreamPathResult{Path: "/b"}
	close(walkCh)

	controller.AddSource(walkCh)
	result, resumeToken, err := controller.WaitForResult()
	require.NoError(t, err)
	assert.Empty(t, resumeToken)
	assert.False(t, result.Reschedule)

	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_AddSourceSetsResumeTokenAndReschedules(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "resume-token"}
	close(walkCh)

	controller.AddSource(walkCh)
	result, resumeToken, err := controller.WaitForResult()
	require.NoError(t, err)
	assert.Equal(t, "resume-token", resumeToken)
	assert.True(t, result.Reschedule)
}

func TestRequestBuildController_AddSourceRejectsConflictingResumeTokens(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.resumeToken = "existing-token"

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "new-token"}
	close(walkCh)

	controller.AddSource(walkCh)
	_, _, err := controller.WaitForResult()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting walk resume tokens")
}

func TestRequestBuildController_AddSourceReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkErr := fmt.Errorf("walk failed")
	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Err: walkErr}
	close(walkCh)

	controller.AddSource(walkCh)
	_, _, err := controller.WaitForResult()
	require.ErrorIs(t, err, walkErr)
}

// TestRequestBuildController_AddSourceConvertsRequestCancelErrorToFailedPrecondition asserts that a
// RequestCancelError on the walk result does not fail the builder job. Instead it is submitted as a
// FAILED_PRECONDITION request carrying the cancellation reason.
func TestRequestBuildController_AddSourceConvertsRequestCancelErrorToFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Path: "/a", Err: &RequestCancelError{Reason: errors.New("cancelled")}}
	close(walkCh)

	controller.AddSource(walkCh)
	_, _, err := controller.WaitForResult()
	require.NoError(t, err)

	close(jobSubmissionCh)
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

	controller.ExecuteBulkOperation("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
	})

	result, resumeToken, err := controller.WaitForResult()
	require.NoError(t, err)
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

	controller.ExecuteBulkOperation("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
	})

	_, _, err := controller.WaitForResult()
	require.ErrorIs(t, err, walkErr)
}

// TestRequestBuildController_ExecuteBulkOperationImmediateErrorsAreNotDropped covers the case where
// every registered bulk manager fails before it ever produces a walk channel (e.g. it could not be
// opened). No walk goroutine is ever spawned, so bulkWalkGroup would previously stay nil and
// WaitForBulkOperations would short-circuit before consulting bulkExecuteResults, silently dropping
// the failure. The result must still be aggregated and returned.
func TestRequestBuildController_ExecuteBulkOperationImmediateErrorsAreNotDropped(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	openErr := fmt.Errorf("failed to open bulk operation")
	controller.ExecuteBulkOperation("mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return nil, nil, openErr
	})

	_, _, err := controller.WaitForResult()
	require.Error(t, err)
	assert.ErrorIs(t, err, openErr)
	assert.Contains(t, err.Error(), "mgr")
}

// TestRequestBuildController_ExecuteBulkOperationMergesRescheduleAcrossManagers asserts that when
// multiple bulk managers report a reschedule, the merged result keeps the smallest delay while every
// manager's error is still surfaced in the aggregate error.
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

	controller.ExecuteBulkOperation("slow", emptyBulkExecuteFn(5*time.Second, nil))
	controller.ExecuteBulkOperation("fast", emptyBulkExecuteFn(2*time.Second, boomErr))

	result, _, err := controller.WaitForResult()
	require.Error(t, err)
	assert.ErrorIs(t, err, boomErr)
	assert.Contains(t, err.Error(), "fast")

	assert.True(t, result.Reschedule)
	assert.Equal(t, 2*time.Second, result.Delay)
	assert.ErrorIs(t, result.Err, boomErr)
}

func TestRequestBuildController_CancelBulkOperationJoinsWaitErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	cancelErr := fmt.Errorf("cancel failed")
	controller.CancelBulkOperation(nil, "mgr", func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		walkCh := make(chan *BulkStreamPathResult)
		close(walkCh)
		return walkCh, func() error { return cancelErr }, nil
	})

	_, _, err := controller.WaitForResult()
	require.Error(t, err)
	assert.ErrorIs(t, err, cancelErr)
	assert.Contains(t, err.Error(), "mgr")
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

	controller.AddSource(walkCh)

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

// submittedPaths closes and drains jobSubmissionCh, returning the path of every submitted request.
// Callers must only invoke this once no further sends can occur, e.g. after
// requestBuildController.WaitForResult().
func submittedPaths(jobSubmissionCh chan *beeremote.JobRequest) []string {
	close(jobSubmissionCh)
	var paths []string
	for req := range jobSubmissionCh {
		paths = append(paths, req.GetPath())
	}
	return paths
}

// drainRequests drains an already-closed jobSubmissionCh, returning every submitted request.
func drainRequests(jobSubmissionCh chan *beeremote.JobRequest) []*beeremote.JobRequest {
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
	})

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
	controller.requestBuilder.planFileState = func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyFn, error) {
		return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
	}
	controller.requestBuilder.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
		return nil
	}

	return controller
}
