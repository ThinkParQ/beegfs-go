package rst

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	"go.uber.org/zap"
)

// testMaxRequests is high enough that the walk is never stopped for reaching it, so tests that only
// care about path processing run the walk to completion.
const testMaxRequests = 1000

func TestRequestBuildController_AddWalkProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a"},
		&filesystem.StreamPathResult{Path: "/b"},
	)

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, testMaxRequests))
	require.NoError(t, controller.WaitForWalk())

	result, resumeToken := controller.GetResults()
	assert.Empty(t, resumeToken, "a walk that ran to completion has nothing left to resume from")
	assert.False(t, result.Reschedule)
	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh))
}

// TestRequestBuildController_AddWalkStopsAtMaxRequests asserts that once the walk has submitted
// maxRequests and the workers are saturated, the controller stops the walk, records the resume token
// carried by the result it stopped on, and asks to be rescheduled. The result it stops on must not
// be submitted: the resume token names the path before it, so a resumed walk re-emits it.
func TestRequestBuildController_AddWalkStopsAtMaxRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	saturated := func() float64 { return walkContinuationWorkerSaturationThreshold }
	controller.workerSaturation = []func() float64{saturated, saturated}
	controller.activeSourceSubmissions.Store(1)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a", ResumeToken: "resume-token"},
	)

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, 1))
	require.NoError(t, controller.WaitForWalk())

	result, resumeToken := controller.GetResults()
	assert.Equal(t, "resume-token", resumeToken)
	assert.True(t, result.Reschedule)
	assert.Empty(t, submittedPaths(jobSubmissionCh), "the path the walk stopped on must not be submitted")
}

// TestRequestBuildController_AddWalkExtendsMaxRequestsWhileWorkersHaveCapacity asserts the opposite
// of the case above: reaching maxRequests while the workers still have capacity raises the ceiling
// and keeps walking rather than paying for a reschedule.
func TestRequestBuildController_AddWalkExtendsMaxRequestsWhileWorkersHaveCapacity(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	idle := func() float64 { return 0 }
	controller.workerSaturation = []func() float64{idle, idle}
	controller.activeSourceSubmissions.Store(1)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a"},
		&filesystem.StreamPathResult{Path: "/b"},
	)

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, 1))
	require.NoError(t, controller.WaitForWalk())

	result, resumeToken := controller.GetResults()
	assert.Empty(t, resumeToken)
	assert.False(t, result.Reschedule)
	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh),
		"both paths should be submitted, showing the ceiling was raised instead of the walk stopping")
}

// TestRequestBuildController_AddWalkRejectsSecondWalk asserts the single-walk guard: the controller
// accumulates one resume token and one scheduling result, so a second walk added before the first is
// retired is rejected and released rather than left to race with it.
func TestRequestBuildController_AddWalkRejectsSecondWalk(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	firstCh, firstStop, _ := newTestWalk(&filesystem.StreamPathResult{Path: "/a"})
	require.NoError(t, controller.AddWalk(firstCh, firstStop, testMaxRequests))

	secondCh, secondStop, secondStopped := newTestWalk(&filesystem.StreamPathResult{Path: "/b"})
	err := controller.AddWalk(secondCh, secondStop, testMaxRequests)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "another walk is already in progress")
	assert.True(t, secondStopped.Load(), "the rejected walk should be released so its producer isn't stranded")

	require.NoError(t, controller.WaitForWalk())
	assert.Equal(t, []string{"/a"}, submittedPaths(jobSubmissionCh))
}

// TestRequestBuildController_WaitForWalkRetiresTheWalk asserts WaitForWalk resets the controller so
// the next walk is accepted, which is what lets a builder job run more than one round.
func TestRequestBuildController_WaitForWalkRetiresTheWalk(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	firstCh, firstStop, _ := newTestWalk(&filesystem.StreamPathResult{Path: "/a"})
	require.NoError(t, controller.AddWalk(firstCh, firstStop, testMaxRequests))
	require.NoError(t, controller.WaitForWalk())

	secondCh, secondStop, _ := newTestWalk(&filesystem.StreamPathResult{Path: "/b"})
	require.NoError(t, controller.AddWalk(secondCh, secondStop, testMaxRequests))
	require.NoError(t, controller.WaitForWalk())

	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_AddWalkReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkErr := fmt.Errorf("walk failed")
	walkCh, stopWalk, _ := newTestWalk(&filesystem.StreamPathResult{Err: walkErr})

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, testMaxRequests))
	require.ErrorIs(t, controller.WaitForWalk(), walkErr)
}

// TestRequestBuildController_AddWalkConvertsRequestCancelErrorToFailedPrecondition asserts that a
// RequestCancelError on the walk result does not fail the builder job. Instead it is submitted as a
// FAILED_PRECONDITION request carrying the cancellation reason.
func TestRequestBuildController_AddWalkConvertsRequestCancelErrorToFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a", Err: &RequestCancelError{Reason: errors.New("cancelled")}},
	)

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, testMaxRequests))
	require.NoError(t, controller.WaitForWalk())

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

	manager := newTestBulkManager(t, "mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
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

	manager := newTestBulkManager(t, "mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
	}, nil)
	controller.ExecuteBulkOperation(manager)

	err := controller.WaitForBulkOperations()
	require.ErrorIs(t, err, walkErr)
}

// TestRequestBuildController_ExecuteBulkOperationFailsManagerOnNonTransientError asserts an execute
// that ends in an error the operation cannot recover from marks the operation permanently failed,
// even when the cancel that follows succeeds. Leaving it un-failed lets the next builder job reopen
// it, repeat the same execute, and append the same error again on every reschedule.
func TestRequestBuildController_ExecuteBulkOperationFailsManagerOnNonTransientError(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	executeErr := fmt.Errorf("retrieve-session expired")
	cancelled := false
	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *SchedulingResult { return &SchedulingResult{Err: executeErr} }, nil
		},
		func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
			cancelled = true
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() error { return nil }, nil
		})
	require.NoError(t, manager.Save())

	controller.ExecuteBulkOperation(manager)
	require.NoError(t, controller.WaitForBulkOperations())

	assert.True(t, cancelled, "the operation must still be cancelled so its pending paths are drained")
	assert.True(t, manager.IsFailed(), "a non-transient execute error must fail the operation permanently")
	require.Error(t, manager.GetErrors())
	assert.Contains(t, manager.GetErrors().Error(), executeErr.Error())

	// The failure has to be durable, otherwise a rescheduled builder job reopens and retries it.
	entries := readTestBulkOperationEntries(t, manager.mountPath, manager.jobId)
	require.Contains(t, entries, manager.Key())
	assert.True(t, entries[manager.Key()].Failed)
	assert.Equal(t, []string{executeErr.Error()}, entries[manager.Key()].Errors)
}

// TestRequestBuildController_ExecuteBulkOperationDoesNotFailManagerWhileShuttingDown asserts an
// operation interrupted by a graceful shutdown stays resumable. Shutdown cancels the builder's
// context to ask it to stop, so whatever error surfaces then describes an interrupted operation, not
// one that can never succeed. Recording it as a permanent failure would persist it and the operation
// would be refused for good after the restart instead of picking up where it left off, stranding
// whatever it had reserved remotely.
func TestRequestBuildController_ExecuteBulkOperationDoesNotFailManagerWhileShuttingDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	// Not a context error: this stands in for a cancellation the provider's client reported as
	// something of its own, which is why isTransientBulkError alone is not enough of a guard.
	interruptedErr := fmt.Errorf("connection reset by peer")
	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *SchedulingResult { return &SchedulingResult{Err: interruptedErr} }, nil
		},
		func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
			t.Fatal("a shutting down builder must not cancel its bulk operations")
			return nil, nil, nil
		})
	require.NoError(t, manager.Save())

	controller.ExecuteBulkOperation(manager)
	cancel()
	_ = controller.WaitForBulkOperations()

	assert.False(t, manager.IsFailed(), "an interrupted operation must stay resumable")
	entries := readTestBulkOperationEntries(t, manager.mountPath, manager.jobId)
	require.Contains(t, entries, manager.Key())
	assert.False(t, entries[manager.Key()].Failed, "the failure must not be persisted across the restart")
}

func TestRequestBuildController_ExecuteBulkOperationNoopWhenManagerAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager(t, "mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		t.Fatal("Execute should not be called for an already-failed manager")
		return nil, nil, nil
	}, nil)
	require.NoError(t, manager.Fail(fmt.Errorf("previously failed permanently")))

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
	manager := newTestBulkManager(t, "mgr",
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

	slowManager := newTestBulkManager(t, "slow", emptyBulkExecuteFn(5*time.Second, nil), noopCancel)
	fastManager := newTestBulkManager(t, "fast", emptyBulkExecuteFn(2*time.Second, boomErr), noopCancel)
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

	manager := newTestBulkManager(t, "mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		t.Fatal("Cancel should not be called for an already-failed manager")
		return nil, nil, nil
	})
	require.NoError(t, manager.Fail(fmt.Errorf("previously failed permanently")))

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))
	require.NoError(t, controller.WaitForBulkOperations())
}

func TestRequestBuildController_CancelBulkOperationSetsManagerFailedWhenCancelErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	cancelErr := fmt.Errorf("cannot cancel")
	manager := newTestBulkManager(t, "mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
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
	manager := newTestBulkManager(t, "mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
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

// TestRequestBuildController_ExecuteBulkOperationInterruptedDoesNotFailManager covers a BeeSync
// shutdown landing on an in-flight bulk operation. Failing a bulk operation is irreversible - the
// flag is persisted and every later run refuses the operation outright - so an execute that only
// stopped because its context died must leave the manager untouched and uncancelled.
func TestRequestBuildController_ExecuteBulkOperationInterruptedDoesNotFailManager(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *SchedulingResult {
				return &SchedulingResult{Err: fmt.Errorf("retrieve-session poll failed: %w", context.Canceled)}
			}, nil
		},
		func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
			t.Fatal("an interrupted bulk operation must not be cancelled")
			return nil, nil, nil
		},
	)

	controller.ExecuteBulkOperation(manager)
	require.NoError(t, controller.WaitForBulkOperations())

	assert.False(t, manager.IsFailed(), "an interrupted bulk operation must stay resumable")
	assert.NoError(t, manager.GetErrors(), "an interruption must not be recorded against the operation")
}

// TestRequestBuildController_CancelBulkOperationSkippedWhenContextCancelled asserts the controller
// does not attempt a cancel it knows will fail. Cancel talks to the provider, so a dead context
// guarantees an error, and that error is what permanently fails the operation.
func TestRequestBuildController_CancelBulkOperationSkippedWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager(t, "mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		t.Fatal("Cancel should not be attempted on a cancelled context")
		return nil, nil, nil
	})

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))
	require.NoError(t, controller.WaitForBulkOperations())

	assert.False(t, manager.IsFailed())
	assert.NoError(t, manager.GetErrors())
}

// TestRequestBuildController_CancelBulkOperationInterruptedDoesNotFailManager covers a cancel that
// starts on a live context and is interrupted partway through, which is the other route to
// permanently failing an operation that is merely paused.
func TestRequestBuildController_CancelBulkOperationInterruptedDoesNotFailManager(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	manager := newTestBulkManager(t, "mgr", nil, func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		walkCh := make(chan *BulkStreamPathResult)
		close(walkCh)
		return walkCh, func() error {
			return fmt.Errorf("unable to determine whether retrieve-session is active: %w", context.Canceled)
		}, nil
	})

	controller.CancelBulkOperation(manager, fmt.Errorf("reason"))
	require.NoError(t, controller.WaitForBulkOperations())

	assert.False(t, manager.IsFailed(), "an interrupted cancel must leave the operation cancellable next run")
	assert.NoError(t, manager.GetErrors())
}

func TestRequestBuildController_WaitForWalkReturnsImmediatelyWhenNoWalk(t *testing.T) {
	controller := &requestBuildController{}
	require.NoError(t, controller.WaitForWalk())
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

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a"},
		&filesystem.StreamPathResult{Path: "/b"},
		&filesystem.StreamPathResult{Path: "/c"},
	)

	require.NoError(t, controller.AddWalk(walkCh, stopWalk, testMaxRequests))

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

	require.NoError(t, controller.WaitForWalk())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, maxInFlight)
	assert.Equal(t, 0, inFlight)
}

// newTestWalk returns a closed walk channel pre-loaded with results, a stopWalk that records that it
// was called, and the flag it records into. Because the channel is already closed, a controller that
// consumes every result observes a completed walk, while one that stops early simply leaves the
// remainder buffered.
//
// The stopped flag only distinguishes a walk AddWalk rejected outright from one it accepted: an
// accepted walk is always released on the way out, so the flag is set whether it finished or was cut
// short. Tests that care about that distinction assert on the resume token and submitted paths.
func newTestWalk(results ...*filesystem.StreamPathResult) (walkCh <-chan *filesystem.StreamPathResult, stopWalk func(), stopped *atomic.Bool) {
	ch := make(chan *filesystem.StreamPathResult, len(results))
	for _, result := range results {
		ch <- result
	}
	close(ch)

	stopped = &atomic.Bool{}
	return ch, func() { stopped.Store(true) }, stopped
}

// submitToChan adapts a channel of submitted requests to a SubmitRequestFn so tests can assert what
// the builder submitted. Every submission succeeds; the channel needs enough capacity for the test
// because a submission blocks the goroutine that built the request.
// errSubmitUnavailable stands in for remote being unreachable until the builder gives up, which is
// what abandons a prepared request now that submission is attempted even while shutting down.
var errSubmitUnavailable = errors.New("remote unavailable")

func submitAlwaysFails(*beeremote.JobRequest) error {
	return errSubmitUnavailable
}

func submitToChan(jobSubmissionCh chan *beeremote.JobRequest) SubmitRequestFn {
	return func(request *beeremote.JobRequest) error {
		jobSubmissionCh <- request
		return nil
	}
}

// testSubmitter records the requests a builder submitted so tests can assert on them. When err is
// set it is returned instead, so tests can exercise the rollback the builder performs for a request
// remote refuses.
type testSubmitter struct {
	ch  chan *beeremote.JobRequest
	err error
}

func newTestSubmitter(size int) *testSubmitter {
	return &testSubmitter{ch: make(chan *beeremote.JobRequest, size)}
}

func (s *testSubmitter) submit(request *beeremote.JobRequest) error {
	if s.err != nil {
		return s.err
	}
	s.ch <- request
	return nil
}

// submittedPaths closes and drains jobSubmissionCh, returning the path of every submitted request.
// Callers must only invoke this once no further sends can occur, e.g. after
// requestBuildController.WaitForWalk()/WaitForBulkOperations().
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
	controller := client.newRequestBuildController(ctx, zap.NewNop(), cfg, submitToChan(jobSubmissionCh), func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
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
	controller.requestBuilder.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
		return func(context.Context, *PathState) (undoFn, error) { return noopUndo, nil }, nil
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
func newTestBulkManager(t *testing.T, operation string, executeFn BulkExecuteFn, cancelFn BulkCancelFn) *bulkOperationManager {
	return &bulkOperationManager{
		clientBulkOperation: &stubBulkOperation{executeFn: executeFn, cancelFn: cancelFn},
		bulkOperationEntry:  &bulkOperationEntry{RstId: 1, Operation: operation},
		// Recording a failure persists the entry, so the manager needs a real mount to write to.
		mountPath: t.TempDir(),
		jobId:     "job-1",
	}
}
