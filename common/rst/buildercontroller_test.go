package rst

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// testMaxRequests is high enough that the walk is never stopped for reaching it, so tests that only
// care about path processing run the walk to completion.
const testMaxRequests = 1000

// testJobId is the builder job id newTestRequestBuildController builds its controller for. A walk
// that drains records its completion as a sentinel resume token naming this job, so tests read the
// token back with parseResumeToken rather than matching the sentinel's format.
const testJobId = "test-job"

// resumeToken returns the token the controller has written back to its work request. The controller
// updates it in place as the walk advances, so it is read from the controller rather than returned
// by UpdateResult.
func resumeToken(t *testing.T, controller *requestBuildController) string {
	t.Helper()
	require.NotNil(t, controller.resumeToken, "the controller was not initialized")
	return *controller.resumeToken
}

// requireWalkComplete asserts the controller's resume token is the sentinel a drained walk records
// for testJobId, and returns the error the walk carried in it.
func requireWalkComplete(t *testing.T, controller *requestBuildController) error {
	t.Helper()
	walkComplete, sentinelErr := parseResumeToken(resumeToken(t, controller), testJobId)
	require.True(t, walkComplete, "a walk that ran to completion records a sentinel resume token")
	return sentinelErr
}

// runTestWalk drives a walk the way RunWalk does, minus the resume token check and the getWalk call,
// so a test can supply the walk results directly. It blocks until the walk and everything it
// dispatched has finished.
func runTestWalk(controller *requestBuildController, walkCh <-chan *filesystem.StreamPathResult, stopWalk func(), maxRequests int64) {
	controller.runWalk(new(errgroup.Group), walkCh, stopWalk, maxRequests)
}

func TestRequestBuildController_RunWalkProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a"},
		&filesystem.StreamPathResult{Path: "/b"},
	)

	runTestWalk(controller, walkCh, stopWalk, testMaxRequests)

	result := &SchedulingResult{}
	controller.UpdateResult(result)
	assert.NoError(t, requireWalkComplete(t, controller))
	assert.False(t, result.Reschedule)
	assert.ElementsMatch(t, []string{"/a", "/b"}, submissions.paths())
}

// TestRequestBuildController_RunWalkStopsAtMaxRequests asserts that once the walk has submitted
// maxRequests and the workers are saturated, the controller stops the walk, records the resume token
// carried by the result it stopped on, and asks to be rescheduled. The result it stops on must not
// be submitted: the resume token names the path before it, so a resumed walk re-emits it.
func TestRequestBuildController_RunWalkStopsAtMaxRequests(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)
	saturated := func() float64 { return walkContinuationWorkerSaturationThreshold }
	controller.workerSaturation = []func() float64{saturated, saturated}

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a", ResumeToken: "resume-token"},
	)

	// The walk counts its own submissions, so a ceiling of zero is what puts the very first result
	// over it. That makes the stop deterministic without waiting for a path to be submitted first.
	runTestWalk(controller, walkCh, stopWalk, 0)

	result := &SchedulingResult{}
	controller.UpdateResult(result)
	assert.Equal(t, "resume-token", resumeToken(t, controller))
	assert.True(t, result.Reschedule)
	assert.Empty(t, submissions.paths(), "the path the walk stopped on must not be submitted")
}

// TestRequestBuildController_RunWalkExtendsMaxRequestsWhileWorkersHaveCapacity asserts the opposite
// of the case above: reaching maxRequests while the workers still have capacity raises the ceiling
// and keeps walking rather than paying for a reschedule.
func TestRequestBuildController_RunWalkExtendsMaxRequestsWhileWorkersHaveCapacity(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)
	idle := func() float64 { return 0 }
	controller.workerSaturation = []func() float64{idle, idle}

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a"},
		&filesystem.StreamPathResult{Path: "/b"},
	)

	// A ceiling of zero puts the first result over it, so both paths are only reached if the idle
	// workers raise the ceiling instead of stopping the walk.
	runTestWalk(controller, walkCh, stopWalk, 0)

	result := &SchedulingResult{}
	controller.UpdateResult(result)
	assert.NoError(t, requireWalkComplete(t, controller))
	assert.False(t, result.Reschedule)
	assert.ElementsMatch(t, []string{"/a", "/b"}, submissions.paths(),
		"both paths should be submitted, showing the ceiling was raised instead of the walk stopping")
}

// TestRequestBuildController_RunWalkSkipsACompletedWalk asserts a resume token that already names a
// completed walk is honoured: no walk is started, the token is left as it was, and any error the
// token carried is reported again. This is what stops a rescheduled builder job re-walking the whole
// source path once the walk is done.
func TestRequestBuildController_RunWalkSkipsACompletedWalk(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	sentinel := buildWalkCompleteSentinel(testJobId, errors.New("a path failed"))
	*controller.resumeToken = sentinel

	walkStarted := false
	controller.getWalk = func() (<-chan *filesystem.StreamPathResult, func(), error) {
		walkStarted = true
		return nil, func() {}, nil
	}

	controller.RunWalk()

	assert.False(t, walkStarted, "a completed walk must not be started again")
	assert.Equal(t, sentinel, resumeToken(t, controller), "the completed token must be left intact")
	assert.ErrorContains(t, controller.GetWalkErrors(), "a path failed",
		"the error the token carried must be reported on every later round")
	assert.Empty(t, submissions.paths())
}

// TestRequestBuildController_RunWalkRetiresFailedWalk asserts a walk that fails leaves its error on
// the controller for GetWalkErrors and retires the walk. The sentinel carries the error, so the next
// round reports it instead of walking the paths the failed round never reached.
func TestRequestBuildController_RunWalkRetiresFailedWalk(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	walkErr := fmt.Errorf("walk failed")
	walkCh, stopWalk, _ := newTestWalk(&filesystem.StreamPathResult{Err: walkErr})

	runTestWalk(controller, walkCh, stopWalk, testMaxRequests)
	require.ErrorIs(t, controller.GetWalkErrors(), walkErr)

	sentinelErr := requireWalkComplete(t, controller)
	require.Error(t, sentinelErr, "the sentinel carries the error that retired the walk")
	assert.Contains(t, sentinelErr.Error(), walkErr.Error())
}

// TestRequestBuildController_RunWalkInterruptedKeepsResumePosition asserts a round sync interrupted
// keeps its resume position even though it accumulated errors. Those errors belong to the paths that
// were in flight as the node shut down. Retiring the walk over them would abandon every path the
// round never reached, which the job resumes after the restart.
func TestRequestBuildController_RunWalkInterruptedKeepsResumePosition(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	// On a sync node shutdownCtx is the parent of every work context, so a shutdown cancels both.
	shutdownCtx, shutdown := context.WithCancel(context.Background())
	workCtx, cancelWork := context.WithCancel(shutdownCtx)
	t.Cleanup(cancelWork)
	controller.shutdownCtx = shutdownCtx
	controller.workCtx = workCtx

	// The channel is left open so the round ends because it was interrupted rather than because it
	// ran out of paths, which is what a real walk does: the walker produces until it is cancelled.
	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Path: "/a", ResumeToken: "/a"}
	t.Cleanup(func() { close(walkCh) })

	// Shut down from inside the path being built, so the round is cut off with a path in flight and
	// that path fails the way one does when the node goes away underneath it.
	controller.requestBuilder.getPathState = func(context.Context, filesystem.Provider, string, PathStateMode) (PathState, error) {
		shutdown()
		return PathState{}, fmt.Errorf("%w: the node is shutting down", ErrGetPathStateFatal)
	}

	runTestWalk(controller, walkCh, func() {}, testMaxRequests)

	walkComplete, _ := parseResumeToken(resumeToken(t, controller), testJobId)
	assert.False(t, walkComplete, "an interrupted round resumes rather than retiring its walk")
	assert.Equal(t, "/a", resumeToken(t, controller), "the round keeps the position the walk reached")
	assert.NoError(t, controller.GetWalkErrors(), "a shutdown is not a walk failure")
	require.NotNil(t, controller.result, "an interrupted round must reschedule itself")
	assert.True(t, controller.result.Reschedule)
}

// TestRequestBuildController_RunWalkConvertsRequestCancelErrorToFailedPrecondition asserts that a
// RequestCancelError on the walk result does not fail the builder job. Instead it is submitted as a
// FAILED_PRECONDITION request carrying the cancellation reason.
func TestRequestBuildController_RunWalkConvertsRequestCancelErrorToFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	walkCh, stopWalk, _ := newTestWalk(
		&filesystem.StreamPathResult{Path: "/a", Err: &RequestCancelError{Reason: errors.New("cancelled")}},
	)

	runTestWalk(controller, walkCh, stopWalk, testMaxRequests)

	requests := submissions.all()
	require.Len(t, requests, 1)
	assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, requests[0].GetGenerationStatus().GetState())
	assert.Equal(t, "cancelled", requests[0].GetGenerationStatus().GetMessage())
}

func TestRequestBuildController_ExecuteBulkOperationProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{
		Path:     "/bulk-a",
		RstId:    1,
		BulkInfo: &flex.BulkJobRequestInfo{Operation: "retrieve"},
	}
	close(bulkCh)

	manager := newTestBulkManager(t, "mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *BulkExecuteResult { return &BulkExecuteResult{} }, nil
	}, nil)
	controller.ExecuteBulkOperation(manager)
	require.NoError(t, controller.WaitForBulkOperations())

	result := &SchedulingResult{}
	controller.UpdateResult(result)
	assert.Empty(t, resumeToken(t, controller), "a bulk operation's walk does not move the source walk's resume token")
	assert.False(t, result.Reschedule)
	assert.Equal(t, []string{"/bulk-a"}, submissions.paths())
}

func TestRequestBuildController_ExecuteBulkOperationReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	walkErr := fmt.Errorf("bulk walk failed")
	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{Err: walkErr}
	close(bulkCh)

	manager := newTestBulkManager(t, "mgr", func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
		return bulkCh, func() *BulkExecuteResult { return &BulkExecuteResult{} }, nil
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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	executeErr := fmt.Errorf("retrieve-session expired")
	cancelled := false
	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *BulkExecuteResult { return &BulkExecuteResult{Err: executeErr} }, nil
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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	// Not a context error: this stands in for a cancellation the provider's client reported as
	// something of its own, which is why isTransientBulkError alone is not enough of a guard.
	interruptedErr := fmt.Errorf("connection reset by peer")
	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *BulkExecuteResult { return &BulkExecuteResult{Err: interruptedErr} }, nil
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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
// multiple bulk managers report a reschedule, the merged result keeps the smallest delay so the
// builder job runs again as soon as the earliest operation is ready for it.
func TestRequestBuildController_ExecuteBulkOperationMergesRescheduleAcrossManagers(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	emptyBulkExecuteFn := func(delay time.Duration) BulkExecuteFn {
		return func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *BulkExecuteResult {
				return &BulkExecuteResult{Reschedule: true, Delay: delay}
			}, nil
		}
	}
	noopCancel := func(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
		t.Fatal("a rescheduling operation must not be cancelled")
		return nil, nil, nil
	}

	// The slow manager reports first so the merge has to replace an already recorded delay rather
	// than simply keeping the first one it saw.
	slowManager := newTestBulkManager(t, "slow", emptyBulkExecuteFn(5*time.Second), noopCancel)
	fastManager := newTestBulkManager(t, "fast", emptyBulkExecuteFn(2*time.Second), noopCancel)
	controller.ExecuteBulkOperation(slowManager)
	controller.ExecuteBulkOperation(fastManager)

	require.NoError(t, controller.WaitForBulkOperations())

	result := &SchedulingResult{}
	controller.UpdateResult(result)
	assert.True(t, result.Reschedule)
	assert.Equal(t, 2*time.Second, result.Delay)
}

func TestRequestBuildController_CancelBulkOperationNoopWhenManagerAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

	manager := newTestBulkManager(t, "mgr",
		func(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
			walkCh := make(chan *BulkStreamPathResult)
			close(walkCh)
			return walkCh, func() *BulkExecuteResult {
				return &BulkExecuteResult{Err: fmt.Errorf("retrieve-session poll failed: %w", context.Canceled)}
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

// TestRequestBuildController_CancelBulkOperationSkippedWhileShuttingDown asserts a shutdown never
// cancels a bulk operation. The builder job is not complete and resumes from its resume token after
// the restart, so cancelling would release what the operation reserved remotely and strand the job.
func TestRequestBuildController_CancelBulkOperationSkippedWhileShuttingDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)

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

func TestRequestBuildController_WaitForBulkOperationsReturnsImmediatelyWhenNoBulkWalk(t *testing.T) {
	controller := &requestBuildController{}
	require.NoError(t, controller.WaitForBulkOperations())
}

// TestRequestBuildController_PathProcessingConcurrencyIsBounded asserts that maxWorkersCh actually
// bounds how many paths are processed concurrently: with a single worker slot, a second path must
// not start processing until the first releases its slot.
func TestRequestBuildController_PathProcessingConcurrencyIsBounded(t *testing.T) {
	ctx := context.Background()
	submissions := newTestSubmitter()
	controller := newTestRequestBuildController(t, ctx, submissions)
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

	// runWalk blocks until the walk and everything it dispatched has finished, so it is driven from
	// a goroutine here to observe the paths while they are still in flight.
	walkDone := make(chan struct{})
	go func() {
		defer close(walkDone)
		runTestWalk(controller, walkCh, stopWalk, testMaxRequests)
	}()

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

	<-walkDone

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
// The stopped flag says only that the walk was released, which always happens on the way out,
// whether it finished or was cut short. Tests that care which one happened assert on the resume
// token and the submitted paths.
func newTestWalk(results ...*filesystem.StreamPathResult) (walkCh <-chan *filesystem.StreamPathResult, stopWalk func(), stopped *atomic.Bool) {
	ch := make(chan *filesystem.StreamPathResult, len(results))
	for _, result := range results {
		ch <- result
	}
	close(ch)

	stopped = &atomic.Bool{}
	return ch, func() { stopped.Store(true) }, stopped
}

// errSubmitUnavailable stands in for remote being unreachable until the builder gives up, which is
// what abandons a prepared request now that submission is attempted even while shutting down.
var errSubmitUnavailable = errors.New("remote unavailable")

// submitAlwaysFails refuses every submission, for tests that only care about how the builder
// resolves a request no job will ever run.
func submitAlwaysFails(ctx context.Context, request *beeremote.JobRequest) error {
	return errSubmitUnavailable
}

// testSubmitter stands in for the sync worker's sendBuilderJobRequest. It records every request the
// builder submitted and returns the outcome the test asked for.
//
// It deliberately never blocks. Submissions run concurrently from every goroutine building a path,
// and a test that submits more than it expected must fail an assertion naming what was submitted
// rather than deadlock a builder goroutine until the package times out.
type testSubmitter struct {
	mu       sync.Mutex
	requests []*beeremote.JobRequest
	// result reports the outcome of submitting request. A nil result accepts every submission. Set
	// it to return the errors remote really returns (errSubmitUnavailable, ErrJobAlreadyExists,
	// ErrJobNotAllowed, ...) to exercise how the builder resolves each one.
	result func(request *beeremote.JobRequest) error
}

func newTestSubmitter() *testSubmitter {
	return &testSubmitter{}
}

// submit records request and reports the outcome the test asked for. The builder mints the job ID a
// reserving submission carries, so an accepted submission has nothing to hand back but a nil error.
func (s *testSubmitter) submit(ctx context.Context, request *beeremote.JobRequest) error {
	s.mu.Lock()
	s.requests = append(s.requests, proto.Clone(request).(*beeremote.JobRequest))
	s.mu.Unlock()
	if s.result != nil {
		return s.result(request)
	}
	return nil
}

// all returns every request submitted so far. Unlike draining a channel it may be called more than
// once, and it does not require proving that no further submission can happen.
func (s *testSubmitter) all() []*beeremote.JobRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

// paths returns the path of every request submitted so far.
func (s *testSubmitter) paths() []string {
	var paths []string
	for _, req := range s.all() {
		paths = append(paths, req.GetPath())
	}
	return paths
}

// len returns how many requests have been submitted so far.
func (s *testSubmitter) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func newTestRequestBuildController(t *testing.T, ctx context.Context, submissions *testSubmitter) *requestBuildController {
	t.Helper()
	client := NewJobBuilderClient(ctx, map[uint32]Provider{1: &MockClient{}}, filesystem.NewMockFS(), DefaultStateRoot)
	// newRequestBuildController takes the work request rather than the config, and rejects one that
	// carries no builder. The builder is the only field it reads, so the rest is left unset.
	workRequest := &flex.WorkRequest{
		JobId: testJobId,
		Type: &flex.WorkRequest_Builder{
			Builder: &flex.BuilderJob{Cfg: &flex.JobRequestCfg{RemoteStorageTarget: 1}},
		},
	}
	// MockClient only offers a request to a bulk operation when its locked info says the file is
	// archived. These tests build no archived paths, so no request may reach addToBulkRequest.
	requestBuilder := client.newJobRequestBuilder(zap.NewNop(), workRequest.GetBuilder().GetCfg(), submissions.submit, requireNoBulkRequest(t), func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
		return nil
	})
	controller := client.newRequestBuildController(ctx, ctx, requestBuilder, nil)
	require.NoError(t, controller.Init(client, workRequest))

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
		return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
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

func (s *stubBulkOperation) UpdateBulkRequest(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
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
		stateRoot: DefaultStateRoot,
		jobId:     "job-1",
	}
}
