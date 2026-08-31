package rst

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Short graces keep the tests fast; the slack is generous because a timer is only guaranteed not to
// fire early.
const (
	testGrace = 50 * time.Millisecond
	testSlack = 5 * time.Second
)

// testRegistry isolates one test's contexts from the rest of the package's.
func testRegistry() *cancellationDelayRegistry {
	return newCancellationDelayRegistry(1)
}

// requireDoneWithin fails unless ctx is cancelled within the given bound.
func requireDoneWithin(t *testing.T, ctx context.Context, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case <-ctx.Done():
		return time.Since(start)
	case <-time.After(within):
		t.Fatalf("context was not cancelled within %s", within)
		return 0
	}
}

// requireDrained asserts nothing is left holding on. Release happens in the watcher goroutine after
// the context is done, so it has to be given a moment.
func requireDrained(t *testing.T, registry *cancellationDelayRegistry) {
	t.Helper()
	var stat *CollectiveCancellationDelayStat
	require.Eventuallyf(t, func() bool {
		stat = getCollectiveCancellationDelayStat(registry)
		return stat.InFlight == 0 && stat.InGrace == 0 && stat.LatestDeadline.IsZero() &&
			stat.EstimatedTime.IsZero()
	}, testSlack, time.Millisecond, "registry never drained: %+v", stat)
}

// TestCancellationDelayShardStride guards the hand written padding: a counter added without updating
// the subtraction puts shards back on shared cache lines, which is silent apart from the contention.
func TestCancellationDelayShardStride(t *testing.T) {
	size := unsafe.Sizeof(cancellationDelayShard{})
	assert.EqualValues(t, cancellationDelayShardStride, size,
		"shard must be exactly one stride so shards never share a cache line")
}

// TestCancellationDelayHoldsGraceThenCancels verifies cancelling the parent does not cancel the
// derived context until the grace period expires.
func TestCancellationDelayHoldsGraceThenCancels(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, _ := withCancellationDelay(testRegistry(), parent, testGrace)
	defer cancel()

	cancelParent()
	assert.NoError(t, ctx.Err(), "the grace period must not have elapsed yet")
	requireDoneWithin(t, ctx, testSlack)
}

// TestCancellationDelayCheckpointExtendsGrace verifies a checkpoint restarts the countdown, so the
// context is cancelled a full grace period after the last one rather than the first.
func TestCancellationDelayCheckpointExtendsGrace(t *testing.T) {
	const extensions = 3
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, checkpoint := withCancellationDelay(testRegistry(), parent, testGrace)
	defer cancel()

	cancelParent()
	start := time.Now()
	// Extend from inside each grace period, so the countdown is restarted rather than re-armed.
	for range extensions {
		time.Sleep(testGrace / 2)
		require.NoError(t, ctx.Err(), "a checkpoint within the grace period must extend it")
		checkpoint(testGrace)
	}

	requireDoneWithin(t, ctx, testSlack)
	assert.GreaterOrEqual(t, time.Since(start), extensions*(testGrace/2)+testGrace,
		"every extension should have added to the time held")
}

// TestCancellationDelayCheckpointsHoldIndefinitely pins down the deliberate absence of a ceiling.
// Bounding this belongs to the caller, so an unbounded checkpoint loop holding on forever is the
// contract working as intended.
func TestCancellationDelayCheckpointsHoldIndefinitely(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, checkpoint := withCancellationDelay(testRegistry(), parent, testGrace)
	defer cancel()

	cancelParent()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(testGrace / 4):
				checkpoint(testGrace)
			}
		}
	})

	// Many times the grace period.
	select {
	case <-ctx.Done():
		t.Fatal("continuous checkpoints must keep the cancellation held off")
	case <-time.After(10 * testGrace):
	}

	// Stopping is what ends it.
	close(stop)
	wg.Wait()
	requireDoneWithin(t, ctx, testSlack)
}

// TestCancellationDelayCheckpointBeforeGraceIsInert verifies a checkpoint made while the parent is
// live only records the next window. This is the regular flow, so it must leave no trace in the
// stat.
func TestCancellationDelayCheckpointBeforeGraceIsInert(t *testing.T) {
	registry := testRegistry()
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, checkpoint := withCancellationDelay(registry, parent, testGrace)
	defer cancel()

	time.Sleep(testGrace / 4)
	for range 20 {
		checkpoint(testGrace)
	}
	require.NoError(t, ctx.Err(), "checkpoints must not cancel a context whose parent is live")

	stat := getCollectiveCancellationDelayStat(registry)
	assert.Equal(t, 1, stat.InFlight)
	assert.Zero(t, stat.InGrace, "nothing is counting down until the parent is cancelled")
	assert.True(t, stat.LatestDeadline.IsZero(), "there is no deadline before the parent is cancelled")
	assert.Zero(t, stat.AverageCheckpointDuration, "units completed before a cancellation must not be measured")

	// The full grace period is still ahead.
	cancelParent()
	assert.NoError(t, ctx.Err())
	requireDoneWithin(t, ctx, testSlack)
}

// TestCancellationDelayCheckpointSetsNextDelay verifies a checkpoint before the grace period still
// decides its length, which is how a path entering cleanup gets the cleanup budget.
func TestCancellationDelayCheckpointSetsNextDelay(t *testing.T) {
	registry := testRegistry()
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, checkpoint := withCancellationDelay(registry, parent, testGrace)
	defer cancel()

	checkpoint(30 * testGrace)
	cancelParent()

	select {
	case <-ctx.Done():
		t.Fatal("the grace period should have been replaced by the longer one")
	case <-time.After(2 * testGrace):
	}
	assert.Positive(t, getCollectiveCancellationDelayStat(registry).InGrace, "the countdown should be running")
}

// TestCancellationDelayIndependentCancel verifies the returned cancel ends everything and releases
// the context's contribution, with the parent untouched.
func TestCancellationDelayIndependentCancel(t *testing.T) {
	registry := testRegistry()
	ctx, cancel, _ := withCancellationDelay(registry, context.Background(), time.Hour)
	require.Equal(t, 1, getCollectiveCancellationDelayStat(registry).InFlight)

	cancel()
	requireDoneWithin(t, ctx, testSlack)
	requireDrained(t, registry)
}

// TestCancellationDelayReleaseDrainsCounters verifies a context folds its whole contribution back
// out however it ends. A counter left behind overstates every later stat.
func TestCancellationDelayReleaseDrainsCounters(t *testing.T) {
	registry := testRegistry()

	// Ended by its grace expiring. The checkpoint has to land after the countdown starts, since that
	// is the only time one is measured.
	parent, cancelParent := context.WithCancel(context.Background())
	expired, cancelExpired, checkpoint := withCancellationDelay(registry, parent, testGrace)
	cancelParent()
	require.Eventually(t, func() bool {
		return getCollectiveCancellationDelayStat(registry).InGrace == 1
	}, testSlack, time.Millisecond)
	checkpoint(testGrace)
	requireDoneWithin(t, expired, testSlack)
	cancelExpired()

	// Ended by its own cancel, never having entered its grace period.
	_, cancelUnused, _ := withCancellationDelay(registry, context.Background(), time.Hour)
	cancelUnused()

	requireDrained(t, registry)

	// The pace survives on purpose: it describes work that happened.
	assert.Positive(t, getCollectiveCancellationDelayStat(registry).AverageCheckpointDuration,
		"the measured pace must not be withdrawn when a context finishes")
}

// TestCancellationDelayStatDeadlineRecedes verifies the reported deadline moves out as work reports
// progress, which is the signal an operator has mid drain.
func TestCancellationDelayStatDeadlineRecedes(t *testing.T) {
	registry := testRegistry()
	parent, cancelParent := context.WithCancel(context.Background())

	// Long enough that only the checkpoints move the deadline.
	const grace = 30 * testGrace
	const contexts = 3
	checkpoints := make([]CancellationCheckpoint, 0, contexts)
	for range contexts {
		_, cancel, checkpoint := withCancellationDelay(registry, parent, grace)
		defer cancel()
		checkpoints = append(checkpoints, checkpoint)
	}

	require.True(t, getCollectiveCancellationDelayStat(registry).LatestDeadline.IsZero(),
		"no deadline exists until the parent is cancelled")
	cancelParent()
	require.Eventually(t, func() bool {
		return getCollectiveCancellationDelayStat(registry).InGrace == contexts
	}, testSlack, time.Millisecond)

	before := getCollectiveCancellationDelayStat(registry)
	require.False(t, before.LatestDeadline.IsZero())

	time.Sleep(testGrace)
	for _, checkpoint := range checkpoints {
		checkpoint(grace)
	}

	// The re-arm happens in the watcher goroutine, so the deadline moves out after the call.
	var after *CollectiveCancellationDelayStat
	require.Eventuallyf(t, func() bool {
		after = getCollectiveCancellationDelayStat(registry)
		return after.LatestDeadline.After(before.LatestDeadline)
	}, testSlack, time.Millisecond, "a checkpoint must push the reported deadline out: %+v", after)

	assert.Equal(t, contexts, after.InFlight)
	assert.Positive(t, after.AverageCheckpointDuration)
}

// runWindDown produces one completed wind down of roughly grace.
func runWindDown(t *testing.T, registry *cancellationDelayRegistry, grace time.Duration) {
	t.Helper()
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel, _ := withCancellationDelay(registry, parent, grace)
	defer cancel()
	cancelParent()
	requireDoneWithin(t, ctx, testSlack)
}

// TestCancellationDelayStatEstimateFromThroughput verifies the estimate is the backlog divided by
// the rate contexts are actually finishing, so it scales with how many are left rather than with how
// long any one of them takes.
func TestCancellationDelayStatEstimateFromThroughput(t *testing.T) {
	registry := testRegistry()

	require.True(t, getCollectiveCancellationDelayStat(registry).EstimatedTime.IsZero(),
		"one completion establishes no throughput, so there is nothing to project")

	// Establish a completion interval by retiring contexts a known distance apart.
	const interval = testGrace
	for range 6 {
		runWindDown(t, registry, interval)
	}
	requireDrained(t, registry)

	settled := getCollectiveCancellationDelayStat(registry)
	require.Positive(t, settled.AverageCompletionInterval)
	assert.InEpsilon(t, interval, settled.AverageCompletionInterval, 0.6,
		"the interval should have converged near the observed gap between completions")
	assert.True(t, settled.EstimatedTime.IsZero(), "nothing is winding down, so there is nothing to project")

	// The backlog is what sets the estimate. Graces are long so these outlive the assertions.
	const backlog = 8
	parent, cancelParent := context.WithCancel(context.Background())
	for range backlog {
		_, cancel, _ := withCancellationDelay(registry, parent, time.Hour)
		defer cancel()
	}
	cancelParent()
	require.Eventually(t, func() bool {
		return getCollectiveCancellationDelayStat(registry).InGrace == backlog
	}, testSlack, time.Millisecond)

	full := getCollectiveCancellationDelayStat(registry)
	expected := time.Duration(backlog) * full.AverageCompletionInterval
	assert.InEpsilon(t, expected, time.Until(full.EstimatedTime), 0.25,
		"the estimate must be the backlog times the completion interval")
}

// TestCancellationDelayStatEstimateShrinksWithBacklog verifies the estimate follows the queue down.
// This is what the previous per context residual could not do: it had no term for how many were
// left, so retiring most of them barely moved it.
func TestCancellationDelayStatEstimateShrinksWithBacklog(t *testing.T) {
	registry := testRegistry()
	for range 4 {
		runWindDown(t, registry, testGrace)
	}

	parent, cancelParent := context.WithCancel(context.Background())
	cancels := make([]context.CancelFunc, 0, 8)
	for range 8 {
		_, cancel, _ := withCancellationDelay(registry, parent, time.Hour)
		defer cancel()
		cancels = append(cancels, cancel)
	}
	cancelParent()
	require.Eventually(t, func() bool {
		return getCollectiveCancellationDelayStat(registry).InGrace == 8
	}, testSlack, time.Millisecond)
	before := time.Until(getCollectiveCancellationDelayStat(registry).EstimatedTime)

	for _, cancel := range cancels[2:] {
		cancel()
	}
	require.Eventually(t, func() bool {
		return getCollectiveCancellationDelayStat(registry).InGrace == 2
	}, testSlack, time.Millisecond)

	after := time.Until(getCollectiveCancellationDelayStat(registry).EstimatedTime)
	assert.Less(t, after, before, "the estimate must fall as the backlog drains")
}

// TestCancellationDelayThroughputReflectsParallelism verifies the point of using throughput: two
// contexts finishing together are counted as twice the rate of two finishing in sequence, without
// anything having to know how many ran at once.
func TestCancellationDelayThroughputReflectsParallelism(t *testing.T) {
	sequential := testRegistry()
	for range 6 {
		runWindDown(t, sequential, testGrace)
	}
	serialInterval := getCollectiveCancellationDelayStat(sequential).AverageCompletionInterval
	require.Positive(t, serialInterval)

	// The same wind down length, but four at a time, so completions arrive in bursts.
	concurrent := testRegistry()
	for range 3 {
		parent, cancelParent := context.WithCancel(context.Background())
		ctxs := make([]context.Context, 0, 4)
		for range 4 {
			ctx, cancel, _ := withCancellationDelay(concurrent, parent, testGrace)
			defer cancel()
			ctxs = append(ctxs, ctx)
		}
		cancelParent()
		for _, ctx := range ctxs {
			requireDoneWithin(t, ctx, testSlack)
		}
	}
	parallelInterval := getCollectiveCancellationDelayStat(concurrent).AverageCompletionInterval
	require.Positive(t, parallelInterval)

	assert.Less(t, parallelInterval, serialInterval,
		"contexts finishing alongside each other must read as higher throughput")
}

// TestCancellationDelayConcurrent exercises the registry the way a shutdown does, so -race can look
// for a torn counter.
func TestCancellationDelayConcurrent(t *testing.T) {
	registry := newCancellationDelayRegistry(4)
	parent, cancelParent := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			ctx, cancel, checkpoint := withCancellationDelay(registry, parent, testGrace/10)
			defer cancel()
			for range 8 {
				checkpoint(testGrace / 10)
				getCollectiveCancellationDelayStat(registry)
			}
			<-ctx.Done()
		})
	}

	cancelParent()
	wg.Wait()
	requireDrained(t, registry)
}

// benchRegistry populates a registry with contexts parked long enough not to expire mid benchmark,
// so what is measured is the cost of reading a registry of that size rather than churn.
func benchRegistry(b *testing.B, procs, contexts int) *cancellationDelayRegistry {
	b.Helper()
	registry := newCancellationDelayRegistry(procs)
	cancels := make([]context.CancelFunc, 0, contexts)
	parent, cancelParent := context.WithCancel(context.Background())
	for range contexts {
		_, cancel, checkpoint := withCancellationDelay(registry, parent, time.Hour)
		cancels = append(cancels, cancel)
		// So the read does the pace and deadline work rather than bailing out early.
		checkpoint(time.Hour)
	}
	cancelParent()
	b.Cleanup(func() {
		for _, cancel := range cancels {
			cancel()
		}
	})
	return registry
}

// BenchmarkGetCollectiveCancellationDelayStat is the claim the design rests on: a read is flat in
// the number of live contexts. Growth from contexts=0 to contexts=4096 means something enumerates.
func BenchmarkGetCollectiveCancellationDelayStat(b *testing.B) {
	for _, contexts := range []int{0, 64, 512, 4096} {
		b.Run(fmt.Sprintf("contexts=%d", contexts), func(b *testing.B) {
			registry := benchRegistry(b, runtime.GOMAXPROCS(0), contexts)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				sinkStat = getCollectiveCancellationDelayStat(registry)
			}
		})
	}
}

// BenchmarkGetCollectiveCancellationDelayStatShards shows where the cost lives: the shard count,
// fixed at construction from GOMAXPROCS.
func BenchmarkGetCollectiveCancellationDelayStatShards(b *testing.B) {
	for _, procs := range []int{1, 8, 64} {
		registry := newCancellationDelayRegistry(procs)
		b.Run(fmt.Sprintf("shards=%d", len(registry.shards)), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				sinkStat = getCollectiveCancellationDelayStat(registry)
			}
		})
	}
}

// BenchmarkGetCollectiveCancellationDelayStatDuringWork is the burden that matters: a reader that
// interfered shows up as the checkpoint getting slower. reading=false is the control.
func BenchmarkGetCollectiveCancellationDelayStatDuringWork(b *testing.B) {
	for _, reading := range []bool{false, true} {
		b.Run(fmt.Sprintf("reading=%t", reading), func(b *testing.B) {
			registry := newCancellationDelayRegistry(runtime.GOMAXPROCS(0))
			_, cancel, checkpoint := withCancellationDelay(registry, context.Background(), time.Hour)
			defer cancel()

			stop := make(chan struct{})
			var wg sync.WaitGroup
			if reading {
				// A tight loop is a deliberately unfair upper bound; a drain reads every 2s.
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
							sinkStat = getCollectiveCancellationDelayStat(registry)
						}
					}
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				checkpoint(time.Hour)
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}

// BenchmarkWithCancellationDelay covers the per context overhead every builder path pays, whether or
// not a shutdown happens.
func BenchmarkWithCancellationDelay(b *testing.B) {
	registry := newCancellationDelayRegistry(runtime.GOMAXPROCS(0))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, cancel, _ := withCancellationDelay(registry, context.Background(), time.Hour)
		cancel()
	}
}

// sinkStat keeps the benchmarked reads from being optimized away.
var sinkStat *CollectiveCancellationDelayStat
