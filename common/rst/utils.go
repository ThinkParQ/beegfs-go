package rst

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CancellationCheckpoint reports that one unit of work finished and that the next should be given
// delay to complete. It may be called from any goroutine and a delay <= 0 is ignored.
//
// Before the parent is cancelled it only records the window the next unit should get. Once the
// parent is cancelled it also restarts the grace period, so checkpointing from an unbounded loop
// holds the cancellation open forever.
type CancellationCheckpoint func(delay time.Duration)

// cancellationDelayState is the bookkeeping for one context from WithCancellationDelay. Everything
// here is measured inside the grace period, so a context that is never cancelled contributes
// nothing but its presence.
type cancellationDelayState struct {
	mu      sync.Mutex
	delay   time.Duration
	inGrace bool
	// armedAt is when the countdown started, lastCheckpointAt when the current unit began. Both are
	// zero until the countdown is armed.
	armedAt          time.Time
	lastCheckpointAt time.Time
	// registry holds the wind down average, shard holds what the live set contributes.
	registry *cancellationDelayRegistry
	shard    *cancellationDelayShard
}

// checkpoint records the window the next unit should get, measures the last one if the countdown is
// running, and reports whether there is a countdown to restart.
func (s *cancellationDelayState) checkpoint(next time.Duration) bool {
	if next <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.delay = next
	if !s.inGrace {
		return false
	}

	now := time.Now()
	s.shard.completedWorkNanos.Add(int64(now.Sub(s.lastCheckpointAt)))
	s.shard.completedUnits.Add(1)
	s.lastCheckpointAt = now
	return true
}

// armCountdown starts or restarts the grace period and returns how long the timer should run for.
// The first call is where this context starts contributing to the wind down statistics.
func (s *cancellationDelayState) armCountdown() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if !s.inGrace {
		s.inGrace = true
		s.armedAt = now
		s.lastCheckpointAt = now
		s.shard.live.inGrace.Add(1)
	}
	s.shard.raise(&s.shard.latestDeadlineNanos, now.Add(s.delay).UnixNano())
	return s.delay
}

// release folds this context back out of its shard. It runs exactly once, when the context is
// permanently done.
func (s *cancellationDelayState) release() {
	s.mu.Lock()
	inGrace := s.inGrace
	armedAt := s.armedAt
	s.mu.Unlock()

	shard := s.shard
	if inGrace {
		// The pace counters are not withdrawn: they describe work that happened, and a drain still
		// needs a pace to report once the contexts that measured it are gone.
		s.registry.recordWindDown(time.Since(armedAt))
		shard.live.inGrace.Add(-1)
	}
	if shard.live.count.Add(-1) == 0 {
		shard.latestDeadlineNanos.Store(0)
	}
}

// WithCancellationDelay returns a context derived from parent that delays propagating the parent's
// cancellation by delay, giving work already in progress a grace period to finish.
//
// The grace period starts when parent is cancelled and is restarted by every call to the returned
// CancellationCheckpoint which are the caller's responsibility. The returned context can also be
// cancelled independently, and the cancel must always be called.
func WithCancellationDelay(parent context.Context, delay time.Duration) (context.Context, context.CancelFunc, CancellationCheckpoint) {
	return withCancellationDelay(delayRegistry, parent, delay)
}

// withCancellationDelay is WithCancellationDelay against an explicit registry, so tests can observe
// only their own contexts.
func withCancellationDelay(
	registry *cancellationDelayRegistry,
	parent context.Context,
	delay time.Duration,
) (context.Context, context.CancelFunc, CancellationCheckpoint) {
	base := context.WithoutCancel(parent)
	ctx, cancel := context.WithCancel(base)

	state := &cancellationDelayState{delay: delay, registry: registry}
	registry.register(state)

	extendTime := make(chan struct{}, 1)
	checkpoint := func(next time.Duration) {
		if !state.checkpoint(next) {
			return
		}
		// One pending token is enough to make the watcher re-read the delay.
		select {
		case extendTime <- struct{}{}:
		default:
		}
	}

	go func() {
		defer state.release()

		select {
		case <-parent.Done():
		case <-ctx.Done():
			return
		}

		timer := time.NewTimer(state.armCountdown())
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				cancel()
				return
			case <-extendTime:
				// Go >= 1.23 guarantees a reset timer never delivers a stale value.
				timer.Stop()
				timer.Reset(state.armCountdown())
			case <-ctx.Done():
				return
			}
		}
	}()

	return ctx, cancel, checkpoint
}

// cancellationDelayCounters is the part of a shard the live set moves through, kept together so the
// counters that must agree share a cache line.
type cancellationDelayCounters struct {
	count   atomic.Int64
	inGrace atomic.Int64
}

// cancellationDelayShardStride is what each shard is padded out to so shards never share a cache
// line.
const cancellationDelayShardStride = 128

// cancellationDelayShard pools the contributions of the contexts assigned to it. Everything is
// atomic so registering, checkpointing and releasing never block each other, and so reading a stat
// never blocks the work it reports on.
type cancellationDelayShard struct {
	live cancellationDelayCounters
	// completedWorkNanos and completedUnits only ever grow, so the pace they yield is a lifetime
	// average rather than a recent one.
	completedWorkNanos atomic.Int64
	completedUnits     atomic.Int64
	// latestDeadlineNanos only rises while the shard is occupied and is cleared when it empties, so
	// it covers the shard's whole occupancy episode rather than just its live contexts.
	latestDeadlineNanos atomic.Int64
	// Padding is blank because nothing reads it. TestCancellationDelayShardStride enforces the
	// subtracted count.
	_ [cancellationDelayShardStride - 5*8]byte
}

// raise sets counter to next when next is larger.
func (sh *cancellationDelayShard) raise(counter *atomic.Int64, next int64) {
	for {
		current := counter.Load()
		if current >= next || counter.CompareAndSwap(current, next) {
			return
		}
	}
}

// cancellationDelayRegistry pools every live context from WithCancellationDelay. It is process wide
// because shutdown is, and nothing here enumerates contexts: each folds its numbers into a shard as
// they change, so a stat costs one pass over the shards.
type cancellationDelayRegistry struct {
	shards []cancellationDelayShard
	// mask turns the round robin counter into a shard index, so the shard count is a power of two.
	mask uint64
	next atomic.Uint64
	// graceEWMANanos is the average wind down as float64 bits. It is one word rather than one per
	// shard because an average like this is a sequence, and it is only written when a context that
	// entered its grace period is released. Zero means no wind down has completed.
	graceEWMANanos atomic.Uint64
	// completionIntervalEWMANanos is the average gap between contexts finishing, which is the
	// drain's aggregate throughput and so already accounts for however much of the work actually
	// runs in parallel. lastCompletionOffsetNanos is what each gap is measured from.
	completionIntervalEWMANanos atomic.Uint64
	lastCompletionOffsetNanos   atomic.Int64
}

// delayEpoch anchors the completion timestamps. Any fixed moment would do.
var delayEpoch = time.Now()

// ewmaAlpha is how much of an average each new sample contributes.
const ewmaAlpha = 0.25

var delayRegistry = newCancellationDelayRegistry(runtime.GOMAXPROCS(0))

// newCancellationDelayRegistry scales the shard count with GOMAXPROCS, since a builder job runs
// 8*GOMAXPROCS paths at once and every one registers a context. GOMAXPROCS is read once: resizing
// would have to move the counters the registry exists to keep.
func newCancellationDelayRegistry(procs int) *cancellationDelayRegistry {
	shards := uint64(16)
	for shards < uint64(4*max(1, procs)) {
		shards *= 2
	}
	return &cancellationDelayRegistry{shards: make([]cancellationDelayShard, shards), mask: shards - 1}
}

// register is everything a context costs outside a shutdown: one atomic add and the shard it will
// report into if it ever winds down.
func (r *cancellationDelayRegistry) register(state *cancellationDelayState) {
	shard := &r.shards[r.next.Add(1)&r.mask]
	state.shard = shard
	shard.live.count.Add(1)
}

// recordWindDown folds one finished context into both rolling averages: how long its own wind down
// took, and how long it has been since the previous one finished.
func (r *cancellationDelayRegistry) recordWindDown(held time.Duration) {
	raiseEWMA(&r.graceEWMANanos, held)

	// The gap is measured between whichever contexts happen to finish next to each other, so it
	// reflects how many were draining at once without having to know the number.
	now := int64(time.Since(delayEpoch))
	if previous := r.lastCompletionOffsetNanos.Swap(now); previous > 0 {
		raiseEWMA(&r.completionIntervalEWMANanos, time.Duration(now-previous))
	}
}

// raiseEWMA folds sample into an exponentially weighted average held as float64 bits. Zero bits mean
// no sample has been taken yet.
func raiseEWMA(counter *atomic.Uint64, sample time.Duration) {
	if sample <= 0 {
		return
	}
	next := float64(sample)
	for {
		current := counter.Load()
		blended := next
		if current != 0 {
			blended = ewmaAlpha*next + (1-ewmaAlpha)*math.Float64frombits(current)
		}
		if counter.CompareAndSwap(current, math.Float64bits(blended)) {
			return
		}
	}
}

// readEWMA reads a rolling average, or zero when no sample has been taken.
func readEWMA(counter *atomic.Uint64) time.Duration {
	bits := counter.Load()
	if bits == 0 {
		return 0
	}
	return time.Duration(math.Float64frombits(bits))
}

// CollectiveCancellationDelayStat is a point in time view of the live delayed contexts, so a
// shutdown can tell what is still holding on and when it expects to be let go. Everything beyond
// InFlight describes the grace period and is zero until a cancellation arrives.
type CollectiveCancellationDelayStat struct {
	// InFlight is how many delayed contexts are alive. Shutdown is over from this vantage point
	// when it reaches zero.
	InFlight int
	// InGrace is how many of those are counting down. A gap below InFlight means work is still
	// being handed out.
	InGrace int
	// AverageCheckpointDuration is how long a unit of work between checkpoints is taking, pooled
	// across every context that has completed one. Units completed before a cancellation arrived
	// are not counted.
	AverageCheckpointDuration time.Duration
	// AverageGraceDuration is the weighted average wind down, from a countdown starting to the
	// context being cancelled. Recent wind downs weigh most. It says how long one takes, not how
	// long the drain has left, since however many run at once is not in it.
	AverageGraceDuration time.Duration
	// AverageCompletionInterval is the weighted average gap between contexts finishing, which is
	// the drain's throughput expressed as a period. Whatever parallelism the work actually achieves
	// is already in it, so it shortens when more drain at once and lengthens when they contend. A
	// burst of simultaneous completions reads as very high throughput until the average recovers.
	AverageCompletionInterval time.Duration
	// EstimatedTime is when the contexts still winding down are expected to have finished, from
	// Little's law: the number left, times the observed gap between completions. It is zero until
	// two contexts have finished, since one completion establishes no throughput.
	EstimatedTime time.Time
	// LatestDeadline is when the running countdowns are due to expire. It is not a bound - every
	// checkpoint pushes it out - so it receding is what says work is still making progress.
	LatestDeadline time.Time
}

// GetCollectiveCancellationDelayStat reads the pooled totals. It costs one pass over the shards
// rather than one over the contexts, so it is cheap enough to call on a timer while a shutdown
// drains. The pass is not an instant, so under churn the counts can reflect slightly different
// moments.
func GetCollectiveCancellationDelayStat() *CollectiveCancellationDelayStat {
	return getCollectiveCancellationDelayStat(delayRegistry)
}

func getCollectiveCancellationDelayStat(registry *cancellationDelayRegistry) *CollectiveCancellationDelayStat {
	stat := &CollectiveCancellationDelayStat{}

	var completedWork, completedUnits, latestDeadlineNanos int64
	for i := range registry.shards {
		shard := &registry.shards[i]
		stat.InFlight += int(shard.live.count.Load())
		stat.InGrace += int(shard.live.inGrace.Load())
		completedWork += shard.completedWorkNanos.Load()
		completedUnits += shard.completedUnits.Load()
		latestDeadlineNanos = max(latestDeadlineNanos, shard.latestDeadlineNanos.Load())
	}

	if completedUnits > 0 {
		stat.AverageCheckpointDuration = time.Duration(completedWork / completedUnits)
	}
	if latestDeadlineNanos > 0 {
		stat.LatestDeadline = time.Unix(0, latestDeadlineNanos)
	}

	stat.AverageGraceDuration = readEWMA(&registry.graceEWMANanos)
	stat.AverageCompletionInterval = readEWMA(&registry.completionIntervalEWMANanos)
	if stat.AverageCompletionInterval > 0 && stat.InGrace > 0 {
		stat.EstimatedTime = time.Now().Add(time.Duration(stat.InGrace) * stat.AverageCompletionInterval)
	}
	return stat
}
