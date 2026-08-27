package rst

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// appendErrors joins each nextErr onto accumulatedErr in order, keeping them all wrapped so
// errors.Is and errors.As still match any one of them. Any argument may be nil, which is what makes
// it convenient for accumulating the failures of steps that all have to run before returning.
func appendErrors(accumulatedErr error, nextErrs ...error) error {
	for _, nextErr := range nextErrs {
		if nextErr == nil {
			continue
		} else if accumulatedErr == nil {
			accumulatedErr = nextErr
		} else {
			accumulatedErr = fmt.Errorf("%w; %w", accumulatedErr, nextErr)
		}
	}
	return accumulatedErr
}

// openFileForAppend returns a handle that durably appends to path, creating it if needed and never
// truncating what is already there. Each Write is committed to stable storage before it returns, so
// an appended record survives a crash without an explicit Sync. A crash part way through a Write
// can still leave a partial record behind, so records must be fixed size or self delimiting.
func openFileForAppend(path string) (*os.File, error) {
	return createPersistentFile(path, unix.O_WRONLY|unix.O_APPEND, 0600)
}

// openFileForUpdate returns a handle that durably rewrites bytes of an existing file in place, such
// as the fixed size records WriteAt addresses by offset. Nothing is created or truncated, so
// os.ErrNotExist means the file was never created or has since been deleted, which is state a
// caller can act on instead of silently recreating.
func openFileForUpdate(path string) (*os.File, error) {
	return updatePersistentFile(path, unix.O_WRONLY)
}

// touchFile durably creates path and leaves an existing file's contents untouched, so it is safe to
// call every time state is opened. Creating state files up front is what makes their later absence
// unambiguous: it means the state was deleted, not that nothing has been written to it yet.
func touchFile(path string) error {
	f, err := createPersistentFile(path, 0, 0600)
	if err != nil {
		return err
	}

	return f.Close()
}

// createPersistentFile opens path for durable writes with the given unix.O_* mode flags, creating
// it if needed but never truncating a file that already exists. perm applies only to a file this
// call creates, and the parent directory has to exist already. Newly created files have their
// parent directory fsynced so the directory entry is persisted. The returned file uses O_DSYNC so
// successful writes are committed to stable storage before returning.
func createPersistentFile(path string, mode int, perm uint32) (*os.File, error) {
	mode |= unix.O_DSYNC | unix.O_CLOEXEC

	fd, err := unix.Open(path, mode|unix.O_CREAT|unix.O_EXCL, perm)
	created := err == nil
	if errors.Is(err, unix.EEXIST) {
		fd, err = unix.Open(path, mode, 0)
	}
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("failed to create file handle for %s", path)
	}

	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return f, nil
}

// updatePersistentFile opens an existing path for durable in-place writes with the given unix.O_*
// mode flags. Nothing is created or truncated, so opening a path that doesn't exist fails. The
// returned file uses O_DSYNC so successful writes are committed to stable storage before returning.
func updatePersistentFile(path string, mode int) (*os.File, error) {
	fd, err := unix.Open(path, mode|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("failed to create file handle for %s", path)
	}

	return f, nil
}

// writePersistentFile atomically replaces path with data, which is how a whole file's worth of
// state should be rewritten. Content is staged in a temporary file that is fsynced before being
// renamed over path, then the parent directory is fsynced so the rename is persisted. A crash
// therefore leaves path either fully replaced or untouched, never truncated part way through a
// rewrite the way an O_TRUNC write would. The parent directory has to exist already.
func writePersistentFile(path string, data []byte, perm uint32) (err error) {
	tmpPath := persistentTmpPath(path)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(perm))
	if err != nil {
		return err
	}

	closed := false
	defer func() {
		if err != nil {
			if !closed {
				_ = f.Close()
			}
			_ = removeIfExists(tmpPath)
		}
	}()

	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("failed to write %s: %w", tmpPath, err)
	}

	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", tmpPath, err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("failed to close %s: %w", tmpPath, err)
	}
	closed = true

	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, path, err)
	}

	return syncDir(filepath.Dir(path))
}

// persistentTmpSuffix distinguishes the staging file writePersistentFile renames from. Listing code
// needs it to tell a leftover apart from the file it was staging for.
const persistentTmpSuffix = ".tmp"

// persistentTmpPath is where writePersistentFile stages content before renaming it over path. A
// crash can leave one behind, so anything that lists a directory of persisted files must skip
// these, and anything deleting state should delete them alongside the file itself.
func persistentTmpPath(path string) string {
	return path + persistentTmpSuffix
}

// syncDir fsyncs the directory at path so entries created, renamed or removed within it are
// persisted. Only the directory's own entries are covered: file contents need their own sync.
func syncDir(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to open %s for sync: %w", path, err)
	}

	syncErr := unix.Fsync(fd)
	closeErr := unix.Close(fd)

	if syncErr != nil {
		return fmt.Errorf("failed to sync %s: %w", path, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close %s: %w", path, closeErr)
	}

	return nil
}

// removeEmptyDirs removes dir and then each of its parents in turn, stopping at the first one that
// is not empty or at stopAt, whichever comes first. stopAt itself is never removed.
//
// A directory that still holds another operation's state ends the walk rather than failing, which is
// what lets every teardown call this without knowing whether it is the last one to finish. Without
// it the per-job and per-operation directories accumulate on the mount forever, since the files
// inside them are removed but nothing ever removes the directories themselves.
func removeEmptyDirs(dir string, stopAt string) error {
	stopAt = filepath.Clean(stopAt)
	for dir = filepath.Clean(dir); dir != stopAt && dir != "." && dir != string(filepath.Separator); {
		if err := os.Remove(dir); err != nil {
			if errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST) {
				// Still in use by another operation, so this is as far up as the walk can go.
				return nil
			}
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("failed to remove empty state directory %s: %w", dir, err)
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return nil
}

// removeIfExists removes path and reports success when it is already gone, for teardown paths that
// cannot assume every state file was created.
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

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
