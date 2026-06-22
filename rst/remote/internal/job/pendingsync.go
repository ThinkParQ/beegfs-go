package job

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

type pendingSync struct {
	cooldown time.Duration
	// readyAt is set by t.now().Add(cooldown) and the resulting time.Time retains a monotonic
	// component which later calls to functions like time.After and time.Until will use instead of
	// wall clock time. As a result we don't need to worry about NTP slew, leap seconds, DST
	// transitions, and other time keeping shenanigans.
	//
	// IMPORTANT: This assumption would break if readyAt were ever persisted to disk and reloaded.
	readyAt time.Time
	rstIDs  []uint32
	// seq matches the most recently pushed heapEntry for this path. Older heap entries are
	// considered stale and discarded on peek/pop.
	seq uint64
}

type heapEntry struct {
	path string
	// readyAt has same limitations when persisted to disk as pendingSync.readyAt.
	readyAt time.Time
	seq     uint64
}

type pendingSyncHeap []heapEntry

func (h pendingSyncHeap) Len() int           { return len(h) }
func (h pendingSyncHeap) Less(i, j int) bool { return h[i].readyAt.Before(h[j].readyAt) }
func (h pendingSyncHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *pendingSyncHeap) Push(x any) { *h = append(*h, x.(heapEntry)) }

func (h *pendingSyncHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// drained carries everything processOne needs for a single submission. All fields are captured
// from the map entry while the lock is held in drainReady, so processOne needs no lock of its own
// for the submit path.
type drained struct {
	path     string
	cooldown time.Duration
	rstIDs   []uint32
}

// pendingSyncTracker holds entries that have received a LAST_WRITER_CLOSED event but should not
// be uploaded until their per-entry cooldown elapses. It is backed by a min-heap keyed by readyAt
// so finding the next due entry is O(log n) rather than O(n) across all pending entries. The
// tracker has no persistence: on Remote restart any pending entries are dropped and the next
// LAST_WRITER_CLOSED for the file re-arms the cooldown.
type pendingSyncTracker struct {
	log     *zap.Logger
	mu      sync.Mutex
	entries map[string]*pendingSync
	heap    pendingSyncHeap
	nextSeq uint64
	// wake coalesces signals from Mark / INUSE-bump to Run; capacity 1 with non-blocking send.
	wake chan struct{}
	// now allows a fakeClock to be injected for testing.
	now func() time.Time
	// submit is the function called after the cooldown elapses for an entry.
	submit func(jr *beeremote.JobRequest) (*beeremote.JobResult, error)
	// recordResult reports the outcome of a deferred upload submission. The deferred UPLOAD for a
	// LAST_WRITER_CLOSED event is actually submitted here (not in dispatchLastWriterClosed), so its
	// submission outcomes are recorded from processOne via this recorder (pre-bound by the manager
	// to the LAST_WRITER_CLOSED event type).
	recordResult dispatchResultRecorder
}

func newPendingSyncTracker(log *zap.Logger, submit func(*beeremote.JobRequest) (*beeremote.JobResult, error), recordResult dispatchResultRecorder) *pendingSyncTracker {
	return &pendingSyncTracker{
		log:          log.With(zap.String("component", "pendingSyncTracker")),
		entries:      make(map[string]*pendingSync),
		wake:         make(chan struct{}, 1),
		now:          time.Now,
		submit:       submit,
		recordResult: recordResult,
	}
}

// pendingCount returns the number of files currently awaiting their cooldown before an automatic
// upload is submitted. Uses len(entries) (the authoritative set) rather than the heap, which can
// hold stale entries.
func (t *pendingSyncTracker) pendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

func (t *pendingSyncTracker) signalWake() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// Mark schedules path to be uploaded once cooldown elapses, capturing the RSTs to submit to. A
// repeat Mark for the same path resets readyAt and overwrites cooldown/rstIDs with the latest
// values; the previous heap entry becomes stale (different seq) and is discarded on pop.
func (t *pendingSyncTracker) Mark(path string, cooldown time.Duration, rstIDs []uint32) {
	t.mu.Lock()
	t.nextSeq++
	seq := t.nextSeq
	readyAt := t.now().Add(cooldown)
	t.entries[path] = &pendingSync{
		cooldown: cooldown,
		readyAt:  readyAt,
		rstIDs:   append([]uint32(nil), rstIDs...),
		seq:      seq,
	}
	heap.Push(&t.heap, heapEntry{path: path, readyAt: readyAt, seq: seq})
	t.mu.Unlock()
	t.signalWake()
}

// Run blocks until ctx is cancelled, waking only when the next-due entry's readyAt elapses or
// when Mark / INUSE-bump signals that the heap head may have moved.
func (t *pendingSyncTracker) Run(ctx context.Context) {
	// This is the idiomatic way to create a reusable Timer that starts in a stopped+drained state
	// to ensure the initial NewTimer(0) never fires.
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		t.mu.Lock()
		next, hasNext := t.nextReadyAtLocked()
		t.mu.Unlock()

		var timerChan <-chan time.Time
		if hasNext {
			d := time.Until(next)
			if d <= 0 {
				t.drainReady(ctx)
				continue
			}
			timer.Reset(d)
			timerChan = timer.C
		}

		// If !hasNext, timerC stays nil, so the select only wakes on ctx or wake.
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-t.wake:
			// Mark or INUSE-bump may have inserted something earlier than next; recompute.
			timer.Stop()
		case <-timerChan:
			t.drainReady(ctx)
		}
	}
}

// nextReadyAtLocked returns the readyAt of the heap head, sweeping stale entries off the front
// as a side effect. Caller must hold t.mu.
func (t *pendingSyncTracker) nextReadyAtLocked() (time.Time, bool) {
	for t.heap.Len() > 0 {
		top := t.heap[0]
		ps, ok := t.entries[top.path]
		// If there is no map entry for this path or the entry is stale due to a sequence mismatch,
		// just ignore and continue.
		if !ok || ps.seq != top.seq {
			heap.Pop(&t.heap)
			continue
		}
		return top.readyAt, true
	}
	return time.Time{}, false
}

// drainReady pops every heap entry whose readyAt has elapsed and processes each one outside the
// lock. Stale entries (seq mismatch or path absent from the map) are silently dropped.
func (t *pendingSyncTracker) drainReady(ctx context.Context) {
	now := t.now()
	t.mu.Lock()
	ready := make([]drained, 0)
	for t.heap.Len() > 0 {
		top := t.heap[0]
		ps, ok := t.entries[top.path]
		if !ok || ps.seq != top.seq {
			heap.Pop(&t.heap)
			continue
		}
		if top.readyAt.After(now) {
			break // min-heap: nothing else is due either
		}
		heap.Pop(&t.heap)
		// A deep copy of ps.RSTIDs is not needed because the underlying array is not mutated after
		// the lock is released. This is because Mark() always constructs a new pendingSync and
		// replaces the map entry pointer. It does not write into an existing ps.rstIDs array.
		ready = append(ready, drained{path: top.path, cooldown: ps.cooldown, rstIDs: ps.rstIDs})
		delete(t.entries, top.path)
	}
	t.mu.Unlock()

	for _, d := range ready {
		if ctx.Err() != nil {
			return
		}
		t.processOne(ctx, d)
	}
}

// processOne takes a drained entry (i.e., cooldown expired) and attempts to trigger a job for it.
// Because we only guarantee we'll honor the cooldown on the initial close after write, if the file
// is reopened the cooldown is not initially reset and instead processOne() is responsible for
// detecting if files are reopened and still INUSE when it is called.
//
// For performance reasons we do not keep the pendingSyncTracker mu locked while processOne() is
// called, so it is possible the file was reopened/closed since drainReady() was called, which would
// have triggered another Mark(). This is acceptable because the second Mark() essentially becomes a
// no-op as the subsequent processOne() call will detect the existing in progress or completed job.
func (t *pendingSyncTracker) processOne(ctx context.Context, d drained) {
	for _, rstID := range d.rstIDs {
		_, err := t.submit(&beeremote.JobRequest{
			Path:                d.path,
			Priority:            3,
			RemoteStorageTarget: rstID,
			Type: &beeremote.JobRequest_Sync{
				Sync: &flex.SyncJob{
					Operation: flex.SyncJob_UPLOAD,
				},
			},
		})
		if err == nil {
			t.log.Debug("automatic sync upload submitted",
				zap.String("path", d.path), zap.Uint32("rstId", rstID))
			t.recordResult(actionSubmitted, reasonSubmitted)
			continue
		}

		// OpsErr_INUSE is a property of the file (meta refused the access-flag transition because
		// sessions are active), so further submits for other RSTs would also fail. Bail out and
		// push a fresh heap entry so we retry after another cooldown.
		if errors.Is(err, beegfs.OpsErr_INUSE) {
			t.log.Debug("automatic sync deferred — file is currently in use",
				zap.String("path", d.path), zap.Uint32("rstId", rstID), zap.Duration("retryIn", d.cooldown))
			t.mu.Lock()
			// Only re-arm if no concurrent Mark already created a newer entry while we were
			// submitting. If one exists, let its heap entry fire instead.
			if _, exists := t.entries[d.path]; !exists {
				t.nextSeq++
				seq := t.nextSeq
				readyAt := t.now().Add(d.cooldown)
				t.entries[d.path] = &pendingSync{cooldown: d.cooldown, readyAt: readyAt, rstIDs: d.rstIDs, seq: seq}
				heap.Push(&t.heap, heapEntry{path: d.path, readyAt: readyAt, seq: seq})
			}
			t.mu.Unlock()
			t.signalWake()
			t.recordResult(actionDeferred, reasonFileInUse)
			return
		}

		// The file was deleted before its deferred upload ran — nothing to sync.
		if errors.Is(err, beegfs.OpsErr_PATHNOTEXISTS) {
			t.log.Debug("automatic sync skipped — file no longer exists",
				zap.String("path", d.path), zap.Uint32("rstId", rstID), zap.Error(err))
			t.recordResult(actionSkipped, reasonPathNotExists)
			continue
		}

		// A job already exists for this path/RST (in progress, already complete, or already
		// offloaded), so no new upload is needed.
		if errors.Is(err, rst.ErrJobAlreadyExists) ||
			errors.Is(err, rst.ErrJobAlreadyComplete) ||
			errors.Is(err, rst.ErrJobAlreadyOffloaded) {
			t.log.Debug("automatic sync skipped — a job already exists for this path/RST",
				zap.String("path", d.path), zap.Uint32("rstId", rstID), zap.Error(err))
			t.recordResult(actionSkipped, reasonAlreadyHandled)
			continue
		}

		// ErrJobNotAllowed means a previous failed job is blocking submission. Warn so the
		// operator knows auto-sync is stuck; manual intervention (clear the failed job) is needed.
		if errors.Is(err, rst.ErrJobNotAllowed) {
			t.log.Warn("automatic sync blocked — a failed job already exists for RST; clear it to resume auto-sync",
				zap.String("path", d.path), zap.Uint32("rstId", rstID), zap.Error(err))
			t.recordResult(actionError, reasonBlockedByFailedJob)
			continue
		}

		t.recordResult(actionError, reasonSubmitFailed)
		t.log.Warn("automatic sync upload failed",
			zap.String("path", d.path), zap.Uint32("rstId", rstID), zap.Error(err))
	}
}
