package job

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
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zaptest"
)

// fakeClock is a manually-advanced clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stubSubmit records every call and returns the next queued response. If the queue is empty for a
// given RST ID, returns success with an empty result.
type stubSubmit struct {
	mu    sync.Mutex
	calls []*beeremote.JobRequest
	// responses keyed by rstID; each call pops the head of the slice. A missing or empty slice
	// returns nil error.
	responses map[uint32][]error
}

func newStubSubmit() *stubSubmit {
	return &stubSubmit{responses: make(map[uint32][]error)}
}

func (s *stubSubmit) queueResponse(rstID uint32, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses[rstID] = append(s.responses[rstID], err)
}

func (s *stubSubmit) submit(jr *beeremote.JobRequest) (*beeremote.JobResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, jr)
	rstID := jr.RemoteStorageTarget
	if errs, ok := s.responses[rstID]; ok && len(errs) > 0 {
		err := errs[0]
		s.responses[rstID] = errs[1:]
		return nil, err
	}
	return &beeremote.JobResult{}, nil
}

func (s *stubSubmit) callsFor(path string) []*beeremote.JobRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*beeremote.JobRequest{}
	for _, c := range s.calls {
		if c.Path == path {
			out = append(out, c)
		}
	}
	return out
}

func newTestTracker(t *testing.T, clock *fakeClock, submit *stubSubmit) *pendingSyncTracker {
	t.Helper()
	tracker := newPendingSyncTracker(zaptest.NewLogger(t), submit.submit, func(dispatchAction, dispatchReason) {})
	tracker.now = clock.Now
	return tracker
}

func TestPendingSyncTracker_DrainBeforeReadyAt(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 10*time.Second, []uint32{1})
	clock.Advance(5 * time.Second)
	tr.drainReady(context.Background())

	assert.Empty(t, stub.callsFor("/foo"), "submit should not be called before readyAt")
	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.True(t, present, "entry should still be present before readyAt")
}

func TestPendingSyncTracker_DrainAfterReadyAt(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 10*time.Second, []uint32{7})
	clock.Advance(11 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 1, "submit should be called exactly once for the single RST")
	assert.Equal(t, uint32(7), calls[0].RemoteStorageTarget)
	assert.Equal(t, flex.SyncJob_UPLOAD, calls[0].GetSync().Operation)
	assert.Equal(t, "/foo", calls[0].Path)

	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.False(t, present, "entry should be removed after a successful submit")
}

func TestPendingSyncTracker_MultipleRSTs(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1, 2, 3})
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 3)
	assert.Equal(t, uint32(1), calls[0].RemoteStorageTarget)
	assert.Equal(t, uint32(2), calls[1].RemoteStorageTarget)
	assert.Equal(t, uint32(3), calls[2].RemoteStorageTarget)
}

func TestPendingSyncTracker_PendingCount(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	assert.Equal(t, 0, tr.pendingCount(), "no entries initially")

	tr.Mark("/a", 10*time.Second, []uint32{1})
	tr.Mark("/b", 10*time.Second, []uint32{1})
	tr.Mark("/c", 10*time.Second, []uint32{1})
	assert.Equal(t, 3, tr.pendingCount(), "three distinct paths pending")

	// Re-marking an existing path must not double-count.
	tr.Mark("/a", 10*time.Second, []uint32{2})
	assert.Equal(t, 3, tr.pendingCount(), "re-marking an existing path should not change the count")

	clock.Advance(11 * time.Second)
	tr.drainReady(context.Background())
	assert.Equal(t, 0, tr.pendingCount(), "all entries submitted and removed after cooldown")
}

// collectPendingSyncGauge collects metrics from the reader and returns the observed value of the
// remote.dispatch.pending_sync gauge.
func collectPendingSyncGauge(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "remote.dispatch.pending_sync" {
				continue
			}
			data, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "remote.dispatch.pending_sync should be a Gauge[int64]")
			require.Len(t, data.DataPoints, 1)
			return data.DataPoints[0].Value
		}
	}
	t.Fatal("remote.dispatch.pending_sync gauge not found")
	return 0
}

func TestPendingSyncTracker_PendingSyncGauge(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// Mirror the gauge wiring done in NewManager against a ManualReader-backed meter so we can
	// assert the callback observes the tracker's pending count.
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("job")
	gauge, err := meter.Int64ObservableGauge("remote.dispatch.pending_sync")
	require.NoError(t, err)
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, int64(tr.pendingCount()))
		return nil
	}, gauge)
	require.NoError(t, err)
	defer func() { _ = reg.Unregister() }()

	assert.Equal(t, int64(0), collectPendingSyncGauge(t, reader))

	tr.Mark("/a", 10*time.Second, []uint32{1})
	tr.Mark("/b", 10*time.Second, []uint32{1})
	assert.Equal(t, int64(2), collectPendingSyncGauge(t, reader))

	clock.Advance(11 * time.Second)
	tr.drainReady(context.Background())
	assert.Equal(t, int64(0), collectPendingSyncGauge(t, reader))
}

func TestPendingSyncTracker_INUSEBailsAndBumpsReadyAt(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	stub.queueResponse(1, beegfs.OpsErr_INUSE)
	tr := newTestTracker(t, clock, stub)

	cooldown := 10 * time.Second
	tr.Mark("/foo", cooldown, []uint32{1, 2, 3})
	clock.Advance(11 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 1, "should bail out after INUSE on the first RST")
	assert.Equal(t, uint32(1), calls[0].RemoteStorageTarget)

	tr.mu.Lock()
	ps, present := tr.entries["/foo"]
	expectedReadyAt := clock.Now().Add(cooldown)
	tr.mu.Unlock()
	require.True(t, present, "entry should remain after INUSE")
	assert.Equal(t, expectedReadyAt, ps.readyAt, "readyAt should be bumped by cooldown from current time")

	// Drain again before the new readyAt: still no submits.
	clock.Advance(5 * time.Second)
	tr.drainReady(context.Background())
	assert.Len(t, stub.callsFor("/foo"), 1, "no further submits before bumped readyAt elapses")

	// Advance past it; submit should now succeed for all RSTs and the entry should be removed.
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())
	assert.Len(t, stub.callsFor("/foo"), 4, "after retry all three RSTs submitted")
	tr.mu.Lock()
	_, present = tr.entries["/foo"]
	tr.mu.Unlock()
	assert.False(t, present)
}

func TestPendingSyncTracker_INUSEThroughWrappedError(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	// Mirrors the real production chain: setAccessFlags wraps the meta's OpsErr_INUSE inside an
	// fmt.Errorf, and the layers above wrap that in turn. errors.Is must still find OpsErr_INUSE.
	wrapped := fmt.Errorf("generate work requests: %w",
		fmt.Errorf("server returned an error setting file access flags: %w", beegfs.OpsErr_INUSE))
	stub.queueResponse(1, wrapped)
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1, 2})
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 1, "wrapped OpsErr_INUSE should be recognized and bail out")

	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.True(t, present, "entry should remain after wrapped-INUSE bailout")
}

func TestPendingSyncTracker_MarkOverwrites(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 10*time.Second, []uint32{1})

	// Second close five seconds later with a different cooldown and RST set.
	clock.Advance(5 * time.Second)
	tr.Mark("/foo", 20*time.Second, []uint32{2, 3})

	tr.mu.Lock()
	ps := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.Equal(t, 20*time.Second, ps.cooldown)
	assert.Equal(t, []uint32{2, 3}, ps.rstIDs)
	assert.Equal(t, clock.Now().Add(20*time.Second), ps.readyAt, "readyAt should reset to now+new cooldown")

	// Right at the *original* readyAt — should not fire because the second Mark pushed it out.
	clock.Advance(6 * time.Second) // total +11s; original readyAt was +10s
	tr.drainReady(context.Background())
	assert.Empty(t, stub.callsFor("/foo"))
}

func TestPendingSyncTracker_TerminalErrorContinuesAndRemoves(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	stub.queueResponse(1, rst.ErrJobAlreadyExists)
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1, 2})
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 2, "terminal error on RST 1 should not stop us from trying RST 2")
	assert.Equal(t, uint32(1), calls[0].RemoteStorageTarget)
	assert.Equal(t, uint32(2), calls[1].RemoteStorageTarget)

	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.False(t, present, "entry should be removed after the loop completes without INUSE")
}

func TestPendingSyncTracker_NotAllowedWarnsContinuesAndRemoves(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	stub.queueResponse(1, rst.ErrJobNotAllowed)
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1, 2})
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 2, "ErrJobNotAllowed on RST 1 should not stop us from trying RST 2")

	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.False(t, present, "entry should be removed after the loop completes")
}

func TestPendingSyncTracker_UnknownErrorContinuesAndRemoves(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	stub.queueResponse(1, errors.New("something unexpected"))
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1, 2})
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 2, "unknown error should not stop us from trying the next RST")

	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	tr.mu.Unlock()
	assert.False(t, present, "entry should be removed even after an unknown error")
}

func TestPendingSyncTracker_MarkCopiesRSTIDs(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	rstIDs := []uint32{1, 2}
	tr.Mark("/foo", 5*time.Second, rstIDs)
	rstIDs[0] = 99 // Mutating the caller's slice must not leak into the tracker.

	tr.mu.Lock()
	stored := tr.entries["/foo"].rstIDs
	tr.mu.Unlock()
	assert.Equal(t, []uint32{1, 2}, stored)
}

func TestPendingSyncTracker_HeapOrdering(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// /a has a much longer cooldown than /b, but they're inserted in that order.
	tr.Mark("/a", 30*time.Second, []uint32{1})
	tr.Mark("/b", 5*time.Second, []uint32{2})

	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	assert.Empty(t, stub.callsFor("/a"), "/a should not fire — its cooldown hasn't elapsed")
	assert.Len(t, stub.callsFor("/b"), 1, "/b should fire because its cooldown elapsed")

	tr.mu.Lock()
	_, aPresent := tr.entries["/a"]
	_, bPresent := tr.entries["/b"]
	tr.mu.Unlock()
	assert.True(t, aPresent, "/a should remain pending")
	assert.False(t, bPresent, "/b should be removed after submit")
}

func TestPendingSyncTracker_StaleHeapEntriesDiscardedAfterMark(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// First Mark with short cooldown; this pushes a heap entry with seq=1.
	tr.Mark("/foo", 5*time.Second, []uint32{1})
	// Second Mark replaces the map entry with seq=2 and pushes a new heap entry. The old (seq=1)
	// heap entry is still in the heap but is now stale.
	tr.Mark("/foo", 30*time.Second, []uint32{1})

	// Advance past the *first* readyAt only. The seq=1 heap entry is at the head and pops, but
	// the seq mismatch means it's discarded. The seq=2 entry isn't due yet.
	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	assert.Empty(t, stub.callsFor("/foo"), "stale heap entry must not trigger a submit")
	tr.mu.Lock()
	_, present := tr.entries["/foo"]
	heapLen := tr.heap.Len()
	tr.mu.Unlock()
	assert.True(t, present, "entry should still be pending until seq=2 readyAt elapses")
	assert.Equal(t, 1, heapLen, "stale heap entry should have been swept on drain")
}

func TestPendingSyncTracker_HeapEntryWithoutMapEntryDiscarded(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// Inject a heap entry that has no corresponding map entry — simulates a leftover from a
	// previously-completed path.
	tr.mu.Lock()
	tr.heap = append(tr.heap, heapEntry{path: "/orphan", readyAt: clock.Now().Add(-time.Second), seq: 99})
	tr.mu.Unlock()

	tr.drainReady(context.Background())
	assert.Empty(t, stub.callsFor("/orphan"), "orphan heap entry must not trigger a submit")
	tr.mu.Lock()
	heapLen := tr.heap.Len()
	tr.mu.Unlock()
	assert.Equal(t, 0, heapLen, "orphan heap entry should be discarded")
}

func TestPendingSyncTracker_MarkDuringProcessOneRace(t *testing.T) {
	// drainReady captures a snapshot into drained and deletes the map entry before calling
	// processOne. A Mark that arrives after the delete creates a new independent entry. processOne
	// uses its own snapshot and submits regardless; the new entry fires its own cooldown later.
	// We simulate this by constructing a drained value manually and calling processOne directly
	// after a concurrent Mark has already populated the map.
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// Simulate what drainReady would have captured before a concurrent Mark arrived.
	d := drained{path: "/foo", cooldown: 5 * time.Second, rstIDs: []uint32{1}}

	// Concurrent Mark arrives while processOne is about to run.
	tr.Mark("/foo", 30*time.Second, []uint32{2, 3})

	tr.processOne(context.Background(), d)

	calls := stub.callsFor("/foo")
	require.Len(t, calls, 1, "processOne submits using its own snapshot")
	assert.Equal(t, uint32(1), calls[0].RemoteStorageTarget, "snapshot RST ID is used, not the concurrent Mark's")

	tr.mu.Lock()
	ps, present := tr.entries["/foo"]
	tr.mu.Unlock()
	require.True(t, present, "concurrent Mark's entry must remain for later fire")
	assert.Equal(t, []uint32{2, 3}, ps.rstIDs, "concurrent Mark's RST IDs must be intact")
}

func TestPendingSyncTracker_DrainEmpty(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	tr.drainReady(context.Background()) // must not panic, must not call submit
	assert.Empty(t, stub.calls)

	tr.mu.Lock()
	_, hasNext := tr.nextReadyAtLocked()
	tr.mu.Unlock()
	assert.False(t, hasNext)
}

func TestPendingSyncTracker_MarkSignalsWake(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	tr := newTestTracker(t, clock, stub)

	// Drain any pre-existing wake (none expected, but be defensive).
	select {
	case <-tr.wake:
	default:
	}

	tr.Mark("/foo", 5*time.Second, []uint32{1})

	select {
	case <-tr.wake:
		// expected
	default:
		t.Fatal("Mark should have signalled the wake channel")
	}
}

func TestPendingSyncTracker_INUSEBumpSignalsWake(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	stub := newStubSubmit()
	stub.queueResponse(1, beegfs.OpsErr_INUSE)
	tr := newTestTracker(t, clock, stub)

	tr.Mark("/foo", 5*time.Second, []uint32{1})
	// Drain Mark's wake signal so the next signal must come from the INUSE bump.
	<-tr.wake

	clock.Advance(6 * time.Second)
	tr.drainReady(context.Background())

	select {
	case <-tr.wake:
		// expected
	default:
		t.Fatal("INUSE bump should have signalled the wake channel")
	}
}
