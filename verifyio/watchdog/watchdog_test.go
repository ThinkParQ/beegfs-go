// This is a unit test.
//
// Coverage: Wait's three timing cases (done closes before ctx is done; done
// closes within the grace period after ctx is done; done never closes, so
// grace elapses and Wait gives up), its rejection of a grace below MinGrace,
// and Activity's Start/Done/Snapshot
// bookkeeping (a worker with no in-flight activity never appears in a
// snapshot; a started-but-not-done worker does, with a real elapsed time;
// Done removes it again).
package watchdog

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitDoneBeforeCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	close(done)

	stuck, err := Wait(ctx, MinGrace, done)
	if err != nil {
		t.Fatalf("Wait returned an unexpected error: %v", err)
	}
	if stuck {
		t.Error("Wait returned true (gave up), want false: done was already closed")
	}
}

func TestWaitDoneWithinGraceAfterCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel() // ctx becomes done...
		time.Sleep(20 * time.Millisecond)
		close(done) // ...then done closes, well within a generous grace period
	}()

	stuck, err := Wait(ctx, MinGrace, done)
	if err != nil {
		t.Fatalf("Wait returned an unexpected error: %v", err)
	}
	if stuck {
		t.Error("Wait returned true (gave up), want false: done closed within the grace period")
	}
}

// TestWaitGivesUpAfterGrace is the core regression case: a worker stuck
// forever (done never closes) must not make Wait block forever too --
// it must give up after grace once ctx is done, so the caller can move on
// and let the process exit reclaim the abandoned work.
func TestWaitGivesUpAfterGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx is already done
	done := make(chan struct{})
	// done is intentionally never closed -- simulates a worker stuck in a
	// blocked syscall that never returns.

	start := time.Now()
	stuck, err := Wait(ctx, MinGrace, done)
	if err != nil {
		t.Fatalf("Wait returned an unexpected error: %v", err)
	}
	if !stuck {
		t.Error("Wait returned false, want true: done never closes, so it must give up")
	}
	if elapsed := time.Since(start); elapsed < MinGrace {
		t.Errorf("Wait returned after %s, want it to wait out the full grace period first", elapsed)
	}
}

// TestWaitRejectsGraceBelowMinGrace pins the input validation rather than the
// race it prevents: with a grace under a second the grace timer can already be
// ready when done closes, and select picks uniformly among ready cases, so Wait
// would report STUCK for work that finished. Rejecting the input makes that tie
// unconstructible -- which is also why there is no test of the tie itself.
func TestWaitRejectsGraceBelowMinGrace(t *testing.T) {
	for _, grace := range []time.Duration{
		0,
		-time.Second,
		time.Millisecond,
		MinGrace - time.Nanosecond,
	} {
		t.Run(grace.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			close(done)

			stuck, err := Wait(ctx, grace, done)
			if !errors.Is(err, ErrGraceTooSmall) {
				t.Errorf("Wait(grace=%s) error = %v, want ErrGraceTooSmall", grace, err)
			}
			if stuck {
				t.Errorf("Wait(grace=%s) returned true, want false alongside the error", grace)
			}
		})
	}
}

func TestActivitySnapshotEmptyWhenNothingStarted(t *testing.T) {
	a := NewActivity(3)
	if got := a.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot() = %v, want empty -- no worker has Start'd anything", got)
	}
}

func TestActivityStartAppearsInSnapshotUntilDone(t *testing.T) {
	a := NewActivity(3)
	a.Start(1, "WriteBlock")

	snap := a.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("Snapshot() = %v, want exactly one in-flight worker", snap)
	}
	if snap[0].ID != 1 || snap[0].Op != "WriteBlock" {
		t.Errorf("Snapshot()[0] = %+v, want {ID:1 Op:WriteBlock ...}", snap[0])
	}
	if snap[0].Elapsed < 0 {
		t.Errorf("Elapsed = %s, want >= 0", snap[0].Elapsed)
	}

	a.Done(1)
	if got := a.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot() after Done = %v, want empty", got)
	}
}

func TestActivityStartOverwritesPriorEntryWithoutRequiringDone(t *testing.T) {
	// A worker whose previous op never returned (and so never called Done)
	// still needs Start for its *next* op to work correctly once it's no
	// longer stuck -- Start must not require a preceding Done.
	a := NewActivity(1)
	a.Start(0, "first-op")
	a.Start(0, "second-op")

	snap := a.Snapshot()
	if len(snap) != 1 || snap[0].Op != "second-op" {
		t.Errorf("Snapshot() = %v, want exactly one entry for second-op", snap)
	}
}

func TestActivityMultipleWorkersIndependent(t *testing.T) {
	a := NewActivity(3)
	a.Start(0, "op-a")
	a.Start(2, "op-c")
	// worker 1 never started anything.

	snap := a.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot() = %v, want exactly 2 in-flight workers", snap)
	}
	byID := map[int]string{}
	for _, s := range snap {
		byID[s.ID] = s.Op
	}
	if byID[0] != "op-a" || byID[2] != "op-c" {
		t.Errorf("Snapshot() = %v, want {0:op-a, 2:op-c}", snap)
	}
}

// TestActivityIgnoresOutOfRangeID pins that the diagnostic machinery does not
// take the run down with it.
//
// Start and Done index a fixed-size slice with a caller-supplied worker id. An
// id outside the range -- 1-based ids, or a supervisor goroutine reusing
// id == numWorkers -- used to panic with index-out-of-range, which means the
// component whose whole job is reporting that a soak is wedged is what kills
// the soak. There are no in-repo callers yet; this is library surface.
func TestActivityIgnoresOutOfRangeID(t *testing.T) {
	const workers = 4
	a := NewActivity(workers)

	for _, id := range []int{-1, workers, workers + 1, 1 << 20} {
		a.Start(id, "read") // must not panic
		a.Done(id)
	}

	// In-range ids still work, so the guard did not disable the thing.
	a.Start(0, "write")
	found := false
	for _, e := range a.Snapshot() {
		if e.Op == "write" {
			found = true
		}
	}
	if !found {
		t.Error("an in-range Start was lost; the bounds check is too aggressive")
	}
}
