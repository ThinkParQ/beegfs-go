// Package watchdog gives a tool built on verifyio a bounded way to stop
// *waiting* on blocked work, even though it has no way to stop the blocked
// call itself. Every verifyio call (Writer.WriteBlock, verifier.VerifyFile,
// etc.) is purely synchronous with no internal cancellation -- Go cannot
// interrupt a blocked syscall, so a call can, in principle, never return
// (exactly the kind of bug this library exists to help find). See
// verifyio/README.md's "Building a tool on verifyio" section.
package watchdog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MinGrace is the smallest grace Wait accepts.
//
// A second, because of what this mechanism is for: bounding a wait on a syscall
// that may never return, on a filesystem that may be under load and struggling.
// A sub-second grace does not express "give the work a chance to finish" -- it
// asserts that syscalls return quickly, which is a latency measurement and wants
// a tool that records durations and reports them, not one that returns STUCK.
const MinGrace = time.Second

// ErrGraceTooSmall is returned by Wait for a grace below MinGrace, zero and
// negative values included.
var ErrGraceTooSmall = errors.New("watchdog: grace below MinGrace")

// Wait blocks until done is closed, or until grace has elapsed after ctx is
// done, whichever comes first. Returns true if it gave up waiting -- done
// never closed within the grace period -- false if done closed normally.
//
// grace must be at least MinGrace; anything smaller returns ErrGraceTooSmall
// without waiting. A grace short enough for the grace timer to already be ready
// when done closes turns the second select into a coin flip -- Go picks
// uniformly among ready cases -- so Wait would report STUCK for work that
// finished. The bool is meaningless when the error is non-nil.
//
// A true return means whatever done was waiting on is still running, most
// likely stuck in a blocked syscall; Wait cannot stop it, only stop waiting
// for it. Callers should treat true as "report STUCK and exit soon" -- the
// process exiting is what actually reclaims the abandoned work, not this
// function.
func Wait(ctx context.Context, grace time.Duration, done <-chan struct{}) (bool, error) {
	if grace < MinGrace {
		return false, fmt.Errorf("%w: got %s, need at least %s", ErrGraceTooSmall, grace, MinGrace)
	}
	select {
	case <-done:
		return false, nil
	case <-ctx.Done():
	}
	select {
	case <-done:
		return false, nil
	case <-time.After(grace):
		return true, nil
	}
}

// WorkerState is one worker's in-flight activity, as reported by Snapshot.
type WorkerState struct {
	ID      int
	Op      string
	Elapsed time.Duration
}

// Activity tracks what each of a fixed set of workers is doing right now, so
// a watchdog firing can report which one(s) are stuck and on what, instead
// of just "something, somewhere". Safe for concurrent use -- each worker
// only ever touches its own slot, but Snapshot may run concurrently with
// Start/Done from any worker.
type Activity struct {
	mu      sync.Mutex
	entries []activityEntry
}

type activityEntry struct {
	op      string // "" means this worker has no in-flight activity
	started time.Time
}

// NewActivity returns an Activity tracking numWorkers workers, identified by
// the ids 0..numWorkers-1.
func NewActivity(numWorkers int) *Activity {
	return &Activity{entries: make([]activityEntry, numWorkers)}
}

// Start records that worker id began op at the current time. Call
// immediately before every blocking call a worker makes. Overwrites any
// previous entry for this worker without requiring a matching Done first --
// a worker that never calls Done because its previous op never returned is
// exactly the stuck case Snapshot exists to report; Start is never reached
// again for that worker once it's truly wedged.
// An id outside 0..numWorkers-1 is ignored rather than panicking. This is
// diagnostic machinery whose whole job is reporting that a run is wedged, so
// taking the run down with an index-out-of-range -- from ids assigned 1-based,
// or a supervisor goroutine reusing id == numWorkers -- would destroy the very
// report it exists to produce. Losing one worker's activity entry degrades the
// report; crashing removes it.
func (a *Activity) Start(id int, op string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id < 0 || id >= len(a.entries) {
		return
	}
	a.entries[id] = activityEntry{op: op, started: time.Now()}
}

// Done clears worker id's current activity. Call immediately after a
// blocking call returns, successfully or not. An out-of-range id is ignored;
// see Start.
func (a *Activity) Done(id int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id < 0 || id >= len(a.entries) {
		return
	}
	a.entries[id] = activityEntry{}
}

// Snapshot returns the current op and elapsed time for every worker with an
// in-flight activity (Start called, Done not yet called) at the moment
// Snapshot runs -- i.e. every worker still blocked on something right now.
func (a *Activity) Snapshot() []WorkerState {
	a.mu.Lock()
	defer a.mu.Unlock()
	states := make([]WorkerState, 0, len(a.entries))
	for id, e := range a.entries {
		if e.op == "" {
			continue
		}
		states = append(states, WorkerState{ID: id, Op: e.op, Elapsed: time.Since(e.started)})
	}
	return states
}
