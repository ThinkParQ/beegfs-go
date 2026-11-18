//go:build linux

// This is a unit test.
//
// Coverage: TryAcquireExclusive's no-overlap success path, union-extent
// computation across pre-existing overlapping records, contention/busy
// detection, lock release (and double-release erroring), ErrSetChanged when
// the overlap set changes between the initial scan and the lock rescan, and
// bad-input rejection; the unionExtent and sameOverlapSet helpers in
// isolation; and the in-process range table's semantics (non-overlapping
// locks don't block each other, overlapping shared locks are refused --
// including the identical-range case -- while disjoint and adjacent ones are
// granted, and an exclusive request conflicts with an existing shared one).
// Also guardSafeToRelease's
// timeout-vs-definite-failure decision in isolation, and the same-process
// double-lock hazard that decision exists to prevent on both the exclusive
// and shared acquire paths (by directly simulating the state a timeout
// leaves behind, since the real trigger is a live network/DLM timing
// condition this suite can't manufacture), plus the identical hazard on
// Release's own unlock path (via hookReleaseUnlockErr, which -- unlike the
// acquire-side tests -- exercises Release's real decision logic rather than
// just the leftover state), from BOTH lock-releasing entry points
// (WriteBlock and Truncate) plus Truncate's unlocked no-op path. Also
// withTimeout's own give-up-and-return-ErrLockTimeout behavior in isolation,
// using a synthetic slow fn rather than a real hung syscall.
// Finally, that a stuck release is never reported through a benign error:
// on the acquire rollback path (both hooks combined, so ErrSetChanged cannot
// mask a leaked guard) and out of Writer.WriteBlock, plus
// ReleaseErrorOrCause's decision table in isolation. Also Writer.Truncate's
// locking: a lease anywhere in the affected region refuses the truncate
// (leaving records and length untouched) and succeeds once released,
// including a lease on a record lying past EOF -- which a bound at EOF would
// have left unprotected while RemoveCovering still deleted it.
package xattrstore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"golang.org/x/sys/unix"
)

// openTarget opens the store's data file for reading+writing and
// registers cleanup so each test's fds are dropped automatically.
// Returns a *os.File suitable to pass to TryAcquireExclusive.
func openTarget(t *testing.T, s *Store) *os.File {
	t.Helper()
	f, err := os.OpenFile(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestTryAcquireExclusiveNoOverlap(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	lease, err := s.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive: %v", err)
	}
	off, ln := lease.Range()
	if off != 0 || ln != 4096 {
		t.Errorf("Range=(%d,%d), want (0,4096) when no overlaps", off, ln)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestTryAcquireExclusiveUnionExtent(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	// Records that overlap the requested range. The union of
	// [50, 550) with (0, 100) and (500, 100) is [0, 600).
	putEntries(t, s,
		[2]int64{0, 100},
		[2]int64{500, 100},
		[2]int64{1000, 100}, // outside the requested range; must NOT enlarge union
	)
	lease, err := s.TryAcquireExclusive(f, 50, 500) // [50, 550)
	if err != nil {
		t.Fatalf("TryAcquireExclusive: %v", err)
	}
	defer lease.Release()
	off, ln := lease.Range()
	if off != 0 || ln != 600 {
		t.Errorf("union extent=(%d,%d), want (0,600)", off, ln)
	}
}

func TestTryAcquireExclusiveContention(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s) // separate fd; the range table is what separates the two acquires

	lease1, err := s.TryAcquireExclusive(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer lease1.Release()

	// Overlapping range on a different fd should fail with ErrLockBusy.
	if _, err := s.TryAcquireExclusive(f2, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("overlapping acquire on second fd: err=%v, want ErrLockBusy", err)
	}

	// Non-overlapping range on the same second fd should succeed.
	lease2, err := s.TryAcquireExclusive(f2, 8192, 4096)
	if err != nil {
		t.Fatalf("non-overlapping acquire on second fd: %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("Release lease2: %v", err)
	}
}

func TestTryAcquireExclusiveReleaseFreesLock(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease, err := s.TryAcquireExclusive(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// After release, another fd must be able to take the same range.
	lease2, err := s.TryAcquireExclusive(f2, 0, 4096)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("Release lease2: %v", err)
	}
}

func TestTryAcquireExclusiveDoubleRelease(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)
	lease, err := s.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := lease.Release(); err == nil {
		t.Errorf("second Release: expected error, got nil")
	}
}

// TestTryAcquireExclusiveSetChanged uses the hookAfterLock injection
// point to add a new overlapping record between the initial scan and
// the rescan. The helper must detect the change, release the lock,
// and return ErrSetChanged.
func TestTryAcquireExclusiveSetChanged(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	// Install hook that adds a new record overlapping the requested
	// range. Hook fires exactly once (then disarms itself) so the
	// post-error verification call can succeed.
	called := false
	hookAfterLock = func() {
		if called {
			return
		}
		called = true
		if err := s.Put(2048, 1024, makeHeader(t, 2048, 1)); err != nil {
			t.Fatalf("hook Put: %v", err)
		}
	}
	t.Cleanup(func() { hookAfterLock = nil })

	if _, err := s.TryAcquireExclusive(f, 0, 4096); !errors.Is(err, ErrSetChanged) {
		t.Errorf("acquire with set-change: err=%v, want ErrSetChanged", err)
	}
	if !called {
		t.Errorf("hookAfterLock was not invoked")
	}

	// Lock should have been released. Acquire again (hook is now a
	// no-op; the new record from the previous attempt is still on
	// disk, which means rescan == initial-scan on this attempt and
	// the call succeeds).
	lease, err := s.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("acquire after set-changed release: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestTryAcquireExclusiveBadInputs(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)
	if _, err := s.TryAcquireExclusive(nil, 0, 4096); err == nil {
		t.Errorf("nil file: expected error")
	}
	if _, err := s.TryAcquireExclusive(f, 0, 0); err == nil {
		t.Errorf("length=0: expected error")
	}
	if _, err := s.TryAcquireExclusive(f, 0, -1); err == nil {
		t.Errorf("length=-1: expected error")
	}
}

// --- unit tests on the pure helpers ---------------------------------------

func TestUnionExtentNoOverlaps(t *testing.T) {
	off, ln := unionExtent(100, 200, nil)
	if off != 100 || ln != 200 {
		t.Errorf("unionExtent (no overlaps) = (%d,%d), want (100,200)", off, ln)
	}
}

func TestUnionExtentExtendsBothEnds(t *testing.T) {
	overlaps := []Entry{
		{Offset: 50, Length: 30},  // extends start to 50, end stays
		{Offset: 280, Length: 50}, // start stays, extends end to 330
		{Offset: 150, Length: 10}, // strictly inside, no effect
	}
	off, ln := unionExtent(100, 200, overlaps) // request [100, 300)
	if off != 50 || ln != 280 {                // union [50, 330)
		t.Errorf("unionExtent = (%d,%d), want (50,280)", off, ln)
	}
}

func TestSameOverlapSet(t *testing.T) {
	a := []Entry{
		{Offset: 0, Length: 100, Name: XAttrName(0, 100)},
		{Offset: 200, Length: 50, Name: XAttrName(200, 50)},
	}
	// Same entries, different order.
	b := []Entry{
		{Offset: 200, Length: 50, Name: XAttrName(200, 50)},
		{Offset: 0, Length: 100, Name: XAttrName(0, 100)},
	}
	if !sameOverlapSet(a, b) {
		t.Errorf("same entries (reordered) reported different")
	}
	// Different length.
	c := []Entry{{Offset: 0, Length: 100, Name: XAttrName(0, 100)}}
	if sameOverlapSet(a, c) {
		t.Errorf("different lengths reported same")
	}
	// Same length, different members.
	d := []Entry{
		{Offset: 0, Length: 100, Name: XAttrName(0, 100)},
		{Offset: 300, Length: 50, Name: XAttrName(300, 50)},
	}
	if sameOverlapSet(a, d) {
		t.Errorf("different members reported same")
	}
}

// TestPosixGuardSafeToRelease pins the decision that caused the
// rescan/acquire-timeout double-exclusive-lock hazard: a timeout means the
// syscall's real outcome is unknown, so the in-process guard must not be
// freed, while a definite failure (or any other error) means the kernel
// never granted the lock, so freeing it is safe.
func TestPosixGuardSafeToRelease(t *testing.T) {
	if guardSafeToRelease(ErrLockTimeout) {
		t.Errorf("ErrLockTimeout: got safe to release, want not safe")
	}
	wrapped := fmt.Errorf("F_SETLK F_WRLCK [0,100): %w (after 5s)", ErrLockTimeout)
	if guardSafeToRelease(wrapped) {
		t.Errorf("wrapped ErrLockTimeout: got safe to release, want not safe")
	}
	if !guardSafeToRelease(unix.EAGAIN) {
		t.Errorf("EAGAIN: got not safe to release, want safe")
	}
	if !guardSafeToRelease(unix.EWOULDBLOCK) {
		t.Errorf("EWOULDBLOCK: got not safe to release, want safe")
	}
	if !guardSafeToRelease(errors.New("some other definite failure")) {
		t.Errorf("other error: got not safe to release, want safe")
	}
}

// TestRescanTimeoutLeaksGuardNotReleasesIt is a regression test for the
// exact hazard TestPosixGuardSafeToRelease's decision logic exists to
// prevent: if the rescan step times out, a second same-process attempt on
// an overlapping range must still be rejected, not incorrectly succeed.
// This can't deterministically force withTimeout's real timer to fire
// against a real (fast, local) syscall, so it instead directly verifies the
// state tryAcquireExclusivePOSIX's timeout path leaves behind: the
// in-process guard still holding the range. If a future change reintroduces
// the bug by calling s.ranges.release on this path, this test's setup
// becomes unnecessary but harmless; the real regression is exercised by
// symptom rather than by trigger, since the trigger is a live network/DLM
// timing condition this test suite cannot manufacture.
func TestRescanTimeoutLeaksGuardNotReleasesIt(t *testing.T) {
	s := makeTempStore(t)
	// Simulate the state left behind by a rescan timeout: the guard was
	// taken (mirroring tryAcquireExclusivePOSIX's tryAcquire call) and,
	// per the fix, deliberately never released.
	if !s.ranges.tryAcquire(0, 4096, true) {
		t.Fatalf("setup: tryAcquire on a clean table unexpectedly failed")
	}

	f := openTarget(t, s)
	if _, err := s.TryAcquireExclusive(f, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("second same-process acquire over the leaked range: err=%v, want ErrLockBusy", err)
	}
}

// TestSharedLockTimeoutLeaksGuardNotReleasesIt is
// tryAcquireSharedPOSIX's counterpart to
// TestRescanTimeoutLeaksGuardNotReleasesIt: this path used to have no
// guardSafeToRelease protection at all -- unlike the exclusive path,
// it unconditionally freed the guard on any error, including an ambiguous
// timeout -- so a second same-process exclusive attempt on the overlapping
// range could incorrectly succeed while the abandoned F_RDLCK goroutine
// might still be about to land. Directly verifies the state
// tryAcquireSharedPOSIX's timeout path now leaves behind, for the same
// reason its exclusive-path sibling does: this suite cannot manufacture a
// live network/DLM timing condition to trigger it for real.
func TestSharedLockTimeoutLeaksGuardNotReleasesIt(t *testing.T) {
	s := makeTempStore(t)
	// Simulate the state left behind by a shared-lock timeout: the guard
	// was taken (mirroring tryAcquireSharedPOSIX's tryAcquire call) and,
	// per the fix, deliberately never released.
	if !s.ranges.tryAcquire(0, 4096, false) {
		t.Fatalf("setup: tryAcquire on a clean table unexpectedly failed")
	}

	f := openTarget(t, s)
	if _, err := s.TryAcquireExclusive(f, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("exclusive acquire over the leaked shared range: err=%v, want ErrLockBusy", err)
	}
}

// TestLeaseReleaseTimeoutLeaksGuardNotReleasesIt is a regression test for
// Release's own guard-withholding fix: unlike the acquire paths, Release used
// to free the in-process range-table guard unconditionally, even when the
// F_SETLK F_UNLCK call itself timed out (an ambiguous outcome -- the real
// unlock may still be in flight). Uses hookReleaseUnlockErr to force
// Release's internal error to ErrLockTimeout deterministically, since the
// real unlock syscall is local and fast and can't be made to time out for
// real. Unlike the sibling acquire-side "leaks guard" tests, this one
// actually calls Release and exercises its real decision logic, rather than
// just manufacturing the leftover state and checking the downstream symptom.
func TestLeaseReleaseTimeoutLeaksGuardNotReleasesIt(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	lease, err := s.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	hookReleaseUnlockErr = func() error { return ErrLockTimeout }
	t.Cleanup(func() { hookReleaseUnlockErr = nil })

	if err := lease.Release(); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Release: err=%v, want wrapped ErrLockTimeout", err)
	}

	// The guard must still be held despite Release "completing": a second
	// same-process acquire on an overlapping range must be rejected, not
	// incorrectly succeed.
	f2 := openTarget(t, s)
	if _, err := s.TryAcquireExclusive(f2, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("second acquire after simulated Release timeout: err=%v, want ErrLockBusy", err)
	}
}

// TestReleaseReportsAMissingRangeEntry pins that Release fails loudly when the
// in-process table has no entry matching the lease, instead of returning success.
//
// The mismatch is reachable through the exclusive flag: Release passes
// l.exclusive to ranges.release, and if that ever disagrees with what tryAcquire
// recorded, nothing matches. Before this, release silently did nothing and
// Release returned nil -- so the guard stayed in the table for the life of the
// Store and every later acquire over that range returned ErrLockBusy, which the
// verifier reports as CoverageContended and the soak treats as "skip and move
// on". The range stops being written and verified, and the run still says PASS.
//
// Corrupting the flag directly is the only way to reach it today, since both
// acquire paths set it consistently. That is the point: it pins the invariant
// rather than a current caller's mistake.
func TestReleaseReportsAMissingRangeEntry(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	lease, err := s.TryAcquireShared(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireShared: %v", err)
	}
	// Desynchronize the lease from the table entry tryAcquire actually recorded.
	lease.exclusive = true

	err = lease.Release()
	if err == nil {
		t.Fatal("Release with no matching table entry: got nil, want an error -- " +
			"the guard is leaked and the caller has no other way to learn it")
	}
	if !strings.Contains(err.Error(), "no in-process range entry") {
		t.Errorf("Release err = %v; want it to name the missing range entry", err)
	}
}

// TestZeroValueLeaseReleaseErrors pins that releasing a zero-value Lease reports
// rather than panicking. It is reachable whenever a caller ignores the acquire
// error and releases the returned value anyway: l.ranges is nil, and the nil
// receiver used to panic inside ranges.release. A panic in the lock-release path
// takes down the machinery that exists to report a stuck lock.
func TestZeroValueLeaseReleaseErrors(t *testing.T) {
	var lease Lease
	err := lease.Release()
	if err == nil {
		t.Fatal("zero-value Lease.Release: got nil, want an error")
	}
	if !strings.Contains(err.Error(), "zero-value Lease") {
		t.Errorf("err = %v; want it to say the Lease was never acquired", err)
	}
}

// TestWithTimeoutFastCallReturnsResult confirms the common case -- fn
// returns well before the timeout -- returns fn's real result normally.
func TestWithTimeoutFastCallReturnsResult(t *testing.T) {
	got, err := withTimeout(time.Second, "TEST-ONLY fast call", func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("withTimeout: %v", err)
	}
	if got != 42 {
		t.Errorf("withTimeout returned %d, want 42", got)
	}
}

// TestWithTimeoutSlowCallReturnsErrLockTimeout is a direct unit test of the
// timeout path itself, decoupled from any real syscall: withTimeout is
// generic over any fn, so a synthetic slow fn exercises exactly the same
// give-up behavior a live network/DLM stall would. fn's own goroutine keeps
// running after withTimeout gives up -- Go cannot interrupt it -- so this
// releases it during cleanup rather than leaving it running past the test.
func TestWithTimeoutSlowCallReturnsErrLockTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	_, err := withTimeout(10*time.Millisecond, "TEST-ONLY simulated stuck syscall", func() (struct{}, error) {
		<-release
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("withTimeout: err=%v, want errors.Is(_, ErrLockTimeout)", err)
	}
	if !strings.Contains(err.Error(), "TEST-ONLY simulated stuck syscall") {
		t.Errorf("withTimeout error %v does not name the call that timed out", err)
	}
}

// TestNonOverlappingLocksDoNotBlock verifies the range-lock table lets
// disjoint ranges proceed concurrently, instead of serialising on a single
// whole-file mutex.
func TestNonOverlappingLocksDoNotBlock(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireExclusive(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer lease1.Release()

	// Overlapping range must fail immediately with ErrLockBusy.
	if _, err := s.TryAcquireExclusive(f2, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("overlapping acquire: err=%v, want ErrLockBusy", err)
	}

	// Disjoint range must succeed even while lease1 is still held.
	lease2, err := s.TryAcquireExclusive(f2, 8192, 4096)
	if err != nil {
		t.Fatalf("disjoint acquire while lease1 held: %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("Release lease2: %v", err)
	}
}

// TestSharedLocksOverlapRefused is the inverse of a test that used to
// assert overlapping same-process shared locks were ALLOWED. That assertion
// was wrong, and the shape of the old test shows why it was easy to believe:
// it checked only the in-process table's bookkeeping, never the kernel lock
// state underneath, so it mistook an implementation detail for a safety
// property.
//
// A POSIX record lock is per-process, so overlapping F_RDLCKs from one process
// collapse into a single kernel lock state, and the first Release destroys it
// under the other holder. The old test made that concrete without noticing: it
// took [0,4096) and [1024,3072), then released the INNER one, punching a hole
// straight through the middle of the outer lease's coverage. A remote writer
// could then take F_WRLCK over those bytes while the outer holder was still
// reading them, and the resulting torn read is reported as BODY_CORRUPT --
// corruption announced on a healthy filesystem.
//
// See rangeLockTable.tryAcquire for why refcounting is not a sufficient fix
// and why excluding same-process overlap costs no concurrency that matters.
func TestSharedLocksOverlapRefused(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireShared(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first shared acquire: %v", err)
	}
	defer lease1.Release()

	// The old test's exact case: overlapping but not identical, releasing the
	// inner range.
	if _, err := s.TryAcquireShared(f2, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("overlapping shared acquire: err=%v, want ErrLockBusy -- "+
			"releasing it would hole the outer lease's kernel lock", err)
	}

	// The soak-realistic case, which the old test did not cover at all: two
	// workers picking the same block. All shared-pool workers share one Store
	// and one block size, so identical ranges are the common collision.
	if _, err := s.TryAcquireShared(f2, 0, 4096); !errors.Is(err, ErrLockBusy) {
		t.Errorf("identical-range shared acquire: err=%v, want ErrLockBusy", err)
	}
}

// TestSharedLocksDisjointAllowed guards against over-correcting the above
// into a whole-file lock: shared locks over ranges that do not overlap must
// still be granted concurrently, which is what makes soak's per-block workers
// useful. overlaps() treats adjacent ranges as disjoint, so block-aligned work
// never self-conflicts.
func TestSharedLocksDisjointAllowed(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireShared(f1, 0, 1024)
	if err != nil {
		t.Fatalf("first shared acquire: %v", err)
	}
	defer lease1.Release()

	// Adjacent: starts exactly where the first ends.
	lease2, err := s.TryAcquireShared(f2, 1024, 1024)
	if err != nil {
		t.Fatalf("adjacent shared acquire: %v, want success", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("Release lease2: %v", err)
	}

	// Well separated.
	lease3, err := s.TryAcquireShared(f2, 8192, 1024)
	if err != nil {
		t.Fatalf("disjoint shared acquire: %v, want success", err)
	}
	if err := lease3.Release(); err != nil {
		t.Fatalf("Release lease3: %v", err)
	}
}

// TestExclusiveConflictsWithShared verifies an exclusive request
// overlapping a held shared lock is rejected with ErrLockBusy.
func TestExclusiveConflictsWithShared(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireShared(f1, 0, 4096)
	if err != nil {
		t.Fatalf("shared acquire: %v", err)
	}
	defer lease1.Release()

	if _, err := s.TryAcquireExclusive(f2, 1024, 2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("exclusive overlapping shared: err=%v, want ErrLockBusy", err)
	}
}

// TestGoroutinesSharingOneStoreExcludeEachOther is the test for the scope that
// F_SETLK cannot cover at all, and that nothing in the tree pinned before
// 2026-08-17: two GOROUTINES, on one Store, contending for overlapping ranges.
//
// The other acquire tests reach the same table from one goroutine, which
// exercises its bookkeeping but not the thing it exists for. Here the second
// acquire runs on another goroutine, so `go test -race` also has something to
// say about the mutex guarding the table.
//
// Nothing below the table would catch this. Both goroutines share a pid, so
// their F_SETLK calls do not conflict -- the kernel merges them into one lock
// state for the (process, range), and the first Release destroys it under the
// other holder. F_OFD_SETLK would have separated them on a local filesystem,
// but not on BeeGFS, which keys record locks by (node, pid) and ignores the
// open file description; that is why the OFD mode was removed rather than kept
// for this case.
func TestGoroutinesSharingOneStoreExcludeEachOther(t *testing.T) {
	s := makeTempStore(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireExclusive(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first goroutine's acquire: %v", err)
	}

	// A second goroutine, overlapping range, same Store: must be refused.
	busy := make(chan error, 1)
	go func() {
		_, err := s.TryAcquireExclusive(f2, 2048, 4096)
		busy <- err
	}()
	if err := <-busy; !errors.Is(err, ErrLockBusy) {
		t.Fatalf("second goroutine while lease1 held: err=%v, want ErrLockBusy", err)
	}

	// Releasing frees the table entry, so the same range is acquirable again.
	if err := lease1.Release(); err != nil {
		t.Fatalf("Release lease1: %v", err)
	}
	acquired := make(chan error, 1)
	go func() {
		lease2, err := s.TryAcquireExclusive(f2, 2048, 4096)
		if err != nil {
			acquired <- err
			return
		}
		acquired <- lease2.Release()
	}()
	if err := <-acquired; err != nil {
		t.Errorf("second goroutine after release: %v, want success", err)
	}
}

// TestAcquireRollbackStuckReleaseSupersedesSetChanged is the regression test
// for the case a plain "don't swallow the error" fix would have missed.
//
// The acquire rollback paths release the lease and return a deliberately benign
// error -- ErrSetChanged, or a rescan failure -- because "the overlap set moved
// under me, pick another region" is normal under contention. But if that
// rollback release is the one that times out, it leaks the range guard, and
// returning bare ErrSetChanged hands the caller a permanently dead range
// wearing a benign label. soakWriteBufferedOp maps ErrSetChanged to nil, so the
// leak would be swallowed and the range would silently stop being exercised
// while the run still reported PASS.
//
// Combining both injection hooks reproduces it: hookAfterLock forces the
// set-changed rollback, hookReleaseUnlockErr forces that rollback's release to
// time out. The stuck condition must win.
func TestAcquireRollbackStuckReleaseSupersedesSetChanged(t *testing.T) {
	s := makeTempStore(t)
	f := openTarget(t, s)

	called := false
	hookAfterLock = func() {
		if called {
			return
		}
		called = true
		// Add an overlapping record so the post-lock rescan disagrees with the
		// initial scan, driving the ErrSetChanged rollback.
		if err := s.Put(4096, 1024, make([]byte, block.HeaderSize)); err != nil {
			t.Fatalf("Put during hook: %v", err)
		}
	}
	t.Cleanup(func() { hookAfterLock = nil })

	hookReleaseUnlockErr = func() error { return ErrLockTimeout }
	t.Cleanup(func() { hookReleaseUnlockErr = nil })

	_, err := s.TryAcquireExclusive(f, 0, 8192)
	if err == nil {
		t.Fatal("TryAcquireExclusive: got nil error, want a stuck-lock error")
	}
	if !called {
		t.Fatal("hookAfterLock was not invoked; the rollback path was not exercised")
	}
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("err = %v; want it to satisfy errors.Is(ErrLockTimeout)", err)
	}
	// The crux: callers treat ErrSetChanged as benign, so the stuck condition
	// must NOT be reachable through it.
	if errors.Is(err, ErrSetChanged) {
		t.Errorf("err = %v; must NOT satisfy errors.Is(ErrSetChanged) -- "+
			"callers map that to nil and would swallow a permanently leaked guard", err)
	}
}

// TestReleaseErrorOrCauseDecisionTable pins the decision table directly,
// since ReleaseErrorOrCause is the shared seam three packages depend on: any
// release error wins outright, uniformly, whether it's the ambiguous
// ErrLockTimeout case or an ordinary definite failure -- neither is ever
// superseded by cause. This is also the sole home of this table; a second
// copy previously lived in store_test.go and pinned the OLD (buggy) rule that
// let an ordinary release failure be discarded in favor of cause -- removed
// there, consolidated here, since ReleaseErrorOrCause lives in this package's
// R4 file set (errors.go), not R3's.
func TestReleaseErrorOrCauseDecisionTable(t *testing.T) {
	other := errors.New("some ordinary release failure")

	for _, tc := range []struct {
		name          string
		relErr, cause error
		wantIs        error
		wantNotIs     error
		wantExactNil  bool
	}{
		{name: "timeout beats ErrSetChanged", relErr: ErrLockTimeout, cause: ErrSetChanged,
			wantIs: ErrLockTimeout, wantNotIs: ErrSetChanged},
		{name: "timeout alone survives", relErr: ErrLockTimeout, wantIs: ErrLockTimeout},
		{name: "an ordinary release failure ALSO wins outright, not just timeout",
			relErr: other, cause: ErrSetChanged, wantIs: other, wantNotIs: ErrSetChanged},
		{name: "an ordinary release failure alone survives", relErr: other, wantIs: other},
		{name: "no release error passes the cause through", cause: ErrSetChanged, wantIs: ErrSetChanged},
		{name: "no release error, no cause", wantExactNil: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ReleaseErrorOrCause(tc.relErr, tc.cause)
			if tc.wantExactNil {
				if got != nil {
					t.Errorf("ReleaseErrorOrCause(nil, nil) = %v, want nil", got)
				}
				return
			}
			if tc.wantIs != nil && !errors.Is(got, tc.wantIs) {
				t.Errorf("ReleaseErrorOrCause(%v, %v) = %v; want errors.Is(%v)",
					tc.relErr, tc.cause, got, tc.wantIs)
			}
			if tc.wantNotIs != nil && errors.Is(got, tc.wantNotIs) {
				t.Errorf("ReleaseErrorOrCause(%v, %v) = %v; must NOT satisfy errors.Is(%v)",
					tc.relErr, tc.cause, got, tc.wantNotIs)
			}
		})
	}
}

// TestWriteBlockSurfacesStuckRelease pins that WriteBlock reports a release
// that leaked the guard instead of dropping it. Without this, the write itself
// succeeds, WriteBlock returns nil, and the caller has no way to learn that the
// offset it just wrote can never be written or verified again.
func TestWriteBlockSurfacesStuckRelease(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	hookReleaseUnlockErr = func() error { return ErrLockTimeout }
	t.Cleanup(func() { hookReleaseUnlockErr = nil })

	err = w.WriteBlock(0, fileops.IOTypeBuffered)
	if err == nil {
		t.Fatal("WriteBlock: got nil, want the stuck-release error surfaced")
	}
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("err = %v; want it to satisfy errors.Is(ErrLockTimeout)", err)
	}
}

// TestTruncateSurfacesStuckRelease is the counterpart to
// TestWriteBlockSurfacesStuckRelease above, and its absence is why a real defect
// shipped: Truncate declared the lock inside its conditional-locking block, so
// `end, err := w.scanTruncateBound()` there introduced a second err scoped to that
// block. The deferred release computed the correct SupersedeIfStuck result and
// assigned it to a variable that went out of scope, and Truncate returned nil
// for a release that had leaked the in-process range guard -- leaving that range
// permanently unacquirable while reporting success.
//
// The two lock-releasing entry points in this package must be covered
// identically; WriteBlock had this test and Truncate did not.
func TestTruncateSurfacesStuckRelease(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// A record to remove, so RemoveCovering has work and the lock range is real.
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	hookReleaseUnlockErr = func() error { return ErrLockTimeout }
	t.Cleanup(func() { hookReleaseUnlockErr = nil })

	// LockExclusive so there is a release to fail at all.
	err = w.Truncate(512)
	if err == nil {
		t.Fatal("Truncate: got nil, want the stuck-release error surfaced")
	}
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("err = %v; want it to satisfy errors.Is(ErrLockTimeout)", err)
	}
}

// TestTruncateUnlockedNeedsNoRelease guards the other half of the shape: with
// LockNone there is no lock, so the deferred release must be a harmless
// no-op rather than reporting a phantom failure. Every in-tree caller of
// Truncate uses LockNone, which is exactly why the bug above stayed latent.
func TestTruncateUnlockedNeedsNoRelease(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Armed, but must never fire: no lock was taken, so nothing releases.
	hookReleaseUnlockErr = func() error { return ErrLockTimeout }
	t.Cleanup(func() { hookReleaseUnlockErr = nil })

	if err := w.Truncate(512); err != nil {
		t.Errorf("Truncate under LockNone: %v, want nil -- there is no lock to release", err)
	}
}

// TestWriterTruncateLockExcludesConcurrentWriter pins that a locking Truncate
// cannot remove a record out from under a writer holding a lease on it.
//
// That is the failure the lock exists to prevent: RemoveCovering deleting a
// locked record makes the holder's post-write read-back find its xattr missing,
// so the holder fails on a file where nothing was actually wrong.
//
// It fails with a wrapped xattr.ErrNotFound from the read-back, NOT with
// ErrLockViolation as this comment used to say -- the Get fails before the byte
// comparison can run, so the violation path is never reached. Measured with the
// hookAfterDataWrite seam:
//
//	WriteBlock err = xattrstore.Writer.WriteBlock: read-back: xattr: attribute not found
//	errors.Is(err, ErrLockViolation) = false
//
// The assertion below is unaffected -- it is about Truncate being refused, not
// about what the holder would have seen -- but the mechanism was wrong, and this
// is the copy a reader trusts for what the lock buys.

// TestWriterTruncateRefusesGrowBeforeLocking pins the ORDER of the grow ban
// against the lock, which is the half that is not obvious from writer.go.
//
// The ban has to come first. Locking then refusing would leave the outcome
// dependent on whether anyone happens to hold a lease: an uncontended grow would
// still reach file.Truncate. So with a conflicting lease held over the region a
// grow moves through, the error must be ErrTruncateGrowUnsupported and NOT
// ErrLockBusy -- the request is rejected on its own terms, before contention is
// ever consulted.
//
// This replaces TestWriterTruncateGrowTakesTheLock, which pinned that a grow
// takes a lock. That test existed because the lock was gated on `end > size`, so
// a grow took none; the gate became min/max, and the case where the bound equals
// size still took none and still grew the file. Growing is withdrawn rather than
// fixed a second time (see ErrTruncateGrowUnsupported).
func TestWriterTruncateRefusesGrowBeforeLocking(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal,
		BlockSize: 1024, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range int64(2) {
		if err := w.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 2048 {
		t.Fatalf("setup: file size = %d, want 2048", size)
	}

	// A holder over the region a grow to 4096 moves through: [2048, 4096).
	holder := openTarget(t, s)
	lease, err := s.TryAcquireExclusive(holder, 2048, 2048)
	if err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	defer func() { _ = lease.Release() }()

	if err := w.Truncate(4096); !errors.Is(err, ErrTruncateGrowUnsupported) {
		t.Errorf("Truncate(grow) with a conflicting lease: err=%v, want ErrTruncateGrowUnsupported", err)
	} else if errors.Is(err, ErrLockBusy) {
		t.Errorf("Truncate(grow) reported contention: err=%v -- the ban must precede the lock, "+
			"or an uncontended grow still reaches file.Truncate", err)
	}
	if size, err := f.Size(); err != nil {
		t.Fatalf("Size: %v", err)
	} else if size != 2048 {
		t.Errorf("file size = %d, want 2048 -- a refused grow must not extend the file", size)
	}
}

func TestWriterTruncateLockExcludesConcurrentWriter(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range int64(4) {
		if err := w.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}

	// Stand in for a worker mid-write on block 2, which Truncate(2048) would
	// otherwise remove.
	holder := openTarget(t, s)
	lease, err := s.TryAcquireExclusive(holder, 2048, 1024)
	if err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	if err := w.Truncate(2048); !errors.Is(err, ErrLockBusy) {
		t.Errorf("Truncate with a conflicting lease: err=%v, want ErrLockBusy", err)
	}
	// Nothing may have been removed or shortened.
	n := 0
	if err := s.ForEachEntry(func(_, _ int64, _ []byte) error { n++; return nil }); err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	if n != 4 {
		t.Errorf("records = %d, want 4 -- a refused Truncate must not have removed any", n)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 4096 {
		t.Errorf("file size = %d, want 4096 -- a refused Truncate must not shorten", size)
	}

	// Once the writer is done, the same Truncate succeeds.
	if err := lease.Release(); err != nil {
		t.Fatalf("holder release: %v", err)
	}
	if err := w.Truncate(2048); err != nil {
		t.Fatalf("Truncate after release: %v", err)
	}
}

// TestWriterTruncateLockCoversRecordsPastEOF pins the upper bound. A record can
// claim bytes past EOF -- a data-only truncate leaves exactly that -- and it is
// removable, so the locked region must reach the furthest extent any record
// claims, not merely the current end of file.
func TestWriterTruncateLockCoversRecordsPastEOF(t *testing.T) {
	s := makeTempStore(t)
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	// A stale record well beyond EOF; the file is only 1024 bytes.
	if err := s.Put(8192, 1024, makeHeader(t, 8192, 1)); err != nil {
		t.Fatalf("Put past EOF: %v", err)
	}

	// A writer holding that far record must block the truncate: bounding the
	// lock at EOF (1024) would leave [8192,9216) unprotected while
	// RemoveCovering still deletes it.
	holder := openTarget(t, s)
	lease, err := s.TryAcquireExclusive(holder, 8192, 1024)
	if err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	defer func() { _ = lease.Release() }()

	if err := w.Truncate(512); !errors.Is(err, ErrLockBusy) {
		t.Errorf("Truncate with a lease on a past-EOF record: err=%v, want ErrLockBusy", err)
	}
}
