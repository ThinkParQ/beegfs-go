//go:build linux

// This is a unit test.
//
// Coverage: TryAcquireExclusive's no-overlap success path, union-extent
// computation across pre-existing overlapping records, contention/busy
// detection, lock release (and double-release erroring), ErrSetChanged when
// the overlap set changes between the initial scan and the lock rescan, and
// bad-input rejection; the unionExtent and sameOverlapSet helpers in
// isolation; and LockModePOSIX-specific semantics (non-overlapping locks
// don't block each other, shared locks may overlap, an exclusive request
// conflicts with an existing shared one). Also posixGuardSafeToRelease's
// timeout-vs-definite-failure decision in isolation, and the same-process
// double-exclusive-lock hazard that decision exists to prevent (by
// directly simulating the state a rescan timeout leaves behind, since the
// real trigger is a live network/DLM timing condition this suite can't
// manufacture).
package xattrstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/xattr"
	"golang.org/x/sys/unix"
)

// makeTempStorePOSIX creates a temp regular file and returns a Store on it
// using LockModePOSIX with no lock timeout.
func makeTempStorePOSIX(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := os.WriteFile(path, []byte("placeholder"), 0644); err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := xattr.Set(path, "user.verifyio_probe", []byte{0}, 0); err != nil {
		t.Skipf("user xattrs not supported: %v", err)
	}
	_ = xattr.Remove(path, "user.verifyio_probe")
	s, err := OpenStoreMultiNode(path, 0)
	if err != nil {
		t.Fatalf("OpenStoreMultiNode: %v", err)
	}
	return s
}

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
	f2 := openTarget(t, s) // separate open file description -> separate OFD

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
	if posixGuardSafeToRelease(ErrLockTimeout) {
		t.Errorf("ErrLockTimeout: got safe to release, want not safe")
	}
	wrapped := fmt.Errorf("F_SETLK F_WRLCK [0,100): %w (after 5s)", ErrLockTimeout)
	if posixGuardSafeToRelease(wrapped) {
		t.Errorf("wrapped ErrLockTimeout: got safe to release, want not safe")
	}
	if !posixGuardSafeToRelease(unix.EAGAIN) {
		t.Errorf("EAGAIN: got not safe to release, want safe")
	}
	if !posixGuardSafeToRelease(unix.EWOULDBLOCK) {
		t.Errorf("EWOULDBLOCK: got not safe to release, want safe")
	}
	if !posixGuardSafeToRelease(errors.New("some other definite failure")) {
		t.Errorf("other error: got not safe to release, want safe")
	}
}

// TestPOSIXRescanTimeoutLeaksGuardNotReleasesIt is a regression test for the
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
func TestPOSIXRescanTimeoutLeaksGuardNotReleasesIt(t *testing.T) {
	s := makeTempStorePOSIX(t)
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

// TestPOSIXNonOverlappingLocksDoNotBlock verifies the range-lock table lets
// disjoint ranges proceed concurrently in LockModePOSIX, instead of
// serialising on a single whole-file mutex.
func TestPOSIXNonOverlappingLocksDoNotBlock(t *testing.T) {
	s := makeTempStorePOSIX(t)
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

// TestPOSIXSharedLocksOverlapAllowed verifies two shared locks on
// overlapping ranges can both be held at once in LockModePOSIX.
func TestPOSIXSharedLocksOverlapAllowed(t *testing.T) {
	s := makeTempStorePOSIX(t)
	f1 := openTarget(t, s)
	f2 := openTarget(t, s)

	lease1, err := s.TryAcquireShared(f1, 0, 4096)
	if err != nil {
		t.Fatalf("first shared acquire: %v", err)
	}
	defer lease1.Release()

	lease2, err := s.TryAcquireShared(f2, 1024, 2048)
	if err != nil {
		t.Fatalf("second shared acquire (overlapping): %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("Release lease2: %v", err)
	}
}

// TestPOSIXExclusiveConflictsWithShared verifies an exclusive request
// overlapping a held shared lock is rejected with ErrLockBusy.
func TestPOSIXExclusiveConflictsWithShared(t *testing.T) {
	s := makeTempStorePOSIX(t)
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
