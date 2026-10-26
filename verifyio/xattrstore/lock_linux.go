// Linux-only. Uses F_SETLK.
//go:build linux

package xattrstore

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// A Lease represents a write lock held over a contiguous byte range of the
// data file. The locked range covers [offset, offset+length) of the
// TryAcquireExclusive call AND every record-extent that overlapped that
// range at acquire time (the "union extent"). It is reported by Range.
//
// The lock is a POSIX F_SETLK on the data file plus an entry in the Store's
// in-process range table.
//
// Both TryAcquireExclusive and TryAcquireShared assume the "one Store per
// file per process" precondition documented on Store's "# Locking
// precondition" section in store.go: the in-process range table is a Store
// field. Two Stores on one file in one process cannot see each other's
// locks, and F_SETLK cannot separate them either, since they share a pid.
// See store.go for the full reasoning.
//
// Lease values must not be copied after creation; the embedded released
// flag would be tracked independently in each copy and could lead to
// double-Release. Pass by pointer or call Release before returning the
// value from a function.
//
// Release must be called exactly once. Subsequent calls return an error.
type Lease struct {
	file        *os.File
	start       int64
	length      int64
	released    bool
	ranges      *rangeLockTable // Release frees the [start,start+length) entry after the POSIX unlock
	exclusive   bool            // which kind of entry was registered in ranges
	lockTimeout time.Duration   // bounds the F_SETLK F_UNLCK call in Release
}

// Range returns the byte range the lease holds locked. Useful for
// logging or for callers that want to assert their write extent is
// inside the lock.
func (l Lease) Range() (offset, length int64) {
	return l.start, l.length
}

// hookReleaseUnlockErr is a test-only injection point. When non-nil, its
// return value replaces the real unlock call's outcome inside Release, letting
// tests deterministically simulate a failure there -- a condition this suite
// cannot trigger for real, since the underlying fcntl call is local and fast
// (see withTimeout's doc comment).
var hookReleaseUnlockErr func() error

// Release drops all locks held by this lease. The POSIX lock is released
// before the in-process range-lock table entry is freed. The ordering is
// required: freeing the table entry first would
// allow another goroutine to issue a new F_SETLK while this goroutine's
// F_UNLCK is still in flight. Because POSIX locks are per-process, the
// kernel records the new goroutine's lock atomically with the existing
// one — our subsequent F_UNLCK would then remove the other goroutine's
// lock, not our own.
//
// If the F_UNLCK call itself times out (ErrLockTimeout; see withTimeout),
// the table entry is deliberately NOT freed, for the same reason the
// acquire paths withhold it on a timeout (see guardSafeToRelease):
// the syscall's goroutine keeps running with an unknown eventual outcome,
// and freeing the guard now would let another same-process goroutine
// acquire this range immediately, only for the real, delayed unlock to
// land later and silently drop that goroutine's lock out from under it.
// The entry is freed on any other outcome, including a definite failure.
//
// Returns an error if the lease was already released or if the unlock
// call fails or times out.
func (l *Lease) Release() error {
	if l.ranges == nil {
		// A zero-value Lease. Reachable when a caller ignores the error from
		// TryAcquireExclusive/TryAcquireShared and releases the returned value
		// anyway. Diagnosed here rather than left to panic in l.ranges.release --
		// the machinery that reports a stuck lock must not be what dies.
		return errors.New("xattrstore.Lease.Release: zero-value Lease, never returned by " +
			"TryAcquireExclusive or TryAcquireShared (check the acquire error first)")
	}
	if l.released {
		return errors.New("xattrstore.Lease.Release: already released")
	}
	l.released = true

	desc := fmt.Sprintf("F_SETLK F_UNLCK [%d,%d)", l.start, l.start+l.length)
	_, err := withTimeout(l.lockTimeout, desc, func() (struct{}, error) {
		return struct{}{}, unix.FcntlFlock(l.file.Fd(), unix.F_SETLK, &unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: int16(unix.SEEK_SET),
			Start:  l.start,
			Len:    l.length,
		})
	})
	if hookReleaseUnlockErr != nil {
		if hookErr := hookReleaseUnlockErr(); hookErr != nil {
			err = hookErr
		}
	}
	if err != nil && !guardSafeToRelease(err) {
		// Ambiguous: don't know whether the kernel has actually released
		// the lock yet. Leak the guard for the life of the Store rather
		// than risk a same-process double-lock collision (see doc comment
		// above and guardSafeToRelease).
		return fmt.Errorf("xattrstore.Lease.Release: %w", err)
	}
	if !l.ranges.release(l.start, l.length, l.exclusive) {
		// The POSIX lock is gone but the in-process guard is not, and never will
		// be: nothing else removes entries. This range is dead for the life of the
		// Store, so say so instead of returning success (see rangeLockTable.release).
		missing := fmt.Errorf("xattrstore.Lease.Release: no in-process range entry for [%d,%d) "+
			"(exclusive=%v); the guard is leaked and this range is now unacquirable",
			l.start, l.start+l.length, l.exclusive)
		return ReleaseErrorOrCause(missing, err)
	}
	if err != nil {
		return fmt.Errorf("xattrstore.Lease.Release: %w", err)
	}
	return nil
}

// hookAfterLock is a test-only injection point. When non-nil, it is
// invoked after the lock is acquired and before the rescan. Tests use
// this to insert xattr changes that the rescan must detect.
var hookAfterLock func()

// withTimeout runs fn in its own goroutine and returns its result, unless
// timeout elapses first, in which case it returns ErrLockTimeout wrapping
// desc -- a description of the call that timed out (e.g. "F_SETLK F_WRLCK
// [4096,8192)"), so callers can tell exactly which syscall got stuck.
//
// If timeout is <= 0, fn is called directly with no timeout.
//
// Go cannot interrupt a blocked syscall: if fn does not return before the
// timeout, its goroutine keeps running until fn actually returns, and its
// result is discarded. Callers rely on this only as a last-resort
// diagnostic -- ErrLockTimeout means the calling goroutine has given up, not
// that the underlying syscall has.
func withTimeout[T any](timeout time.Duration, desc string, fn func() (T, error)) (T, error) {
	if timeout <= 0 {
		return fn()
	}
	type result struct {
		val T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := fn()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.val, r.err
	case <-time.After(timeout):
		var zero T
		return zero, fmt.Errorf("%s: %w (after %s)", desc, ErrLockTimeout, timeout)
	}
}

// unionExtent returns the smallest contiguous range that covers
// [offset, offset+length) and every Entry in overlaps. Because every
// Entry in overlaps already intersects the requested range (that is
// the contract of Overlapping), the union is always contiguous.
func unionExtent(offset, length int64, overlaps []Entry) (start, totalLen int64) {
	start = offset
	end := offset + length
	for _, e := range overlaps {
		if e.Offset < start {
			start = e.Offset
		}
		if e.Offset+e.Length > end {
			end = e.Offset + e.Length
		}
	}
	return start, end - start
}

// sameOverlapSet reports whether a and b contain the same set of
// records, identified by xattr Name (which uniquely encodes (offset,
// length)). Order does not matter.
func sameOverlapSet(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, e := range a {
		set[e.Name] = struct{}{}
	}
	for _, e := range b {
		if _, ok := set[e.Name]; !ok {
			return false
		}
	}
	return true
}

// TryAcquireShared takes a non-blocking shared (read) lock over
// [offset, offset+length): a POSIX F_RDLCK combined with an entry in the
// Store's in-process range-lock table.
//
// Against WRITERS this is a reader-writer lock at every scope. An exclusive
// request over an overlapping range is rejected while this lease is held --
// from another goroutine on this Store by the table, from another process or
// another node by F_RDLCK.
//
// Against other READERS it is stricter than a reader-writer lock, but only
// within this process: the table conflicts on any overlap, so a second
// goroutine asking for an overlapping shared lock gets ErrLockBusy rather than
// a second lease. That is deliberate and load-bearing -- see rangeLockTable.
// Overlapping shared locks from other processes and other nodes are unaffected
// and still coexist, which is what shared locks are for.
//
// Returns ErrLockBusy on either conflict.
func (s *Store) TryAcquireShared(f *os.File, offset, length int64) (Lease, error) {
	if f == nil {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: nil file")
	}
	if length <= 0 {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: length must be > 0 (got %d)", length)
	}
	if !s.ranges.tryAcquire(offset, length, false) {
		return Lease{}, ErrLockBusy
	}
	desc := fmt.Sprintf("F_SETLK F_RDLCK [%d,%d)", offset, offset+length)
	_, err := withTimeout(s.lockTimeout, desc, func() (struct{}, error) {
		return struct{}{}, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
			Type:   unix.F_RDLCK,
			Whence: int16(unix.SEEK_SET),
			Start:  offset,
			Len:    length,
		})
	})
	if err != nil {
		if !guardSafeToRelease(err) {
			// Same hazard TryAcquireExclusive's F_SETLK step guards against:
			// the syscall itself didn't return before lockTimeout, so
			// withTimeout's goroutine keeps running and we don't know whether
			// the kernel eventually grants the lock. Do NOT free the
			// in-process guard -- if the lock lands after we've given up,
			// another same-process goroutine must not be able to wrongly
			// believe this range is free and successfully re-acquire it.
			return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: %w", err)
		}
		// Return value ignored deliberately: tryAcquire registered exactly this
		// (offset, length, false) a few lines above and nothing removes an entry
		// but this call, so a miss is unreachable here.
		s.ranges.release(offset, length, false)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return Lease{}, ErrLockBusy
		}
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: F_SETLK: %w", err)
	}
	return Lease{file: f, start: offset, length: length, ranges: &s.ranges, exclusive: false, lockTimeout: s.lockTimeout}, nil
}

// guardSafeToRelease reports whether it's safe to free the in-process
// range-table guard after a failed F_SETLK or rescan syscall. It is NOT safe
// when the syscall may have timed out rather than definitively failed:
// withTimeout gives up waiting, but the syscall's goroutine keeps running and
// its eventual outcome (did the kernel grant the lock or not?) is unknown.
// Freeing the guard in that case would let another same-process goroutine
// wrongly believe an overlapping range is free -- same-process F_SETLK calls
// never conflict with each other, which is the entire reason the guard exists.
func guardSafeToRelease(err error) bool {
	return !errors.Is(err, ErrLockTimeout)
}

// TryAcquireExclusive takes a non-blocking exclusive lock over the union
// of [offset, offset+length) and every existing record-extent that
// overlaps that range:
//
//  1. Scan xattrs; compute union extent.
//  2. Register the union extent in the Store's in-process range table, which
//     supplies the goroutine-level exclusivity F_SETLK cannot (POSIX record
//     locks are per-process). Returns ErrLockBusy on conflict.
//  3. F_SETLK F_WRLCK (non-blocking), which the filesystem propagates to its
//     distributed lock manager, covering other processes on this node and
//     other nodes. Returns ErrLockBusy on conflict, freeing the table entry
//     first.
//  4. Rescan. Returns ErrSetChanged if the overlap set changed, releasing
//     both the POSIX lock and the table entry.
//
// f must be an *os.File open on the same file the Store targets with at
// least read access. The caller retains ownership of f.
func (s *Store) TryAcquireExclusive(f *os.File, offset, length int64) (Lease, error) {
	if f == nil {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: nil file")
	}
	if length <= 0 {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: length must be > 0 (got %d)", length)
	}

	initial, err := withTimeout(s.lockTimeout, fmt.Sprintf("initial xattr scan [%d,%d)", offset, offset+length),
		func() ([]Entry, error) { return s.Overlapping(offset, length) })
	if err != nil {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: %w", err)
	}
	lockOff, lockLen := unionExtent(offset, length, initial)

	if !s.ranges.tryAcquire(lockOff, lockLen, true) {
		return Lease{}, ErrLockBusy
	}

	desc := fmt.Sprintf("F_SETLK F_WRLCK [%d,%d)", lockOff, lockOff+lockLen)
	_, err = withTimeout(s.lockTimeout, desc, func() (struct{}, error) {
		return struct{}{}, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: int16(unix.SEEK_SET),
			Start:  lockOff,
			Len:    lockLen,
		})
	})
	if err != nil {
		if !guardSafeToRelease(err) {
			// The F_SETLK syscall itself didn't return before lockTimeout --
			// which can mean a slow round-trip to BeeGFS's distributed lock
			// manager, not a local blocking call, and withTimeout's
			// goroutine keeps running in the background. We
			// don't know whether the kernel eventually grants the lock. Do
			// NOT free the in-process guard: if the lock lands after we've
			// given up, another same-process goroutine must not be able to
			// wrongly believe this range is free and successfully
			// re-acquire it (same-process F_SETLK calls never conflict with
			// each other -- see rangeLockTable's doc comment). The range is
			// leaked in-process for the life of the Store rather than risk
			// that collision.
			return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: %w", err)
		}
		// A definite, immediate failure -- the kernel did not grant the
		// lock, so freeing the guard is safe.
		// Return value ignored deliberately, same reason as the shared path above:
		// this exact entry was registered a few lines up and only release removes one.
		s.ranges.release(lockOff, lockLen, true)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return Lease{}, ErrLockBusy
		}
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: F_SETLK: %w", err)
	}

	lease := Lease{file: f, start: lockOff, length: lockLen, ranges: &s.ranges, exclusive: true, lockTimeout: s.lockTimeout}

	if hookAfterLock != nil {
		hookAfterLock()
	}

	recheck, err := withTimeout(s.lockTimeout, fmt.Sprintf("rescan [%d,%d)", offset, offset+length),
		func() ([]Entry, error) { return s.Overlapping(offset, length) })
	if err != nil {
		if !guardSafeToRelease(err) {
			// The Overlapping call (a plain listxattr, unrelated to the lock
			// itself) is stuck -- but the F_SETLK above already succeeded,
			// so the real POSIX lock is genuinely held right now, with no
			// ambiguity. Releasing the lease would issue another syscall
			// (F_SETLK F_UNLCK) that may also hang, and freeing the
			// in-process guard without that unlock would let another
			// same-process goroutine wrongly believe this range is free
			// (same-process F_SETLK calls never conflict with each other).
			// Deliberately leak both the real lock and the guard for the
			// life of the process rather than reopen that collision.
			return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: %w", err)
		}
		return Lease{}, ReleaseErrorOrCause(lease.Release(),
			fmt.Errorf("xattrstore.TryAcquireExclusive: rescan: %w", err))
	}
	if !sameOverlapSet(initial, recheck) {
		return Lease{}, ReleaseErrorOrCause(lease.Release(), ErrSetChanged)
	}
	return lease, nil
}
