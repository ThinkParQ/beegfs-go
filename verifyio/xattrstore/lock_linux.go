// Linux-only: OFD locks (F_OFD_SETLK) require Linux 3.15+.
//go:build linux

package xattrstore

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Lease represents a write lock held over a contiguous byte range of the
// data file. The locked range covers [offset, offset+length) of the
// TryAcquireExclusive call AND every record-extent that overlapped that
// range at acquire time (the "union extent"). It is reported by Range.
//
// The underlying mechanism depends on the Store's LockMode:
//   - LockModeOFD: an OFD F_WRLCK on the data file
//   - LockModePOSIX: a POSIX F_SETLK on the data file plus the Store's
//     in-process mutex
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
	ranges      *rangeLockTable // non-nil in LockModePOSIX; Release frees the [start,start+length) entry after POSIX unlock
	exclusive   bool            // LockModePOSIX only; which kind of entry was registered in ranges
	lockTimeout time.Duration   // LockModePOSIX only; bounds the F_SETLK F_UNLCK call in Release
}

// Range returns the byte range the lease holds locked. Useful for
// logging or for callers that want to assert their write extent is
// inside the lock.
func (l Lease) Range() (offset, length int64) {
	return l.start, l.length
}

// Release drops all locks held by this lease. In LockModePOSIX the
// POSIX lock is released before the in-process range-lock table entry is
// freed. The ordering is required: freeing the table entry first would
// allow another goroutine to issue a new F_SETLK while this goroutine's
// F_UNLCK is still in flight. Because POSIX locks are per-process, the
// kernel records the new goroutine's lock atomically with the existing
// one — our subsequent F_UNLCK would then remove the other goroutine's
// lock, not our own.
// Returns an error if the lease was already released or if an unlock
// call fails (the table entry is always freed even on error).
func (l *Lease) Release() error {
	if l.released {
		return errors.New("xattrstore.Lease.Release: already released")
	}
	l.released = true

	if l.ranges != nil {
		// LockModePOSIX: release the POSIX lock, then free the range-table entry.
		desc := fmt.Sprintf("F_SETLK F_UNLCK [%d,%d)", l.start, l.start+l.length)
		_, err := withTimeout(l.lockTimeout, desc, func() (struct{}, error) {
			return struct{}{}, unix.FcntlFlock(l.file.Fd(), unix.F_SETLK, &unix.Flock_t{
				Type:   unix.F_UNLCK,
				Whence: int16(unix.SEEK_SET),
				Start:  l.start,
				Len:    l.length,
			})
		})
		l.ranges.release(l.start, l.length, l.exclusive)
		if err != nil {
			return fmt.Errorf("xattrstore.Lease.Release: %w", err)
		}
		return nil
	}

	// LockModeOFD: release the OFD lock.
	if err := unix.FcntlFlock(l.file.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{
		Type:   unix.F_UNLCK,
		Whence: int16(unix.SEEK_SET),
		Start:  l.start,
		Len:    l.length,
	}); err != nil {
		return fmt.Errorf("xattrstore.Lease.Release: F_OFD_SETLK F_UNLCK: %w", err)
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
// result is discarded. Callers in LockModePOSIX rely on this only as a
// last-resort diagnostic -- ErrLockTimeout means the calling goroutine has
// given up, not that the underlying syscall has.
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
// [offset, offset+length).
//
// In LockModeOFD, an OFD F_RDLCK is used; multiple callers with
// distinct open file descriptions may hold overlapping shared locks
// simultaneously.
//
// In LockModePOSIX, a POSIX F_RDLCK combined with the Store's in-process
// range-lock table provides cross-node and intra-process exclusivity
// against concurrent writers. Readers on this node are serialised against
// local writers -- an exclusive request over an overlapping range is
// rejected while any shared lock on it is held -- but not against each
// other: multiple overlapping shared locks may be held simultaneously,
// matching ordinary POSIX read-lock semantics.
//
// Returns ErrLockBusy if an exclusive holder conflicts with the range.
func (s *Store) TryAcquireShared(f *os.File, offset, length int64) (Lease, error) {
	if f == nil {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: nil file")
	}
	if length <= 0 {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: length must be > 0 (got %d)", length)
	}
	if s.lockMode == LockModePOSIX {
		return s.tryAcquireSharedPOSIX(f, offset, length)
	}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{
		Type:   unix.F_RDLCK,
		Whence: int16(unix.SEEK_SET),
		Start:  offset,
		Len:    length,
	}); err != nil {
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return Lease{}, ErrLockBusy
		}
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: F_OFD_SETLK: %w", err)
	}
	return Lease{file: f, start: offset, length: length}, nil
}

func (s *Store) tryAcquireSharedPOSIX(f *os.File, offset, length int64) (Lease, error) {
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
		s.ranges.release(offset, length, false)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return Lease{}, ErrLockBusy
		}
		if errors.Is(err, ErrLockTimeout) {
			return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: %w", err)
		}
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireShared: F_SETLK: %w", err)
	}
	return Lease{file: f, start: offset, length: length, ranges: &s.ranges, exclusive: false, lockTimeout: s.lockTimeout}, nil
}

// TryAcquireExclusive takes a non-blocking exclusive lock over the union
// of [offset, offset+length) and every existing record-extent that
// overlaps that range. The locking mechanism depends on the Store's
// LockMode:
//
// LockModeOFD (single-node):
//  1. Scan xattrs; compute union extent.
//  2. F_OFD_SETLK F_WRLCK (non-blocking). Returns ErrLockBusy on conflict.
//  3. Rescan. Returns ErrSetChanged if the overlap set changed.
//
// LockModePOSIX (multi-node):
//  1. Lock the Store's in-process mutex (serialises goroutines on this node).
//  2. Scan xattrs; compute union extent.
//  3. F_SETLK F_WRLCK (non-blocking). Returns ErrLockBusy on conflict,
//     unlocking the mutex first.
//  4. Rescan. Returns ErrSetChanged if the overlap set changed, releasing
//     both the POSIX lock and the mutex.
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

	if s.lockMode == LockModePOSIX {
		return s.tryAcquireExclusivePOSIX(f, offset, length)
	}
	return s.tryAcquireExclusiveOFD(f, offset, length)
}

func (s *Store) tryAcquireExclusiveOFD(f *os.File, offset, length int64) (Lease, error) {
	initial, err := s.Overlapping(offset, length)
	if err != nil {
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: initial scan: %w", err)
	}
	lockOff, lockLen := unionExtent(offset, length, initial)

	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: int16(unix.SEEK_SET),
		Start:  lockOff,
		Len:    lockLen,
	}); err != nil {
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return Lease{}, ErrLockBusy
		}
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: F_OFD_SETLK: %w", err)
	}

	lease := Lease{file: f, start: lockOff, length: lockLen}

	if hookAfterLock != nil {
		hookAfterLock()
	}

	recheck, err := s.Overlapping(offset, length)
	if err != nil {
		_ = lease.Release()
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: rescan: %w", err)
	}
	if !sameOverlapSet(initial, recheck) {
		_ = lease.Release()
		return Lease{}, ErrSetChanged
	}
	return lease, nil
}

// posixGuardSafeToRelease reports whether it's safe to free the in-process
// range-table guard after a failed F_SETLK or rescan syscall in
// tryAcquireExclusivePOSIX. It is NOT safe when the syscall may have timed
// out rather than definitively failed: withTimeout gives up waiting, but
// the syscall's goroutine keeps running and its eventual outcome (did the
// kernel grant the lock or not?) is unknown. Freeing the guard in that case
// would let another same-process goroutine wrongly believe an overlapping
// range is free -- same-process F_SETLK calls never conflict with each
// other, which is the entire reason the guard exists.
func posixGuardSafeToRelease(err error) bool {
	return !errors.Is(err, ErrLockTimeout)
}

func (s *Store) tryAcquireExclusivePOSIX(f *os.File, offset, length int64) (Lease, error) {
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
		if !posixGuardSafeToRelease(err) {
			// The F_SETLK syscall itself didn't return before lockTimeout --
			// on LockModePOSIX this can mean a slow round-trip to BeeGFS's
			// distributed lock manager, not a local blocking call, and
			// withTimeout's goroutine keeps running in the background. We
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
		if !posixGuardSafeToRelease(err) {
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
		_ = lease.Release()
		return Lease{}, fmt.Errorf("xattrstore.TryAcquireExclusive: rescan: %w", err)
	}
	if !sameOverlapSet(initial, recheck) {
		_ = lease.Release()
		return Lease{}, ErrSetChanged
	}
	return lease, nil
}
