// The in-process range-lock table. Split out from store.go so the locking
// mechanism and the record store are separable: everything here belongs to the
// former, everything left in store.go to the latter. Deliberately untagged --
// Store embeds a rangeLockTable, and store.go builds on every GOOS.
//
// The interval helper this uses, overlaps(), stays in store.go: Overlapping()
// needs it too, so it belongs to neither half exclusively.

package xattrstore

import "sync"

// rangeLock describes one in-process lock held over a byte range of the
// target file.
type rangeLock struct {
	offset    int64
	length    int64
	exclusive bool
}

// rangeLockTable tracks in-process locks held over byte ranges of a
// Store's target file. It exists because POSIX record locks (F_SETLK) do not
// conflict with other locks held by the same process -- without this table,
// two goroutines on the same node could both successfully F_SETLK overlapping
// ranges. The table supplies exactly the scope F_SETLK does not: goroutines
// within one process. F_SETLK covers the other two, separate processes on one
// node and separate nodes (via the filesystem's distributed lock manager).
//
// The table is consulted only to decide whether an in-process conflict
// exists; it never blocks. tryAcquire returns false immediately on
// conflict, matching the non-blocking contract of TryAcquireShared and
// TryAcquireExclusive. The mutex is held only for the brief scan/insert,
// never across a syscall.
type rangeLockTable struct {
	mu    sync.Mutex
	locks []rangeLock
}

// tryAcquire registers a lock over [offset, offset+length) and returns
// true, unless it overlaps an existing entry.
//
// ANY overlap conflicts, including shared-against-shared. That is stricter
// than a reader-writer lock, deliberately, because the thing this table
// guards is not one: a POSIX record lock is per-PROCESS, so two goroutines
// that F_SETLK F_RDLCK overlapping ranges do not get two locks -- the kernel
// holds a single lock state for the (process, range). Whichever goroutine
// releases first issues an unconditional, range-scoped F_SETLK F_UNLCK and
// destroys that state while the other still holds a live Lease and believes it
// is protected. A remote writer can then take F_WRLCK and mutate the bytes
// mid-read, so the reader reports BODY_CORRUPT or BODY_CRC_MISMATCH: the tool
// announcing data corruption that never happened.
//
// Refcounting the shared holders would not be enough either, because holders
// need not share a range: releasing [1024,3072) punches a hole through the
// middle of a concurrently-held [0,4096).
//
// This costs no concurrency that matters. Shared locks exist so multiple NODES
// can read one range at once, and that is untouched -- cross-node F_RDLCK
// behaves exactly as before. Only two goroutines inside one process are
// excluded, where the kernel lock is indivisible anyway, and where both would
// be reading identical bytes to no additional effect. The loser gets
// ErrLockBusy, which callers already treat as "skip and move on" (soak) or
// report as CoverageContended (the verifier).
//
// exclusive is still recorded, because release matches on it and because the
// kernel-level distinction (F_RDLCK vs F_WRLCK) still governs cross-node
// behaviour. It just no longer affects the in-process conflict decision.
func (t *rangeLockTable) tryAcquire(offset, length int64, exclusive bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range t.locks {
		if overlaps(offset, length, l.offset, l.length) {
			return false
		}
	}
	t.locks = append(t.locks, rangeLock{offset: offset, length: length, exclusive: exclusive})
	return true
}

// release removes one entry matching (offset, length, exclusive) and reports
// whether it found one.
//
// A false return is a bug in this package, not a state to absorb: the caller
// registered the entry itself and no other path removes it. Silently returning
// had a specific cost -- a Lease whose exclusive flag disagreed with what
// tryAcquire recorded would fail to match, leave the guard in the table for the
// life of the Store, and still report a successful Release. Every later acquire
// over that range then returns ErrLockBusy, which callers correctly treat as
// benign contention, so the range quietly stops being written and verified while
// the run still reports PASS.
func (t *rangeLockTable) release(offset, length int64, exclusive bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, l := range t.locks {
		if l.offset == offset && l.length == length && l.exclusive == exclusive {
			t.locks = append(t.locks[:i], t.locks[i+1:]...)
			return true
		}
	}
	return false
}
