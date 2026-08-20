package xattrstore

import "errors"

// ErrLockBusy is returned by TryAcquireExclusive when another lock
// holder conflicts with the requested range — either an OFD holder on
// this node or a POSIX holder on another node. Callers should treat
// this as "pick a different file or region" rather than retrying
// immediately.
var ErrLockBusy = errors.New("xattrstore: range lock busy")

// ErrSetChanged is returned by TryAcquireExclusive when the set of
// records overlapping the requested range changed between the initial
// scan and the post-lock re-scan. The lock has already been released
// before this error is returned. Callers should treat this as "pick a
// different file or region" rather than retrying immediately.
var ErrSetChanged = errors.New("xattrstore: overlap set changed during lock acquisition")

// ErrLockViolation is returned by Writer.WriteBlock when a post-write
// xattr read-back does not match what was just written. This indicates
// that another writer modified the range while the locks were held —
// i.e. the filesystem's distributed locking did not provide the
// expected exclusivity. This is always a hard test failure.
var ErrLockViolation = errors.New("xattrstore: lock violation — xattr overwritten while lock was held")

// ErrLockTimeout is returned by TryAcquireExclusive and TryAcquireShared in
// LockModePOSIX when an underlying syscall (F_SETLK, or one of the xattr
// scans used to compute the lock range) does not return within the Store's
// lock timeout.
//
// F_SETLK is documented as non-blocking, but BeeGFS's distributed lock
// manager can cause it — and xattr operations on the same file — to block
// indefinitely if the metadata server is unresponsive or deadlocked.
// ErrLockTimeout is the "give up and report" signal for that condition.
//
// Go cannot interrupt a blocked syscall: the goroutine that issued the call
// keeps running until the kernel returns, which may be never. After
// ErrLockTimeout the Store's in-process mutex has already been released by
// the caller, but the underlying POSIX lock state is no longer reliable —
// callers should treat this as a hard failure for the affected Store, not a
// transient condition to retry.
var ErrLockTimeout = errors.New("xattrstore: timed out waiting for syscall to return")
