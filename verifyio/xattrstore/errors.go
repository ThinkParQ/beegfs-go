package xattrstore

import (
	"errors"
	"fmt"
)

// ErrLockBusy is returned by TryAcquireExclusive and TryAcquireShared when
// another lock holder conflicts with the requested range: a holder in this
// process (detected by the Store's in-process range table), in another process
// on this node, or on another node (both detected by F_SETLK). Callers should
// treat this as "pick a different file or region" rather than retrying
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

// ErrExtentOverlap is returned by Writer.WriteBlock and Writer.Truncate when the
// bytes a shred freed are still claimed by a surviving record — i.e. the Store's
// record extents overlap, which the Writer documents as a precondition it cannot
// itself establish.
//
// The zero-fill is refused entirely rather than applied to the gaps that look
// safe: an extent that overlaps another is an extent this package has no reason
// to trust, and a tool whose job is to report an untrustworthy extent must not
// act on it. Nothing was written when this is returned.
//
// Recovering the evidence needs the log, not a later sweep. By the time the
// refusal happens ReplaceRange has already removed the record whose extent was
// the problem, so the store is coherent again — survivors intact, data intact,
// overlap gone — and a verifier pass afterwards finds nothing to report. The
// Writer logs the refused range and the claimant at Error level for that reason.
//
// A caller writing one block at a time (iotest-util write) should treat this as
// fatal. A soak with many writers and a periodic verifier should count it,
// report it, and keep going: one corrupt xattr name is not a reason to end the
// run, and continuing is how the next sweep gets a chance to characterise it.
var ErrExtentOverlap = errors.New("xattrstore: record extents overlap — refusing to zero bytes a surviving record still claims")

// ErrTruncateGrowUnsupported is returned by Writer.Truncate when size is past
// the current end of file. Truncate shrinks only, for now.
//
// Growing was implemented and is withdrawn rather than fixed. Its lock range is
// derived from max(EOF, furthest record extent), which collapses two facts the
// grow direction needs kept apart: a shrink changes bytes from size upward, but
// a grow changes them from EOF upward, and EOF is no longer recoverable once
// maxed together. So any grow past a record sitting beyond EOF moved bytes with
// no lock held over them -- and where the record was the furthest extent, with
// no lock taken at all. Nothing shipped calls it: both binaries only ever
// Truncate(0).
//
// The ban is advisory, not structural: it compares size against an EOF read
// before the lock is taken, so an out-of-band shrink in that window makes size
// past EOF again after the check has passed, and file.Truncate grows the file
// with the lock covering the wrong range -- or, in the hi == lo case, with no
// lock at all. A concurrent Writer.Truncate cannot cause this (it removes the
// covering records first, so the recomputed bound follows the file down); a raw
// truncate by anything else can.
//
// TODO: to support growing again, scanTruncateBound must return EOF and the
// furthest record extent separately, and Truncate must lock
// [min(size, EOF), max(size, recordEnd)). EOF must be established UNDER the
// lock, or re-checked after taking it -- deriving the bound from a pre-lock read
// is what left every previous version of this gate open. TryAcquireExclusive
// already rescans the record set across the acquire and returns ErrSetChanged;
// the file length is the fact that rescan does not cover. A test must cover a
// grow past a record beyond EOF, which is the case the collapsed bound hid.
var ErrTruncateGrowUnsupported = errors.New("xattrstore: growing a file with Truncate is not supported yet")

// ErrLockTimeout is returned by TryAcquireExclusive and TryAcquireShared when
// an underlying syscall (F_SETLK, or one of the xattr scans used to compute
// the lock range) does not return within the Store's lock timeout.
//
// F_SETLK is documented as non-blocking, but BeeGFS's distributed lock
// manager can cause it — and xattr operations on the same file — to block
// indefinitely if the metadata server is unresponsive or deadlocked.
// ErrLockTimeout is the "give up and report" signal for that condition.
//
// Go cannot interrupt a blocked syscall: the goroutine that issued the call
// keeps running until the kernel returns, which may be never. Because that
// goroutine's eventual outcome is unknown, the Store's in-process
// rangeLockTable guard for the affected range is deliberately NOT released
// when this error occurs (see guardSafeToRelease) — freeing it would
// let another same-process goroutine wrongly believe the range is free and
// re-acquire it, only for the real, delayed syscall to land later and
// silently invalidate that second goroutine's lock too. The underlying
// POSIX lock state is unreliable either way — callers should treat this as
// a hard failure for the affected Store, not a transient condition to retry.
var ErrLockTimeout = errors.New("xattrstore: timed out waiting for syscall to return")

// ReleaseErrorOrCause combines the error from releasing a lock with the error
// the caller was otherwise going to return. A release error always wins: it is
// treated as fatal for the affected Store regardless of what the caller was
// about to report.
//
// Uniformly, and not only for the ambiguous ErrLockTimeout case that leaks the
// in-process range guard (see Lease.Release). Ranking a "definite" F_UNLCK
// failure below cause would let it be folded into ErrSetChanged or ErrLockBusy
// — exactly the errors callers legitimately treat as benign "skip and move on"
// signals — and dropped with no trace. A leaked guard OR a plain unlock failure
// reported as either one is silently fatal to coverage: the range stops being
// written and verified, the ops counter keeps climbing, and the run still
// reports PASS.
//
// cause is preserved in the message for diagnosis, not for matching: the %v
// (never %w) is deliberate, so errors.Is(result, cause) stays false and no
// benign-mapping branch upstream can swallow a release error through it.
// Callers in three packages need this, so it lives here rather than being
// re-derived per site.
func ReleaseErrorOrCause(relErr, cause error) error {
	if relErr == nil {
		return cause
	}
	if cause == nil {
		return relErr
	}
	//nolint:errorlint // see doc comment: wrapping cause would re-open the masking hole
	return fmt.Errorf("%w (while unwinding after: %v)", relErr, cause)
}
