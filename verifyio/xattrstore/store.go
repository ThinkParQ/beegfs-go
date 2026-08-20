// Package xattrstore stores per-record headers as extended attributes
// on a target data file, providing a small key/value layer keyed by
// (offset, length) -- the byte range the record occupies on disk.
//
// Each entry's xattr name is "user.verifyio.<offset>-<length>", and the
// value is the marshaled block header from package block (block.HeaderSize
// bytes). Length is the on-disk extent of the block's DATA alone --
// block.BlockDataSize(bodyLen), i.e. body + stripe CRCs. The header itself
// is never written to the data file; it exists only as this xattr's value,
// so it does not contribute to length. Nor is length the header's BodyLen
// field, which is smaller still: BodyLen counts just the body, excluding
// the stripe CRCs that make up the rest of length.
//
// Including length in the name lets callers identify each record's
// extent without reading its value, which makes overlap queries
// (needed for variable-sized records) cheap: list xattrs once, parse
// names, done.
//
// Multiple writers may set xattrs at the same (offset, length); on
// Linux this is last-write-wins at the kernel level. Verifiers can
// use the header's TimeNs field to disambiguate when they have access
// to multiple histories.
//
// # Overlap policy: replace-and-shred
//
// Put does NOT check whether a new record's extent overlaps existing
// records. Two entries with overlapping (offset, length) extents can
// coexist on the same file, and a file-level verifier will flag them
// as a CoverageMany anomaly.
//
// Example: a Put(1024, 3072, headerA) writes a record covering
// [1024, 4096) (xattr "user.verifyio.1024-3072"). A second, unrelated
// Put(2048, 2048, headerB) -- meant to describe a different block that
// happens to start at offset 2048 -- writes "user.verifyio.2048-2048",
// covering [2048, 4096). Put does not check headerA's existing claim, so
// both xattrs persist side by side. A verifier walking the file sees
// [2048, 4096) claimed by two records at once and reports CoverageMany
// for that span, even though [1024, 2048) is still cleanly CoverageOne.
//
// Callers that want overlap-free semantics should write through
// ReplaceRange instead, which implements replace-and-shred: every
// overlapping record is removed entirely before the new entry is
// written. Old records are not split,
// even if only part of their extent intersects the new range. Bytes
// that were covered by a removed record but are not inside the new
// range become uncovered ("sparse" from the xattr layer's point of
// view), and the caller is responsible for zeroing them in the data
// file. The library deliberately keeps xattr management and data-file
// management in separate hands.
//
// Example: an existing record covers [0, 4096). A caller calls
// ReplaceRange(0, 1024, newHeader) to write a smaller record at the same
// starting offset. The old [0, 4096) record is removed in its entirety --
// not truncated or split to keep a [1024, 4096) remainder -- and the new
// [0, 1024) record is written in its place. Bytes [1024, 4096) are now
// uncovered: no xattr claims them, even though the data file may still
// physically hold whatever was written there before. ReplaceRange only
// manages the xattr side; the caller must zero those bytes itself, or a
// verifier will read stale leftover data in a span that claims to be
// unwritten.
//
// Inside ReplaceRange, overlapping records are removed before the new
// entry is written. A crash in the middle therefore leaves uncovered
// bytes (recoverable, verifier-flaggable) rather than two records
// claiming the same range (harder to reconcile).
//
// This package is the on-file variant only -- attributes are written
// directly to the data file's inode. A sharded sidecar variant (for
// filesystems with tight per-inode xattr budgets) may follow as a
// separate Store implementation in a later release.
package xattrstore

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

// LockMode controls the locking strategy used by TryAcquireExclusive.
type LockMode int

const (
	// LockModeOFD uses Linux OFD locks (F_OFD_SETLK) for exclusive access.
	// Each caller must use a distinct *os.File (open file description) to
	// achieve goroutine-level conflict detection. Provides single-node
	// exclusivity only — OFD locks are not propagated across nodes on
	// shared filesystems.
	LockModeOFD LockMode = iota

	// LockModePOSIX uses POSIX process locks (F_SETLK) for cross-node
	// exclusivity via the filesystem's distributed lock manager (e.g.
	// BeeGFS), combined with an in-process range-lock table for
	// goroutine coordination. Use this when multiple nodes share the
	// same data files.
	LockModePOSIX
)

// ErrNotFound is returned by Get when no header has been recorded at
// the given offset. Aliased to xattr.ErrNotFound so callers can check
// either with errors.Is.
var ErrNotFound = xattr.ErrNotFound

// xattrPrefix is the namespace for every entry this package writes.
// Kept short so the per-inode xattr budget on tight filesystems is
// dominated by the value bytes rather than the names.
const xattrPrefix = "user.verifyio."

// Store is the xattr backing for one data file. All operations target
// the file directly; no sidecar directory is created.
//
// The xattr methods (Put, Get, Remove, etc.) are safe for concurrent
// use — the underlying syscalls are atomic per attribute. The locking
// methods (TryAcquireExclusive, TryAcquireShared) follow the protocol
// determined by the LockMode the Store was opened with.
type Store struct {
	target      string
	lockMode    LockMode
	lockTimeout time.Duration  // LockModePOSIX only; <=0 means wait indefinitely
	ranges      rangeLockTable // used only in LockModePOSIX; tracks in-process locks per byte range
}

// rangeLock describes one in-process lock held over a byte range of the
// target file under LockModePOSIX.
type rangeLock struct {
	offset    int64
	length    int64
	exclusive bool
}

// rangeLockTable tracks in-process locks held over byte ranges of a
// Store's target file. It exists because traditional POSIX record locks
// (F_SETLK) do not conflict with other locks held by the same process --
// without this table, two goroutines on the same node could both
// successfully F_SETLK overlapping ranges. OFD locks don't have this
// problem (different open file descriptions in the same process do
// conflict), but OFD locks aren't propagated across nodes, which is why
// LockModePOSIX exists in the first place.
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
// true, unless it would conflict with an existing entry. An exclusive
// request conflicts with any overlapping entry; a shared request
// conflicts only with overlapping exclusive entries.
func (t *rangeLockTable) tryAcquire(offset, length int64, exclusive bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range t.locks {
		if !overlaps(offset, length, l.offset, l.length) {
			continue
		}
		if exclusive || l.exclusive {
			return false
		}
	}
	t.locks = append(t.locks, rangeLock{offset: offset, length: length, exclusive: exclusive})
	return true
}

// release removes one entry matching (offset, length, exclusive). It is a
// no-op if no such entry exists.
func (t *rangeLockTable) release(offset, length int64, exclusive bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, l := range t.locks {
		if l.offset == offset && l.length == length && l.exclusive == exclusive {
			t.locks = append(t.locks[:i], t.locks[i+1:]...)
			return
		}
	}
}

func openStore(targetPath string, mode LockMode, lockTimeout time.Duration) (*Store, error) {
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.OpenStore: stat %s: %w", targetPath, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("xattrstore.OpenStore: %s is not a regular file (mode=%s)",
			targetPath, info.Mode())
	}
	return &Store{target: targetPath, lockMode: mode, lockTimeout: lockTimeout}, nil
}

// OpenStore returns a Store using OFD locking (LockModeOFD). Each
// caller must use a distinct *os.File per goroutine to achieve proper
// conflict detection. Suitable for single-node use.
func OpenStore(targetPath string) (*Store, error) {
	return openStore(targetPath, LockModeOFD, 0)
}

// OpenStoreMultiNode returns a Store using POSIX locking (LockModePOSIX).
// POSIX locks are propagated by shared filesystems (e.g. BeeGFS) to
// their distributed lock manager, providing cross-node exclusivity.
// An in-process mutex serialises goroutines on this node.
//
// lockTimeout bounds how long TryAcquireExclusive and TryAcquireShared will
// wait for F_SETLK and the xattr scans used to compute the lock range before
// giving up with ErrLockTimeout. Pass <=0 to wait indefinitely (the original,
// pre-timeout behavior).
func OpenStoreMultiNode(targetPath string, lockTimeout time.Duration) (*Store, error) {
	return openStore(targetPath, LockModePOSIX, lockTimeout)
}

// Target returns the data-file path this Store operates on.
func (s *Store) Target() string {
	return s.target
}

// XAttrName returns the xattr name for a record occupying [offset,
// offset+length) bytes on disk. Exposed so verifiers and tests can
// construct the same names without duplicating the format string.
func XAttrName(offset, length int64) string {
	return fmt.Sprintf("%s%d-%d", xattrPrefix, offset, length)
}

// parseXAttrName recovers (offset, length) from an xattr name. Returns
// ok=false for names that don't match the expected form.
func parseXAttrName(name string) (offset, length int64, ok bool) {
	if !strings.HasPrefix(name, xattrPrefix) {
		return 0, 0, false
	}
	rest := name[len(xattrPrefix):]
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return 0, 0, false
	}
	off, err := strconv.ParseInt(rest[:dash], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	ln, err := strconv.ParseInt(rest[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return off, ln, true
}

// validRange rejects (offset, length) pairs the store cannot represent or
// reason about safely: a negative offset (the xattr name "<offset>-<length>"
// cannot round-trip one, so the entry would be silently dropped on read), a
// non-positive length, or an extent whose end overflows int64 (which would make
// the overlap/union interval math wrap and silently misreport).
func validRange(offset, length int64) error {
	if offset < 0 {
		return fmt.Errorf("offset must be >= 0 (got %d)", offset)
	}
	if length <= 0 {
		return fmt.Errorf("length must be > 0 (got %d)", length)
	}
	if offset > math.MaxInt64-length {
		return fmt.Errorf("offset+length overflows int64 (offset=%d length=%d)", offset, length)
	}
	return nil
}

// Put stores header as the xattr value for the record occupying
// [offset, offset+length) on disk. header must be exactly
// block.HeaderSize bytes; shorter or longer values are rejected
// without touching the filesystem. length must be > 0.
//
// Uses default flags ("create or replace"), so concurrent writers
// converge on last-write-wins at the kernel level.
//
// Put does not check for overlap with existing records. For "delete
// all overlapping records, then write this one" semantics, use the
// ReplaceRange helper (or build it on top of List + Remove + Put).
//
// offset must be >= 0 and length > 0, and offset+length must not
// overflow int64 (see validRange).
func (s *Store) Put(offset, length int64, header []byte) error {
	if len(header) != block.HeaderSize {
		return fmt.Errorf("xattrstore.Put: header is %d bytes, want %d",
			len(header), block.HeaderSize)
	}
	if err := validRange(offset, length); err != nil {
		return fmt.Errorf("xattrstore.Put: %w", err)
	}
	name := XAttrName(offset, length)
	if err := xattr.Set(s.target, name, header, 0); err != nil {
		return fmt.Errorf("xattrstore.Put: setxattr %s on %s: %w", name, s.target, err)
	}
	return nil
}

// Get returns the header stored for the record at (offset, length).
// Returns nil, ErrNotFound if no record has been set with that exact
// (offset, length) pair. Callers that don't know the length should
// use List or ForEachEntry to discover it first.
func (s *Store) Get(offset, length int64) ([]byte, error) {
	name := XAttrName(offset, length)
	val, err := xattr.Get(s.target, name)
	if err != nil {
		if errors.Is(err, xattr.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("xattrstore.Get: getxattr %s on %s: %w", name, s.target, err)
	}
	if len(val) != block.HeaderSize {
		return nil, fmt.Errorf("xattrstore.Get: %s on %s has size %d, want %d",
			name, s.target, len(val), block.HeaderSize)
	}
	return val, nil
}

// ForEachEntry iterates every verifyio record-header xattr on the
// target and invokes fn once for each (offset, length, header) tuple.
// Iteration order is whatever xattr.List returns -- typically
// undefined; sort by offset on the caller side if you need order.
// Iteration stops if fn returns a non-nil error and that error is
// returned to the caller.
//
// Names that don't match the verifyio prefix are skipped. Entries whose
// stored size doesn't match block.HeaderSize are skipped silently;
// callers that want to surface such anomalies should iterate via List
// directly.
func (s *Store) ForEachEntry(fn func(offset, length int64, header []byte) error) error {
	names, err := xattr.List(s.target)
	if err != nil {
		return fmt.Errorf("xattrstore.ForEachEntry: listxattr %s: %w", s.target, err)
	}
	for _, n := range names {
		offset, length, ok := parseXAttrName(n)
		if !ok {
			continue
		}
		val, err := xattr.Get(s.target, n)
		if err != nil {
			if errors.Is(err, xattr.ErrNotFound) {
				// Concurrent removal; not fatal.
				continue
			}
			return fmt.Errorf("xattrstore.ForEachEntry: getxattr %s on %s: %w",
				n, s.target, err)
		}
		if len(val) != block.HeaderSize {
			continue
		}
		if err := fn(offset, length, val); err != nil {
			return err
		}
	}
	return nil
}

// Remove deletes the header for the record at (offset, length).
// Returns ErrNotFound if no such record exists.
func (s *Store) Remove(offset, length int64) error {
	return xattr.Remove(s.target, XAttrName(offset, length))
}

// Entry identifies one stored record by its on-disk extent and its
// xattr name. The name is exposed so callers (verifiers, ReplaceRange)
// can pass it back to Remove without recomputing it.
//
// Entry intentionally does not carry the header bytes: Overlapping is
// built on xattr.List + parseXAttrName, which does not require a
// per-entry getxattr. Callers that need the header should call Get with
// (Offset, Length) explicitly.
type Entry struct {
	Offset int64
	Length int64
	Name   string
}

// overlaps reports whether half-open intervals [aOff, aOff+aLen) and
// [bOff, bOff+bLen) share at least one byte. Zero-length intervals
// never overlap anything (Put already rejects length<=0, but the helper
// is defensive).
func overlaps(aOff, aLen, bOff, bLen int64) bool {
	if aLen <= 0 || bLen <= 0 {
		return false
	}
	return aOff < bOff+bLen && bOff < aOff+aLen
}

// Overlapping returns every stored record whose on-disk extent
// intersects [offset, offset+length). Adjacent-but-not-overlapping
// records (e.g. one ending exactly at offset) are not included.
//
// offset must be >= 0 and length > 0 (with offset+length not overflowing
// int64); an invalid range yields an error rather than silently returning all
// or no entries.
//
// Implementation note: this uses xattr.List + name parsing only -- no
// per-entry getxattr -- so it is cheap to call before every write in a
// hot path. Entries whose names don't match the verifyio prefix are
// skipped.
func (s *Store) Overlapping(offset, length int64) ([]Entry, error) {
	if err := validRange(offset, length); err != nil {
		return nil, fmt.Errorf("xattrstore.Overlapping: %w", err)
	}
	names, err := xattr.List(s.target)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.Overlapping: listxattr %s: %w", s.target, err)
	}
	var out []Entry
	for _, n := range names {
		off, ln, ok := parseXAttrName(n)
		if !ok {
			continue
		}
		if overlaps(offset, length, off, ln) {
			out = append(out, Entry{Offset: off, Length: ln, Name: n})
		}
	}
	return out, nil
}

// ReplaceRange implements replace-and-shred at the xattr layer:
//
//  1. Find every existing record that overlaps [offset, offset+length).
//  2. Remove each one.
//  3. Set the new (offset, length) entry with the given header.
//
// Overlapping records are removed entirely -- they are not split, and
// no attempt is made to preserve the non-overlapping bytes' headers.
// That is the "shred" half of replace-and-shred.
//
// Order matters for crash safety. Removing first means a crash in the
// middle of ReplaceRange leaves "uncovered bytes" -- ranges no record
// claims -- which a file-level verifier can flag and which are
// recoverable. The alternative (Put-first, then remove) could briefly
// leave two records claiming overlapping ranges, which is harder for a
// verifier to reconcile. Do not reorder these steps.
//
// ReplaceRange does NOT touch the data file. Bytes that were previously
// covered by a now-removed record but are not inside [offset,
// offset+length) become sparse from the xattr layer's point of view;
// the caller is responsible for writing zeros (or otherwise reconciling
// content) to the data file. Keeping xattr management and data
// management in separate hands is a deliberate API choice.
//
// header must be exactly block.HeaderSize bytes; length must be > 0.
// Concurrent ReplaceRange on overlapping ranges is unsafe at this
// layer -- callers should serialize via a file lock that covers the
// union of the new range and every existing overlap.
func (s *Store) ReplaceRange(offset, length int64, header []byte) error {
	if len(header) != block.HeaderSize {
		return fmt.Errorf("xattrstore.ReplaceRange: header is %d bytes, want %d",
			len(header), block.HeaderSize)
	}
	if err := validRange(offset, length); err != nil {
		return fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}

	overlapping, err := s.Overlapping(offset, length)
	if err != nil {
		return fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}
	for _, e := range overlapping {
		// Skip the exact-match case so the Put below cleanly overwrites
		// it -- avoids a remove+set syscall pair and a transient
		// uncovered window for the no-overlap-changes case.
		if e.Offset == offset && e.Length == length {
			continue
		}
		if err := xattr.Remove(s.target, e.Name); err != nil {
			if errors.Is(err, xattr.ErrNotFound) {
				// Concurrent removal; treat as already-done.
				continue
			}
			return fmt.Errorf("xattrstore.ReplaceRange: remove %s on %s: %w",
				e.Name, s.target, err)
		}
	}
	if err := s.Put(offset, length, header); err != nil {
		return fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}
	return nil
}
