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
// # Record writing and Overlaps
//
// When a caller wants to write a record, it has two interface options;
// Put and ReplaceRange.
// Put just writes: it sets the xattr for (offset, length) and
// does not look at what else is already on the file. ReplaceRange also
// writes, but first removes every existing record whose extent overlaps
// the new one. That overlap handling is the only difference between the
// two, and it is what the rest of this section covers.
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
// That separation is why this package's primitives are not the layer most
// code should use. Put, ReplaceRange, and RemoveCovering each maintain one
// half of a two-part truth: the records, and the bytes they describe. Keeping
// the halves consistent is Writer's job -- it is the only type holding both a
// Store and a fileops.File. Writer.WriteBlock writes through ReplaceRange and
// zeroes what shredding freed; Writer.Truncate drops the records covering the
// removed region before shortening the file. Reach for the Store primitives
// directly only when there is no data file to keep in step, and prefer Writer
// otherwise: an invariant spanning both halves cannot be enforced from inside
// either one.
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
// # Locking
//
// A record write is not one atomic operation -- it is an xattr header write
// plus a data write() (see Writer.WriteBlock), and Put/Get are only
// atomic per-attribute, not across that pair. Without something coordinating
// the two, a concurrent reader could see the new header alongside old data,
// or a concurrent writer could interleave with either half. A Lease is what
// makes the pair look atomic: TryAcquireExclusive keeps every other writer
// and reader off the byte range for the duration of both xattr/data writes;
// TryAcquireShared lets a verifier read that range and know no writer holds
// it.
//
// TryAcquireExclusive and TryAcquireShared cover three scopes with two
// mechanisms: an in-process range table for goroutines, and F_SETLK -- which
// the filesystem propagates to its distributed lock manager -- for other
// processes on this node and for other nodes. POSIX record locks are
// per-process, so F_SETLK alone cannot separate two goroutines; the table
// supplies exactly that scope.
//
// It follows that ALL THREADS IN A PROCESS OPERATING ON A FILE MUST SHARE ONE
// Store for it. The table is a Store field, so two Stores in one process see
// nothing of each other, and F_SETLK will not separate them either, since they
// share a pid. See Store for why this is a documented contract rather than a
// defended one.
//
// # Capacity ceiling
//
// This package is the on-file variant only -- attributes are written directly to
// the data file's inode. That caps how much of a file it can describe, and the
// cap is LOW: measured directly against the local filesystem, 2437 records
// (9 MiB at 4 KiB blocks) on tmpfs, 31 records (124 KiB) on ext4.
//
// Two different limits produce that, and which one binds depends on the
// filesystem:
//
//   - setxattr runs out of per-inode xattr space (ENOSPC). Filesystem-specific:
//     ext4 has a single-block budget and binds at 31 records; tmpfs accepted
//     44,000+.
//   - listxattr cannot retrieve a name list larger than XATTR_LIST_MAX, which is
//     65536 bytes (E2BIG). This one is a KERNEL limit enforced in the VFS, so it
//     is the same on every filesystem -- a generous per-inode xattr budget
//     cannot be reached through it. On tmpfs it binds at 2437 records, ~18x
//     below what setxattr would have allowed.
//
// The listxattr cap applies to reads AND writes: Overlapping lists the full
// namespace on every call, so ReplaceRange (and therefore Writer.WriteBlock),
// Truncate, TryAcquireExclusive, and every verifier sweep all fail with E2BIG
// past the ceiling. A run that exceeds it does not degrade -- it stops, partway
// through, with "argument list too long" on a filesystem with terabytes free.
//
// # On BeeGFS specifically
//
// BeeGFS does not just inherit the two local-filesystem limits above -- its
// metadata server enforces the same XATTR_LIST_MAX ceiling itself, proactively,
// server-side. Measured 2026-08-20 on a three-node cluster with an xfs-backed
// meta target: a client writing through the BeeGFS mount failed at 1902 records
// with ENOSPC, while the identical write loop run locally and directly against
// the meta target's own xfs filesystem (bypassing BeeGFS) passed 20480 with no
// failure. The ceiling here comes from BeeGFS, not from xfs -- xfs's own true
// setxattr limit is untested past 20480.
//
// The mechanism (setUserXAttr, beegfs-core meta/source/toolkit/XAttrTk.cpp):
// when creating a new (not replacing) xattr, the meta server checks whether
// adding it would push the cumulative listxattr name-list size over 65536
// bytes, and returns ENOSPC immediately if so -- without ever attempting the
// real setxattr. The meta server also stores every user xattr under its own
// internal "user.bgXA." prefix (10 bytes), on top of whatever name this
// package already uses, so the effective per-record overhead is larger than
// this package's own name accounts for: 1902 * ~34.5 bytes/name ~= 65536, the
// same ceiling as above, just reached sooner because of that prefix.
//
// This guard (App::initXAttrLimit, beegfs-core meta/source/app/App.cpp) is
// enabled whenever storeUseExtendedAttribs is on, and disabled only when the
// meta target's local filesystem is ext3/ext4 -- ext4's own much smaller
// per-inode limit (~31 records) binds first there anyway, so the guard would
// be redundant. On every other backend, including xfs, expect BeeGFS's own
// ENOSPC to arrive before either of the two local-filesystem limits above
// would.
//
// A file-backed store that writes records to a separate data file instead of
// the inode's xattrs is planned future work, with this package kept as a
// selectable mode (exercising the filesystem's xattr path, on BeeGFS and off,
// is itself part of what verifyio tests). Sharding xattrs across shadow files
// was considered and rejected: it only multiplies a small ceiling. See
// verifyio/TODO.md for the design and the questions still open on it.
package xattrstore

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

// ErrNotFound is returned by Get when no header has been recorded at
// the given offset. Aliased to xattr.ErrNotFound so callers can check
// either with errors.Is.
var ErrNotFound = xattr.ErrNotFound

// xattrPrefix is the namespace for every entry this package writes.
// Kept short so the per-inode xattr budget on tight filesystems is
// dominated by the value bytes rather than the names.
// XAttrPrefix is the namespace every record header is stored under. Exported so
// callers can recognize a file as a verifyio artifact without hardcoding the
// string -- e.g. before doing something destructive to it.
const XAttrPrefix = "user.verifyio."

const xattrPrefix = XAttrPrefix

// Store is the xattr backing for one data file. All xattr operations go
// through a file descriptor opened once, at construction time, and held
// for the Store's lifetime -- never re-resolved from target by path. A
// path-based xattr call re-resolves its target on every invocation, so a
// long-lived Store issuing dozens of calls over its life would silently
// follow the path to whatever it currently names if the original file
// were ever replaced (e.g. a symlink swap); the held fd was resolved
// once and can't be redirected that way. Callers must call Close when
// done with a Store.
//
// The xattr methods (Put, Get, Remove, etc.) are safe for concurrent
// use — the underlying syscalls are atomic per attribute.
//
// # Locking precondition: one Store per file per process, shared by every thread
//
// All threads in a process operating on a file must share ONE Store instance
// for it. Neither of the two locking mechanisms can substitute for that: the
// range table is a Store field, so separate Stores in one process see nothing
// of each other, and F_SETLK will not separate them either, since they share a
// pid -- two separate processes each holding their own Store on the same file
// are fine; it is specifically two Stores in the same process that collide.
// Two Stores on one file in one process therefore both believe they hold an
// exclusive lock over the same range, and either one's release drops the
// other's kernel lock.
//
// This is a documented contract rather than something a lock defends, because
// there is no mechanism available that would catch it: OFD locks would have,
// on a local filesystem, but BeeGFS keys record locks by (node, pid) and
// ignores the open file description even for F_OFD_SETLK commands — measured
// 2026-08-17 on a three-node cluster. See TryAcquireExclusive for what the two
// mechanisms do cover.
type Store struct {
	target      string
	file        *os.File       // opened once at construction; all xattr ops go through its fd
	lockTimeout time.Duration  // <=0 means wait indefinitely
	ranges      rangeLockTable // tracks in-process locks per byte range
}

// DefaultLockTimeout bounds F_SETLK and the xattr scans used to compute a lock
// range. BeeGFS can block these indefinitely if a metadata server is
// unresponsive; see ErrLockTimeout.
const DefaultLockTimeout = 30 * time.Second

// OpenStore returns a Store for targetPath. Every thread in this process
// operating on that file must share the returned Store -- see the
// comments above.
//
// It opens targetPath once, with O_NOFOLLOW so a symlink planted at that path
// is refused rather than silently followed, and keeps the resulting fd for every
// subsequent xattr operation the returned Store performs.
//
// lockTimeout bounds how long TryAcquireExclusive and TryAcquireShared will
// wait for F_SETLK and the xattr scans used to compute the lock range before
// giving up with ErrLockTimeout. Pass DefaultLockTimeout unless there is a
// reason not to; <=0 waits indefinitely, which on BeeGFS means a stalled
// metadata server hangs the caller with no diagnostic.
func OpenStore(targetPath string, lockTimeout time.Duration) (*Store, error) {
	f, err := os.OpenFile(targetPath, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.OpenStore: open %s: %w", targetPath, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("xattrstore.OpenStore: stat %s: %w", targetPath, err)
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("xattrstore.OpenStore: %s is not a regular file (mode=%s)",
			targetPath, info.Mode())
	}
	return &Store{target: targetPath, file: f, lockTimeout: lockTimeout}, nil
}

// Target returns the data-file path this Store operates on.
func (s *Store) Target() string {
	return s.target
}

// Close releases the file descriptor opened by OpenStore. The Store must not
// be used after Close returns.
func (s *Store) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("xattrstore.Close: %w", err)
	}
	return nil
}

// XAttrName returns the xattr name for a record occupying [offset,
// offset+length) bytes on disk. Exposed so verifiers and tests can
// construct the same names without duplicating the format string.
func XAttrName(offset, length int64) string {
	return fmt.Sprintf("%s%d-%d", xattrPrefix, offset, length)
}

// parseXAttrName recovers (offset, length) from an xattr name. Returns
// ok=false for names that don't match the expected form, or whose
// (offset, length) fails validRange (e.g. a negative length, from a name
// like "<prefix>100--50", or an offset+length that overflows int64) --
// the same validity check Put already enforces on the write side.
// Overlapping's own overlaps() helper already guards non-positive lengths,
// and -- given Overlapping validates its own
// query as non-negative -- a stored length large enough to overflow the
// interval math can only ever make overlaps() under-report, never
// false-positive; this check makes that exclusion deliberate and consistent
// with ForEachEntry rather than an accident of the arithmetic.
//
// Also rejects a name that parses but is not the EXACT string XAttrName(off,
// ln) would produce -- e.g. "<prefix>0516-516" (leading zero) or
// "<prefix>+0-516". strconv.ParseInt accepts spellings Put's "%d" formatting
// never emits, so a name like that can only be corruption or tampering, never
// this package's own output. Every caller that reaches a covering entry via
// this function (verifier.VerifyFile in particular) re-derives the xattr name
// from (offset, length) with XAttrName rather than keeping the original
// string, in order to re-read the header under a lock; a non-canonical
// spelling would parse here, sweep in as a legitimate CoverageOne record, and
// then silently miss on that later Get (ErrNotFound), which callers treat as
// benign lock contention -- reporting PASS over a record whose header was
// never actually re-verified. Requiring the round-trip turns that into what
// it actually is: a MalformedEntry.
func parseXAttrName(name string) (offset, length int64, ok bool) {
	off, ln, reason := parseXAttrNameReason(name)
	return off, ln, reason == ""
}

// parseXAttrNameReason is parseXAttrName plus the specific reason a name was
// rejected; reason == "" means it parsed. It carries the whole implementation so
// that the reason reported to an operator cannot drift from the check that
// actually rejected the name.
func parseXAttrNameReason(name string) (offset, length int64, reason string) {
	if !strings.HasPrefix(name, xattrPrefix) {
		return 0, 0, "name is not in this package's xattr namespace"
	}
	rest := name[len(xattrPrefix):]
	offStr, lnStr, found := strings.Cut(rest, "-")
	if !found {
		return 0, 0, "name does not have the form <offset>-<length>"
	}
	off, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Sprintf("offset %q is not a base-10 int64", offStr)
	}
	ln, err := strconv.ParseInt(lnStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Sprintf("length %q is not a base-10 int64", lnStr)
	}
	if err := ValidRange(off, ln); err != nil {
		return 0, 0, fmt.Sprintf("offset %d length %d is not a valid range: %v", off, ln, err)
	}
	if canonical := XAttrName(off, ln); canonical != name {
		// Reached only by a spelling ParseInt accepts and Put's "%d" never
		// emits -- a leading zero, a plus sign. It parses, so it is not a bad
		// range; it is a name this package did not write.
		return 0, 0, fmt.Sprintf("name is not canonically spelled (this package would write %q)",
			canonical)
	}
	return off, ln, ""
}

// ValidRange rejects (offset, length) pairs the store cannot represent or
// reason about safely: a negative offset (the xattr name "<offset>-<length>"
// cannot round-trip one, so the entry would be silently dropped on read), a
// non-positive length, or an extent whose end overflows int64 (which would make
// the overlap/union interval math wrap and silently misreport).
//
// Exported because it is the validity rule for a byte range in this package's
// terms, and callers outside it take byte ranges too: verifier.VerifyFile's
// Options.Range is one, and it silently swallowed an invalid range (reporting
// no anomalies for a region it never swept) for want of this check. One
// definition, so an invalid range is rejected the same way wherever it enters.
func ValidRange(offset, length int64) error {
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

// ReadableLen returns how many bytes of the extent [offset, offset+length) can
// actually be read from a file of fileSize bytes: length when the extent lies
// wholly inside the file, the remainder when it straddles EOF, and 0 when it
// begins at or past EOF.
//
// Every caller that sizes a buffer from a record extent must route through
// this. A record's length reaches us from the xattr *name* and validRange only
// checks it for sign and int64 overflow -- never against the file -- so a
// corrupt or hostile name like "<prefix>0-1099511627776" otherwise becomes a
// 1 TiB make([]byte, ...). That is an unrecoverable "fatal error: runtime: out
// of memory", not a catchable panic, so it takes down the whole sweep: the
// verifier dies on precisely the corruption it exists to report. Clamping
// instead yields a short or empty buffer, which block.VerifyBlock already
// classifies as VerdictTruncated -- the accurate verdict for a record whose
// bytes no longer exist.
func ReadableLen(offset, length, fileSize int64) int64 {
	if offset < 0 || length <= 0 || offset >= fileSize {
		return 0
	}
	if rem := fileSize - offset; rem < length {
		return rem
	}
	return length
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
// ReplaceRange helper (or build it on top of Overlapping + Remove + Put).
//
// offset must be >= 0 and length > 0, and offset+length must not
// overflow int64 (see validRange).
func (s *Store) Put(offset, length int64, header []byte) error {
	if len(header) != block.HeaderSize {
		return fmt.Errorf("xattrstore.Put: header is %d bytes, want %d",
			len(header), block.HeaderSize)
	}
	if err := ValidRange(offset, length); err != nil {
		return fmt.Errorf("xattrstore.Put: %w", err)
	}
	name := XAttrName(offset, length)
	if err := xattr.SetFd(int(s.file.Fd()), name, header, 0); err != nil {
		return fmt.Errorf("xattrstore.Put: setxattr %s on %s: %w", name, s.target, err)
	}
	return nil
}

// Get returns the raw header bytes stored for the record at (offset, length),
// exactly as stored -- Get does not validate or interpret them. Returns nil,
// ErrNotFound if no record has been set with that exact (offset, length)
// pair. Callers that don't know the length should discover it first with
// Overlapping (by extent) or ForEachEntry (by usable record).
//
// Deliberately does not reject a value whose length differs from today's
// block.HeaderSize: block.UnmarshalHeader already fully owns interpreting
// the returned bytes (too short -> ErrTruncated; long enough -> its own
// CRC/Version checks, which is exactly what distinguishes real corruption
// from a differently (e.g. newer) versioned header). Gating on size here,
// ahead of that, would reject a longer, self-consistent, newer-version
// header before UnmarshalHeader ever got a chance to classify it as such --
// turning a per-span verdict into a hard error that aborts an entire
// verifier sweep the day HeaderVersion changes size.
func (s *Store) Get(offset, length int64) ([]byte, error) {
	name := XAttrName(offset, length)
	val, err := xattr.GetFd(int(s.file.Fd()), name)
	if err != nil {
		if errors.Is(err, xattr.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("xattrstore.Get: getxattr %s on %s: %w", name, s.target, err)
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
// Names that don't match the verifyio prefix are skipped, and so are entries in
// this namespace whose name doesn't parse -- those have no extent, so there is
// nothing a caller could place them at. Use ForEachEntryStrict to be told about
// the latter rather than have them silently dropped.
//
// No in-tree caller outside tests uses this. Every real tool (iotest-dump,
// iotest-verify, verifier.go) routes through ForEachEntryStrict instead, since a
// silently-skipped malformed entry is exactly the false-PASS risk this package's
// test-profile calibration treats as most severe. Kept as a simpler iterator for
// callers -- currently just tests -- that genuinely don't need malformed-entry
// visibility.
//
// The header is passed through EXACTLY as stored, including when its size is not
// block.HeaderSize. This mirrors Get's documented policy and for the same
// reason: block.UnmarshalHeader owns interpreting these bytes (short ->
// ErrTruncated -> VerdictHeadTruncated; long enough -> its own Version/CRC
// checks, which is what separates real corruption from a newer, self-consistent
// header format). A size gate here would pre-empt that classification, and it
// used to: a wrong-sized value was dropped from the iteration entirely, so
// verifier.VerifyFile never saw the record. That made a file with corrupt
// metadata sweep clean whenever the bytes underneath happened to be ZERO -- a
// false PASS in the one tool whose job is to catch lost data. (With non-zero
// bytes it was caught only by accident, via verifyUnclaimedSpan's recheck through
// Overlapping, which has no size gate.) Callers wanting only records that parse
// as today's format should check the size themselves; the two dump commands and
// the verifier all correctly route the bytes to UnmarshalHeader and report what
// it says.
func (s *Store) ForEachEntry(fn func(offset, length int64, header []byte) error) error {
	_, err := s.ForEachEntryStrict(fn)
	return err
}

// MalformedEntry is an xattr in this package's namespace whose name does not
// parse as <offset>-<length>, and which therefore describes no byte range.
//
// This is the one anomaly that cannot be reported as a per-span verdict, which
// is why it needs a channel of its own: a record with a bad *value* still has an
// extent, so it reaches the verifier and gets a verdict, but a record with a bad
// *name* has nowhere on the file to attach to. Reporting it is not optional --
// an unparseable name in verifyio's own namespace means something wrote metadata
// this code cannot account for, and staying quiet about it is the fail-quiet
// failure this package exists to catch.
type MalformedEntry struct {
	// Name is the raw xattr name, which is the only handle on the entry: with no
	// parseable offset/length, Remove cannot address it.
	//
	// It is RAW listxattr bytes and therefore attacker-controlled: the kernel
	// accepts any non-NUL byte sequence in a user.* name, including newlines,
	// carriage returns and ANSI escapes. Render it with String (or %q), never
	// with %s -- see String.
	Name string
	// Reason says how it is malformed, in a form fit for an error message.
	Reason string
}

// String renders the entry with its name QUOTED, which is load-bearing rather
// than cosmetic.
//
// Name is raw listxattr bytes, so a name containing newlines lets whoever
// planted the xattr write arbitrary lines into the output of every tool that
// reports it. iotest-dump is the worst case, because its exit status is always
// 0 and the printed text IS the verdict: a single planted name reproduced a
// byte-exact forged span line and a line beginning "PASS:", under exit 0. The
// tools this package serves exist to be believed about whether data is intact,
// so output an attacker can compose is a false-PASS door in its own right.
//
// Quoting here rather than at each call site is deliberate: three sites render
// these (iotest-dump, iotest-verify, and verifier's error text), and a rule that
// every future one must remember to quote is the kind that rots.
func (m MalformedEntry) String() string {
	return fmt.Sprintf("%q: %s", m.Name, m.Reason)
}

// ForEachEntryStrict is ForEachEntry, and additionally returns every entry in
// this namespace that it had to skip because the name did not parse.
//
// Deliberately one pass, not a separate query: this can be called very frequently
// with lots of threads. The malformed list is collected
// during the walk the caller was already paying for.
func (s *Store) ForEachEntryStrict(fn func(offset, length int64, header []byte) error) ([]MalformedEntry, error) {
	fd := int(s.file.Fd())
	names, err := xattr.ListFd(fd)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.ForEachEntry: listxattr %s: %w", s.target, err)
	}
	var malformed []MalformedEntry
	for _, n := range names {
		if !strings.HasPrefix(n, xattrPrefix) {
			continue // not ours, so not ours to report on
		}
		offset, length, reason := parseXAttrNameReason(n)
		if reason != "" {
			malformed = append(malformed, MalformedEntry{Name: n, Reason: reason})
			continue
		}
		val, err := xattr.GetFd(fd, n)
		if err != nil {
			if errors.Is(err, xattr.ErrNotFound) {
				// Concurrent removal; not fatal and not malformed.
				continue
			}
			return malformed, fmt.Errorf("xattrstore.ForEachEntry: getxattr %s on %s: %w",
				n, s.target, err)
		}
		// No size gate -- see ForEachEntry.
		if err := fn(offset, length, val); err != nil {
			return malformed, err
		}
	}
	return malformed, nil
}

// Remove deletes the header for the record at (offset, length).
// Returns ErrNotFound if no such record exists.
func (s *Store) Remove(offset, length int64) error {
	return xattr.RemoveFd(int(s.file.Fd()), XAttrName(offset, length))
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
	if err := ValidRange(offset, length); err != nil {
		return nil, fmt.Errorf("xattrstore.Overlapping: %w", err)
	}
	names, err := xattr.ListFd(int(s.file.Fd()))
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
// The removed records are returned precisely so the caller CAN meet that
// responsibility: their extents, minus [offset, offset+length), are exactly
// the byte ranges now claimed by nothing and still holding whatever was
// written there before. Ignoring the return value leaves stale data in a span
// that reads as unwritten -- see Writer.WriteBlock, which zeroes them. The
// exact-match record (if any) is overwritten in place rather than removed, so
// it is not returned: no bytes are freed in that case.
//
// header must be exactly block.HeaderSize bytes; length must be > 0.
// Concurrent ReplaceRange on overlapping ranges is unsafe at this
// layer -- callers should serialize via a file lock that covers the
// union of the new range and every existing overlap.
func (s *Store) ReplaceRange(offset, length int64, header []byte) ([]Entry, error) {
	if len(header) != block.HeaderSize {
		return nil, fmt.Errorf("xattrstore.ReplaceRange: header is %d bytes, want %d",
			len(header), block.HeaderSize)
	}
	if err := ValidRange(offset, length); err != nil {
		return nil, fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}

	overlapping, err := s.Overlapping(offset, length)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}
	var removed []Entry
	for _, e := range overlapping {
		// Skip the exact-match case so the Put below cleanly overwrites
		// it -- avoids a remove+set syscall pair and a transient
		// uncovered window for the no-overlap-changes case.
		if e.Offset == offset && e.Length == length {
			continue
		}
		if err := xattr.RemoveFd(int(s.file.Fd()), e.Name); err != nil {
			if errors.Is(err, xattr.ErrNotFound) {
				// Concurrent removal; treat as already-done.
				continue
			}
			return removed, fmt.Errorf("xattrstore.ReplaceRange: remove %s on %s: %w",
				e.Name, s.target, err)
		}
		removed = append(removed, e)
	}
	if err := s.Put(offset, length, header); err != nil {
		return removed, fmt.Errorf("xattrstore.ReplaceRange: %w", err)
	}
	return removed, nil
}

// RemoveCovering removes every record that claims any byte at or beyond from,
// returning the records it removed. A record straddling from is removed in its
// entirety -- records are never split, and a partially-present block is not
// verifiable as a block.
//
// from == 0 therefore purges every record whose NAME parses -- including records
// whose stored value is not a usable header, which selection by extent covers
// and selection by header did not.
//
// It does NOT remove an xattr in this namespace whose *name* fails to parse
// (a negative offset, a non-positive length, an extent overflowing int64).
// Such an entry has no placeable extent, so it cannot be selected by one, and
// it is equally invisible to Overlapping, to ForEachEntry, and to the verifier's
// sweep -- inert, occupying xattr space and nothing else. Removing it requires
// the raw name; see Store.Remove.
//
// This is the xattr half of a truncation. It does not touch the data file; see
// Writer.Truncate, which sequences both halves in the order that keeps a crash
// recoverable.
func (s *Store) RemoveCovering(from int64) ([]Entry, error) {
	if from < 0 {
		return nil, fmt.Errorf("xattrstore.RemoveCovering: from must be >= 0 (got %d)", from)
	}
	// Selection is by EXTENT, via Overlapping, not by header via ForEachEntry.
	//
	// [from, MaxInt64) is the widest range validRange accepts, and overlaps()
	// over it reduces to exactly "the record ends after from".
	if from == math.MaxInt64 {
		// No valid record can end past MaxInt64 (validRange forbids the
		// overflow), so nothing can be at or beyond here -- and the range
		// Overlapping would need is zero-length, which validRange rejects.
		return nil, nil
	}
	// Collect before removing: Overlapping walks a listxattr snapshot, and
	// mutating attributes while iterating it is not something the kernel
	// guarantees anything about.
	doomed, err := s.Overlapping(from, math.MaxInt64-from)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.RemoveCovering: %w", err)
	}

	var removed []Entry
	for _, e := range doomed {
		if err := xattr.RemoveFd(int(s.file.Fd()), e.Name); err != nil {
			if errors.Is(err, xattr.ErrNotFound) {
				continue // concurrent removal; already done
			}
			return removed, fmt.Errorf("xattrstore.RemoveCovering: remove %s on %s: %w",
				e.Name, s.target, err)
		}
		removed = append(removed, e)
	}
	return removed, nil
}
