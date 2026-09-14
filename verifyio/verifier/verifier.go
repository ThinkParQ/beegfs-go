// Package verifier provides file-level verification for data files written
// by verifyio. It classifies every byte range of a file by how many
// xattr records claim it, verifies each covered range against its stored
// header, and reports results via a streaming callback so large files
// can be processed without buffering all spans in memory.
//
// Typical usage:
//
//	err := verifier.VerifyFile(store, f, verifier.Options{}, func(span verifier.Span) error {
//	    if span.Coverage != verifier.CoverageOne || span.Verdict != block.VerdictOK {
//	        log.Printf("anomaly at offset %d: coverage=%s verdict=%s", ...)
//	    }
//	    return nil
//	})
package verifier

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
	"go.uber.org/zap"
)

// Coverage classifies how many xattr records claim a given byte range.
type Coverage int

const (
	CoverageNone      Coverage = iota // no record claims these bytes
	CoverageOne                       // exactly one record covers this range
	CoverageMany                      // two or more records overlap; violates replace-and-shred
	CoverageContended                 // shared lock was busy; body not verified
)

// String returns the printable name of a Coverage value.
func (c Coverage) String() string {
	switch c {
	case CoverageNone:
		return "none"
	case CoverageOne:
		return "one"
	case CoverageMany:
		return "many"
	case CoverageContended:
		return "contended"
	default:
		return fmt.Sprintf("coverage(%d)", int(c))
	}
}

// ContendedReason says why a CoverageContended span was skipped. An operator
// chases the two differently: a busy lock means someone else was working in
// the range, while a changed store means the read itself was fine but the
// records moved under it, so what the span would have been judged against no
// longer describes it.
type ContendedReason int

const (
	ContendedNone         ContendedReason = iota // not a contended span
	ContendedLockBusy                            // another holder had the range locked
	ContendedStoreChanged                        // records appeared or vanished under the read
)

// String returns the printable name of a ContendedReason value.
func (r ContendedReason) String() string {
	switch r {
	case ContendedNone:
		return "none"
	case ContendedLockBusy:
		return "lock busy"
	case ContendedStoreChanged:
		return "store changed"
	default:
		return fmt.Sprintf("contendedReason(%d)", int(r))
	}
}

// LockMode controls whether VerifyFile acquires shared range locks before
// reading each CoverageOne span's body.
type LockMode int

const (
	// LockShared acquires a shared range lock on each CoverageOne span before
	// reading its body. Writers that hold exclusive range locks while writing
	// (as Writer.WriteBlock does) guarantee the verifier never observes a
	// partial write. If the lock is contended, the span is reported as
	// CoverageContended and the body is not read.
	//
	// This is the zero value of LockMode, so Options{} uses LockShared by
	// default.
	//
	// On non-Linux platforms -- which verifyio does not support -- the
	// underlying trySharedLock is a no-op that always reports success; see
	// verifier_lock_other.go. LockShared is accepted there without error,
	// but the torn-write guarantee above does not hold: verification
	// proceeds with no locking at all, identical to LockCallerHeld. Check each
	// resulting Span's Locked field rather than assume Options{}'s default
	// actually provided the guarantee -- it's false on non-Linux.
	LockShared LockMode = iota

	// LockCallerHeld bypasses all per-span locking in the verifyOneRecordSpan
	// function because the caller already holds a lock covering the verification
	// range. It does NOT mean "unlocked": a second shared lock on the same Store
	// fails with ErrLockBusy, which verifyOneRecordSpan reports as
	// CoverageContended -- so a caller holding a lease that leaves the default
	// in place has every covered span reported unverified, and a whole-file
	// lease yields a false INCOMPLETE rather than an error naming the mistake.
	//
	// Named for the caller's obligation rather than for the absence of a
	// syscall, and deliberately not "LockNone": xattrstore has its own
	// LockNone, on the writer side, meaning something different (never take a
	// write lock at all).
	LockCallerHeld
)

// ByteRange restricts verification to a specific byte range of the file.
type ByteRange struct {
	Offset int64
	Length int64
}

// Options controls VerifyFile behavior.
type Options struct {
	// Range restricts verification to [Range.Offset, Range.Offset+Range.Length).
	// nil verifies the whole file [0, fileSize).
	Range *ByteRange
	// LockMode controls range locking before each body read. Defaults to
	// LockShared (zero value).
	LockMode LockMode
	// Log receives a trace entry for every span. nil disables tracing.
	// Anomalous spans (CoverageMany, failed verdict, xattr mismatch,
	// non-zero CoverageNone) are logged at Warn; all others at Debug.
	Log *zap.Logger
}

// Span describes one contiguous byte range and its verification outcome.
// Spans are emitted in ascending offset order, are non-overlapping, and
// together cover the verified range exactly.
type Span struct {
	Offset   int64
	Length   int64
	Coverage Coverage

	// Contended says why a CoverageContended span was skipped.
	// ContendedNone for every other Coverage.
	Contended ContendedReason

	// Entries lists every xattr record whose extent overlaps this range.
	// Empty for CoverageNone; one entry for CoverageOne and CoverageContended;
	// two or more for CoverageMany.
	Entries []xattrstore.Entry

	// Header and Verdict are populated for CoverageOne spans where the
	// shared lock was acquired and the block was read and verified against
	// the xattr header. Both are zero/nil for all other Coverage values.
	Header  *block.Header
	Verdict block.Verdict

	// AllZero is true when every byte in a CoverageNone span reads as zero
	// (the expected state after a replace-and-shred that zeroed the uncovered
	// region). Only meaningful for CoverageNone spans.
	AllZero bool

	// XattrMatch is true when Coverage is CoverageOne and the xattr header
	// was successfully parsed (i.e. no header-level error verdict).
	XattrMatch bool

	// Diag carries forensic detail for a CoverageOne span whose Verdict is not
	// VerdictOK (stripe-CRC self-consistency, read-body CRC, first differing
	// byte). It is computed from the same buffer that produced Verdict, so its
	// numbers always agree with it. Nil for OK spans and non-CoverageOne spans.
	Diag *block.Diagnosis

	// Locked is true if a real lock was actually held while this span's body
	// was read: opts.LockMode != LockCallerHeld AND the platform actually supports
	// range locking. On non-Linux, trySharedLock is a no-op that always reports
	// success (see verifier_lock_other.go), so Options{}'s default LockShared
	// silently provides no torn-write guarantee there -- check Locked rather
	// than assume a healthy-looking sweep was actually protected. Only
	// meaningful for CoverageOne spans.
	Locked bool
}

// ErrMalformedEntries reports that the target carries at least one xattr in
// verifyio's own namespace whose name does not parse as a record extent. Test
// for it with errors.Is; the concrete error is *MalformedEntriesError, which
// names the offending entries.
var ErrMalformedEntries = errors.New("malformed verifyio xattr records")

// MalformedEntriesError is returned by VerifyFile when the sweep completed but
// the target carries entries in verifyio's namespace whose names do not parse.
//
// It is an error rather than a Span verdict because such an entry describes no
// byte range, so there is no span for a verdict to attach to. A record with a
// corrupt *value* needs no such channel -- it still has an extent, so it becomes
// a span and gets VerdictHeadTruncated or VerdictHeadBadFormat like any other
// unreadable header. Only a corrupt *name* is unplaceable, and "something wrote
// metadata in verifyio's namespace that verifyio cannot account for" is a
// finding this tool must not swallow.
type MalformedEntriesError struct {
	Target  string
	Entries []xattrstore.MalformedEntry
}

func (e *MalformedEntriesError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d malformed iotest xattr record(s)", e.Target, len(e.Entries))
	for _, m := range e.Entries {
		fmt.Fprintf(&b, "\n  %s", m)
	}
	return b.String()
}

// Unwrap makes errors.Is(err, ErrMalformedEntries) true.
func (e *MalformedEntriesError) Unwrap() error { return ErrMalformedEntries }

// VerifyFile walks the file and calls fn once per non-overlapping span,
// in ascending offset order. The spans cover [0, fileSize) unless
// opts.Range restricts the range.
//
// fn is called synchronously. If fn returns a non-nil error, VerifyFile
// stops immediately and returns that error.
//
// If the target carries entries whose names do not parse (see
// xattrstore.ForEachEntryStrict), VerifyFile still emits every span it can and
// reports them afterwards as a *MalformedEntriesError. Reporting late rather
// than aborting up front is deliberate and load-bearing: the spans are the
// diagnostic the operator came for, and refusing to produce them because some
// metadata is corrupt would abort on exactly the input this tool exists to
// characterise.
func VerifyFile(s *xattrstore.Store, f *fileops.File, opts Options, fn func(Span) error) error {
	// Stat the held fd rather than the path: an fd survives a rename or an
	// unlink, so the size bounding this sweep describes the same inode every
	// subsequent read in it comes from.
	info, err := f.LockFd().Stat()
	if err != nil {
		return fmt.Errorf("verifier.VerifyFile: stat %s: %w", s.Target(), err)
	}
	fileSize := info.Size()

	// Snapshot which xattr entries exist (offset/length only). The header is
	// re-read per span under the shared lock in verifyOneRecordSpan, so we
	// intentionally do not cache the header bytes here.
	//
	// This runs before the range computation below because the sweep domain
	// depends on how far the records reach, not just on how big the file is.
	//
	// Strict, so records this walk cannot use are reported rather than dropped.
	// malformed is deliberately NOT returned here -- see the doc comment; it is
	// returned by every path below that finishes its sweep.
	//
	// Extent only, never the header bytes: verifyOneRecordSpan re-reads the
	// header under the shared lock so the body is verified against a header
	// consistent with it. Caching one here would reinstate the false
	// BODY_CORRUPT on a benign concurrent reseed.
	var entries []xattrstore.Entry
	var maxRecordEnd int64
	malformed, err := s.ForEachEntryStrict(func(offset, length int64, _ []byte) error {
		entries = append(entries, xattrstore.Entry{
			Offset: offset,
			Length: length,
			Name:   xattrstore.XAttrName(offset, length),
		})
		if end := offset + length; end > maxRecordEnd {
			maxRecordEnd = end
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("verifier.VerifyFile: listing entries: %w", err)
	}
	malformedErr := func() error {
		if len(malformed) == 0 {
			return nil
		}
		return &MalformedEntriesError{Target: s.Target(), Entries: malformed}
	}

	// Effective range. The upper bound is the greater of the file size and the
	// furthest extent any record claims.
	//
	// It is possible to have records claiming bytes the file no longer holds;
	// for example, a node failure between WriteBlock's xattr and its data write,
	// or a dropped write.
	//
	// Extending past EOF is safe because every read is clamped separately:
	// verifyOneRecordSpan and verifyUnclaimedSpan both size their buffers with
	// xattrstore.ReadableLen, so a record claiming more than the file holds
	// yields a short buffer and a VerdictTruncated rather than an unbounded
	// allocation.
	sweepEnd := fileSize
	if maxRecordEnd > sweepEnd {
		sweepEnd = maxRecordEnd
	}
	rangeStart, rangeEnd := int64(0), sweepEnd
	if opts.Range != nil {
		// Validated, not clamped. The arithmetic below silently absorbs a bad
		// range -- Range{Offset: X, Length: math.MaxInt64}, the natural spelling
		// of "from here to the end", wraps negative, so rangeStart >= rangeEnd
		// fires and VerifyFile returns nil having emitted nothing. A caller
		// reads that as "no anomalies" for a region never read. Through
		// xattrstore's rule rather than a fourth copy of it: Put, Overlapping
		// and ReplaceRange all reject an invalid extent this way.
		if err := xattrstore.ValidRange(opts.Range.Offset, opts.Range.Length); err != nil {
			return fmt.Errorf("verifier.VerifyFile: invalid Range: %w", err)
		}
		if opts.Range.Offset > rangeStart {
			rangeStart = opts.Range.Offset
		}
		if end := opts.Range.Offset + opts.Range.Length; end < rangeEnd {
			rangeEnd = end
		}
	}
	if rangeStart >= rangeEnd {
		// Nothing to sweep, but malformed records must still be reported: an
		// empty file whose only records are malformed lands here, and it is the
		// one case where the corruption is the ENTIRE finding.
		return malformedErr()
	}

	// Split the file at the range endpoints and every record edge, so that every
	// byte in a region is covered by the same records and the whole region can be
	// judged at once: exactly one record (verify the block), none (expect zeros),
	// or more than one (records overlap, which is an anomaly).
	posSet := map[int64]struct{}{rangeStart: {}, rangeEnd: {}}
	for _, e := range entries {
		posSet[e.Offset] = struct{}{}
		posSet[e.Offset+e.Length] = struct{}{}
	}
	positions := make([]int64, 0, len(posSet))
	for p := range posSet {
		positions = append(positions, p)
	}
	slices.Sort(positions)

	// Walk consecutive boundary pairs. Skip pairs fully outside the
	// effective range; clip any that straddle a boundary.
	for i := 0; i+1 < len(positions); i++ {
		lo, hi := positions[i], positions[i+1]
		if hi <= rangeStart || lo >= rangeEnd {
			continue
		}
		if lo < rangeStart {
			lo = rangeStart
		}
		if hi > rangeEnd {
			hi = rangeEnd
		}

		var coveringRecords []xattrstore.Entry
		for j := range entries {
			e := &entries[j]
			if e.Offset < hi && e.Offset+e.Length > lo {
				coveringRecords = append(coveringRecords, *e)
			}
		}

		// An uncovered span lying wholly beyond EOF is the gap between the end
		// of the file and a stray record further out. There are no bytes there
		// and nothing claims them, so there is nothing to report -- emitting it
		// would just add a vacuous multi-terabyte CoverageNone span next to the
		// truncated record that actually matters.
		if len(coveringRecords) == 0 && lo >= fileSize {
			continue
		}

		span, err := processSpan(s, f, opts, coveringRecords, lo, hi)
		if err != nil {
			return err
		}
		if opts.Log != nil {
			logSpan(opts.Log, span)
		}
		if err := fn(span); err != nil {
			return err
		}
	}
	// Last, so a caller that aborted the sweep (processSpan or fn returning an
	// error) gets its own error rather than this one: an aborted sweep has not
	// established that these are the only malformed records.
	return malformedErr()
}

// processSpan classifies [lo, hi) and verifies it based on coverage.
//
// Deliberately NOT passed the fileSize snapshotted once at the top of
// VerifyFile: verifyUnclaimedSpan and verifyOneRecordSpan each re-stat
// immediately before sizing a read buffer, so a size captured before the
// sweep started (and now stale under a live writer) can never leak into a
// byte-clamping decision. That staleness would produce both a false
// VerdictTruncated on a complete record (buffer clamped to a size smaller
// than the file had grown to) and a false AllZero=true over real, non-zero
// bytes written past the stale EOF (the zero-scan never read far enough to
// see them). fileSize remains a closure variable in VerifyFile for the
// walk-domain decisions (sweepEnd, the "beyond EOF" skip), where a
// point-in-time snapshot is the correct and intended semantics.
func processSpan(s *xattrstore.Store, f *fileops.File, opts Options, coveringRecords []xattrstore.Entry, lo, hi int64) (Span, error) {
	switch len(coveringRecords) {
	case 0:
		return verifyUnclaimedSpan(s, f, opts, lo, hi)
	case 1:
		return verifyOneRecordSpan(s, f, opts, coveringRecords[0], coveringRecords, lo, hi)
	default:
		return Span{
			Offset:   lo,
			Length:   hi - lo,
			Coverage: CoverageMany,
			Entries:  coveringRecords,
		}, nil
	}
}

// unclaimedScanChunk bounds how much of an uncovered span's zero-scan is ever
// held in memory at once. An uncovered span's length is the GAP between two
// xattr records, and substantial sparse regions are quite possible.
// Capping the size of chunks viewed makes sure we don't need to allocate a
// large buffer and keeps a consistent memory profile.
const unclaimedScanChunk = 4 << 20 // 4 MiB

// zeroBlock is the comparison operand for allZero. 64 KiB rather than a
// chunk-sized global: it stays in cache across the whole scan, which measured
// faster than comparing against a 4 MiB one.
var zeroBlock [64 << 10]byte

// allZero reports whether b is all zero bytes.
//
// Through bytes.Equal -- runtime.memequal, which is SIMD assembly -- rather
// than a byte loop, which measured ~18x slower on a 4 MiB chunk. bytes.Count
// is faster still on a fully-zero buffer but cannot stop early, and the
// anomalous case is usually a gap whose first byte is already stale.
func allZero(b []byte) bool {
	for len(b) > 0 {
		n := min(len(b), len(zeroBlock))
		if !bytes.Equal(b[:n], zeroBlock[:n]) {
			return false
		}
		b = b[n:]
	}
	return true
}

// scanChunk reads one chunk into buf and reports whether every byte of it is
// zero. contended is true when a lock was busy, in which case nothing was read.
//
// The two implementations differ only in whether they take a range lock, which
// is the ONLY thing LockMode changes about a gap scan. Keeping that difference
// behind one signature is deliberate: the locked and unlocked scans used to be
// two copies of the whole chunk loop, and when the locked copy was added the
// tests stayed on the unlocked one -- so a mutation that scanned only the first
// chunk, turning a corrupt file into PASS at exit 0, passed every package.
type scanChunk func(s *xattrstore.Store, f *fileops.File, opts Options, buf []byte, offset int64) (zero bool, n int, contended bool, err error)

// scanChunkLocked reads one chunk under a shared range lock.
//
// The lock is per chunk rather than over the whole gap so a writer keeps
// working in the parts of a large sparse region the scan is not currently
// looking at; a writer that does collide skips the block and picks another.
func scanChunkLocked(s *xattrstore.Store, f *fileops.File, opts Options, buf []byte, offset int64) (zero bool, n int, contended bool, retErr error) {
	length := int64(len(buf))
	unlock, busy, _, err := trySharedLock(s, f.LockFd(), offset, length)
	if err != nil {
		return false, 0, false, fmt.Errorf("verifier: lock [%d, %d): %w", offset, offset+length, err)
	}
	if busy {
		return false, 0, true, nil
	}
	// Fatal, as in verifyOneRecordSpan: an ambiguous release leaks the range
	// guard for the life of the Store, and every later acquire then reads as
	// benign contention.
	defer func() {
		if rerr := unlock(); rerr != nil {
			if opts.Log != nil {
				opts.Log.Error("lock release failed",
					zap.Int64("offset", offset),
					zap.Int64("length", length),
					zap.Error(rerr),
				)
			}
			retErr = xattrstore.ReleaseErrorOrCause(
				fmt.Errorf("verifier: release [%d, %d): %w", offset, offset+length, rerr), retErr)
		}
	}()

	return readChunkZero(f, buf, offset)
}

// scanChunkUnlocked reads one chunk with no lock of its own, for LockCallerHeld
// -- the caller already holds one covering the range, and taking a second on
// the same Store returns ErrLockBusy rather than nesting. It can never report
// contended.
func scanChunkUnlocked(_ *xattrstore.Store, f *fileops.File, _ Options, buf []byte, offset int64) (bool, int, bool, error) {
	return readChunkZero(f, buf, offset)
}

// readChunkZero is the read both chunk scanners share, kept in one place so a
// change to the short-read or EOF handling cannot apply to only one of them.
func readChunkZero(f *fileops.File, buf []byte, offset int64) (bool, int, bool, error) {
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, n, false, err
	}
	return allZero(buf[:n]), n, false, nil
}

// scanAllZero reports whether every readable byte in [offset, offset+length) is
// zero, reading in unclaimedScanChunk-sized pieces rather than one allocation
// sized to the whole range. length is clamped to fileSize via
// xattrstore.ReadableLen first, exactly as a single-shot read would be; only the
// SIZE of each individual read is bounded here.
//
// opts.LockMode picks the chunk reader: LockCallerHeld scans unlocked because
// the caller's own lock already covers the range, anything else takes a shared
// lock per chunk. The choice is made once, here, so the loop below is the only
// copy of the chunking, clamping and short-read accounting that exists.
//
// It stops at the first contended chunk and reports the whole scan as
// contended: one span cannot honestly say "first half verified, second half
// skipped". A LockCallerHeld scan never reports contended.
func scanAllZero(s *xattrstore.Store, f *fileops.File, opts Options, offset, length, fileSize int64) (zero bool, contended bool, err error) {
	scan := scanChunk(scanChunkLocked)
	if opts.LockMode == LockCallerHeld {
		scan = scanChunkUnlocked
	}

	remaining := xattrstore.ReadableLen(offset, length, fileSize)
	if remaining == 0 {
		return true, false, nil
	}
	buf := make([]byte, min(remaining, unclaimedScanChunk))
	for remaining > 0 {
		want := min(int64(len(buf)), remaining)
		chunkZero, n, busy, err := scan(s, f, opts, buf[:want], offset)
		if err != nil {
			return false, false, err
		}
		if busy {
			return false, true, nil
		}
		if !chunkZero {
			return false, false, nil
		}
		if int64(n) < want {
			// A short read means the file shrank since the fileSize stat above
			// -- nothing further out is readable either.
			break
		}
		offset += int64(n)
		remaining -= int64(n)
	}
	return true, false, nil
}

// verifyUnclaimedSpan reads [lo, hi) and reports whether all bytes are zero.
//
// Each chunk is read under a shared range lock, so a writer holding an
// exclusive one cannot change the bytes mid-read; a busy lock reports the span
// contended rather than verified, which is the honest answer and costs the
// writer only a skipped block. Under LockCallerHeld the caller's own lock
// already covers the range, so the per-chunk locking is skipped.
//
// Non-zero bytes are not an anomaly on their own. A write landing between
// VerifyFile's snapshot and this read leaves records the sweep never saw, and
// its bytes are what the scan just found, so the store is rechecked: if
// anything now claims the range the span is reported contended
// (ContendedStoreChanged) rather than judged. Judging it would mean either
// reporting an uncovered remainder as covered -- clean, if the record it found
// verifies -- or calling two disjoint records an overlap. Only a range still
// claimed by nothing is a real anomaly.
//
// None of this helps against an xattrstore.LockNone writer, which takes no
// lock at all.
func verifyUnclaimedSpan(s *xattrstore.Store, f *fileops.File, opts Options, lo, hi int64) (Span, error) {
	// Stat fresh, right before sizing the read: VerifyFile's pre-sweep snapshot
	// goes stale under a growing file, and clamping the buffer to the old size
	// silently never zero-scans the bytes written past it. See processSpan's
	// doc comment.
	fileSize, err := f.Size()
	if err != nil {
		return Span{}, fmt.Errorf("verifier: stat before uncovered read [%d, %d): %w", lo, hi, err)
	}
	// scanAllZero reads LockMode itself: LockCallerHeld scans unlocked, because
	// the caller's lock already covers this range and a second one on the same
	// Store returns ErrLockBusy -- the same bypass verifyOneRecordSpan makes.
	zero, contended, err := scanAllZero(s, f, opts, lo, hi-lo, fileSize)
	if err != nil {
		return Span{}, fmt.Errorf("verifier: read [%d, %d): %w", lo, hi, err)
	}
	if contended {
		return Span{
			Offset:    lo,
			Length:    hi - lo,
			Coverage:  CoverageContended,
			Contended: ContendedLockBusy,
		}, nil
	}
	if zero {
		return Span{Offset: lo, Length: hi - lo, Coverage: CoverageNone, AllZero: true}, nil
	}

	// Non-zero bytes in a range nothing claimed. Before calling that an
	// anomaly, ask whether the store moved: a write landing between the
	// snapshot and this read leaves records the sweep never saw, and its bytes
	// are what the scan just found.
	recheck, recheckErr := s.Overlapping(lo, hi-lo)
	if recheckErr != nil {
		return Span{}, fmt.Errorf("verifier: recheck [%d, %d): %w", lo, hi, recheckErr)
	}
	if len(recheck) > 0 {
		// Report it unverified rather than judging it. Re-dispatching the whole
		// span to a record covering only part of it reports the rest as covered
		// -- clean, if that record verifies -- and calling two disjoint records
		// CoverageMany invents an overlap that is not there.
		return Span{
			Offset:    lo,
			Length:    hi - lo,
			Coverage:  CoverageContended,
			Contended: ContendedStoreChanged,
			Entries:   recheck,
		}, nil
	}
	// Still genuinely uncovered -- a real anomaly.
	return Span{Offset: lo, Length: hi - lo, Coverage: CoverageNone, AllZero: false}, nil
}

// logSpan emits a trace entry for span. Anomalies go at Warn, normal
// spans at Debug.
func logSpan(log *zap.Logger, span Span) {
	fields := []zap.Field{
		zap.Int64("offset", span.Offset),
		zap.Int64("length", span.Length),
		zap.String("coverage", span.Coverage.String()),
	}
	if span.Header != nil {
		fields = append(fields,
			zap.Int32("worker", span.Header.WorkerID),
			zap.Uint64("cycle", span.Header.Cycle),
			zap.String("ioType", fileops.IOType(span.Header.Tag&block.TagIOTypeMask).String()),
			zap.Bool("fsynced", span.Header.Tag&block.TagFsynced != 0),
			zap.String("verdict", span.Verdict.String()),
		)
	}
	anomaly := span.Coverage == CoverageMany ||
		(span.Coverage == CoverageOne && span.Verdict != block.VerdictOK) ||
		(span.Coverage == CoverageNone && !span.AllZero)
	if anomaly {
		log.Warn("span anomaly", fields...)
	} else {
		log.Debug("span ok", fields...)
	}
}

// verifyOneRecordSpan verifies the single record covering [lo, hi). Unless
// opts.LockMode is LockCallerHeld, it acquires a shared lock over the full
// record extent before reading, so the verifier never observes a partial write
// in progress.
//
// Callers that already hold a lock covering [lo, hi) (e.g. soakVerifyRangeOp,
// which pre-acquires a shared lock over the whole verification range) must
// pass LockCallerHeld. Acquiring a second shared lock on the same Store returns
// ErrLockBusy: TryAcquireShared's non-blocking in-process rangeLockTable
// rejects a same-process conflict immediately rather than blocking on it.
func verifyOneRecordSpan(s *xattrstore.Store, f *fileops.File, opts Options, rec xattrstore.Entry, entries []xattrstore.Entry, lo, hi int64) (_ Span, retErr error) {
	unlock := func() error { return nil }
	var locked bool
	if opts.LockMode != LockCallerHeld {
		// trySharedLock is defined in verifier_lock_linux.go / verifier_lock_other.go.
		// It returns (unlock, contended, locked, err). On non-Linux, locked is
		// always false -- locking isn't implemented there (see Span.Locked).
		var contended bool
		var err error
		unlock, contended, locked, err = trySharedLock(s, f.LockFd(), rec.Offset, rec.Length)
		if err != nil {
			return Span{}, fmt.Errorf("verifier: lock [%d, %d): %w",
				rec.Offset, rec.Offset+rec.Length, err)
		}
		if contended {
			return Span{
				Offset:    lo,
				Length:    hi - lo,
				Coverage:  CoverageContended,
				Contended: ContendedLockBusy,
				Entries:   entries,
			}, nil
		}
	}
	// Surface a failed release rather than dropping it: an ambiguous release
	// leaks the range guard for the life of the Store, and the ErrLockBusy that
	// every later acquire then gets is treated as benign contention upstream.
	// Soak reaches verifyOneRecordSpan with LockCallerHeld (it holds its own
	// lease), so in practice this guards the `iotest verify` path, where
	// LockShared is the default.
	defer func() {
		if rerr := unlock(); rerr != nil {
			// Logged before combining, the way Writer.WriteBlock does it, for
			// immediate visibility in the log stream even though
			// ReleaseErrorOrCause also now always propagates a release
			// failure to the caller.
			if opts.Log != nil {
				opts.Log.Error("lock release failed",
					zap.Int64("offset", rec.Offset),
					zap.Int64("length", rec.Length),
					zap.Error(rerr),
				)
			}
			retErr = xattrstore.ReleaseErrorOrCause(
				fmt.Errorf("verifier: release [%d, %d): %w",
					rec.Offset, rec.Offset+rec.Length, rerr),
				retErr)
		}
	}()

	// Re-read the header under the lock rather than trusting the pre-lock
	// snapshot: a shared lock excludes any writer that takes an exclusive one
	// (LockNone writers excepted), so the (header, body) pair is stable here,
	// while a stale snapshot header reports a benign reseed as BODY_CORRUPT.
	rawHeader, getErr := s.Get(rec.Offset, rec.Length)
	if errors.Is(getErr, xattrstore.ErrNotFound) {
		// The record was removed or replaced between the snapshot and our lock —
		// a benign race under concurrent writes; nothing claims this extent now.
		return Span{
			Offset:    lo,
			Length:    hi - lo,
			Coverage:  CoverageContended,
			Contended: ContendedStoreChanged,
			Entries:   entries,
		}, nil
	}
	if getErr != nil {
		return Span{}, fmt.Errorf("verifier: re-read header at %d: %w", rec.Offset, getErr)
	}

	// Unmarshal the header and check it first, then read the body and verify it
	// against that header. The two arms below reach a verdict from the header
	// alone, so the body is never read for them.
	h, hdrErr := block.UnmarshalHeader(rawHeader)
	var verdict block.Verdict
	var diag *block.Diagnosis
	var buf []byte
	// Only meaningful once the header parsed; the hdrErr arm below runs first.
	var selfCheck block.Verdict
	if hdrErr == nil {
		selfCheck = block.RecordSelfCheck(&h, rec.Offset, rec.Length)
	}
	switch {
	case hdrErr != nil:
		verdict = block.VerdictForHeaderError(hdrErr)

	// The xattr NAME carries the record's placement --
	// user.verifyio.<offset>-<length>, parsed into rec.Offset and rec.Length.
	// The header stored under that name repeats both facts, as h.Offset and
	// (via BodyLen) the block's size, so a record can disagree with itself. The
	// header must agree with the name on both axes:
	//
	//   length -- VerifyBlock only rejects a buffer SHORTER than
	//     BlockDataSize(h.BodyLen), so surplus bytes in an over-long record go
	//     unexamined: unchecked, a valid 516-byte record filed under a name
	//     claiming 1 TiB verifies OK and the verifier reports a terabyte as
	//     cleanly covered by one good block.
	//   offset -- both are written by every record, and nothing else
	//     cross-checks them: unchecked, a block's data cloned to a second offset
	//     and re-filed under a name for that offset verifies OK too. This axis
	//     catches metadata contradicting itself; misdirected DATA is caught
	//     separately, because offset is one of DeriveSeed's coordinates, so a
	//     body generated for another offset fails VerifyBlock as BODY_CORRUPT.
	//
	// Neither is visible to VerifyBlock, which sees only buf -- whose length
	// ReadableLen has deliberately clamped to the file, so it cannot recover the
	// claimed size, and which carries no offset at all. The PREDICATE lives in
	// block (RecordSelfCheck) so iotest-dump and iotest-smoke reach the same
	// verdict for the same record; the facts it needs are passed in, because
	// verifyOneRecordSpan is the lowest layer holding all three.
	//
	// Ahead of the read for the same reason it is ahead of it at all: a size
	// this large is exactly what would otherwise drive a file-sized allocation,
	// and a record whose own metadata contradicts itself is not worth reading.
	case selfCheck != block.VerdictOK:
		verdict = selfCheck
		if opts.Log != nil {
			opts.Log.Warn("record contradicts its own header",
				zap.String("verdict", verdict.String()),
				zap.Int64("nameOffset", rec.Offset),
				zap.Uint64("headerOffset", h.Offset),
				zap.Int64("nameSize", rec.Length),
				zap.Uint64("headerBodyLen", h.BodyLen),
				zap.Int64("headerImpliedSize", block.HeaderImpliedSize(&h)),
			)
		}

	default:
		// Stat fresh, under the lock just acquired above, rather than reusing
		// VerifyFile's pre-sweep snapshot: that snapshot goes stale exactly
		// like the pre-lock header snapshot the comment above this switch
		// already refuses to use, and for the same reason -- a size captured
		// before the sweep started can under-clamp the read once the file has
		// since grown, reporting a complete record as VerdictTruncated.
		fileSize, sizeErr := f.Size()
		if sizeErr != nil {
			return Span{}, fmt.Errorf("verifier: stat record at %d: %w", rec.Offset, sizeErr)
		}
		// Read the full record extent (may extend beyond the current span if the
		// entry straddles the range boundary), bounded by what the file can
		// supply. rec.Length arrives from the xattr name and has never been
		// checked against the file, so ReadableLen is what stops a corrupt record
		// from turning this into an unbounded allocation; a record wholly past
		// EOF clamps to an empty buffer, which VerifyBlock reports as
		// VerdictTruncated.
		var n int
		buf = make([]byte, xattrstore.ReadableLen(rec.Offset, rec.Length, fileSize))
		if len(buf) > 0 {
			var readErr error
			n, readErr = f.ReadAt(buf, rec.Offset)
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return Span{}, fmt.Errorf("verifier: read record at %d: %w", rec.Offset, readErr)
			}
		}
		buf = buf[:n]

		var verifyErr error
		verdict, verifyErr = block.VerifyBlock(buf, &h, nil)
		if verifyErr != nil && opts.Log != nil {
			// VerifyBlock's only error is an unrecognized header Kind, which it
			// already reports as VerdictHeadBadFormat -- so the span is not
			// silently wrong without this. Log it anyway: the error names the
			// offending kind value, which the verdict alone does not, and that
			// is what identifies which writer produced it.
			opts.Log.Warn("block header names an unrecognized kind",
				zap.Int64("offset", rec.Offset),
				zap.String("verdict", verdict.String()),
				zap.Error(verifyErr),
			)
		}
		// Only anomalies get the extra forensic pass; OK spans (the common case
		// on a healthy verify sweep) pay nothing beyond VerifyBlock itself.
		if verdict != block.VerdictOK {
			d := block.Diagnose(buf, &h)
			diag = &d
		}
	}

	hCopy := h
	return Span{
		Offset:     lo,
		Length:     hi - lo,
		Coverage:   CoverageOne,
		Entries:    entries,
		Header:     &hCopy,
		Verdict:    verdict,
		XattrMatch: hdrErr == nil,
		Diag:       diag,
		Locked:     locked,
	}, nil
}
