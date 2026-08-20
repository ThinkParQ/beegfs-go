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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

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

// LockMode controls whether VerifyFile acquires OFD shared locks before
// reading each CoverageOne span's body.
type LockMode int

const (
	// LockShared acquires a shared OFD lock on each CoverageOne span before
	// reading its body. Writers that hold exclusive OFD locks while writing
	// (as Writer.WriteBlock does) guarantee the verifier never observes a
	// partial write. If the lock is contended, the span is reported as
	// CoverageContended and the body is not read.
	//
	// This is the zero value of LockMode, so Options{} uses LockShared by
	// default.
	//
	// On non-Linux platforms (OFD locks are Linux-only), the underlying
	// trySharedLock is a no-op that always reports success -- see
	// verifier_lock_other.go. LockShared is accepted there without error,
	// but the torn-write guarantee above does not hold: verification
	// proceeds with no locking at all, identical to LockNone.
	LockShared LockMode = iota

	// LockNone bypasses all per-span locking in oneSpan. Callers must use
	// this if they already hold a lock covering the verification range —
	// see oneSpan for why a redundant lock is unsafe in LockModePOSIX.
	LockNone
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
	// LockMode controls OFD locking before each body read. Defaults to
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
}

// storedEntry holds an xattrstore.Entry discovered during the pre-lock snapshot.
// The header bytes are deliberately NOT cached here: oneSpan re-reads the header
// under the shared lock so the body is verified against a header consistent with
// it, avoiding a false BODY_CORRUPT verdict on a benign concurrent reseed.
type storedEntry struct {
	entry xattrstore.Entry
}

// VerifyFile walks the file and calls fn once per non-overlapping span,
// in ascending offset order. The spans cover [0, fileSize) unless
// opts.Range restricts the range.
//
// fn is called synchronously. If fn returns a non-nil error, VerifyFile
// stops immediately and returns that error.
func VerifyFile(s *xattrstore.Store, f *fileops.File, opts Options, fn func(Span) error) error {
	info, err := os.Stat(s.Target())
	if err != nil {
		return fmt.Errorf("verifier.VerifyFile: stat %s: %w", s.Target(), err)
	}
	fileSize := info.Size()

	// Effective range: clamp the requested range to [0, fileSize).
	rangeStart, rangeEnd := int64(0), fileSize
	if opts.Range != nil {
		if opts.Range.Offset > rangeStart {
			rangeStart = opts.Range.Offset
		}
		if end := opts.Range.Offset + opts.Range.Length; end < rangeEnd {
			rangeEnd = end
		}
	}
	if rangeStart >= rangeEnd {
		return nil
	}

	// Snapshot which xattr entries exist (offset/length only). The header is
	// re-read per span under the shared lock in oneSpan, so we intentionally do
	// not cache the header bytes here.
	var entries []storedEntry
	if err := s.ForEachEntry(func(offset, length int64, _ []byte) error {
		entries = append(entries, storedEntry{
			entry: xattrstore.Entry{
				Offset: offset,
				Length: length,
				Name:   xattrstore.XAttrName(offset, length),
			},
		})
		return nil
	}); err != nil {
		return fmt.Errorf("verifier.VerifyFile: listing entries: %w", err)
	}

	// Build the sweep-line boundary set: range endpoints plus every entry
	// start and end. Including all entry boundaries (even those outside the
	// effective range) ensures spans inside the range align with entry edges.
	posSet := map[int64]struct{}{rangeStart: {}, rangeEnd: {}}
	for _, e := range entries {
		posSet[e.entry.Offset] = struct{}{}
		posSet[e.entry.Offset+e.entry.Length] = struct{}{}
	}
	positions := make([]int64, 0, len(posSet))
	for p := range posSet {
		positions = append(positions, p)
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })

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

		var covering []storedEntry
		for j := range entries {
			e := &entries[j]
			if e.entry.Offset < hi && e.entry.Offset+e.entry.Length > lo {
				covering = append(covering, *e)
			}
		}

		span, err := processSpan(s, f, opts, covering, lo, hi)
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
	return nil
}

// processSpan classifies [lo, hi) and verifies it based on coverage.
func processSpan(s *xattrstore.Store, f *fileops.File, opts Options, covering []storedEntry, lo, hi int64) (Span, error) {
	public := make([]xattrstore.Entry, len(covering))
	for i, se := range covering {
		public[i] = se.entry
	}

	switch len(covering) {
	case 0:
		return noneSpan(s, f, opts, lo, hi)
	case 1:
		return oneSpan(s, f, opts, covering[0], public, lo, hi)
	default:
		return Span{
			Offset:   lo,
			Length:   hi - lo,
			Coverage: CoverageMany,
			Entries:  public,
		}, nil
	}
}

// noneSpan reads [lo, hi) and reports whether all bytes are zero.
//
// Unlike oneSpan -- which holds a lock across its whole read specifically
// because its pre-lock header snapshot can go stale -- noneSpan has no
// record, and therefore nothing to lock, at the moment VerifyFile's
// snapshot was taken. So if a writer's Put (and its data write) lands in
// this genuinely-uncovered range after the snapshot but before this read
// runs, a non-zero read here is an ordinary successful write racing the
// scan, not corruption. Before reporting a hard anomaly, noneSpan rechecks
// for a record that wasn't in the snapshot and, if one now exists,
// reclassifies against the fresh data -- the same dispatch VerifyFile's
// initial scan would have used had it seen the record in time.
func noneSpan(s *xattrstore.Store, f *fileops.File, opts Options, lo, hi int64) (Span, error) {
	buf := make([]byte, hi-lo)
	n, err := f.ReadAt(buf, lo)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return Span{}, fmt.Errorf("verifier: read [%d, %d): %w", lo, hi, err)
	}
	buf = buf[:n]
	allZero := true
	for _, b := range buf {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return Span{Offset: lo, Length: hi - lo, Coverage: CoverageNone, AllZero: true}, nil
	}

	recheck, recheckErr := s.Overlapping(lo, hi-lo)
	if recheckErr != nil {
		return Span{}, fmt.Errorf("verifier: recheck [%d, %d): %w", lo, hi, recheckErr)
	}
	switch len(recheck) {
	case 0:
		// Still genuinely uncovered -- a real anomaly.
		return Span{Offset: lo, Length: hi - lo, Coverage: CoverageNone, AllZero: false}, nil
	case 1:
		return oneSpan(s, f, opts, storedEntry{entry: recheck[0]}, recheck, lo, hi)
	default:
		return Span{Offset: lo, Length: hi - lo, Coverage: CoverageMany, Entries: recheck}, nil
	}
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

// oneSpan verifies the single record covering [lo, hi). Unless
// opts.LockMode is LockNone, it acquires a shared lock over the full record
// extent before reading, so the verifier never observes a partial write in
// progress.
//
// Callers that already hold a lock covering [lo, hi) (e.g. soakVerifyRangeOp,
// which pre-acquires a shared lock over the whole verification range) must
// pass LockNone. Acquiring a second shared lock on the same Store would be
// redundant in LockModeOFD and self-deadlocks in LockModePOSIX, where
// TryAcquireShared takes the Store's non-reentrant in-process mutex.
func oneSpan(s *xattrstore.Store, f *fileops.File, opts Options, se storedEntry, entries []xattrstore.Entry, lo, hi int64) (Span, error) {
	unlock := func() error { return nil }
	if opts.LockMode != LockNone {
		// trySharedLock is defined in verifier_lock_linux.go / verifier_lock_other.go.
		// It returns (unlock, contended, err). On non-Linux it is a no-op.
		var contended bool
		var err error
		unlock, contended, err = trySharedLock(s, f.LockFd(), se.entry.Offset, se.entry.Length)
		if err != nil {
			return Span{}, fmt.Errorf("verifier: lock [%d, %d): %w",
				se.entry.Offset, se.entry.Offset+se.entry.Length, err)
		}
		if contended {
			return Span{
				Offset:   lo,
				Length:   hi - lo,
				Coverage: CoverageContended,
				Entries:  entries,
			}, nil
		}
	}
	defer func() { _ = unlock() }()

	// Re-read the header UNDER the shared lock rather than using the pre-lock
	// snapshot. The writer holds an exclusive lock across xattr-Put + data-write,
	// so while we hold the shared lock the (header, body) pair is stable and
	// mutually consistent. Verifying the body against a stale snapshot header
	// would report a benign concurrent reseed (new body, old captured header) as
	// BODY_CORRUPT.
	headerBytes, getErr := s.Get(se.entry.Offset, se.entry.Length)
	if errors.Is(getErr, xattrstore.ErrNotFound) {
		// The record was removed or replaced between the snapshot and our lock —
		// a benign race under concurrent writes; nothing claims this extent now.
		return Span{
			Offset:   lo,
			Length:   hi - lo,
			Coverage: CoverageContended,
			Entries:  entries,
		}, nil
	}
	if getErr != nil {
		return Span{}, fmt.Errorf("verifier: re-read header at %d: %w", se.entry.Offset, getErr)
	}

	// Read the full record extent (may extend beyond the current span if
	// the entry straddles the range boundary).
	buf := make([]byte, se.entry.Length)
	n, readErr := f.ReadAt(buf, se.entry.Offset)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return Span{}, fmt.Errorf("verifier: read record at %d: %w", se.entry.Offset, readErr)
	}
	buf = buf[:n]

	// Unmarshal the header read above and use it to verify the on-disk block.
	h, hdrErr := block.UnmarshalHeader(headerBytes)
	var verdict block.Verdict
	var diag *block.Diagnosis
	if hdrErr != nil {
		switch {
		case errors.Is(hdrErr, block.ErrBadMagic):
			verdict = block.VerdictHeadBadMagic
		case errors.Is(hdrErr, block.ErrBadVersion):
			verdict = block.VerdictHeadBadFormat
		case errors.Is(hdrErr, block.ErrTruncated):
			verdict = block.VerdictTruncated
		default:
			verdict = block.VerdictHeadBadCRC
		}
	} else {
		verdict, _ = block.VerifyBlock(buf, &h, nil)
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
	}, nil
}
