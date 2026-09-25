// This is a unit test.
//
// Coverage: the refuse-guard in zeroShredded -- that the Writer will not zero
// bytes a surviving record still claims, that it writes NOTHING when it refuses,
// that the refusal reaches the log (the only evidence that survives, because
// ReplaceRange has already removed the offending record by then), and that it
// stays inert on the legitimate shred it must not break. Also the INPUT to
// Truncate's fsync gate -- the byte count zeroShredded returns, zero for a full
// reset and non-zero for a straddling record -- by calling zeroShredded
// directly. The gate itself reads that count under a lock policy, and is pinned
// in writer_test.go by the three TestTruncate*Fsync tests, which assert
// fileops.File.Syncs() across a real Writer.Truncate.
//
// The three firing cases are the reproductions from the R5 review, reproduced
// here so a regression is caught by the suite rather than by hand.
package xattrstore

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
)

// wideClaimLen is the extent the corrupt name in the R5 reproductions claims:
// far past EOF, which validRange permits because it checks sign and overflow
// against int64, not against the file.
const wideClaimLen = 40960000

// observedWriter builds a Writer whose log can be inspected, so a test can pin
// that a refusal was reported and not merely returned.
func observedWriter(t *testing.T, s *Store, blockSize int) (*Writer, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.ErrorLevel)
	w, err := NewWriter(WriterConfig{
		File: openWriterTarget(t, s), Store: s, WorkerID: 0,
		Kind: block.KindDecimal, BlockSize: blockSize,
		Locking: LockNone, Log: zap.New(core),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, logs
}

// seedBlocks writes n consecutive blocks from offset 0 and returns the writer.
func seedBlocks(t *testing.T, s *Store, blockSize, n int) *Writer {
	t.Helper()
	w, err := NewWriter(WriterConfig{
		File: openWriterTarget(t, s), Store: s, WorkerID: 0,
		Kind: block.KindDecimal, BlockSize: blockSize, Locking: LockNone,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Truncate(0); err != nil {
		t.Fatalf("reset: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := w.WriteBlock(int64(i)*int64(blockSize), fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	return w
}

// seedWideCorruptClaim plants a valid NAME claiming a huge extent, with a valid
// header value. This is what a misbehaving filesystem produces and what the
// Writer's coherence precondition cannot rule out: extents that overlap.
func seedWideCorruptClaim(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Put(0, wideClaimLen, makeHeader(t, 0, 99)); err != nil {
		t.Fatalf("seed wide claim: %v", err)
	}
}

// verifyBlocks reports how many of the first n blocks still verify. Used to show
// the guard preserved live data rather than merely returning an error.
func verifyBlocks(t *testing.T, s *Store, f *fileops.File, blockSize, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		off := int64(i) * int64(blockSize)
		hdrBytes, err := s.Get(off, int64(blockSize))
		if err != nil {
			continue
		}
		hdr, err := block.UnmarshalHeader(hdrBytes)
		if err != nil {
			continue
		}
		buf := make([]byte, blockSize)
		if _, err := f.ReadAt(buf, off); err != nil {
			continue
		}
		if v, err := block.VerifyBlock(buf, &hdr, nil); err == nil && v == block.VerdictOK {
			ok++
		}
	}
	return ok
}

// TestWriteBlockRefusesToZeroClaimedGap is R5 reproduction 1. A corrupt name
// claiming [0,wideClaimLen) coexists with seven ordinary blocks. Writing block 7
// shreds the wide record -- and the bytes that shred "frees" are the seven live
// blocks. Before the guard, this zeroed all seven and destroyed the anomaly that
// would have explained them.
func TestWriteBlockRefusesToZeroClaimedGap(t *testing.T) {
	const blockSize, blocks = 4096, 7
	s := makeTempStore(t)
	seedBlocks(t, s, blockSize, blocks)
	seedWideCorruptClaim(t, s)

	w, logs := observedWriter(t, s, blockSize)
	err := w.WriteBlock(int64(blocks)*int64(blockSize), fileops.IOTypeBuffered)
	if !errors.Is(err, ErrExtentOverlap) {
		t.Fatalf("WriteBlock err = %v, want ErrExtentOverlap", err)
	}

	// The point of refusing: the live blocks are still there.
	f := openWriterTarget(t, s)
	if got := verifyBlocks(t, s, f, blockSize, blocks); got != blocks {
		t.Errorf("%d of %d blocks still verify; the guard is supposed to leave them alone", got, blocks)
	}

	// Refinement 2: the error is the only surviving evidence, so it must be
	// logged. ReplaceRange already removed the corrupt record, so the store is
	// coherent again and no later sweep can rediscover this.
	if logs.FilterMessageSnippet("refusing to zero shredded bytes").Len() == 0 {
		t.Error("the refusal was returned but never logged; a later sweep cannot rediscover it")
	}
}

// TestTruncateRefusesToZeroClaimedGap is R5 reproduction 2: a no-op Truncate at
// the current EOF. RemoveCovering drops the wide claim, whose surviving extent
// below EOF is every block in the file.
func TestTruncateRefusesToZeroClaimedGap(t *testing.T) {
	const blockSize, blocks = 4096, 8
	s := makeTempStore(t)
	w := seedBlocks(t, s, blockSize, blocks)
	seedWideCorruptClaim(t, s)

	size := int64(blocks) * int64(blockSize)
	if err := w.Truncate(size); !errors.Is(err, ErrExtentOverlap) {
		t.Fatalf("Truncate(%d) err = %v, want ErrExtentOverlap", size, err)
	}

	f := openWriterTarget(t, s)
	if got := verifyBlocks(t, s, f, blockSize, blocks); got != blocks {
		t.Errorf("%d of %d blocks still verify after a refused truncate", got, blocks)
	}
}

// TestWriteBlockRefusalWritesNothing pins the ordering the guard depends on: the
// check pass completes before the write pass.
//
// The gap ORDER is what makes this a real test rather than a restatement of the
// case above. Gaps are emitted before-then-after per removed extent, so the
// layout below puts an unclaimed gap FIRST and the claimed one second. A
// check-as-you-go loop returns the same error, but only after zeroing the first
// gap -- so this fails on the damage rather than on the verdict. Put the claimed
// gap first and the mutant survives, which is exactly what happened to the first
// version of this test.
func TestWriteBlockRefusalWritesNothing(t *testing.T) {
	const blockSize = 1024
	s := makeTempStore(t)

	// One live block high up, nothing below it: [0,1024) is genuinely unclaimed,
	// and the block at 4096 is what makes the LATER gap claimed.
	w := seedBlocks(t, s, blockSize, 0)
	if err := w.WriteBlock(4096, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock at 4096: %v", err)
	}

	// Recognisable bytes in the unclaimed low gap. Without them it is a sparse
	// hole, and zeroing a hole is invisible -- the mutant would survive again.
	f := openWriterTarget(t, s)
	filler := make([]byte, blockSize)
	for i := range filler {
		filler[i] = 0xAB
	}
	if err := f.Write(fileops.IOTypeBuffered, filler, 0); err != nil {
		t.Fatalf("seed filler: %v", err)
	}

	if err := s.Put(0, wideClaimLen, makeHeader(t, 0, 7)); err != nil {
		t.Fatalf("seed wide claim: %v", err)
	}

	// Writing at 1024 shreds the wide claim, yielding [0,1024) -- unclaimed, so
	// safe to zero -- and then [2048,EOF), which the block at 4096 still claims.
	w2, _ := observedWriter(t, s, blockSize)
	if err := w2.WriteBlock(1024, fileops.IOTypeBuffered); !errors.Is(err, ErrExtentOverlap) {
		t.Fatalf("WriteBlock err = %v, want ErrExtentOverlap", err)
	}

	after := make([]byte, blockSize)
	if _, err := f.ReadAt(after, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(after) != string(filler) {
		t.Error("the unclaimed gap was zeroed before the refusal; the check pass did not complete before the write pass")
	}
}

// TestWriteBlockStillShredsUnclaimedGap is the inert case, and the one that
// matters most: the guard must not break the legitimate shred. Rewriting a
// 4096-byte region with a 1024-byte block frees [1024,4096), nothing claims it,
// and it must still be zeroed. A guard that fired here would break the shipped
// changed-blocksize workflow.
func TestWriteBlockStillShredsUnclaimedGap(t *testing.T) {
	s := makeTempStore(t)
	seedBlocks(t, s, 4096, 1)

	// Seed recognisable non-zero bytes across the region so a missed zero-fill
	// is visible rather than coincidentally already zero.
	f := openWriterTarget(t, s)
	filler := make([]byte, 4096)
	for i := range filler {
		filler[i] = 0xAB
	}
	if err := f.Write(fileops.IOTypeBuffered, filler, 0); err != nil {
		t.Fatalf("seed filler: %v", err)
	}

	w, logs := observedWriter(t, s, 1024)
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if n := logs.Len(); n != 0 {
		t.Errorf("the legitimate shred logged %d error(s); the guard is supposed to be inert here", n)
	}

	freed := make([]byte, 3072)
	if _, err := f.ReadAt(freed, 1024); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	for i, b := range freed {
		if b != 0 {
			t.Fatalf("byte %d of the freed range is %#x, want 0; the shred zero-fill did not run", 1024+i, b)
		}
	}
}

// TestWriteBlockShredKeepsTheBlockItJustWrote closes the gap the existing shred
// tests leave: they assert the resulting extent set and that the FREED bytes are
// zero, and nothing asserts the KEPT bytes are still a valid block.
//
// That gap let six independent over-zeroing mutations survive the whole suite --
// keep-length forced to 0, min/max swapped in either gap bound, keepOffset
// ignored, keepEnd ignored, and keepEnd computed without keepLen. Each of them
// zeroes the block WriteBlock just wrote, and each turns a clean write into
// VerdictBodyCorrupt on the next sweep: a false FAIL manufactured by the tool's
// own writer, with the same observable signature as a real stale read.
//
// One probe covers all six, because they all fail the same way: re-read the
// surviving record and verify it.
func TestWriteBlockShredKeepsTheBlockItJustWrote(t *testing.T) {
	s := makeTempStore(t)

	// A wide block first, then a narrow one inside it, so the shred has a gap on
	// BOTH sides of the kept range -- the layout that exercises every bound.
	seedBlocks(t, s, 4096, 1)

	w, err := NewWriter(WriterConfig{
		File: openWriterTarget(t, s), Store: s, WorkerID: 0,
		Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	const keep = int64(1024)
	if err := w.WriteBlock(keep, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	hdrBytes, err := s.Get(keep, 1024)
	if err != nil {
		t.Fatalf("Get the surviving record: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}

	f := openWriterTarget(t, s)
	buf := make([]byte, 1024)
	if _, err := f.ReadAt(buf, keep); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	verdict, err := block.VerifyBlock(buf, &hdr, nil)
	if err != nil {
		t.Fatalf("VerifyBlock: %v", err)
	}
	if verdict != block.VerdictOK {
		t.Errorf("the block WriteBlock just wrote verifies as %v, want OK -- the shred zero-fill "+
			"reached into the kept range and manufactured a false FAIL", verdict)
	}
}

// TestNewWriterRejectsMismatchedFileAndStore pins the same-inode check. The
// Writer holds two independent descriptors and nothing else compares them, so a
// caller that pairs the wrong two gets metadata aimed at another file's data --
// silently, with every individual operation succeeding.
func TestNewWriterRejectsMismatchedFileAndStore(t *testing.T) {
	a, b := makeTempStore(t), makeTempStore(t)

	t.Run("mismatched is refused", func(t *testing.T) {
		if _, err := NewWriter(WriterConfig{
			File: openWriterTarget(t, a), Store: b, WorkerID: 0,
			Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone,
		}); err == nil {
			t.Error("NewWriter accepted a File and Store on different files")
		}
	})

	// The control case: the check must not reject the normal pairing, which is
	// two separate opens of the SAME path -- different descriptions, one inode.
	t.Run("same file via separate opens is accepted", func(t *testing.T) {
		if _, err := NewWriter(WriterConfig{
			File: openWriterTarget(t, a), Store: a, WorkerID: 0,
			Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone,
		}); err != nil {
			t.Errorf("NewWriter rejected two opens of one file: %v", err)
		}
	})
}

// TestWriteBlockIntentBeforeData pins the ordering WriteBlock's own comment
// declares in capital letters ("do NOT reorder these") and that nothing enforced:
// reversing ReplaceRange and file.Write survived all 16 tests in this package.
//
// The two orders fail in opposite directions, which is the whole point. Correct
// order leaves an xattr intent with no data -- a verifier flags it and can
// recover. Reversed leaves data with no record claiming it, which the same
// comment calls unrecoverable.
//
// No fault injection needed: fileops.Write implements only IOTypeBuffered and
// returns ErrIOTypeNotSupported for the rest, so IOTypeODirect fails the data
// write on demand while leaving the xattr step untouched. That turns the
// documented residual into an assertion for free.
func TestWriteBlockIntentBeforeData(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{
		File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal,
		BlockSize: 4096, Locking: LockNone,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// makeTempStore seeds the file with placeholder bytes, so the baseline is
	// whatever length it already had -- not zero.
	sizeBefore, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}

	err = w.WriteBlock(0, fileops.IOTypeODirect)
	if !errors.Is(err, fileops.ErrIOTypeNotSupported) {
		t.Fatalf("WriteBlock err = %v, want ErrIOTypeNotSupported", err)
	}

	// The intent must be on disk: the xattr went down BEFORE the write that
	// failed. If the order were reversed there would be no record here.
	if _, err := s.Get(0, 4096); err != nil {
		t.Errorf("no xattr intent after a failed data write (%v); the xattr is supposed to "+
			"be written first, so a crashed write is flaggable rather than invisible", err)
	}

	// And the data really did not land, so this is the intent-with-no-data state
	// rather than a write that quietly succeeded.
	sizeAfter, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if sizeAfter != sizeBefore {
		t.Errorf("file size went %d -> %d; the data write was supposed to fail", sizeBefore, sizeAfter)
	}
}

// TestTruncateResetSkipsFsync pins Decision 4's gate. Truncate(0) is the reset
// both shipped tools call on every invocation: it removes records and writes no
// zeros, so it must not pay a whole-file fsync. Gating on len(removed) instead
// of on bytes written would add one to every reset of a multi-GiB target.
//
// Asserted through zeroShredded's return value, which is what the gate reads.
func TestTruncateResetSkipsFsync(t *testing.T) {
	const blockSize, blocks = 1024, 4
	s := makeTempStore(t)
	w := seedBlocks(t, s, blockSize, blocks)

	removed, err := s.RemoveCovering(0)
	if err != nil {
		t.Fatalf("RemoveCovering: %v", err)
	}
	if len(removed) != blocks {
		t.Fatalf("removed %d records, want %d", len(removed), blocks)
	}
	if err := w.file.Truncate(0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	written, err := w.zeroShredded(removed, 0, 0)
	if err != nil {
		t.Fatalf("zeroShredded: %v", err)
	}
	if written != 0 {
		t.Errorf("a full reset wrote %d zero bytes, want 0 -- the fsync gate would fire for nothing", written)
	}
}

// TestTruncatePartialReportsBytesWritten is the other side of the gate: a
// non-aligned truncate leaves a surviving prefix that IS zeroed, so the fsync is
// owed and the byte count has to be non-zero for the gate to fire.
func TestTruncatePartialReportsBytesWritten(t *testing.T) {
	const blockSize = 4096
	s := makeTempStore(t)
	w := seedBlocks(t, s, blockSize, 2)

	// Cut mid-block: the record straddling 5120 is removed whole, so [4096,5120)
	// survives the truncate unclaimed and must be zeroed.
	removed, err := s.RemoveCovering(5120)
	if err != nil {
		t.Fatalf("RemoveCovering: %v", err)
	}
	if err := w.file.Truncate(5120); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	written, err := w.zeroShredded(removed, 5120, 0)
	if err != nil {
		t.Fatalf("zeroShredded: %v", err)
	}
	if written != 1024 {
		t.Errorf("zeroed %d bytes, want 1024 (the surviving prefix of the straddling record)", written)
	}
}
