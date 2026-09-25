// This is a unit test.
//
// Coverage: Writer.WriteBlock's intent-before-data ordering (the xattr
// header and on-disk body agree after a write), and the LockExclusive path's
// platform-conditional fsync + TagFsynced re-stamp -- Linux only, since
// lockRegion is a stub returning "not supported" on other platforms (see
// writer_lock_other.go). Also the two data/xattr coherence operations Writer
// owns because it is the only type holding both halves: WriteBlock's
// replace-and-shred (a narrower write shreds the wider record rather than
// coexisting with it) plus the zeroing of what shredding freed, and
// Truncate's removal of records covering the truncated-away region --
// including a record straddling the new size, and size 0 as a full purge. Also
// that removal and the lock bound both select by EXTENT rather than by header,
// so a record with a valid name and a wrong-sized value is neither left behind
// by a reset nor excluded from what a truncate may touch, plus the documented
// residual that an unparseable NAME survives (it has no placeable extent).
package xattrstore

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

// openWriterTarget opens s's target file via fileops for use with a Writer.
func openWriterTarget(t *testing.T, s *Store) *fileops.File {
	t.Helper()
	f, err := fileops.Open(s.Target(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("fileops.Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestWriteBlockWritesConsistentDataAndXattr(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 7, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const offset = 4096
	if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	hdrBytes, err := s.Get(offset, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	if hdr.WorkerID != 7 {
		t.Errorf("WorkerID = %d, want 7", hdr.WorkerID)
	}
	if hdr.Offset != uint64(offset) {
		t.Errorf("Offset = %d, want %d", hdr.Offset, offset)
	}
	if hdr.Tag&block.TagFsynced != 0 {
		t.Errorf("Tag has TagFsynced set, want unset (LockNone never syncs)")
	}

	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	verdict, err := block.VerifyBlock(buf, &hdr, nil)
	if err != nil {
		t.Fatalf("VerifyBlock: %v", err)
	}
	if verdict != block.VerdictOK {
		t.Errorf("VerifyBlock verdict = %v, want VerdictOK", verdict)
	}
}

// TestWriteBlockTakeLockStampsFsyncedTag covers WriteBlock's LockExclusive
// path: the fsync + TagFsynced re-stamp only happens when a lock is taken,
// and the read-back check that follows must not false-positive on the
// ordinary single-writer case.
func TestWriteBlockTakeLockStampsFsyncedTag(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 3, Kind: block.KindZeros, BlockSize: 4096, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const offset = 0
	err = w.WriteBlock(offset, fileops.IOTypeBuffered)
	if runtime.GOOS != "linux" {
		if err == nil {
			t.Fatalf("WriteBlock under LockExclusive: got nil error on %s, want lockRegion's not-supported error", runtime.GOOS)
		}
		return
	}
	if err != nil {
		t.Fatalf("WriteBlock under LockExclusive: %v", err)
	}

	hdrBytes, err := s.Get(offset, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	if hdr.Tag&block.TagFsynced == 0 {
		t.Errorf("Tag = %#x, want TagFsynced set after a LockExclusive write", hdr.Tag)
	}
}

// TestWriteBlockFsyncedTagImpliesAnActualFsync pins the tag to the flush rather
// than to the code that performs it.
//
// The test above asserts only the re-stamp, so removing w.file.Sync() while
// leaving the stamp in place used to survive the whole suite -- and that mutant
// is the one that matters. TagFsynced is what tells a verifier a BODY_CORRUPT
// block really did reach the storage servers, so a stamp with no flush behind it
// makes the verifier report a coherence failure against the filesystem for what
// is actually this tool's own missing flush: the tool blaming BeeGFS for its own
// bug, in the one output it exists to produce.
//
// fileops.File.Syncs() counts successful flushes, so the assertion is on the
// flush having happened, not on the call still being written down.
func TestWriteBlockFsyncedTagImpliesAnActualFsync(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("LockExclusive needs lockRegion, which is a stub on %s", runtime.GOOS)
	}
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 3, Kind: block.KindZeros,
		BlockSize: 4096, Locking: LockExclusive, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	before := f.Syncs()
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	after := f.Syncs()

	hdrBytes, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}

	if stamped := hdr.Tag&block.TagFsynced != 0; stamped != (after > before) {
		t.Errorf("TagFsynced stamped = %v but Syncs went %d -> %d; the tag and the flush disagree",
			stamped, before, after)
	}
}

// TestWriteBlockNoLockNoFsync is the other half: under LockNone there is no
// flush, so the tag must be clear. Without this the test above is satisfiable by
// always syncing and always stamping.
func TestWriteBlockNoLockNoFsync(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 3, Kind: block.KindZeros,
		BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	before := f.Syncs()
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if after := f.Syncs(); after != before {
		t.Errorf("Syncs went %d -> %d under LockNone; nothing should flush there", before, after)
	}

	hdrBytes, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	if hdr.Tag&block.TagFsynced != 0 {
		t.Errorf("Tag = %#x has TagFsynced set with no flush behind it", hdr.Tag)
	}
}

// seedForTruncateFsync builds a Writer over a File the test still holds, so
// Syncs() can be read, and writes two blocks from offset 0.
//
// Callers snapshot Syncs() AFTER this returns: under LockExclusive WriteBlock
// flushes on its own, and it is Truncate's gate that is under test here.
func seedForTruncateFsync(t *testing.T, locking LockPolicy, blockSize int) (*Writer, *fileops.File) {
	t.Helper()
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{
		File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal,
		BlockSize: blockSize, Locking: locking,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Truncate(0); err != nil {
		t.Fatalf("reset: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := w.WriteBlock(int64(i)*int64(blockSize), fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	return w, f
}

// TestTruncateZeroingImpliesAnActualFsync is the Truncate counterpart to
// TestWriteBlockFsyncedTagImpliesAnActualFsync above, and exists for the same
// reason: the shredguard tests pin what zeroShredded RETURNS, which is the
// gate's input, so deleting w.file.Sync() from Truncate survived the whole
// suite.
//
// The stake is in Truncate's own comment at the gate. Without the flush the
// zeros sit in this node's page cache while the record removals travel the
// metadata path, so another node reads the old block's bytes in a span nothing
// claims -- the tool reporting a coherence failure against the filesystem for
// its own missing flush, in the one output it exists to produce.
func TestTruncateZeroingImpliesAnActualFsync(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("LockExclusive needs lockRegion, which is a stub on %s", runtime.GOOS)
	}
	const blockSize = 4096
	w, f := seedForTruncateFsync(t, LockExclusive, blockSize)

	// Cut mid-block: the record straddling 5120 is removed whole, so [4096,5120)
	// survives unclaimed and is zeroed -- written > 0, and the fsync is owed.
	before := f.Syncs()
	if err := w.Truncate(5120); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if after := f.Syncs(); after == before {
		t.Errorf("Truncate zeroed a surviving prefix under LockExclusive but Syncs stayed at %d; "+
			"the zeros can sit in this node's page cache while the record removals are already visible elsewhere", before)
	}
}

// TestTruncateNoLockNoFsync pins the other half of the gate's condition: the
// flush belongs to the lock policy that promises cross-node visibility, so a
// LockNone Writer -- which declares it is the sole writer, or that the caller
// provides its own exclusion -- owes nothing.
func TestTruncateNoLockNoFsync(t *testing.T) {
	const blockSize = 4096
	w, f := seedForTruncateFsync(t, LockNone, blockSize)

	before := f.Syncs()
	if err := w.Truncate(5120); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if after := f.Syncs(); after != before {
		t.Errorf("Syncs went %d -> %d under LockNone; the truncate fsync is owed only where the lock promises cross-node visibility", before, after)
	}
}

// TestTruncateResetDoesNotFsync pins the reason the gate reads `written > 0`
// rather than `len(removed) > 0`, which Truncate's comment states and nothing
// enforced: Truncate(0) is the reset both tools call on every invocation, and it
// routinely removes records while writing no zeros at all. Gating on the record
// count would add a whole-file fsync to every reset of a multi-GiB target for no
// benefit.
func TestTruncateResetDoesNotFsync(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("LockExclusive needs lockRegion, which is a stub on %s", runtime.GOOS)
	}
	const blockSize = 4096
	w, f := seedForTruncateFsync(t, LockExclusive, blockSize)

	before := f.Syncs()
	if err := w.Truncate(0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if after := f.Syncs(); after != before {
		t.Errorf("a full reset under LockExclusive synced (%d -> %d); it removes records but zeroes nothing, so the gate must not fire", before, after)
	}
}

// entryExtents returns every record's extent, sorted, for exact assertions.
func entryExtents(t *testing.T, s *Store) [][2]int64 {
	t.Helper()
	var out [][2]int64
	if err := s.ForEachEntry(func(offset, length int64, _ []byte) error {
		out = append(out, [2]int64{offset, length})
		return nil
	}); err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// TestWriteBlockShredsWiderRecordAndZeroesRemainder pins replace-and-shred on
// the write path, plus its data-side half.
//
// WriteBlock used Store.Put, which is keyed by (offset, length) and so only
// replaces a record of the identical extent. Writing the same offset at a
// smaller block size left the old, wider record in place beside the new one,
// and the verifier reported the doubly-claimed bytes as CoverageMany -- which
// reads as the filesystem allowing two writers to claim one range, the most
// alarming thing this tool can report, for what was really just a changed
// --blocksize between runs.
//
// The freed tail must also be zeroed: shredding a [0,4096) record to write
// [0,1024) leaves [1024,4096) claimed by nothing while still physically
// holding the old block, which reads as stale data in a span that says it was
// never written.
func TestWriteBlockShredsWiderRecordAndZeroesRemainder(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	wideW, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(4096): %v", err)
	}
	if err := wideW.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(4096): %v", err)
	}
	if got := entryExtents(t, s); len(got) != 1 || got[0] != [2]int64{0, 4096} {
		t.Fatalf("after wide write: extents = %v, want [[0 4096]]", got)
	}

	// Re-write offset 0 at a quarter of the block size.
	narrowW, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(1024): %v", err)
	}
	if err := narrowW.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(1024): %v", err)
	}

	// Exactly one record, the new one: the wider record was shredded, not left
	// beside it.
	if got := entryExtents(t, s); len(got) != 1 || got[0] != [2]int64{0, 1024} {
		t.Errorf("after narrow write: extents = %v, want exactly [[0 1024]] "+
			"(a leftover [0 4096] is the CoverageMany false alarm)", got)
	}

	// The bytes shredding freed must read as zero, not as the old block.
	tail := make([]byte, 4096-1024)
	if _, err := f.ReadAt(tail, 1024); err != nil {
		t.Fatalf("ReadAt tail: %v", err)
	}
	for i, b := range tail {
		if b != 0 {
			t.Errorf("freed byte at offset %d = 0x%02x, want 0 "+
				"(stale data in a span nothing claims)", 1024+i, b)
			break
		}
	}
}

// TestWriterTruncateDropsRecordsCoveringRemovedRegion pins that truncation is
// xattr-aware. truncate(2) frees data blocks and leaves xattrs untouched, so a
// data-only truncate leaves records describing bytes that no longer exist --
// which a verifier reports as an anomaly, correctly, since it cannot
// distinguish an intentional truncate from lost data.
//
// A record straddling the new size is removed in full: records are never
// split, and a block whose bytes are only partly present is not verifiable.
func TestWriterTruncateDropsRecordsCoveringRemovedRegion(t *testing.T) {
	const blockSize = 1024
	for _, tc := range []struct {
		name  string
		size  int64
		want  [][2]int64
		bytes int64
	}{
		{"exact block boundary", 2 * blockSize, [][2]int64{{0, 1024}, {1024, 1024}}, 2048},
		// 2560 splits block 2 ([2048,3072)); it must go entirely.
		{"mid-block removes the straddling record", 2560, [][2]int64{{0, 1024}, {1024, 1024}}, 2560},
		{"zero purges every record", 0, nil, 0},
		{"no-op when size covers everything", 4 * blockSize,
			[][2]int64{{0, 1024}, {1024, 1024}, {2048, 1024}, {3072, 1024}}, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := makeTempStore(t)
			f := openWriterTarget(t, s)
			w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: LockNone, Log: nil})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			for i := range int64(4) {
				if err := w.WriteBlock(i*blockSize, fileops.IOTypeBuffered); err != nil {
					t.Fatalf("WriteBlock %d: %v", i, err)
				}
			}

			if err := w.Truncate(tc.size); err != nil {
				t.Fatalf("Truncate(%d): %v", tc.size, err)
			}

			if got := entryExtents(t, s); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("extents = %v, want %v", got, tc.want)
			}
			size, err := f.Size()
			if err != nil {
				t.Fatalf("Size: %v", err)
			}
			if size != tc.bytes {
				t.Errorf("file size = %d, want %d", size, tc.bytes)
			}
		})
	}
}

// TestWriterTruncateZeroesSurvivingPrefixOfStraddlingRecord covers the gap the
// invariant test in package verifier found after the first version of Truncate
// shipped: a record straddling the new size is removed whole, so the part of it
// BELOW the size survives while nothing claims it any more -- still holding the
// old block's bytes. Only a non-block-aligned size reaches this.
func TestWriterTruncateZeroesSurvivingPrefixOfStraddlingRecord(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range int64(3) {
		if err := w.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}

	// 2560 splits the third record [2048,3072): it goes entirely, leaving
	// [2048,2560) present but unclaimed.
	if err := w.Truncate(2560); err != nil {
		t.Fatalf("Truncate(2560): %v", err)
	}

	tail := make([]byte, 2560-2048)
	if _, err := f.ReadAt(tail, 2048); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	for i, b := range tail {
		if b != 0 {
			t.Errorf("surviving byte at %d = 0x%02x, want 0 -- a straddling "+
				"record's remainder must not be left as stale unclaimed data", 2048+i, b)
			break
		}
	}
}

// TestRemoveCoveringSelectsByExtentEnd pins the predicate in isolation: a
// record is doomed when it claims any byte at or beyond from, so the boundary
// case (a record ending exactly at from) must survive.
func TestRemoveCoveringSelectsByExtentEnd(t *testing.T) {
	s := makeTempStore(t)
	for _, e := range [][2]int64{{0, 1024}, {1024, 1024}, {2048, 1024}} {
		if err := s.Put(e[0], e[1], makeHeader(t, e[0], 1)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// from == 2048: [1024,2048) ends exactly at the boundary and must stay.
	removed, err := s.RemoveCovering(2048)
	if err != nil {
		t.Fatalf("RemoveCovering: %v", err)
	}
	if len(removed) != 1 || removed[0].Offset != 2048 {
		t.Errorf("removed = %v, want just the [2048,3072) record", removed)
	}
	if got := entryExtents(t, s); !reflect.DeepEqual(got, [][2]int64{{0, 1024}, {1024, 1024}}) {
		t.Errorf("remaining extents = %v, want [[0 1024] [1024 1024]]", got)
	}

	if _, err := s.RemoveCovering(-1); err == nil {
		t.Error("RemoveCovering(-1): expected an error")
	}
}

// TestWriteBlockZeroesBothSidesOfShreddedRecord covers the case where the
// shredded record extends past the new block in BOTH directions, so zeroing
// has a gap on each side. Writing into the middle of a wider record is the
// only way to reach the "before" branch of the gap computation; a
// same-offset narrower write (the case above) only ever produces a trailing
// gap, so that branch would otherwise be untested.
func TestWriteBlockZeroesBothSidesOfShreddedRecord(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	wide, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(4096): %v", err)
	}
	if err := wide.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(4096): %v", err)
	}

	// Write a 1024-byte block at offset 1024: inside [0,4096), touching
	// neither end. Shredding frees [0,1024) and [2048,4096).
	narrow, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(1024): %v", err)
	}
	if err := narrow.WriteBlock(1024, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(1024): %v", err)
	}

	if got := entryExtents(t, s); !reflect.DeepEqual(got, [][2]int64{{1024, 1024}}) {
		t.Errorf("extents = %v, want [[1024 1024]]", got)
	}

	for _, gap := range [][2]int64{{0, 1024}, {2048, 4096}} {
		buf := make([]byte, gap[1]-gap[0])
		if _, err := f.ReadAt(buf, gap[0]); err != nil {
			t.Fatalf("ReadAt [%d,%d): %v", gap[0], gap[1], err)
		}
		for i, b := range buf {
			if b != 0 {
				t.Errorf("freed byte at %d = 0x%02x, want 0 (gap [%d,%d) not zeroed)",
					gap[0]+int64(i), b, gap[0], gap[1])
				break
			}
		}
	}
}

// TestWriteBlockShredsEveryOverlappingRecord covers the widening direction:
// one write spanning several narrower records must shred all of them, leaving
// exactly one. Nothing needs zeroing here, since the new block covers every
// freed byte -- which also exercises zeroShredded's empty-gap path.
func TestWriteBlockShredsEveryOverlappingRecord(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	narrow, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(1024): %v", err)
	}
	for i := range int64(4) {
		if err := narrow.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	if got := entryExtents(t, s); len(got) != 4 {
		t.Fatalf("setup: extents = %v, want 4 records", got)
	}

	wide, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(4096): %v", err)
	}
	if err := wide.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(4096): %v", err)
	}

	if got := entryExtents(t, s); !reflect.DeepEqual(got, [][2]int64{{0, 4096}}) {
		t.Errorf("extents = %v, want exactly [[0 4096]] -- all four must be shredded", got)
	}
}

// TestWriteBlockZeroingClampsToEOF pins that zeroing a freed range never
// extends the file. A record can claim bytes past EOF (a data-only truncate
// leaves exactly that state), and writing zeros over the part beyond EOF would
// grow the file with bytes nothing claims -- replacing one inconsistency with
// a bigger one.
func TestWriteBlockZeroingClampsToEOF(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	wide, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(4096): %v", err)
	}
	if err := wide.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Data-only truncate, deliberately bypassing Writer.Truncate: the record
	// still claims [0,4096) while the file holds only 2048 bytes.
	if err := f.Truncate(2048); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	narrow, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(1024): %v", err)
	}
	if err := narrow.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(1024): %v", err)
	}

	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 2048 {
		t.Errorf("file size = %d, want 2048 -- zeroing a freed range must not extend the file", size)
	}
}

// TestWriterTruncateRejectsGrow replaces TestWriterTruncateGrowKeepsRecords,
// which asserted a grow succeeds and left records alone. Growing is withdrawn
// (see ErrTruncateGrowUnsupported), so the property under test is now that it is
// refused and nothing moves: neither the records nor the file length.
//
// A refusal that still mutated would be worse than the bug it replaces, which is
// why both halves are asserted rather than just the error.
func TestWriterTruncateRejectsGrow(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range int64(2) {
		if err := w.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}

	if err := w.Truncate(8192); !errors.Is(err, ErrTruncateGrowUnsupported) {
		t.Fatalf("Truncate(8192) err = %v, want ErrTruncateGrowUnsupported", err)
	}
	if got := entryExtents(t, s); !reflect.DeepEqual(got, [][2]int64{{0, 1024}, {1024, 1024}}) {
		t.Errorf("extents = %v, want both records intact after a refused grow", got)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 2048 {
		t.Errorf("file size = %d, want 2048 -- a refused grow must not extend the file", size)
	}
}

// TestWriterTruncateAllowsShrinkToEOF is the boundary control for the grow ban:
// size == EOF is not a grow and must still be accepted. Without this, tightening
// the guard to `size >= eof` would pass every other test in the package -- the
// no-op-truncate table case reaches it, but asserts an outcome a refusal would
// also produce if the refusal were silent.
func TestWriterTruncateAllowsShrinkToEOF(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range int64(2) {
		if err := w.WriteBlock(i*1024, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	if err := w.Truncate(2048); err != nil {
		t.Fatalf("Truncate(2048) at exactly EOF: %v, want success", err)
	}
	if got := entryExtents(t, s); !reflect.DeepEqual(got, [][2]int64{{0, 1024}, {1024, 1024}}) {
		t.Errorf("extents = %v, want both records intact", got)
	}

	// The other side of the same boundary, and the half that was missing: one byte
	// past EOF must be refused. Without this, loosening the guard to size > eof+1
	// passes every other test in the package.
	if err := w.Truncate(2049); !errors.Is(err, ErrTruncateGrowUnsupported) {
		t.Errorf("Truncate(2049) one byte past EOF: err = %v, want ErrTruncateGrowUnsupported", err)
	}
	if size, err := f.Size(); err != nil {
		t.Fatalf("Size: %v", err)
	} else if size != 2048 {
		t.Errorf("file size = %d, want 2048 -- a refused grow must not extend the file", size)
	}
}

// TestRemoveCoveringSelectsByExtentNotHeader pins the distinction that caused a
// real defect: RemoveCovering must select victims by EXTENT, not by header. A
// record with a valid name and a wrong-sized value is an unambiguous claim on
// bytes, and selecting it by anything that has to parse the value cannot see it.
//
// What this rejects is a selection KEY derived from the stored header rather than
// from the name. Key off block.UnmarshalHeader(val) and the short-valued record
// below stops being selectable, so removed is 1 instead of the asserted 2 --
// which makes two documented contracts false at once: RemoveCovering's "from == 0
// purges every record" and Writer.Truncate's "the two halves stay consistent".
//
// Not the ITERATOR, which is a distinction that no longer exists: ForEachEntry
// and Overlapping both derive extents from parseXAttrName and neither gates on
// value size, so swapping one for the other here is an equivalent mutant. The
// size gate this test was originally written against was removed deliberately
// (see ForEachEntry, and TestForEachEntryPassesWrongSizedValueThrough, which
// pins its absence).
func TestRemoveCoveringSelectsByExtentNotHeader(t *testing.T) {
	s := makeTempStore(t)

	// A valid NAME with a value one byte short of a header: not a usable record,
	// but an unambiguous claim on [1024,1536).
	shortName := XAttrName(1024, 512)
	if err := xattr.Set(s.Target(), shortName, make([]byte, block.HeaderSize-1), 0); err != nil {
		t.Fatalf("seed short-valued record: %v", err)
	}
	// A normal record alongside it, so the control case is covered too.
	if err := s.Put(0, 512, makeHeader(t, 0, 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	removed, err := s.RemoveCovering(0)
	if err != nil {
		t.Fatalf("RemoveCovering: %v", err)
	}
	if len(removed) != 2 {
		t.Errorf("removed %d records, want 2 (the short-valued one is still a record)", len(removed))
	}
	if _, err := xattr.Get(s.Target(), shortName); err == nil {
		t.Errorf("%s survived RemoveCovering(0), whose contract is to purge every named record", shortName)
	}

	// The two views of the store must now agree -- that disagreement was the
	// dangerous part, not the leftover itself.
	ov, err := s.Overlapping(0, 4096)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	n := 0
	if err := s.ForEachEntry(func(_, _ int64, _ []byte) error { n++; return nil }); err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	if len(ov) != 0 || n != 0 {
		t.Errorf("after purge Overlapping sees %d and ForEachEntry sees %d; want both 0", len(ov), n)
	}
}

// TestRemoveCoveringLeavesUnparseableNames documents the deliberate residual: an
// xattr in this namespace whose NAME does not parse has no placeable extent, so it
// cannot be selected by one. It is equally invisible to Overlapping, ForEachEntry
// and the verifier's sweep -- inert rather than dangerous -- and removing it needs
// the raw name.
func TestRemoveCoveringLeavesUnparseableNames(t *testing.T) {
	s := makeTempStore(t)
	bad := xattrPrefix + "100--50" // parses as two ints, fails validRange
	if err := xattr.Set(s.Target(), bad, makeHeader(t, 100, 1), 0); err != nil {
		t.Fatalf("seed malformed-name xattr: %v", err)
	}

	if _, err := s.RemoveCovering(0); err != nil {
		t.Fatalf("RemoveCovering: %v", err)
	}
	if _, err := xattr.Get(s.Target(), bad); err != nil {
		t.Errorf("the unparseable-name entry was removed; the documented behaviour is that it survives (%v)", err)
	}
}

// TestTruncateRemovesShortValuedRecord is the end-to-end shape of the same bug:
// Writer.Truncate(0) is the "reset both halves" step smoke and iotest-util write
// perform, and it left short-valued records behind in the new run's data.
func TestTruncateRemovesShortValuedRecord(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	shortName := XAttrName(4096, 1024)
	if err := xattr.Set(s.Target(), shortName, make([]byte, 12), 0); err != nil {
		t.Fatalf("seed short-valued record: %v", err)
	}

	if err := w.Truncate(0); err != nil {
		t.Fatalf("Truncate(0): %v", err)
	}
	if got := entryExtents(t, s); len(got) != 0 {
		t.Errorf("extents after reset = %v, want none", got)
	}
	if _, err := xattr.Get(s.Target(), shortName); err == nil {
		t.Errorf("%s survived the reset", shortName)
	}
}

// TestScanTruncateBoundCountsShortValuedRecords pins the locking half. scanTruncateBound sets
// Truncate's lock upper bound, and it has to agree with the set RemoveCovering
// will remove and with the union extent TryAcquireExclusive computes. A bound
// computed from anything that parses the stored value misses the short-valued
// record below, which is then removed from outside the range that was locked --
// three views of one file disagreeing.
//
// The bound therefore has to come from the name's extent. The iterator is not
// what is at stake: ForEachEntry and Overlapping agree on wrong-sized values
// since the size gate was removed (see ForEachEntry), so the discrimination here
// is the selection key, not the walk.
func TestScanTruncateBoundCountsShortValuedRecords(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}

	// A short-valued record claiming well past EOF.
	if err := xattr.Set(s.Target(), XAttrName(1<<20, 4096), make([]byte, 3), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	end, err := w.scanTruncateBound()
	if err != nil {
		t.Fatalf("scanTruncateBound: %v", err)
	}
	if want := int64(1<<20) + 4096; end != want {
		t.Errorf("scanTruncateBound = %d, want %d (file size %d); a short-valued record still bounds "+
			"what a truncate can touch", end, want, size)
	}
}

// TestZeroShreddedChunksLargeGaps is the regression test for zeroShredded
// sizing one allocation from the whole gap.
//
// ReadableLen clamps a gap to the FILE, which is the right bound for a read but
// not for an allocation: on a multi-GiB soak target that clamp is the file's own
// size, so a single make([]byte, n) became a multi-GiB allocation -- an
// uncatchable "fatal error: runtime: out of memory", killing the soak on
// precisely the corruption it exists to report. Reached both by a hostile xattr
// name claiming an oversized extent and by the routine case of a large-blocksize
// record being shredded by a smaller one.
//
// The allocation assertion is the one that pins the fix: the correctness checks
// below pass either way, since a single oversized buffer writes exactly the same
// zeros.
func TestZeroShreddedChunksLargeGaps(t *testing.T) {
	// Several chunks' worth, so the loop runs more than once and the pre-fix
	// single allocation is far enough above the cap to be unambiguous.
	const gap = 4 * zeroWriteChunk

	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{File: f, Store: s, WorkerID: 1, Kind: block.KindDecimal, BlockSize: 4096, Locking: LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Fill with non-zero bytes first, so a gap the fix failed to cover shows up
	// as surviving data rather than as an already-zero file.
	fill := make([]byte, 1<<20)
	for i := range fill {
		fill[i] = 0xAB
	}
	for off := int64(0); off < gap; off += int64(len(fill)) {
		if err := f.Write(fileops.IOTypeBuffered, fill, off); err != nil {
			t.Fatalf("seeding the file at %d: %v", off, err)
		}
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := w.zeroShredded([]Entry{{Offset: 0, Length: gap}}, 0, 0); err != nil {
		t.Fatalf("zeroShredded: %v", err)
	}
	runtime.ReadMemStats(&after)

	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*zeroWriteChunk {
		t.Errorf("zeroShredded allocated %d bytes to zero a %d-byte gap; the buffer is "+
			"supposed to be capped at zeroWriteChunk (%d)", allocated, gap, zeroWriteChunk)
	}

	buf := make([]byte, 1<<20)
	for off := int64(0); off < gap; off += int64(len(buf)) {
		if _, err := f.ReadAt(buf, off); err != nil {
			t.Fatalf("read back at %d: %v", off, err)
		}
		for i, b := range buf {
			if b != 0 {
				t.Fatalf("byte at %d was not zeroed (%#x) -- a chunk was skipped", off+int64(i), b)
			}
		}
	}

	// Zeroing must not extend the file: bytes past EOF that nothing claims turn
	// one inconsistency into a larger one.
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != gap {
		t.Errorf("file size = %d after zeroing, want %d", size, gap)
	}
}

// TestNewWriterRequiresLockPolicy pins the one piece of enforcement behind the
// locking discipline: the policy has to be stated.
//
// WriterConfig is a struct, so an omitted Locking field takes the zero value.
// If that zero value were a real policy, every caller who forgot the field
// would silently get it -- and "forgot to think about locking" would be
// indistinguishable from "decided not to lock", which is exactly the decision
// the type exists to force. LockPolicyUnset is therefore invalid.
func TestNewWriterRequiresLockPolicy(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	base := WriterConfig{File: f, Store: s, WorkerID: 0, Kind: block.KindDecimal, BlockSize: 1024}

	t.Run("omitted", func(t *testing.T) {
		if _, err := NewWriter(base); err == nil {
			t.Error("NewWriter accepted a config with no Locking set")
		}
	})

	t.Run("explicit zero value", func(t *testing.T) {
		cfg := base
		cfg.Locking = LockPolicyUnset
		if _, err := NewWriter(cfg); err == nil {
			t.Error("NewWriter accepted LockPolicyUnset")
		}
	})

	t.Run("out of range", func(t *testing.T) {
		cfg := base
		cfg.Locking = LockPolicy(99)
		if _, err := NewWriter(cfg); err == nil {
			t.Error("NewWriter accepted an undefined LockPolicy")
		}
	})

	for name, policy := range map[string]LockPolicy{"LockNone": LockNone, "LockExclusive": LockExclusive} {
		t.Run(name+" accepted", func(t *testing.T) {
			cfg := base
			cfg.Locking = policy
			if _, err := NewWriter(cfg); err != nil {
				t.Errorf("NewWriter rejected %s: %v", name, err)
			}
		})
	}
}
