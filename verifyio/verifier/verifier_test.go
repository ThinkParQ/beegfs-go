// This is a unit test.
//
// Coverage: VerifyFile's sweep-line classification -- an all-OK sweep,
// CoverageNone for unwritten byte ranges, CoverageMany for overlapping
// records, a caller-supplied byte-range filter, an empty file, and the
// caller's per-span callback short-circuiting the sweep on error -- plus two
// concurrent-write guarantees: a block reseeded between the xattr snapshot
// and the per-span lock is verified against its fresh header rather than
// being falsely reported as BODY_CORRUPT against the stale snapshot, and a
// write landing in a snapshot-uncovered range before the sweep reaches it
// is detected and verified rather than being falsely reported as a
// CoverageNone anomaly.
package verifier_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/testxattr"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// testEnv creates a temp file with xattr support verified, returns the
// path and an open Store. Skips the test if xattrs are not supported.
func testEnv(t *testing.T) (path string, store *xattrstore.Store) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "data.dat")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("create file: %v", err)
	}
	testxattr.RequireSupport(t, path)
	var err error
	store, err = xattrstore.OpenStore(path, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return path, store
}

// collectSpans runs VerifyFile with the given options and returns all spans.
func collectSpans(t *testing.T, store *xattrstore.Store, f *fileops.File, opts verifier.Options) []verifier.Span {
	t.Helper()
	var spans []verifier.Span
	if err := verifier.VerifyFile(store, f, opts, func(s verifier.Span) error {
		spans = append(spans, s)
		return nil
	}); err != nil {
		t.Fatalf("VerifyFile: %v", err)
	}
	return spans
}

// writeRecords writes numBlocks consecutive blocks with blockSize bytes each
// starting at offset 0, using Worker 0 and KindDecimal.
func writeRecords(t *testing.T, path string, store *xattrstore.Store, blockSize, numBlocks int) {
	t.Helper()
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: xattrstore.LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	stride := int64(blockSize)
	for i := 0; i < numBlocks; i++ {
		if err := w.WriteBlock(int64(i)*stride, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
}

func TestVerifyFileAllOK(t *testing.T) {
	const (
		blockSize = 1024
		numBlocks = 3
	)
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, numBlocks)

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})

	if len(spans) != numBlocks {
		t.Fatalf("got %d spans, want %d", len(spans), numBlocks)
	}
	for i, s := range spans {
		wantOffset := int64(i) * int64(blockSize)
		if s.Offset != wantOffset {
			t.Errorf("span %d: offset=%d, want %d", i, s.Offset, wantOffset)
		}
		if s.Length != int64(blockSize) {
			t.Errorf("span %d: length=%d, want %d", i, s.Length, blockSize)
		}
		if s.Coverage != verifier.CoverageOne {
			t.Errorf("span %d: coverage=%v, want CoverageOne", i, s.Coverage)
		}
		if s.Verdict != block.VerdictOK {
			t.Errorf("span %d: verdict=%v, want VerdictOK", i, s.Verdict)
		}
		if s.Header == nil {
			t.Errorf("span %d: Header is nil", i)
		}
	}
}

func TestVerifyFileCoverageNone(t *testing.T) {
	// Write two records with a gap between them.
	//   record 0: [0, blockDataSize)
	//   record 1: [2048, 2048+blockDataSize)  ← gap [blockDataSize, 2048) is uncovered
	const blockSize = 1024
	path, store := testEnv(t)

	blockDataSize := int64(blockSize)
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: xattrstore.LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock 0: %v", err)
	}
	if err := w.WriteBlock(2048, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock 2048: %v", err)
	}
	f.Close()

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer fv.Close()

	spans := collectSpans(t, store, fv, verifier.Options{})

	// Expect: [0,blockDataSize) one, [blockDataSize,2048) none, [2048,2048+blockDataSize) one.
	type want struct {
		offset   int64
		length   int64
		coverage verifier.Coverage
	}
	wantSpans := []want{
		{0, blockDataSize, verifier.CoverageOne},
		{blockDataSize, 2048 - blockDataSize, verifier.CoverageNone},
		{2048, blockDataSize, verifier.CoverageOne},
	}
	if len(spans) != len(wantSpans) {
		t.Fatalf("got %d spans, want %d: %v", len(spans), len(wantSpans), spans)
	}
	for i, w := range wantSpans {
		s := spans[i]
		if s.Offset != w.offset || s.Length != w.length || s.Coverage != w.coverage {
			t.Errorf("span %d: got {%d, %d, %v}, want {%d, %d, %v}",
				i, s.Offset, s.Length, s.Coverage,
				w.offset, w.length, w.coverage)
		}
		if w.coverage == verifier.CoverageNone && !s.AllZero {
			t.Errorf("span %d: gap is not all-zero (os should zero it)", i)
		}
	}
}

// TestVerifyFileRacedWriteReportsContended pins what an unclaimed span does
// when a write lands in it after VerifyFile's snapshot: it reports the span
// unverified, not judged.
//
// Two wrong answers are being avoided, and this test used to assert the first
// of them. Re-dispatching the span to the record the recheck found reports the
// WHOLE span as covered -- clean, if that record verifies -- even where the
// record covers only part of it. Calling two disjoint records CoverageMany
// invents an overlap that is not there. Neither is knowable from the store, so
// the span is reported contended with ContendedStoreChanged and left out of
// the verified count.
//
// This uses VerifyFile's own per-span callback as a synchronization point
// (deterministic, no goroutines/timing needed): while the callback is still
// processing the span for the FIRST block, it writes into the range the
// snapshot said was empty, before the sweep reaches it.
func TestVerifyFileRacedWriteReportsContended(t *testing.T) {
	const blockSize = 1024
	for _, tc := range []struct {
		name string
		// fileSize sets how much of the gap follows the first block, and
		// midScan lists the offsets written while the sweep is in flight.
		fileSize int64
		midScan  []int64
	}{
		// The record arriving mid-scan covers the whole gap.
		{name: "covers the whole span", fileSize: 2 * blockSize, midScan: []int64{blockSize}},
		// It covers only the front of the gap: the rest is uncovered and
		// non-zero, which the old re-dispatch reported as covered and clean.
		{name: "covers part of the span", fileSize: 4 * blockSize, midScan: []int64{blockSize}},
		// Two records, disjoint from each other: not an overlap, so not
		// CoverageMany.
		{name: "two disjoint records", fileSize: 5 * blockSize, midScan: []int64{blockSize, 3 * blockSize}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, store := testEnv(t)

			f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer f.Close()
			w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: xattrstore.LockNone, Log: nil})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
				t.Fatalf("WriteBlock 0: %v", err)
			}
			// Extend the file so the sweep covers the gap after the first
			// block, while leaving it genuinely uncovered (no xattr,
			// zero-filled by the OS) at snapshot time.
			if err := os.Truncate(path, tc.fileSize); err != nil {
				t.Fatalf("truncate: %v", err)
			}

			var spans []verifier.Span
			err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
				if s.Offset == 0 && len(spans) == 0 {
					for _, off := range tc.midScan {
						if err := w.WriteBlock(off, fileops.IOTypeBuffered); err != nil {
							t.Fatalf("mid-scan WriteBlock at %d: %v", off, err)
						}
					}
				}
				spans = append(spans, s)
				return nil
			})
			if err != nil {
				t.Fatalf("VerifyFile: %v", err)
			}

			if len(spans) != 2 {
				t.Fatalf("got %d spans, want 2: %+v", len(spans), spans)
			}
			gap := spans[1]
			if gap.Offset != blockSize || gap.Length != tc.fileSize-blockSize {
				t.Fatalf("gap span = {offset=%d, length=%d}, want {%d, %d}",
					gap.Offset, gap.Length, blockSize, tc.fileSize-blockSize)
			}
			if gap.Coverage != verifier.CoverageContended {
				t.Errorf("gap span coverage=%v, want CoverageContended -- a write landed mid-sweep, so the span was not verified", gap.Coverage)
			}
			if gap.Contended != verifier.ContendedStoreChanged {
				t.Errorf("gap span contended=%v, want ContendedStoreChanged", gap.Contended)
			}
			if gap.Coverage == verifier.CoverageOne && gap.Verdict == block.VerdictOK {
				t.Error("gap span reported clean over a range that includes uncovered non-zero bytes")
			}
			if gap.Coverage == verifier.CoverageMany {
				t.Error("gap span reported CoverageMany for records that do not overlap each other")
			}
		})
	}
}

func TestVerifyFileCoverageMany(t *testing.T) {
	// Directly Put two overlapping xattrs without using ReplaceRange,
	// which is the condition CoverageMany is designed to surface.
	path, store := testEnv(t)

	makeHdr := func(offset, seed int64) []byte {
		h := block.Header{
			Version: block.HeaderVersion,
			Kind:    block.KindZeros,
			Seed:    uint64(seed),
			Offset:  uint64(offset),
		}
		buf := make([]byte, block.HeaderSize)
		if err := block.MarshalHeader(buf, &h); err != nil {
			t.Fatalf("MarshalHeader: %v", err)
		}
		return buf
	}
	if err := store.Put(0, 2048, makeHdr(0, 1)); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := store.Put(1024, 2048, makeHdr(1024, 2)); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	// Write enough bytes to the file so ReadAt doesn't fail.
	if err := os.WriteFile(path, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Re-open store after truncation.
	store, err := xattrstore.OpenStore(path, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	// Re-put (WriteFile truncates xattrs... actually no, WriteFile uses O_TRUNC
	// which shouldn't touch xattrs). Let's verify xattrs are still there.
	if err := store.Put(0, 2048, makeHdr(0, 1)); err != nil {
		t.Fatalf("re-Put A: %v", err)
	}
	if err := store.Put(1024, 2048, makeHdr(1024, 2)); err != nil {
		t.Fatalf("re-Put B: %v", err)
	}

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer fv.Close()

	spans := collectSpans(t, store, fv, verifier.Options{})

	// Sort by offset for deterministic comparison.
	sort.Slice(spans, func(i, j int) bool { return spans[i].Offset < spans[j].Offset })

	foundMany := false
	for _, s := range spans {
		if s.Coverage == verifier.CoverageMany {
			foundMany = true
		}
	}
	if !foundMany {
		t.Errorf("expected at least one CoverageMany span; got: %v", spans)
	}
}

func TestVerifyFileRangeFilter(t *testing.T) {
	// Write 5 records. Verify only the middle 3 (blocks 1-3).
	const (
		blockSize = 1024
		numBlocks = 5
	)
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, numBlocks)

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer fv.Close()

	blockDataSize := int64(blockSize)
	opts := verifier.Options{
		Range: &verifier.ByteRange{Offset: blockDataSize, Length: 3 * blockDataSize},
	}
	spans := collectSpans(t, store, fv, opts)

	if len(spans) != 3 {
		t.Fatalf("got %d spans, want 3", len(spans))
	}
	for i, s := range spans {
		wantOffset := blockDataSize * int64(1+i)
		if s.Offset != wantOffset {
			t.Errorf("span %d: offset=%d, want %d", i, s.Offset, wantOffset)
		}
		if s.Coverage != verifier.CoverageOne {
			t.Errorf("span %d: coverage=%v, want CoverageOne", i, s.Coverage)
		}
		if s.Verdict != block.VerdictOK {
			t.Errorf("span %d: verdict=%v, want VerdictOK", i, s.Verdict)
		}
	}
}

// TestVerifyFileEmptyFileNoRecords covers a zero-length file that has no xattr
// records either: there is genuinely nothing to verify, so 0 spans is correct.
//
// This deliberately does NOT cover a zero-length file that still HAS records --
// the truncated-away case, where 0 spans would be a false PASS on lost data.
// That is TestVerifyFileTruncatedRecordsStillVerified below. The two look alike
// and the distinction is the whole point, so keep them named apart.
func TestVerifyFileEmptyFileNoRecords(t *testing.T) {
	path, store := testEnv(t)
	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fv.Close()
	spans := collectSpans(t, store, fv, verifier.Options{})
	if len(spans) != 0 {
		t.Errorf("empty file, no records: got %d spans, want 0", len(spans))
	}
}

// TestVerifyFileTruncatedRecordsStillVerified pins the invariant that the
// RECORDS, not the file's current size, define what a sweep is answerable for.
//
// truncate(2) frees data blocks but leaves xattrs untouched, so records outlive
// the bytes they describe. Before the sweep domain was widened to cover them,
// any record lying at or past EOF was skipped entirely and the file reported
// "0 anomalies / PASS" -- a false negative in the one tool whose job is to
// catch lost data. Only the mid-block case worked, and only by accident (the
// straddling record got clipped, so its buffer came up short).
func TestVerifyFileTruncatedRecordsStillVerified(t *testing.T) {
	const (
		blockSize = 1024
		numBlocks = 4
	)
	for _, tc := range []struct {
		name          string
		truncateTo    int64
		wantTruncated int
		wantOK        int
	}{
		// Every record gone: all 4 must still be reported.
		{"truncate to zero", 0, 4, 0},
		// Whole-block loss on a boundary: 2 survive intact, 2 are gone.
		{"truncate to block boundary", 2 * blockSize, 2, 2},
		// Partial loss inside the third block: it reads short, the 4th is gone.
		{"truncate mid-block", 2*blockSize + 500, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, store := testEnv(t)
			writeRecords(t, path, store, blockSize, numBlocks)

			if err := os.Truncate(path, tc.truncateTo); err != nil {
				t.Fatalf("Truncate(%d): %v", tc.truncateTo, err)
			}

			fv, err := fileops.Open(path, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer fv.Close()

			var gotTruncated, gotOK int
			for _, s := range collectSpans(t, store, fv, verifier.Options{}) {
				if s.Coverage != verifier.CoverageOne {
					continue
				}
				switch s.Verdict {
				case block.VerdictTruncated:
					gotTruncated++
				case block.VerdictOK:
					gotOK++
				}
			}
			if gotTruncated != tc.wantTruncated {
				t.Errorf("truncated spans: got %d, want %d", gotTruncated, tc.wantTruncated)
			}
			if gotOK != tc.wantOK {
				t.Errorf("OK spans: got %d, want %d", gotOK, tc.wantOK)
			}
		})
	}
}

// TestVerifyFileOversizedRecordIsReportedNotFatal is the joint regression test
// for widening the sweep domain and clamping the reads it drives.
//
// A record's length arrives from the xattr NAME and xattrstore only checks it
// for sign and int64 overflow -- never against the file. Sizing a buffer from
// it directly makes a corrupt name like "<prefix>0-1099511627776" a 1 TiB
// allocation, which is an unrecoverable "fatal error: runtime: out of memory",
// not a catchable panic: the verifier dies on exactly the corruption it exists
// to report, taking the rest of the sweep with it. Widening the domain (so
// beyond-EOF records are visited at all) makes that path strictly MORE
// reachable, which is why the clamp and the widening belong to one change.
//
// If this regresses, the test does not fail -- the test binary is OOM-killed.
func TestVerifyFileOversizedRecordIsReportedNotFatal(t *testing.T) {
	const hugeLen = int64(1) << 40 // 1 TiB, far past any plausible temp file

	path, store := testEnv(t)
	if err := os.WriteFile(path, make([]byte, 1024), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	// A structurally valid header (correct magic, version, and HeadCRC) whose
	// record claims vastly more than the file holds. The header CRC is a
	// checksum, not a MAC, so a valid one is computable for any BodyLen -- the
	// value passing its own integrity check is precisely why the length cannot
	// be trusted on that basis.
	hdr := make([]byte, block.HeaderSize)
	h := block.Header{
		Version: block.HeaderVersion,
		Kind:    block.KindDecimal,
		BodyLen: uint64(hugeLen),
	}
	if err := block.MarshalHeader(hdr, &h); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	if err := xattr.Set(path, xattrstore.XAttrName(0, hugeLen), hdr, 0); err != nil {
		t.Skipf("cannot set oversized-record xattr here: %v", err)
	}

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fv.Close()

	var reported int
	for _, s := range collectSpans(t, store, fv, verifier.Options{}) {
		if s.Coverage == verifier.CoverageOne && s.Verdict != block.VerdictOK {
			reported++
		}
	}
	if reported == 0 {
		t.Error("oversized record produced no anomalous span; it must be reported, not skipped")
	}
}

func TestVerifyFileCallbackError(t *testing.T) {
	const (
		blockSize = 1024
		numBlocks = 3
	)
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, numBlocks)

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fv.Close()

	sentinel := os.ErrInvalid
	saw := 0
	err = verifier.VerifyFile(store, fv, verifier.Options{}, func(s verifier.Span) error {
		saw++
		return sentinel
	})
	if err != sentinel {
		t.Errorf("VerifyFile returned %v, want sentinel", err)
	}
	if saw != 1 {
		t.Errorf("callback called %d times after error, want 1", saw)
	}
}

// TestVerifyFileReseedBetweenSnapshotAndLock is a regression test for the
// snapshot-vs-lock race. VerifyFile snapshots the entry set before taking
// per-span shared locks, but must re-read each header UNDER the lock. Because
// the writer holds its exclusive lock across both the xattr and the data write,
// a shared-lock holder always observes a consistent (header, body) pair. Before
// the fix, verifying the new body against the stale snapshot header produced a
// false VerdictBodyCorrupt.
//
// The reseed is driven from VerifyFile's own span callback, which is a
// deterministic synchronisation point: it runs after the entry snapshot and
// before the next span's lock and header re-read, which is exactly the window
// the race lives in. Reseeding record B from record A's callback therefore hits
// that window every run.
//
// This replaces a version that raced a background goroutine against a
// 300-iteration sweep. It could not fail: measured across 40 processes it
// caught the pre-fix shape 0 times, because the shared lock masks the very race
// it was trying to observe -- when the writer genuinely holds the exclusive
// lock the span comes back CoverageContended and no verdict is produced at all.
// The losing interleaving needed a whole WriteBlock inside the microsecond
// between the header read and the lock acquire; 900 sweeps produced none.
//
// One ordering is deliberately NOT pinned: moving s.Get above the lock
// acquisition inside verifyOneRecordSpan. Catching it needs an in-function
// seam, and the window is a microsecond in a forensic, post-workload tool. The
// re-read's placement is documented at its own site instead.
func TestVerifyFileReseedBetweenSnapshotAndLock(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)

	fw, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer fw.Close()
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{
		File: fw, Store: store, WorkerID: 0, Kind: block.KindDecimal,
		BlockSize: blockSize, Locking: xattrstore.LockExclusive,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Skip if range locking isn't available on this platform/fs.
	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Skipf("locked write not supported here: %v", err)
	}
	if err := w.WriteBlock(blockSize, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock B: %v", err)
	}

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open verifier: %v", err)
	}
	defer fv.Close()

	seen := 0
	err = verifier.VerifyFile(store, fv, verifier.Options{LockMode: verifier.LockShared},
		func(s verifier.Span) error {
			if s.Offset == 0 {
				// Reseed B -- new header AND new body, under the writer's
				// exclusive lock -- after the sweep's entry snapshot.
				if werr := w.WriteBlock(blockSize, fileops.IOTypeBuffered); werr != nil {
					t.Fatalf("reseed: %v", werr)
				}
				return nil
			}
			seen++
			if s.Coverage != verifier.CoverageOne {
				t.Fatalf("span at %d: coverage %v, want one", s.Offset, s.Coverage)
			}
			if s.Verdict != block.VerdictOK {
				t.Fatalf("span at %d: verdict %v, want OK -- the header must be re-read under the shared lock",
					s.Offset, s.Verdict)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("VerifyFile: %v", err)
	}
	if seen != 1 {
		t.Fatalf("saw %d reseeded spans, want 1", seen)
	}
}

// TestVerifyFileGrowthDuringSweepNotClampedByStaleSize pins that a span's read
// buffer is sized against the file's CURRENT size, not one snapshot taken at the
// top of the sweep. A concurrent Writer.WriteBlock can grow the file while the
// sweep is still on an earlier span, and a stale boundary breaks it in two
// directions, both confirmed against a real Writer: a record completed after the
// snapshot reads as VerdictTruncated, and real non-zero bytes past the boundary
// inside an otherwise-uncovered span are never scanned, so the span reports
// AllZero=true.
func TestVerifyFileGrowthDuringSweepNotClampedByStaleSize(t *testing.T) {
	const blockSize = 516 // BlockDataSize(512) -- exactly one stripe plus its CRC

	t.Run("record finished after snapshot is not falsely TRUNCATED", func(t *testing.T) {
		path, store := testEnv(t)

		// Get record B's xattr into the store, claiming [2000, 2516), then blow
		// away its data bytes: truncate(2) frees data blocks but leaves xattrs
		// untouched, so this leaves the store in exactly the state a writer
		// that has Put the header but not yet finished pwriting the body would
		// -- cheaper than hand-marshaling a header to get there directly.
		writeAt(t, path, store, blockSize, 2000)
		if err := os.Truncate(path, 516); err != nil {
			t.Fatalf("truncate down: %v", err)
		}
		// Record A, fully written, entirely inside the now-truncated file.
		writeAt(t, path, store, blockSize, 0)

		f, err := fileops.Open(path, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open for verify: %v", err)
		}
		defer f.Close()

		var spans []verifier.Span
		err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
			if s.Offset == 0 {
				// Simulate the concurrent writer finishing record B's data
				// while the sweep -- which already knows B's extent from the
				// xattr snapshot taken at the top of VerifyFile -- is still
				// working through an earlier span.
				writeAt(t, path, store, blockSize, 2000)
			}
			spans = append(spans, s)
			return nil
		})
		if err != nil {
			t.Fatalf("VerifyFile: %v", err)
		}

		var gotB bool
		for _, s := range spans {
			if s.Offset != 2000 {
				continue
			}
			gotB = true
			if s.Verdict != block.VerdictOK {
				t.Errorf("record B verdict = %v, want VerdictOK -- it was fully written before "+
					"its span was reached. This test's target is the stale pre-sweep SIZE "+
					"snapshot, which clamps the read buffer to 0 and gives VerdictTruncated; "+
					"a BODY_CORRUPT here is the stale-HEADER defect instead, pinned by "+
					"TestVerifyFileReseedBetweenSnapshotAndLock", s.Verdict)
			}
		}
		if !gotB {
			t.Fatalf("no span at offset 2000 was reported")
		}
	})

	t.Run("bytes written past stale boundary are not falsely AllZero", func(t *testing.T) {
		path, store := testEnv(t)

		// Record A anchors the low end of an uncovered gap.
		writeAt(t, path, store, blockSize, 0)
		// Record C's xattr only (no data write) anchors the high end: it
		// pushes the sweep domain out to 5516 without touching the file's
		// real length, and its own verdict is irrelevant to this test.
		if err := store.Put(5000, blockSize, make([]byte, block.HeaderSize)); err != nil {
			t.Fatalf("Put record C header: %v", err)
		}
		// Grow the file to 3000 with a plain hole (sparse, reads as zero) --
		// this is the size VerifyFile's top-of-sweep Stat will observe. The
		// gap [516, 5000) therefore starts before that snapshot and extends
		// past it, so it is NOT skipped by the "wholly beyond EOF" early-out
		// (that only applies to a span starting at or past the snapshot).
		if err := os.Truncate(path, 3000); err != nil {
			t.Fatalf("truncate up: %v", err)
		}

		f, err := fileops.Open(path, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open for verify: %v", err)
		}
		defer f.Close()

		var spans []verifier.Span
		err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
			if s.Offset == 0 {
				// Simulate a write landing past the stale 3000-byte boundary,
				// still inside the uncovered gap, while the sweep is still
				// working through an earlier span. No xattr describes these
				// bytes -- this models a race the sweep must still catch by
				// actually reading this far, not the "concurrent Put" case
				// verifyUnclaimedSpan's own recheck-via-Overlapping already handles.
				rw, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatalf("open for raw write: %v", err)
				}
				defer rw.Close()
				buf := make([]byte, 500)
				for i := range buf {
					buf[i] = 0x7A
				}
				if _, err := rw.WriteAt(buf, 3000); err != nil {
					t.Fatalf("raw WriteAt: %v", err)
				}
			}
			spans = append(spans, s)
			return nil
		})
		if err != nil {
			t.Fatalf("VerifyFile: %v", err)
		}

		var gotGap bool
		for _, s := range spans {
			if s.Coverage != verifier.CoverageNone || s.Offset != 516 {
				continue
			}
			gotGap = true
			if s.AllZero {
				t.Error("gap reported AllZero=true, but real non-zero bytes exist past the " +
					"stale pre-sweep size snapshot -- a stale clamp on the zero-scan buffer " +
					"never read far enough to see them")
			}
		}
		if !gotGap {
			t.Fatalf("no CoverageNone span at offset 516 was reported")
		}
	})
}

// TestVerifyFileMixedHeaderVersionYieldsPerSpanVerdict is a regression test
// for xattrstore.Store.Get's exact-size gate, which used to reject any
// stored header whose length differed from today's block.HeaderSize before
// block.UnmarshalHeader ever got a chance to classify it. A header that is a
// different (but internally self-consistent -- CRC still passes) version,
// stored at a size other than today's HeaderSize (simulating a future format
// that grew the header and bumped HeaderVersion), must be reported as a
// normal per-span VerdictHeadBadFormat -- not abort the entire VerifyFile
// sweep with a hard error.
func TestVerifyFileMixedHeaderVersionYieldsPerSpanVerdict(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)

	// Take today's valid, stored header as a base, then mutate it into a
	// different, self-consistent version -- same technique as block_test.go's
	// "bad version (self-consistent CRC)" case: change Version, then
	// recompute the CRC over the mutated bytes, exactly as a real
	// differently-versioned writer's own CRC would be. Finally pad it longer
	// than HeaderSize, simulating a newer format with extra trailing fields.
	hdrBuf, err := store.Get(0, blockSize)
	if err != nil {
		t.Fatalf("Get baseline header: %v", err)
	}
	mixed := make([]byte, len(hdrBuf), len(hdrBuf)+4)
	copy(mixed, hdrBuf)
	binary.LittleEndian.PutUint16(mixed[8:10], 999)
	newCRC := crc32.Checksum(mixed[:86], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(mixed[86:90], newCRC)
	mixed = append(mixed, 0xAA, 0xBB, 0xCC, 0xDD)

	name := xattrstore.XAttrName(0, blockSize)
	if err := xattr.Set(path, name, mixed, 0); err != nil {
		t.Fatalf("xattr.Set mixed-version header: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	// collectSpans itself t.Fatalf's if VerifyFile returns an error -- so
	// reaching the assertions below already confirms the sweep didn't abort.
	spans := collectSpans(t, store, f, verifier.Options{})

	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Verdict != block.VerdictHeadBadFormat {
		t.Errorf("verdict = %v, want VerdictHeadBadFormat", spans[0].Verdict)
	}
}

// TestVerifyFileSpanLockedReflectsPlatform is a regression test for
// Span.Locked: on Linux, a healthy CoverageOne span verified with the
// default Options{} (LockShared) must report Locked=true -- a real lock
// was actually held while the body was read. On non-Linux, trySharedLock is
// a no-op (verifyio is Linux-only), so it must report Locked=false --
// Options{}'s default silently provides no torn-write guarantee there, and
// this is the queryable signal a caller can check instead of wrongly
// assuming a healthy-looking sweep was actually protected.
func TestVerifyFileSpanLockedReflectsPlatform(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{}) // default LockShared
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	wantLocked := runtime.GOOS == "linux"
	if spans[0].Locked != wantLocked {
		t.Errorf("Locked = %v, want %v (GOOS=%s)", spans[0].Locked, wantLocked, runtime.GOOS)
	}
}

// writeAt writes one block of blockSize bytes at offset through a fresh Writer.
func writeAt(t *testing.T, path string, store *xattrstore.Store, blockSize int, offset int64) {
	t.Helper()
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: xattrstore.LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter(%d): %v", blockSize, err)
	}
	if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock(%d @ bs=%d): %v", offset, blockSize, err)
	}
}

// truncateVia calls Writer.Truncate, the coherent (data + xattr) truncation.
func truncateVia(t *testing.T, path string, store *xattrstore.Store, blockSize int, size int64) {
	t.Helper()
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: blockSize, Locking: xattrstore.LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Truncate(size); err != nil {
		t.Fatalf("Truncate(%d): %v", size, err)
	}
}

// countAnomalies sweeps path and returns the anomalous spans, using the
// strictest reading of the invariant: no range may be claimed twice, no
// claimed block may fail verification, and no unclaimed range may hold
// non-zero bytes.
func countAnomalies(t *testing.T, path string, store *xattrstore.Store) []verifier.Span {
	t.Helper()
	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()
	var bad []verifier.Span
	for _, s := range collectSpans(t, store, f, verifier.Options{}) {
		switch s.Coverage {
		case verifier.CoverageMany:
			bad = append(bad, s)
		case verifier.CoverageOne:
			if s.Verdict != block.VerdictOK {
				bad = append(bad, s)
			}
		case verifier.CoverageNone:
			if !s.AllZero {
				bad = append(bad, s)
			}
		}
	}
	return bad
}

// TestWriterOperationsLeaveNoFlaggedState is the invariant test for the
// data/xattr coherence layer: whatever sequence of Writer operations runs, the
// file it leaves behind must verify clean.
//
// Each individual mechanism has its own unit test in xattrstore, but those
// assert extents and bytes. This asserts the property those mechanisms exist to
// serve, which is the thing that actually matters and the thing that was
// silently false before: every case below except the control produced a
// verifier anomaly on a file whose history contains nothing an operator would
// consider wrong.
func TestWriterOperationsLeaveNoFlaggedState(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, path string, s *xattrstore.Store)
	}{
		{"uniform blocks", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
		}},
		{"widening rewrite shreds many records", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
			writeAt(t, p, s, 4096, 0)
		}},
		{"narrowing rewrite at same offset", func(t *testing.T, p string, s *xattrstore.Store) {
			writeAt(t, p, s, 4096, 0)
			writeAt(t, p, s, 1024, 0)
		}},
		{"narrowing rewrite inside a wider record", func(t *testing.T, p string, s *xattrstore.Store) {
			writeAt(t, p, s, 4096, 0)
			writeAt(t, p, s, 1024, 1024)
		}},
		{"truncate on a block boundary", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
			truncateVia(t, p, s, 1024, 2048)
		}},
		{"truncate mid-block", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
			truncateVia(t, p, s, 1024, 2560)
		}},
		{"truncate to zero", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
			truncateVia(t, p, s, 1024, 0)
		}},
		{"reuse after purge at a new block size", func(t *testing.T, p string, s *xattrstore.Store) {
			for i := range int64(4) {
				writeAt(t, p, s, 1024, i*1024)
			}
			truncateVia(t, p, s, 1024, 0)
			writeAt(t, p, s, 4096, 0)
		}},
		// The "grow" case is gone: Writer.Truncate refuses a size past EOF
		// (xattrstore.ErrTruncateGrowUnsupported), so there is no writer operation
		// left that grows a file, and nothing for this invariant to check. The
		// refusal itself is pinned in xattrstore, not here.
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, store := testEnv(t)
			tc.run(t, path, store)
			if bad := countAnomalies(t, path, store); len(bad) != 0 {
				for _, s := range bad {
					t.Errorf("anomaly: [%d,%d) coverage=%s verdict=%s allZero=%v",
						s.Offset, s.Offset+s.Length, s.Coverage, s.Verdict, s.AllZero)
				}
			}
		})
	}
}

// TestWriterInvariantTestCanDetectFailure is the negative control for the test
// above. A suite that only ever asserts "clean" passes just as happily when its
// anomaly predicate is broken, so this constructs the incoherent state
// deliberately -- a data-only truncate, bypassing Writer.Truncate -- and
// requires that it IS flagged.
func TestWriterInvariantTestCanDetectFailure(t *testing.T) {
	path, store := testEnv(t)
	for i := range int64(4) {
		writeAt(t, path, store, 1024, i*1024)
	}

	// The bug this layer exists to prevent: shorten the data, keep the records.
	f, err := fileops.Open(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := f.Truncate(2048); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	_ = f.Close()

	if bad := countAnomalies(t, path, store); len(bad) == 0 {
		t.Error("a data-only truncate left no flagged span; the anomaly predicate " +
			"cannot detect the very state this layer prevents, so the invariant " +
			"test above proves nothing")
	}
}

// TestVerifyFileShortValuedRecordOverZeroDataIsReported is the regression test
// for the false PASS that ForEachEntry's size gate used to produce.
//
// The setup is the nastiest shape available: a record whose name parses (so it
// claims a real extent) but whose stored value is too short to be a header, over
// data that is entirely ZERO. The gate dropped such a record from the entry set,
// so the sweep saw an uncovered range; the recheck that would have caught it
// fires only on non-zero bytes, and there are none here. Result: "1 span, 0
// anomalies, PASS" on a file whose metadata is corrupt.
//
// Zero data is not a contrived detail -- it is what a sparse or freshly
// truncate-extended region reads back as, so this is the case most likely to
// occur for real and the least likely to be noticed.
func TestVerifyFileShortValuedRecordOverZeroDataIsReported(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)

	// Zero-filled data, no legitimate records: nothing here is non-zero, so
	// verifyUnclaimedSpan's recheck cannot rescue the classification.
	if err := os.WriteFile(path, make([]byte, blockSize), 0644); err != nil {
		t.Fatalf("write zero data: %v", err)
	}
	name := xattrstore.XAttrName(0, blockSize)
	if err := xattr.Set(path, name, []byte("way too short"), 0); err != nil {
		t.Fatalf("xattr.Set short value: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	// The record must be visible AS a record -- the old gate made this
	// CoverageNone, which over zero data is a clean span.
	if spans[0].Coverage != verifier.CoverageOne {
		t.Errorf("coverage = %v, want CoverageOne (the record must not be dropped)", spans[0].Coverage)
	}
	if spans[0].Verdict != block.VerdictHeadTruncated {
		t.Errorf("verdict = %v, want VerdictHeadTruncated", spans[0].Verdict)
	}
}

// TestVerifyFileMalformedNameReportedAfterFullSweep pins both halves of the
// unparseable-name contract: every span is still emitted, AND the sweep ends in
// ErrMalformedEntries naming the entry.
//
// Both halves matter. Dropping the error silently tolerates metadata verifyio
// cannot account for in its own namespace; aborting before the callback would
// withhold the span report on exactly the corrupt input the operator ran this
// tool to understand.
func TestVerifyFileMalformedNameReportedAfterFullSweep(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)

	// Parses as two integers but fails validRange, so it has no usable extent
	// and cannot be placed on the sweep line at all.
	bad := xattrstore.XAttrPrefix + "100--50"
	if err := xattr.Set(path, bad, make([]byte, block.HeaderSize), 0); err != nil {
		t.Fatalf("xattr.Set malformed name: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	var spans []verifier.Span
	err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
		spans = append(spans, s)
		return nil
	})
	if !errors.Is(err, verifier.ErrMalformedEntries) {
		t.Fatalf("VerifyFile error = %v, want ErrMalformedEntries", err)
	}
	// The good record's span must still have been delivered.
	if len(spans) != 1 {
		t.Errorf("got %d spans, want 1 -- the sweep must complete before reporting", len(spans))
	} else if spans[0].Verdict != block.VerdictOK {
		t.Errorf("verdict = %v, want VerdictOK for the healthy record", spans[0].Verdict)
	}

	var me *verifier.MalformedEntriesError
	if !errors.As(err, &me) {
		t.Fatalf("error is not *MalformedEntriesError: %T", err)
	}
	if len(me.Entries) != 1 || me.Entries[0].Name != bad {
		t.Errorf("Entries = %+v, want exactly %q", me.Entries, bad)
	}
	if !strings.Contains(err.Error(), bad) {
		t.Errorf("error text %q does not name the offending xattr", err.Error())
	}
}

// TestVerifyFileMalformedNameOnEmptyFileStillReported covers the early-return
// path. An empty file with no usable records has nothing to sweep, so VerifyFile
// returns before the sweep loop -- and that is precisely the case where the
// malformed entry is the entire finding, so returning nil there would report a
// clean run over a file whose only metadata is corrupt.
func TestVerifyFileMalformedNameOnEmptyFileStillReported(t *testing.T) {
	path, store := testEnv(t)

	bad := xattrstore.XAttrPrefix + "0-0" // zero length: parses, fails validRange
	if err := xattr.Set(path, bad, make([]byte, block.HeaderSize), 0); err != nil {
		t.Fatalf("xattr.Set malformed name: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	called := 0
	err = verifier.VerifyFile(store, f, verifier.Options{}, func(verifier.Span) error {
		called++
		return nil
	})
	if called != 0 {
		t.Errorf("callback ran %d times, want 0 on an empty file", called)
	}
	if !errors.Is(err, verifier.ErrMalformedEntries) {
		t.Fatalf("VerifyFile error = %v, want ErrMalformedEntries", err)
	}
}

// TestVerifyFileNonCanonicalNameReportedAsMalformed pins that a
// corrupted-but-parseable xattr name is reported as malformed, not as
// contention. Such a name sweeps in as a legitimate CoverageOne record and then
// misses on the re-Get under the shared lock, which reconstructs the canonical
// spelling from the parsed offset/length -- so without this the whole span
// reports CoverageContended, "benign lock contention", and the run prints PASS
// over real corruption.
func TestVerifyFileNonCanonicalNameReportedAsMalformed(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)

	// "0516" parses to the same int64 as "516" via strconv.ParseInt, but every
	// name this package ever writes comes from XAttrName's "%d" formatting,
	// which never emits a leading zero. Such a name can only be corruption or
	// tampering.
	bad := xattrstore.XAttrPrefix + "0516-516"
	if err := xattr.Set(path, bad, make([]byte, block.HeaderSize), 0); err != nil {
		t.Fatalf("xattr.Set non-canonical name: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	var spans []verifier.Span
	err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
		spans = append(spans, s)
		return nil
	})
	if !errors.Is(err, verifier.ErrMalformedEntries) {
		t.Fatalf("VerifyFile error = %v, want ErrMalformedEntries", err)
	}
	for _, s := range spans {
		if s.Coverage == verifier.CoverageContended {
			t.Errorf("span %+v: non-canonical name must be reported as malformed, "+
				"not silently swept in and reported as contended", s)
		}
	}
	var me *verifier.MalformedEntriesError
	if !errors.As(err, &me) {
		t.Fatalf("error is not *MalformedEntriesError: %T", err)
	}
	if len(me.Entries) != 1 || me.Entries[0].Name != bad {
		t.Errorf("Entries = %+v, want exactly %q", me.Entries, bad)
	}
}

// TestVerifyFileCallbackErrorBeatsMalformedReport pins the precedence: a sweep
// the caller aborted has not established that the reported entries are the only
// malformed ones, so the caller's own error must win rather than being replaced
// by a partial malformed report.
func TestVerifyFileCallbackErrorBeatsMalformedReport(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 2)

	if err := xattr.Set(path, xattrstore.XAttrPrefix+"5--5", make([]byte, block.HeaderSize), 0); err != nil {
		t.Fatalf("xattr.Set malformed name: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	sentinel := errors.New("caller stopped the sweep")
	err = verifier.VerifyFile(store, f, verifier.Options{}, func(verifier.Span) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("VerifyFile error = %v, want the caller's sentinel", err)
	}
	if errors.Is(err, verifier.ErrMalformedEntries) {
		t.Error("an aborted sweep must not report ErrMalformedEntries as its result")
	}
}

// refileWithSize moves the record at (0, blockSize) to a new name claiming size
// keeping its header bytes byte-for-byte. That is the shape a corrupted xattr
// NAME produces: an intact, CRC-valid header claiming a byte range that its own
// BodyLen cannot account for.
func refileWithSize(t *testing.T, path string, store *xattrstore.Store, blockSize, newSize int64) {
	t.Helper()
	hdr, err := store.Get(0, blockSize)
	if err != nil {
		t.Fatalf("Get baseline header: %v", err)
	}
	if err := store.Remove(0, blockSize); err != nil {
		t.Fatalf("Remove original: %v", err)
	}
	if err := xattr.Set(path, xattrstore.XAttrName(0, newSize), hdr, 0); err != nil {
		t.Fatalf("Set refiled record: %v", err)
	}
}

// refileWithOffsetMismatch rewrites the header stored at (offset, blockSize)
// so its Offset field disagrees with the offset its own xattr name encodes,
// while keeping the header otherwise self-consistent -- a freshly computed,
// valid HeadCRC over the changed bytes -- and the extent untouched. This is
// the shape a record filed under the wrong xattr name produces: intact,
// CRC-valid metadata that contradicts itself.
func refileWithOffsetMismatch(t *testing.T, store *xattrstore.Store, offset, blockSize int64, wrongOffset uint64) {
	t.Helper()
	raw, err := store.Get(offset, blockSize)
	if err != nil {
		t.Fatalf("Get baseline header: %v", err)
	}
	h, err := block.UnmarshalHeader(raw)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	h.Offset = wrongOffset
	out := make([]byte, block.HeaderSize)
	if err := block.MarshalHeader(out, &h); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	if err := store.Put(offset, blockSize, out); err != nil {
		t.Fatalf("Put refiled header: %v", err)
	}
}

// TestVerifyFileOffsetMismatchIsReported pins the cross-check between
// Header.Offset and the offset its own xattr name encodes -- the offset half of
// what TestVerifyFileOversizedRecordIsReported covers for (size, BodyLen). Both
// are written by every record, and nothing downstream reconciles them, so without
// the check a record mis-filed under the wrong name with an otherwise
// self-consistent header verifies OK -- or, on a genuine body difference, reports
// BODY_CORRUPT, whose explanation points at cross-node cache coherence for a
// record whose own metadata is self-contradictory.
func TestVerifyFileOffsetMismatchIsReported(t *testing.T) {
	const blockSize = 516 // BlockDataSize(512) -- exactly one stripe plus its CRC
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)
	refileWithOffsetMismatch(t, store, 0, blockSize, 4096)

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Verdict != block.VerdictOffsetMismatch {
		t.Errorf("verdict = %v, want VerdictOffsetMismatch", spans[0].Verdict)
	}
}

// TestVerifyFileOversizedRecordIsReported is the regression test for a record
// whose size vastly exceeds what its header describes verifying OK.
//
// A valid 516-byte record refiled under a 1 TiB name reported
// "span [0,1099511627776) cov=one verdict=OK" -- the verifier declaring a
// terabyte cleanly covered by one good block. VerifyBlock rejects only a buffer
// SHORTER than BlockDataSize(h.BodyLen), so the surplus was never examined.
func TestVerifyFileOversizedRecordIsReported(t *testing.T) {
	const blockSize = 516 // BlockDataSize(512) -- exactly one stripe plus its CRC
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)
	refileWithSize(t, path, store, blockSize, int64(1)<<40)

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Verdict != block.VerdictSizeMismatch {
		t.Errorf("verdict = %v, want VerdictSizeMismatch", spans[0].Verdict)
	}
	// Diag is the observable proxy for "the body was never read": it is only
	// populated on the path that reads and diagnoses the buffer. A 1 TiB size
	// is precisely the input that would otherwise drive a file-sized allocation.
	if spans[0].Diag != nil {
		t.Error("Diag is populated, so the body was read -- the size check must " +
			"precede the read, which is the allocation it exists to avoid")
	}
}

// TestVerifyFileSizeMismatchIsNotTruncated pins the distinction between two
// faults that both used to surface as VerdictTruncated, and must not be conflated
// again: TRUNCATED is "the size agrees with the header but the FILE is too
// short", SIZE_MISMATCH is "the size and the header disagree, whatever the
// file holds". They point at opposite halves of the store -- the data vs the
// metadata -- so an operator needs them apart.
func TestVerifyFileSizeMismatchIsNotTruncated(t *testing.T) {
	const blockSize = 516

	t.Run("short file, consistent record -> TRUNCATED", func(t *testing.T) {
		path, store := testEnv(t)
		writeRecords(t, path, store, blockSize, 1)
		// Shorten the DATA only. The record still claims exactly
		// BlockDataSize(BodyLen), so it is internally consistent.
		if err := os.Truncate(path, 100); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		f, err := fileops.Open(path, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer f.Close()
		spans := collectSpans(t, store, f, verifier.Options{})
		if len(spans) != 1 {
			t.Fatalf("got %d spans, want 1", len(spans))
		}
		if spans[0].Verdict != block.VerdictTruncated {
			t.Errorf("verdict = %v, want VerdictTruncated (the record is consistent; "+
				"only the file is short)", spans[0].Verdict)
		}
	})

	t.Run("undersized record -> SIZE_MISMATCH", func(t *testing.T) {
		path, store := testEnv(t)
		writeRecords(t, path, store, blockSize, 1)
		// Size smaller than the header describes. This used to read a short
		// buffer and land on VerdictTruncated, blaming the file for a fault that
		// is entirely in the record's own metadata.
		refileWithSize(t, path, store, blockSize, 100)
		f, err := fileops.Open(path, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer f.Close()
		// Two spans, not one: shrinking the record to 100 leaves the file's
		// remaining 416 bytes claimed by nothing, so the sweep reports the record
		// AND the unclaimed tail. Both halves are real findings and the test
		// asserts each -- the tail is what replace-and-shred exists to prevent.
		spans := collectSpans(t, store, f, verifier.Options{})
		if len(spans) != 2 {
			t.Fatalf("got %d spans, want 2 (the record plus the now-unclaimed tail)", len(spans))
		}
		if spans[0].Verdict != block.VerdictSizeMismatch {
			t.Errorf("span 0 verdict = %v, want VerdictSizeMismatch", spans[0].Verdict)
		}
		if spans[1].Coverage != verifier.CoverageNone {
			t.Errorf("span 1 coverage = %v, want CoverageNone", spans[1].Coverage)
		}
		if spans[1].AllZero {
			t.Error("span 1 reported all-zero, but it holds the original block's " +
				"leftover bytes -- stale data in a range nothing claims")
		}
	})
}

// TestVerifyFileZeroBodyLenOverRealSizeIsReported covers the shape a writer
// produces when it files a placeholder header -- BodyLen unset -- under a real
// block size. BlockDataSize(0) is 0, so the record claims blockSize bytes while
// describing none, and every such record verified OK over a file of pure zeros:
// a whole file of nothing reported clean. iotest-util xattr-capacity wrote
// exactly this shape before it began removing its artifact.
func TestVerifyFileZeroBodyLenOverRealSizeIsReported(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	if err := os.WriteFile(path, make([]byte, blockSize), 0644); err != nil {
		t.Fatalf("write zero data: %v", err)
	}
	hdrBuf := make([]byte, block.HeaderSize)
	h := block.Header{Version: block.HeaderVersion, Kind: block.KindDecimal}
	if err := block.MarshalHeader(hdrBuf, &h); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	if err := store.Put(0, blockSize, hdrBuf); err != nil {
		t.Fatalf("Put: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Verdict != block.VerdictSizeMismatch {
		t.Errorf("verdict = %v, want VerdictSizeMismatch", spans[0].Verdict)
	}
}

// TestChangedBlockSizeNeverYieldsCoverageMany pins the claim at writer.go:146 --
// that routing WriteBlock through ReplaceRange makes a doubly-claimed range
// "unreachable rather than merely documented" when the block size changes.
//
// The claim is about what the VERIFIER concludes, so only a cross-package test
// can check it: every xattrstore test asserts record counts, and every verifier
// test used a single blockSize per file, so the two halves of the invariant were
// each covered and the join between them was not.
//
// Why it matters: `Put` is keyed by (offset, length), so writing 1032 bytes where
// a 4096-byte record already sits creates a SECOND name rather than replacing the
// first. The verifier then reports the overlap as CoverageMany, which says the
// filesystem let two writers claim one range -- the most alarming thing this tool
// can report -- for what is only a re-run with a different --blocksize.
func TestChangedBlockSizeNeverYieldsCoverageMany(t *testing.T) {
	const (
		bigBlock   = 4096
		smallBlock = 1032 // 2 * (StripeSize + 4): a valid body/CRC split
	)
	path, store := testEnv(t)

	// First pass: one 4096-byte block at offset 0.
	writeRecords(t, path, store, bigBlock, 1)

	// Second pass: same offset, smaller block. A fresh Writer, exactly as a
	// re-run with a different --blocksize produces.
	func() {
		f, err := fileops.Open(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("reopen for second pass: %v", err)
		}
		defer f.Close()
		w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: smallBlock, Locking: xattrstore.LockNone, Log: nil})
		if err != nil {
			t.Fatalf("NewWriter(%d): %v", smallBlock, err)
		}
		if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock at the new block size: %v", err)
		}
	}()

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) == 0 {
		t.Fatal("no spans; the sweep must actually cover the file for this to mean anything")
	}

	for _, s := range spans {
		if s.Coverage == verifier.CoverageMany {
			t.Errorf("span [%d,%d) is CoverageMany -- the old %d-byte record survived beside "+
				"the new %d-byte one, so replace-and-shred is not being enforced",
				s.Offset, s.Offset+s.Length, bigBlock, smallBlock)
		}
	}

	// The new record must verify, and the bytes the shredded record freed must
	// read as zero rather than as the old block's leftovers -- otherwise the
	// verifier reports stale data in a range nothing claims.
	var covered, freed int64
	for _, s := range spans {
		switch s.Coverage {
		case verifier.CoverageOne:
			if s.Verdict != block.VerdictOK {
				t.Errorf("span [%d,%d) verdict = %v, want OK", s.Offset, s.Offset+s.Length, s.Verdict)
			}
			covered += s.Length
		case verifier.CoverageNone:
			if !s.AllZero {
				t.Errorf("span [%d,%d) is unclaimed but non-zero: the freed tail still holds "+
					"the old block", s.Offset, s.Offset+s.Length)
			}
			freed += s.Length
		}
	}
	if covered != smallBlock {
		t.Errorf("claimed bytes = %d, want %d (exactly the new record)", covered, smallBlock)
	}
	if freed != bigBlock-smallBlock {
		t.Errorf("freed bytes = %d, want %d", freed, bigBlock-smallBlock)
	}
}

// TestOverlapPolicyDocExample is a doc-driven test: it executes the worked
// example in xattrstore's package doc ("Overlap policy: replace-and-shred") and
// checks the verifier really behaves as that example predicts.
//
// The example is the package's central explanation of why callers should write
// through ReplaceRange rather than Put, and it makes a precise, checkable claim
// with concrete offsets -- Put(1024, 3072) then Put(2048, 2048) yields
// "[2048, 4096) claimed by two records at once ... even though [1024, 2048) is
// still cleanly CoverageOne". Nothing executed it. A doc example that has never
// been run is exactly the kind of claim this sweep exists to find: if it drifts,
// every reader learning the overlap policy learns it wrong, and the drift is
// invisible because no build or test depends on it (-> A.3.4, A.3.5).
//
// Scoped to what the doc actually claims: the COVERAGE classification. It says
// nothing about the verdicts, which depend on header/body agreement the example
// does not specify.
func TestOverlapPolicyDocExample(t *testing.T) {
	path, store := testEnv(t)

	// A 4096-byte file so the sweep has bytes to walk under both records.
	if err := os.WriteFile(path, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("size the file: %v", err)
	}

	hdr := func(offset int64) []byte {
		t.Helper()
		buf := make([]byte, block.HeaderSize)
		h := block.Header{Version: block.HeaderVersion, Kind: block.KindDecimal, Offset: uint64(offset)}
		if err := block.MarshalHeader(buf, &h); err != nil {
			t.Fatalf("MarshalHeader: %v", err)
		}
		return buf
	}

	// Verbatim from the doc.
	if err := store.Put(1024, 3072, hdr(1024)); err != nil {
		t.Fatalf("Put(1024, 3072): %v", err)
	}
	if err := store.Put(2048, 2048, hdr(2048)); err != nil {
		t.Fatalf("Put(2048, 2048): %v", err)
	}

	// "both xattrs persist side by side" -- Put must not have replaced anything.
	entries, err := store.Overlapping(0, 4096)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("store holds %d records (%v), want 2 -- the doc says Put does not "+
			"check the existing claim", len(entries), entries)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	got := map[string]verifier.Coverage{}
	for _, s := range collectSpans(t, store, f, verifier.Options{}) {
		got[fmt.Sprintf("[%d,%d)", s.Offset, s.Offset+s.Length)] = s.Coverage
	}

	for _, want := range []struct {
		span     string
		coverage verifier.Coverage
		why      string
	}{
		{"[0,1024)", verifier.CoverageNone, "below both records"},
		{"[1024,2048)", verifier.CoverageOne, `the doc: "still cleanly CoverageOne"`},
		{"[2048,4096)", verifier.CoverageMany, `the doc: "claimed by two records at once"`},
	} {
		if c, ok := got[want.span]; !ok {
			t.Errorf("no span %s was emitted; got %v", want.span, got)
		} else if c != want.coverage {
			t.Errorf("span %s coverage = %v, want %v (%s)", want.span, c, want.coverage, want.why)
		}
	}
	if len(got) != 3 {
		t.Errorf("got %d spans (%v), want exactly 3 -- the example implies these boundaries "+
			"and no others", len(got), got)
	}
}

// TestVerifyFileHeaderCRCCorruptionIsHeadBadCRC is the reachability half of
// block.VerdictForHeaderError's default arm: a header byte flipped without the
// CRC being recomputed -- ordinary bit-rot -- has to surface as HEAD_BAD_CRC
// through a real sweep, not merely map to it in isolation.
//
// Distinct from TestVerifyFileMixedHeaderVersionYieldsPerSpanVerdict, which
// recomputes the CRC so the header stays self-consistent and therefore lands on
// the version arm. Here the CRC is deliberately left stale, which is what a
// genuinely damaged byte looks like.
func TestVerifyFileHeaderCRCCorruptionIsHeadBadCRC(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, 1)

	hdrBuf, err := store.Get(0, blockSize)
	if err != nil {
		t.Fatalf("Get baseline header: %v", err)
	}
	corrupt := make([]byte, len(hdrBuf))
	copy(corrupt, hdrBuf)
	// Byte 50 is in the Cycle field, which the HeadCRC at [86:90] covers. The
	// CRC is deliberately NOT recomputed -- that is what makes this bit-rot
	// rather than a differently-versioned writer.
	corrupt[50] ^= 0x01

	name := xattrstore.XAttrName(0, blockSize)
	if err := xattr.Set(path, name, corrupt, 0); err != nil {
		t.Fatalf("xattr.Set corrupt header: %v", err)
	}

	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer f.Close()

	// collectSpans itself t.Fatalf's if VerifyFile returns an error, so reaching
	// the assertions below already confirms the sweep did not abort.
	spans := collectSpans(t, store, f, verifier.Options{})
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Verdict != block.VerdictHeadBadCRC {
		t.Errorf("verdict = %v, want VerdictHeadBadCRC -- a header carrying a stale "+
			"CRC is damaged, and any verdict that is not a fault here is a false PASS",
			spans[0].Verdict)
	}
}
