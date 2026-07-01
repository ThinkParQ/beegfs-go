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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
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
	if err := xattr.Set(path, "user.verifyio_probe", []byte{0}, 0); err != nil {
		t.Skipf("user xattrs not supported: %v", err)
	}
	_ = xattr.Remove(path, "user.verifyio_probe")
	var err error
	store, err = xattrstore.OpenStore(path)
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
	w, err := xattrstore.NewWriter(f, store, 0, block.KindDecimal, blockSize, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	stride := int64(blockSize)
	for i := 0; i < numBlocks; i++ {
		if err := w.WriteBlock(int64(i)*stride, fileops.IOTypeBuffered, false); err != nil {
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
	w, err := xattrstore.NewWriter(f, store, 0, block.KindDecimal, blockSize, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered, false); err != nil {
		t.Fatalf("WriteBlock 0: %v", err)
	}
	if err := w.WriteBlock(2048, fileops.IOTypeBuffered, false); err != nil {
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

// TestVerifyFileNoneSpanRechecksBeforeReportingAnomaly is a regression test
// for a false-positive window: unlike oneSpan (which holds a lock across
// its whole read specifically because its pre-lock header snapshot can go
// stale), the CoverageNone path had no record -- and therefore nothing to
// lock -- at the moment VerifyFile's snapshot was taken, so a write landing
// in a genuinely-uncovered range after the snapshot but before the read
// used to be misreported as corruption (non-zero bytes in an uncovered
// region) instead of being recognized as an ordinary write racing the scan.
//
// This uses VerifyFile's own per-span callback as a synchronization point
// (deterministic, no goroutines/timing needed): while the callback is
// still processing the span for the FIRST block, it writes a second block
// into the range the snapshot said was empty, before the sweep reaches it.
func TestVerifyFileNoneSpanRechecksBeforeReportingAnomaly(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)

	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := xattrstore.NewWriter(f, store, 0, block.KindDecimal, blockSize, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(0, fileops.IOTypeBuffered, false); err != nil {
		t.Fatalf("WriteBlock 0: %v", err)
	}
	// Extend the file so VerifyFile's sweep covers [blockSize, 2*blockSize)
	// too, while leaving it genuinely uncovered (no xattr, zero-filled by
	// the OS) at snapshot time.
	if err := os.Truncate(path, 2*blockSize); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	var spans []verifier.Span
	err = verifier.VerifyFile(store, f, verifier.Options{}, func(s verifier.Span) error {
		if s.Offset == 0 && len(spans) == 0 {
			// Simulate a writer landing in the snapshot-uncovered range
			// before the sweep reaches it.
			if err := w.WriteBlock(blockSize, fileops.IOTypeBuffered, false); err != nil {
				t.Fatalf("mid-scan WriteBlock: %v", err)
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
	second := spans[1]
	if second.Offset != blockSize || second.Length != blockSize {
		t.Fatalf("second span = {offset=%d, length=%d}, want {%d, %d}",
			second.Offset, second.Length, blockSize, blockSize)
	}
	if second.Coverage != verifier.CoverageOne {
		t.Errorf("second span coverage=%v, want CoverageOne (the mid-scan write should have been detected, not reported as a CoverageNone anomaly)", second.Coverage)
	}
	if second.Verdict != block.VerdictOK {
		t.Errorf("second span verdict=%v, want VerdictOK", second.Verdict)
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
	store, err := xattrstore.OpenStore(path)
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

func TestVerifyFileEmptyFile(t *testing.T) {
	path, store := testEnv(t)
	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fv.Close()
	spans := collectSpans(t, store, fv, verifier.Options{})
	if len(spans) != 0 {
		t.Errorf("empty file: got %d spans, want 0", len(spans))
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

// TestVerifyFileConcurrentReseedNoFalseCorrupt is a regression test for the
// snapshot-vs-lock race. VerifyFile snapshots the entry set before taking
// per-span shared locks, but must re-read each header UNDER the lock. Here a
// writer reseeds one block (new header + new body, under its exclusive lock) in
// a loop while the verifier sweeps concurrently. Because the writer holds the
// exclusive lock across both the xattr and the data write, a shared-lock holder
// always observes a consistent (header, body) pair, so the verifier must never
// report BODY_CORRUPT / BODY_CRC_MISMATCH. Before the fix, verifying the new
// body against the stale snapshot header produced a false VerdictBodyCorrupt.
func TestVerifyFileConcurrentReseedNoFalseCorrupt(t *testing.T) {
	const blockSize = 1024
	path, store := testEnv(t)

	fw, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer fw.Close()
	w, err := xattrstore.NewWriter(fw, store, 0, block.KindDecimal, blockSize, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// First locked write; skip if OFD locking isn't available on this platform/fs.
	if err := w.WriteBlock(0, fileops.IOTypeBuffered, true); err != nil {
		t.Skipf("OFD-locked write not supported here: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = w.WriteBlock(0, fileops.IOTypeBuffered, true) // reseed under exclusive lock
				time.Sleep(50 * time.Microsecond)                 // leave the verifier lock-free windows
			}
		}
	}()
	defer func() { close(stop); <-done }()

	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open verifier: %v", err)
	}
	defer fv.Close()

	for i := 0; i < 300; i++ {
		verr := verifier.VerifyFile(store, fv, verifier.Options{LockMode: verifier.LockShared},
			func(s verifier.Span) error {
				if s.Verdict == block.VerdictBodyCorrupt || s.Verdict == block.VerdictBodyCRCMismatch {
					return fmt.Errorf("false %s at offset %d (iteration %d)", s.Verdict, s.Offset, i)
				}
				return nil
			})
		if verr != nil {
			t.Fatalf("VerifyFile under concurrent reseed: %v — the verifier must re-read the header under the shared lock", verr)
		}
	}
}
