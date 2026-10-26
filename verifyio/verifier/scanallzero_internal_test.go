// This is a unit test, internal to package verifier so it can exercise
// scanAllZero and unclaimedScanChunk directly rather than only through the
// public VerifyFile entry point.
//
// Coverage: the chunked zero-scan finds a non-zero byte wherever it lands --
// in a later chunk, and past the first 64 KiB compare window inside a chunk --
// under both lock modes, and it does not allocate memory proportional to the
// scanned range.
//
// Both modes are run over the same table because LockMode changes exactly one
// thing about a gap scan: whether each chunk is read under a range lock. When
// the locked scan was a separate copy of the loop, these tests covered only the
// unlocked copy -- which is the branch nothing ships -- and a mutation that
// scanned only the first chunk turned a corrupt file into PASS at exit 0 with
// every package green.
//
// An uncovered span's length comes from the gap between xattr records, and on a
// legitimately large sparse target that gap is as large as the file, so
// xattrstore.ReadableLen's clamp-to-filesize does not bound it. A single
// make([]byte, gapLen) would allocate 1.00 GiB for a 1 GiB gap, and 100x that is
// an uncatchable "fatal error: runtime: out of memory".
package verifier

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// lockModes is every mode scanAllZero branches on. Table-driven so a new mode
// cannot be added without a test author noticing this list.
var lockModes = []struct {
	name string
	opts Options
}{
	{"LockShared", Options{}},
	{"LockCallerHeld", Options{LockMode: LockCallerHeld}},
}

// openSparse creates a sparse file of the given size and returns it with a
// Store over the same path -- scanAllZero needs the Store for its per-chunk
// shared locks.
func openSparse(t *testing.T, size int64) (*fileops.File, *xattrstore.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.dat")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate to %d: %v", size, err)
	}
	f, err := fileops.Open(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	store, err := xattrstore.OpenStore(path, xattrstore.DefaultLockTimeout)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return f, store
}

// scan runs the scan and fails on an error or on unexpected contention --
// nothing else holds a lock in these tests, so a busy report is a bug in the
// test, not a result.
func scan(t *testing.T, s *xattrstore.Store, f *fileops.File, opts Options, offset, length, fileSize int64) bool {
	t.Helper()
	zero, contended, err := scanAllZero(s, f, opts, offset, length, fileSize)
	if err != nil {
		t.Fatalf("scanAllZero: %v", err)
	}
	if contended {
		t.Fatal("scanAllZero reported contended with no other lock holder")
	}
	return zero
}

// TestScanAllZeroFindsNonZeroByte pins that neither of the scan's two nested
// loops stops early. They fail independently, so each probe position names the
// loop it covers: a byte in a later chunk catches an outer loop that scans only
// the first chunk, and a byte past 64 KiB inside a chunk catches an allZero that
// compares only its first zeroBlock-sized window.
//
// The second position is the one that was missing. The probe used to sit at
// chunk-relative offset 10, so a mutation reducing allZero to a single 64 KiB
// compare was invisible: a non-zero byte anywhere above 64 KiB in its chunk read
// as all-zero, and a corrupt file passed at exit 0.
func TestScanAllZeroFindsNonZeroByte(t *testing.T) {
	const size = 3 * unclaimedScanChunk
	for _, mode := range lockModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				offset int64
			}{
				{"later chunk", unclaimedScanChunk + 10},
				{"past the first compare window, same chunk", (64 << 10) + 10},
				{"both: later chunk, past its compare window", unclaimedScanChunk + (64 << 10) + 10},
				{"last byte of the range", size - 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					f, store := openSparse(t, size)
					if !scan(t, store, f, mode.opts, 0, size, size) {
						t.Fatal("all-zero sparse range reported non-zero")
					}
					if _, err := f.LockFd().WriteAt([]byte{0x7A}, tc.offset); err != nil {
						t.Fatalf("WriteAt: %v", err)
					}
					if scan(t, store, f, mode.opts, 0, size, size) {
						t.Errorf("byte at %d was not found -- the scan stopped early", tc.offset)
					}
				})
			}
		})
	}
}

// TestScanAllZeroDoesNotAllocateProportionalToRange pins that scanning a large
// uncovered span does not allocate a buffer sized to the span. 512 MiB is chosen
// to be safely non-destructive in CI -- a single make([]byte, ...) of that size
// succeeds, and only ~100x it is an uncatchable OOM -- while still separating
// "chunked" from "single-shot" clearly in total bytes allocated.
func TestScanAllZeroDoesNotAllocateProportionalToRange(t *testing.T) {
	const size = 512 << 20 // 512 MiB, all zero (sparse)
	for _, mode := range lockModes {
		t.Run(mode.name, func(t *testing.T) {
			f, store := openSparse(t, size)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			if !scan(t, store, f, mode.opts, 0, size, size) {
				t.Fatal("all-zero sparse range reported non-zero")
			}

			runtime.ReadMemStats(&after)
			delta := after.TotalAlloc - before.TotalAlloc
			// A single-shot allocation would show up as a ~512 MiB delta;
			// chunked scanning should show up as a small multiple of
			// unclaimedScanChunk (4 MiB). 64 MiB leaves generous headroom over
			// test-process background noise while still being 8x smaller than
			// the scanned range.
			const budget = 64 << 20
			if delta > budget {
				t.Errorf("scanAllZero over a %d-byte range allocated %d bytes (budget %d) -- "+
					"looks like a single allocation sized to the range rather than bounded chunks",
					size, delta, budget)
			}
		})
	}
}

// TestScanAllZeroLockModeSelectsTheChunkReader pins the one thing LockMode
// changes about a gap scan, from both sides.
//
// This is the seam the unified scanner introduced, and it needs its own test
// precisely because it is a choice rather than a computation: a scanAllZero
// that ignored opts and never locked passed every other test in this file, as
// did one that reported a busy lock as "all zero, verified". Both are silent
// false-cleans -- the failure mode this whole tool exists to prevent.
//
// A competing lease on the SAME Store is enough to contend; the range table
// refuses any overlap, including shared-against-shared, and the nested acquire
// gets ErrLockBusy rather than blocking. No helper process needed.
func TestScanAllZeroLockModeSelectsTheChunkReader(t *testing.T) {
	const size = 2 * unclaimedScanChunk
	f, store := openSparse(t, size)

	lease, err := store.TryAcquireExclusive(f.LockFd(), 0, size)
	if err != nil {
		t.Skipf("range locking unavailable here: %v", err)
	}
	defer func() {
		if rerr := lease.Release(); rerr != nil {
			t.Errorf("release: %v", rerr)
		}
	}()

	// LockShared must notice the held lock and refuse to certify what it could
	// not read. Reporting zero=true here is the false-clean.
	zero, contended, err := scanAllZero(store, f, Options{}, 0, size, size)
	if err != nil {
		t.Fatalf("scanAllZero (LockShared): %v", err)
	}
	if !contended {
		t.Error("LockShared scan did not report contended against a held exclusive lease -- " +
			"either it took no lock, or a busy chunk was not propagated")
	}
	if zero {
		t.Error("LockShared scan reported all-zero for a range it never read")
	}

	// LockCallerHeld takes no lock of its own, so the same held lease is not an
	// obstacle -- that is the entire point of the mode, and it must still read.
	zero, contended, err = scanAllZero(store, f, Options{LockMode: LockCallerHeld}, 0, size, size)
	if err != nil {
		t.Fatalf("scanAllZero (LockCallerHeld): %v", err)
	}
	if contended {
		t.Error("LockCallerHeld scan reported contended -- it must not take a lock of its own")
	}
	if !zero {
		t.Error("LockCallerHeld scan did not read the sparse range as all-zero")
	}
}
