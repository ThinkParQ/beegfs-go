// This is a unit test. It exercises real xattr syscalls against a temp file
// (skipped if the underlying filesystem doesn't support user xattrs).
//
// Coverage: OpenStore validation (missing path, non-regular-file); Put/Get
// round-trip, missing-record and wrong-size/zero-length/invalid-range
// rejection, and variable-length records; ForEachEntry iteration (including
// ignoring xattrs outside this package's namespace and stopping on a
// callback error); Overlapping's range classification (exact match, strict
// subset/superset, partial start/end, adjacent-no-overlap, multiple matches,
// unrelated entries/namespaces ignored, malformed-length entries ignored);
// ReplaceRange's shred-and-replace semantics (no overlap, full-overlap exact
// match, partial-overlap shredding, multiple overlaps collapsing into one);
// XAttrName<->offset/length round-trip; ReadableLen's clamping of a
// record's claimed extent against the actual file size; and RemoveCovering's
// extent-end predicate (a record ending exactly at the boundary survives).
package xattrstore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/internal/testxattr"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

// makeTempStore creates a temp regular file and returns a Store on it.
// Skips the test if user xattrs aren't supported on the underlying tmp
// filesystem.
func makeTempStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := os.WriteFile(path, []byte("placeholder"), 0644); err != nil {
		t.Fatalf("create file: %v", err)
	}
	testxattr.RequireSupport(t, path)
	s, err := OpenStore(path, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// makeHeader returns a HeaderSize-byte buffer containing a valid header
// (enough to round-trip via block.UnmarshalHeader).
func makeHeader(t *testing.T, offset int64, seed uint64) []byte {
	t.Helper()
	h := block.Header{
		Version: block.HeaderVersion,
		Kind:    block.KindZeros,
		Offset:  uint64(offset),
		Seed:    seed,
		TimeNs:  1700000000,
	}
	buf := make([]byte, block.HeaderSize)
	if err := block.MarshalHeader(buf, &h); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	return buf
}

// recordLen is a synthetic on-disk extent used by tests where the
// length value doesn't matter beyond being positive and consistent
// across Put/Get/Remove.
const recordLen = 4096

func TestOpenStoreMissingPath(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "nope")
	if _, err := OpenStore(missing, 0); err == nil {
		t.Errorf("OpenStore on missing path: expected error, got nil")
	}
}

func TestOpenStoreOnDirectory(t *testing.T) {
	dir := t.TempDir() // directory, not a regular file
	if _, err := OpenStore(dir, 0); err == nil {
		t.Errorf("OpenStore on directory: expected error, got nil")
	}
}

// TestOpenStoreOnFIFO pins the non-regular-file guard on the one case
// TestOpenStoreOnDirectory cannot reach: open(2) on a directory fails EISDIR
// on its own, before OpenStore's IsRegular check ever runs, so that test
// passes even with the guard deleted. A FIFO reaches the guard instead --
// OpenStore's O_RDWR open succeeds immediately on a FIFO with no reader (an
// O_RDONLY open would block waiting for one), so the IsRegular check is what
// has to refuse it. The timeout guards against a future flag change
// reintroducing a hang: a regression here is a wedge, not a wrong value.
func TestOpenStoreOnFIFO(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "planted-fifo")
	if err := syscall.Mkfifo(fifo, 0666); err != nil {
		t.Skipf("mkfifo unsupported on this filesystem: %v", err)
	}

	type result struct {
		s   *Store
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := OpenStore(fifo, 0)
		done <- result{s, err}
	}()

	select {
	case got := <-done:
		if got.err == nil {
			got.s.Close()
			t.Fatal("OpenStore(fifo): got nil error, want a refusal to open a non-regular file")
		}
		if !strings.Contains(got.err.Error(), "not a regular file") {
			t.Errorf("OpenStore(fifo): err=%v, want it to say 'not a regular file'", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OpenStore(fifo) did not return within 5s -- it wedged instead of refusing")
	}
}

// TestStoreSurvivesPathSwap is the Store-level regression test for the same
// TOCTOU exposure xattr.TestFdVariantsSurvivePathSwap covers at the syscall
// layer: a Store opens its target once, at construction, and every
// subsequent Put/Get goes through that held fd -- never re-resolving
// s.target from disk. Replacing what the path points to after OpenStore
// must not affect a Store already using it.
func TestStoreSurvivesPathSwap(t *testing.T) {
	s := makeTempStore(t)
	origPath := s.Target()
	hdr := makeHeader(t, 0, 1)
	if err := s.Put(0, recordLen, hdr); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Swap the path this Store was opened from to point at a different file.
	dir := t.TempDir()
	other := filepath.Join(dir, "other")
	if err := os.WriteFile(other, []byte("other file"), 0644); err != nil {
		t.Fatalf("create other file: %v", err)
	}
	if err := os.Remove(origPath); err != nil {
		t.Fatalf("remove original path: %v", err)
	}
	if err := os.Symlink(other, origPath); err != nil {
		t.Fatalf("symlink swap: %v", err)
	}

	// The Store's own fd is unaffected by the swap.
	got, err := s.Get(0, recordLen)
	if err != nil {
		t.Fatalf("Get after path swap: %v", err)
	}
	if !bytes.Equal(got, hdr) {
		t.Errorf("Get after path swap = %v, want %v (Store must be immune to the swap)", got, hdr)
	}

	// A fresh, path-based xattr.Get on the same path now resolves to the
	// swapped-in file, which never had this attribute -- demonstrating
	// exactly the exposure a held fd closes.
	if _, err := xattr.Get(origPath, XAttrName(0, recordLen)); !errors.Is(err, xattr.ErrNotFound) {
		t.Errorf("xattr.Get(origPath) after swap: err = %v, want ErrNotFound", err)
	}
}

func TestPutGet(t *testing.T) {
	s := makeTempStore(t)
	hdr := makeHeader(t, 0x1000, 42)

	if err := s.Put(0x1000, recordLen, hdr); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(0x1000, recordLen)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, hdr) {
		t.Errorf("Get returned different bytes than Put")
	}

	// Replace at the same (offset, length).
	hdr2 := makeHeader(t, 0x1000, 99)
	if err := s.Put(0x1000, recordLen, hdr2); err != nil {
		t.Fatalf("Put replace: %v", err)
	}
	got, _ = s.Get(0x1000, recordLen)
	if !bytes.Equal(got, hdr2) {
		t.Errorf("after replace, Get returned old bytes")
	}
}

func TestGetMissing(t *testing.T) {
	s := makeTempStore(t)
	_, err := s.Get(0xdead, recordLen)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of missing offset: err=%v, want ErrNotFound", err)
	}
}

// TestGetPassesThroughShortValue and TestGetPassesThroughLongValue are
// regression tests: Get must not reject a value whose size differs from
// today's block.HeaderSize. block.UnmarshalHeader already fully classifies
// both cases (too short -> ErrTruncated, long enough -> CRC/Version checks)
// as a normal per-span verifier verdict; Get rejecting size mismatches
// itself, ahead of that, turned a longer-but-otherwise-valid header (e.g. a
// future HeaderVersion) into a hard error that aborted an entire verifier
// sweep instead. Both write via xattr.Set directly, bypassing Store.Put's
// own (correct, unrelated) exact-size check on the write side -- Get must
// still return whatever is actually stored, since a real mismatch like this
// can come from another node's differently-versioned client, not just a
// same-process bug.
func TestGetPassesThroughShortValue(t *testing.T) {
	s := makeTempStore(t)
	name := XAttrName(0x1000, recordLen)
	short := []byte{1, 2, 3}
	if err := xattr.Set(s.target, name, short, 0); err != nil {
		t.Fatalf("xattr.Set: %v", err)
	}

	got, err := s.Get(0x1000, recordLen)
	if err != nil {
		t.Fatalf("Get: %v, want no error -- classification is UnmarshalHeader's job", err)
	}
	if !bytes.Equal(got, short) {
		t.Errorf("Get = %v, want %v (the raw stored bytes, unmodified)", got, short)
	}
}

func TestGetPassesThroughLongValue(t *testing.T) {
	s := makeTempStore(t)
	name := XAttrName(0x1000, recordLen)
	long := append(makeHeader(t, 0x1000, 42), 0xAA, 0xBB, 0xCC, 0xDD) // longer than block.HeaderSize
	if err := xattr.Set(s.target, name, long, 0); err != nil {
		t.Fatalf("xattr.Set: %v", err)
	}

	got, err := s.Get(0x1000, recordLen)
	if err != nil {
		t.Fatalf("Get: %v, want no error -- classification is UnmarshalHeader's job", err)
	}
	if !bytes.Equal(got, long) {
		t.Errorf("Get returned %d bytes, want the full %d-byte stored value unmodified", len(got), len(long))
	}
}

func TestPutWrongSize(t *testing.T) {
	s := makeTempStore(t)
	if err := s.Put(0, recordLen, []byte{1, 2, 3}); err == nil {
		t.Errorf("Put with wrong size: expected error")
	}
}

func TestPutZeroLength(t *testing.T) {
	s := makeTempStore(t)
	if err := s.Put(0, 0, makeHeader(t, 0, 0)); err == nil {
		t.Errorf("Put with length=0: expected error")
	}
}

// TestRejectsInvalidRange verifies Put, Overlapping, and ReplaceRange reject
// ranges the store cannot represent: a negative offset (the "<offset>-<length>"
// xattr name cannot round-trip one, so the entry would be silently dropped on
// read) and an offset+length that overflows int64 (which would wrap the
// overlap/union interval math and silently misreport). A large but valid range
// must still be accepted.
func TestRejectsInvalidRange(t *testing.T) {
	s := makeTempStore(t)
	hdr := makeHeader(t, 0, 0)

	bad := []struct {
		name           string
		offset, length int64
	}{
		{"negative offset", -1, recordLen},
		{"offset+length overflows", math.MaxInt64 - 100, 1000},
		{"length overflows at max offset", math.MaxInt64, 1},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			if err := s.Put(c.offset, c.length, hdr); err == nil {
				t.Errorf("Put(%d, %d): expected error", c.offset, c.length)
			}
			if _, err := s.Overlapping(c.offset, c.length); err == nil {
				t.Errorf("Overlapping(%d, %d): expected error", c.offset, c.length)
			}
			if _, err := s.ReplaceRange(c.offset, c.length, hdr); err == nil {
				t.Errorf("ReplaceRange(%d, %d): expected error", c.offset, c.length)
			}
		})
	}

	// A large but valid range must still be accepted (guards against over-rejection).
	if err := s.Put(1<<40, recordLen, hdr); err != nil {
		t.Errorf("Put(1<<40, %d): unexpected error: %v", recordLen, err)
	}
}

// TestPutVariableLengths confirms two records at the same offset with
// different lengths are stored independently (the (offset, length)
// pair is the key, not offset alone).
func TestPutVariableLengths(t *testing.T) {
	s := makeTempStore(t)
	hdrA := makeHeader(t, 0, 1)
	hdrB := makeHeader(t, 0, 2)
	if err := s.Put(0, 1024, hdrA); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := s.Put(0, 4096, hdrB); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	gotA, err := s.Get(0, 1024)
	if err != nil {
		t.Fatalf("Get A: %v", err)
	}
	gotB, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get B: %v", err)
	}
	if !bytes.Equal(gotA, hdrA) || !bytes.Equal(gotB, hdrB) {
		t.Errorf("variable-length entries leaked into each other")
	}
}

func TestForEachEntry(t *testing.T) {
	s := makeTempStore(t)
	type entry struct{ off, length int64 }
	entries := []entry{
		{0, 4096},
		{4096, 8192},
		{12288, 1024},
		{13312, 2048},
	}
	for _, e := range entries {
		if err := s.Put(e.off, e.length, makeHeader(t, e.off, uint64(e.off))); err != nil {
			t.Fatalf("Put %+v: %v", e, err)
		}
	}

	var got []entry
	err := s.ForEachEntry(func(off, length int64, hdr []byte) error {
		got = append(got, entry{off, length})
		// Sanity: stored bytes parse as a valid header with matching offset.
		h, err := block.UnmarshalHeader(hdr)
		if err != nil {
			t.Errorf("ForEach: header at offset %d does not parse: %v", off, err)
		}
		if int64(h.Offset) != off {
			t.Errorf("ForEach: header.Offset=%d, callback offset=%d", h.Offset, off)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].off < got[j].off })
	if len(got) != len(entries) {
		t.Fatalf("ForEach saw %d entries, want %d (got %+v)", len(got), len(entries), got)
	}
	for i, e := range entries {
		if got[i] != e {
			t.Errorf("ForEach[%d]=%+v, want %+v", i, got[i], e)
		}
	}
}

func TestForEachIgnoresUnrelatedXattrs(t *testing.T) {
	s := makeTempStore(t)
	// Pollute with a non-verifyio xattr.
	if err := xattr.Set(s.Target(), "user.something_else", []byte("hi"), 0); err != nil {
		t.Fatalf("seed unrelated xattr: %v", err)
	}
	// And one verifyio entry.
	if err := s.Put(0, recordLen, makeHeader(t, 0, 0)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	count := 0
	if err := s.ForEachEntry(func(off, length int64, hdr []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	if count != 1 {
		t.Errorf("ForEach saw %d entries, want 1 (unrelated xattr should be ignored)", count)
	}
}

// TestForEachEntrySkipsMalformedName is a regression test: a malformed or
// corrupted verifyio-prefixed xattr *name* must not reach the callback at all --
// verifier.VerifyFile trusts ForEachEntry's output directly into its sweep-line
// with no validation of its own, and a name that yields no valid extent has no
// place on it.
//
// The name is the ONLY thing skipped. A malformed *value* is passed straight
// through (TestForEachEntryPassesWrongSizedValueThrough), because it still has a
// usable extent and block.UnmarshalHeader owns judging its contents. Skipping it
// here instead produced a false PASS over corrupt metadata; that entry is
// reported via ForEachEntryStrict, not dropped
// (TestForEachEntryStrictReportsUnparseableName).
func TestForEachEntrySkipsMalformedName(t *testing.T) {
	s := makeTempStore(t)
	// A name that parses as two valid integers but fails validRange
	// (negative length) -- e.g. bit-rot on the name, or a foreign writer.
	if err := xattr.Set(s.Target(), xattrPrefix+"100--50", makeHeader(t, 100, 1), 0); err != nil {
		t.Fatalf("seed malformed-name xattr: %v", err)
	}
	if err := s.Put(0, recordLen, makeHeader(t, 0, 0)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	count := 0
	if err := s.ForEachEntry(func(off, length int64, hdr []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	if count != 1 {
		t.Errorf("ForEach saw %d entries, want 1 (malformed-name entry should be skipped)", count)
	}
}

func TestForEachStopsOnError(t *testing.T) {
	s := makeTempStore(t)
	for _, o := range []int64{0, 4096, 8192} {
		if err := s.Put(o, recordLen, makeHeader(t, o, 0)); err != nil {
			t.Fatalf("Put %d: %v", o, err)
		}
	}
	stopErr := errors.New("stop")
	saw := 0
	err := s.ForEachEntry(func(off, length int64, hdr []byte) error {
		saw++
		return stopErr
	})
	if !errors.Is(err, stopErr) {
		t.Errorf("ForEach error: got %v, want stopErr", err)
	}
	if saw != 1 {
		t.Errorf("ForEach should stop after first non-nil error; saw %d", saw)
	}
}

func TestRemove(t *testing.T) {
	s := makeTempStore(t)
	if err := s.Put(0, recordLen, makeHeader(t, 0, 0)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Remove(0, recordLen); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := s.Get(0, recordLen); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Remove: err=%v, want ErrNotFound", err)
	}
}

// --- Overlapping -----------------------------------------------------------

// putEntries seeds the store with the given (offset, length) extents,
// each carrying a header whose Seed is derived from the offset so test
// failures can distinguish them.
func putEntries(t *testing.T, s *Store, ents ...[2]int64) {
	t.Helper()
	for _, e := range ents {
		off, ln := e[0], e[1]
		if err := s.Put(off, ln, makeHeader(t, off, uint64(off))); err != nil {
			t.Fatalf("Put (%d,%d): %v", off, ln, err)
		}
	}
}

// extentSet collects (offset, length) pairs from a slice of Entry,
// sorted by offset so order-of-iteration in xattr.List doesn't matter.
func extentSet(ents []Entry) [][2]int64 {
	out := make([][2]int64, 0, len(ents))
	for _, e := range ents {
		out = append(out, [2]int64{e.Offset, e.Length})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

func equalExtents(a, b [][2]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestOverlappingExactMatch(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 4096})
	got, err := s.Overlapping(0, 4096)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{0, 4096}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("exact match: got %v, want %v", extentSet(got), want)
	}
	// Entry.Name should round-trip to (Offset, Length).
	if got[0].Name != XAttrName(0, 4096) {
		t.Errorf("Entry.Name=%q, want %q", got[0].Name, XAttrName(0, 4096))
	}
}

func TestOverlappingStrictSubset(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 4096})
	// Query strictly inside the stored block.
	got, err := s.Overlapping(1024, 1024)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{0, 4096}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("strict subset: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingStrictSuperset(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{1024, 1024})
	// Query strictly contains the stored block.
	got, err := s.Overlapping(0, 4096)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{1024, 1024}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("strict superset: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingPartialStart(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{1024, 2048}) // [1024, 3072)
	// Query [0, 1500) overlaps the start of the stored block.
	got, err := s.Overlapping(0, 1500)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{1024, 2048}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("partial start: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingPartialEnd(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{1024, 2048}) // [1024, 3072)
	// Query [2048, 4096) overlaps the tail of the stored block.
	got, err := s.Overlapping(2048, 2048)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{1024, 2048}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("partial end: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingAdjacentNoOverlap(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{1024, 1024}) // [1024, 2048)
	// Query starts exactly where the stored record ends -- no shared byte.
	got, err := s.Overlapping(2048, 1024)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("adjacent ranges should not overlap; got %v", extentSet(got))
	}
	// And the symmetric case (query ends where stored record starts).
	got, err = s.Overlapping(0, 1024)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("adjacent ranges (symmetric) should not overlap; got %v", extentSet(got))
	}
}

func TestOverlappingMultiple(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s,
		[2]int64{0, 1024},    // [0, 1024)     - overlaps query at [512,1024)
		[2]int64{1024, 1024}, // [1024, 2048)  - entirely inside query
		[2]int64{2048, 1024}, // [2048, 3072)  - adjacent, NOT overlapping
	)
	got, err := s.Overlapping(512, 1536) // [512, 2048)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{0, 1024}, {1024, 1024}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("multiple overlap: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingIgnoresUnrelatedEntries(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s,
		[2]int64{0, 1024},
		[2]int64{8192, 1024},
	)
	got, err := s.Overlapping(0, 2048)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{0, 1024}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("unrelated entry leaked into result: got %v, want %v",
			extentSet(got), want)
	}
}

func TestOverlappingIgnoresNonIotestXattrs(t *testing.T) {
	s := makeTempStore(t)
	if err := xattr.Set(s.Target(), "user.something_else", []byte("hi"), 0); err != nil {
		t.Fatalf("seed unrelated xattr: %v", err)
	}
	putEntries(t, s, [2]int64{0, 1024})
	got, err := s.Overlapping(0, 2048)
	if err != nil {
		t.Fatalf("Overlapping: %v", err)
	}
	want := [][2]int64{{0, 1024}}
	if !equalExtents(extentSet(got), want) {
		t.Errorf("non-verifyio xattr leaked: got %v, want %v", extentSet(got), want)
	}
}

func TestOverlappingBadLength(t *testing.T) {
	s := makeTempStore(t)
	if _, err := s.Overlapping(0, 0); err == nil {
		t.Errorf("Overlapping(_, 0): expected error")
	}
	if _, err := s.Overlapping(0, -1); err == nil {
		t.Errorf("Overlapping(_, -1): expected error")
	}
}

// --- ReplaceRange ----------------------------------------------------------

// listExtents returns every verifyio entry on the store, sorted, as
// (offset, length) pairs. Used by ReplaceRange tests to assert exact
// post-state.
func listExtents(t *testing.T, s *Store) [][2]int64 {
	t.Helper()
	var got [][2]int64
	err := s.ForEachEntry(func(off, length int64, _ []byte) error {
		got = append(got, [2]int64{off, length})
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachEntry: %v", err)
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i][0] != got[j][0] {
			return got[i][0] < got[j][0]
		}
		return got[i][1] < got[j][1]
	})
	return got
}

func TestReplaceRangeNoOverlap(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 1024})
	newHdr := makeHeader(t, 2048, 0xbeef)
	if _, err := s.ReplaceRange(2048, 1024, newHdr); err != nil {
		t.Fatalf("ReplaceRange: %v", err)
	}
	want := [][2]int64{{0, 1024}, {2048, 1024}}
	if !equalExtents(listExtents(t, s), want) {
		t.Errorf("no-overlap: got %v, want %v", listExtents(t, s), want)
	}
	// New header is readable.
	got, err := s.Get(2048, 1024)
	if err != nil {
		t.Fatalf("Get after ReplaceRange: %v", err)
	}
	if !bytes.Equal(got, newHdr) {
		t.Errorf("Get returned different bytes than ReplaceRange wrote")
	}
}

// TestReplaceRangeFullOverlapExactMatch covers the Writer's single
// most-travelled path: rewriting the same extent, which every tool does on every
// block after the first run.
//
// The `removed` assertion is the load-bearing one. The exact-match skip exists so
// the Put overwrites in place rather than doing a remove+set pair with a
// transient window where the extent is claimed by nothing -- and a verifier
// sweeping that window sees bytes no record claims, which is the anomaly
// signature. Both the extents and the stored bytes come out identical whether or
// not the skip is there, so `removed` is the ONLY externally visible difference:
// replacing the skip's condition with `if false` left all ten packages green
// while this test discarded it.
//
// It also matters downstream. A spurious entry in `removed` is handed to
// WriteBlock's zero-fill and to the refuse-guard's overlap check, both of which
// reason about extents the caller has stopped claiming.
func TestReplaceRangeFullOverlapExactMatch(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 4096})
	newHdr := makeHeader(t, 0, 0xc0de)
	removed, err := s.ReplaceRange(0, 4096, newHdr)
	if err != nil {
		t.Fatalf("ReplaceRange: %v", err)
	}
	if len(removed) != 0 {
		t.Errorf("exact-match rewrite reported %d removed record(s) (%v), want none -- "+
			"the record was removed and re-set rather than overwritten in place", len(removed), removed)
	}
	want := [][2]int64{{0, 4096}}
	if !equalExtents(listExtents(t, s), want) {
		t.Errorf("exact-match overwrite: got %v, want %v", listExtents(t, s), want)
	}
	got, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, newHdr) {
		t.Errorf("exact-match overwrite did not replace header bytes")
	}
}

func TestReplaceRangePartialOverlapShreds(t *testing.T) {
	// Existing record covers [0, 4096); new range covers only the
	// second half [2048, 4096). Replace-and-shred: the old record is
	// removed entirely, not split. The first half [0, 2048) becomes
	// uncovered (caller's responsibility to zero on the data side).
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 4096})
	newHdr := makeHeader(t, 2048, 1)
	if _, err := s.ReplaceRange(2048, 2048, newHdr); err != nil {
		t.Fatalf("ReplaceRange: %v", err)
	}
	want := [][2]int64{{2048, 2048}}
	if !equalExtents(listExtents(t, s), want) {
		t.Errorf("partial overlap should shred old entry; got %v, want %v",
			listExtents(t, s), want)
	}
	if _, err := s.Get(0, 4096); !errors.Is(err, ErrNotFound) {
		t.Errorf("old (0,4096) entry should be gone; got err=%v", err)
	}
}

func TestReplaceRangeMultipleOverlapsCollapse(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s,
		[2]int64{0, 1024},    // overlaps new range at [512, 1024)
		[2]int64{1024, 1024}, // entirely inside new range
		[2]int64{2048, 1024}, // adjacent, NOT overlapping -> untouched
	)
	newHdr := makeHeader(t, 512, 7)
	if _, err := s.ReplaceRange(512, 1536, newHdr); err != nil { // [512, 2048)
		t.Fatalf("ReplaceRange: %v", err)
	}
	want := [][2]int64{{512, 1536}, {2048, 1024}}
	if !equalExtents(listExtents(t, s), want) {
		t.Errorf("multi-overlap collapse: got %v, want %v",
			listExtents(t, s), want)
	}
	// Untouched neighbor still has its original header.
	got, err := s.Get(2048, 1024)
	if err != nil {
		t.Fatalf("Get(2048,1024): %v", err)
	}
	wantHdr := makeHeader(t, 2048, 2048)
	if !bytes.Equal(got, wantHdr) {
		t.Errorf("untouched neighbor's header changed")
	}
}

func TestReplaceRangeBadInputs(t *testing.T) {
	s := makeTempStore(t)
	if _, err := s.ReplaceRange(0, 1024, []byte{1, 2, 3}); err == nil {
		t.Errorf("ReplaceRange wrong-size header: expected error")
	}
	if _, err := s.ReplaceRange(0, 0, makeHeader(t, 0, 0)); err == nil {
		t.Errorf("ReplaceRange length=0: expected error")
	}
	if _, err := s.ReplaceRange(0, -1, makeHeader(t, 0, 0)); err == nil {
		t.Errorf("ReplaceRange length=-1: expected error")
	}
}

// --- existing helpers below -----------------------------------------------

func TestXAttrNameRoundTrip(t *testing.T) {
	cases := []struct{ off, length int64 }{
		{0, 1},
		{1, 1024},
		{0x1000, 4096},
		{1<<32 - 1, 8192},
		{1 << 50, 1024},
	}
	for _, c := range cases {
		name := XAttrName(c.off, c.length)
		gotOff, gotLen, ok := parseXAttrName(name)
		if !ok {
			t.Errorf("parseXAttrName(%q) ok=false", name)
			continue
		}
		if gotOff != c.off || gotLen != c.length {
			t.Errorf("round-trip (%d,%d): got (%d,%d)", c.off, c.length, gotOff, gotLen)
		}
	}
	if _, _, ok := parseXAttrName("user.verifyio.123"); ok {
		t.Errorf("name without -length should not parse")
	}
	if _, _, ok := parseXAttrName("user.verifyio.notanint-1024"); ok {
		t.Errorf("non-numeric offset should not parse")
	}
	if _, _, ok := parseXAttrName("user.verifyio.123-notanint"); ok {
		t.Errorf("non-numeric length should not parse")
	}
	if _, _, ok := parseXAttrName("user.other.123-456"); ok {
		t.Errorf("non-verifyio prefix should not parse")
	}
}

// TestParseXAttrNameRejectsInvalidRange is a regression test: a malformed or
// corrupted xattr *name* -- as opposed to a malformed *value*, which
// ForEachEntry already filters by size -- must not parse as ok=true just
// because both halves are individually valid integers. Without this,
// ForEachEntry (no filter at all) or Overlapping (safe only by accident,
// since a large-enough positive length can still overflow the interval math)
// could inject a bogus Entry into a caller's sweep-line with no error and no
// log line.
func TestParseXAttrNameRejectsInvalidRange(t *testing.T) {
	cases := []struct {
		name   string
		reason string
	}{
		{xattrPrefix + "100--50", "negative length (offset=100, length=-50)"},
		{xattrPrefix + "-1-1024", "negative offset"},
		{xattrPrefix + "0-0", "zero length"},
		{xattrPrefix + fmt.Sprintf("%d-%d", int64(1), int64(math.MaxInt64)), "offset+length overflows int64"},
	}
	for _, c := range cases {
		if _, _, ok := parseXAttrName(c.name); ok {
			t.Errorf("parseXAttrName(%q) = ok=true, want false (%s)", c.name, c.reason)
		}
	}
}

// TestMalformedEntryStringQuotesName pins that a malformed name cannot compose
// output.
//
// Name is raw listxattr bytes and the kernel accepts any non-NUL byte in a
// user.* name. All three sites that report these -- iotest-dump, iotest-verify
// and verifier's MalformedEntriesError -- render the entry with %s, so they
// inherit whatever String does. An unquoted name lets a planted xattr write
// arbitrary lines into their output -- and against iotest-dump, whose exit is
// always 0 and whose printed text IS the verdict, that reaches a byte-exact
// forged span line plus a line beginning "PASS:".
func TestMalformedEntryStringQuotesName(t *testing.T) {
	forged := xattrPrefix + "x\n" +
		"offset=0          length=4096     coverage=one      verdict=OK                 ok\n" +
		"PASS: everything is fine"
	got := MalformedEntry{Name: forged, Reason: "whatever"}.String()

	if strings.Contains(got, "\n") {
		t.Errorf("String emitted a raw newline, so a planted name can forge lines:\n%s", got)
	}
	// The two specific forgeries from the reproduction, as whole lines.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "PASS:") {
			t.Errorf("a planted name produced a line starting with PASS: %q", line)
		}
	}
	if !strings.Contains(got, `\n`) {
		t.Errorf("String should render the newline escaped; got %s", got)
	}
	for _, esc := range []string{"\r", "\x1b"} {
		out := MalformedEntry{Name: xattrPrefix + "a" + esc + "b", Reason: "r"}.String()
		if strings.Contains(out, esc) {
			t.Errorf("String passed through a raw %q: %s", esc, out)
		}
	}
}

// TestMalformedEntryReasonNamesTheActualFault pins that the reason describes
// the check that rejected the name.
//
// One catch-all message used to describe every rejection as "negative offset,
// non-positive length, or an extent overflowing int64". For a non-canonically
// spelled name -- which parses fine and is the case most likely to mean
// tampering rather than a bad range -- all three of those are false, and the
// operator reading the report is the one who has to tell corruption from
// tampering.
func TestMalformedEntryReasonNamesTheActualFault(t *testing.T) {
	for _, c := range []struct {
		name       string
		wantSubstr string
	}{
		{xattrPrefix + "0516-516", "not canonically spelled"},
		{xattrPrefix + "+0-516", "not canonically spelled"},
		{xattrPrefix + "123", "does not have the form"},
		{xattrPrefix + "notanint-1024", "is not a base-10 int64"},
		{xattrPrefix + "123-notanint", "is not a base-10 int64"},
		{xattrPrefix + "0-0", "not a valid range"},
		{xattrPrefix + "100--50", "not a valid range"},
	} {
		_, _, reason := parseXAttrNameReason(c.name)
		if reason == "" {
			t.Errorf("parseXAttrNameReason(%q) accepted it", c.name)
			continue
		}
		if !strings.Contains(reason, c.wantSubstr) {
			t.Errorf("parseXAttrNameReason(%q):\n got %q\nwant it to mention %q",
				c.name, reason, c.wantSubstr)
		}
	}
	// The canonical spelling of a non-canonical name is worth naming, since it
	// is what tells the operator the record was not written by this package.
	if _, _, reason := parseXAttrNameReason(xattrPrefix + "0516-516"); !strings.Contains(reason, xattrPrefix+"516-516") {
		t.Errorf("reason should name the canonical spelling; got %q", reason)
	}
}

// TestReadableLen pins the clamp that stands between a record's claimed extent
// and every buffer allocated from it. A record's length reaches the store from
// the xattr *name*, and validRange checks it only for sign and int64 overflow
// -- never against the file -- so this is the only thing preventing a corrupt
// name from driving an unbounded allocation. See ReadableLen's doc comment.
func TestReadableLen(t *testing.T) {
	const fileSize = 1024
	for _, c := range []struct {
		name           string
		offset, length int64
		want           int64
	}{
		{"wholly inside", 0, 512, 512},
		{"exactly the file", 0, fileSize, fileSize},
		{"straddles EOF", 1000, 512, 24},
		{"ends exactly at EOF", 512, 512, 512},
		{"starts exactly at EOF", fileSize, 512, 0},
		{"starts past EOF", 4096, 512, 0},
		{"absurd length from a corrupt name", 0, 1 << 40, fileSize},
		{"absurd length past EOF", 4096, 1 << 40, 0},
		{"empty file", 0, 512, 0},
		{"non-positive length", 0, 0, 0},
		{"negative length", 0, -1, 0},
		{"negative offset", -1, 512, 0},
	} {
		size := int64(fileSize)
		if c.name == "empty file" {
			size = 0
		}
		if got := ReadableLen(c.offset, c.length, size); got != c.want {
			t.Errorf("%s: ReadableLen(%d, %d, %d) = %d, want %d",
				c.name, c.offset, c.length, size, got, c.want)
		}
	}
}

// The ReleaseErrorOrCause decision table used to be pinned here too; that
// copy is now consolidated into lock_linux_test.go's
// TestReleaseErrorOrCauseDecisionTable, since ReleaseErrorOrCause lives in
// errors.go (R4's file set), not this file's (R3's). See that test for the
// current (and only) copy.

// TestForEachEntryPassesWrongSizedValueThrough pins that iteration applies no
// size gate to the stored value, matching Get's documented policy: interpreting
// header bytes belongs to block.UnmarshalHeader, and a gate here pre-empts it.
//
// It used to gate, which silently deleted the record from every caller's view --
// including the verifier's sweep line, where it produced a clean PASS over
// corrupt metadata (see TestVerifyFileShortValuedRecordOverZeroDataIsReported).
// Both a SHORT and an OVERLONG value are checked: short is corruption, overlong
// is what a newer header format looks like, and the same gate hid both.
func TestForEachEntryPassesWrongSizedValueThrough(t *testing.T) {
	s := makeTempStore(t)

	cases := []struct {
		name string
		val  []byte
	}{
		{"short", []byte("too short to be a header")},
		{"overlong", make([]byte, block.HeaderSize+16)},
	}
	for i, c := range cases {
		offset := int64(i) * 4096
		if err := xattr.Set(s.Target(), XAttrName(offset, 1024), c.val, 0); err != nil {
			t.Fatalf("Set %s value: %v", c.name, err)
		}
	}

	got := map[int64]int{}
	malformed, err := s.ForEachEntryStrict(func(offset, _ int64, header []byte) error {
		got[offset] = len(header)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachEntryStrict: %v", err)
	}
	if len(malformed) != 0 {
		t.Errorf("malformed = %v, want none -- a bad VALUE still has a usable extent", malformed)
	}
	for i, c := range cases {
		offset := int64(i) * 4096
		if got[offset] != len(c.val) {
			t.Errorf("%s: iterated header len = %d, want %d (value must arrive unmodified)",
				c.name, got[offset], len(c.val))
		}
	}
}

// TestForEachEntryStrictReportsUnparseableName pins the one category iteration
// genuinely cannot hand to a caller -- a name with no usable extent -- and that
// ForEachEntry keeps skipping it while continuing past it to the healthy records.
func TestForEachEntryStrictReportsUnparseableName(t *testing.T) {
	s := makeTempStore(t)

	if err := s.Put(0, 1024, makeHeader(t, 0, 1)); err != nil {
		t.Fatalf("Put good record: %v", err)
	}
	bad := []string{
		xattrPrefix + "100--50", // negative length
		xattrPrefix + "-1-1024", // negative offset
		xattrPrefix + "abc-def", // not integers at all
	}
	for _, n := range bad {
		if err := xattr.Set(s.Target(), n, makeHeader(t, 0, 1), 0); err != nil {
			t.Fatalf("Set %s: %v", n, err)
		}
	}
	// A foreign xattr outside our namespace must NOT be reported: this package
	// only speaks for its own prefix.
	if err := xattr.Set(s.Target(), "user.someoneelse.thing", []byte{1}, 0); err != nil {
		t.Fatalf("Set foreign xattr: %v", err)
	}

	good := 0
	malformed, err := s.ForEachEntryStrict(func(offset, length int64, _ []byte) error {
		good++
		if offset != 0 || length != 1024 {
			t.Errorf("iterated unexpected record (%d, %d)", offset, length)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachEntryStrict: %v", err)
	}
	if good != 1 {
		t.Errorf("iterated %d usable records, want 1", good)
	}
	if len(malformed) != len(bad) {
		t.Fatalf("malformed = %v, want %d entries", malformed, len(bad))
	}
	for _, m := range malformed {
		if !slices.Contains(bad, m.Name) {
			t.Errorf("reported unexpected name %q", m.Name)
		}
		if m.Reason == "" {
			t.Errorf("%q reported with no reason", m.Name)
		}
		if !strings.Contains(m.String(), m.Name) {
			t.Errorf("String() = %q does not include the name", m.String())
		}
	}
}
