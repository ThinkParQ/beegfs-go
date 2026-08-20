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
// and XAttrName<->offset/length round-trip.
package xattrstore

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
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
	if err := xattr.Set(path, "user.verifyio_probe", []byte{0}, 0); err != nil {
		t.Skipf("user xattrs not supported: %v", err)
	}
	_ = xattr.Remove(path, "user.verifyio_probe")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
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
	if _, err := OpenStore(missing); err == nil {
		t.Errorf("OpenStore on missing path: expected error, got nil")
	}
}

func TestOpenStoreOnDirectory(t *testing.T) {
	dir := t.TempDir() // directory, not a regular file
	if _, err := OpenStore(dir); err == nil {
		t.Errorf("OpenStore on directory: expected error, got nil")
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
			if err := s.ReplaceRange(c.offset, c.length, hdr); err == nil {
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
	if err := s.ReplaceRange(2048, 1024, newHdr); err != nil {
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

func TestReplaceRangeFullOverlapExactMatch(t *testing.T) {
	s := makeTempStore(t)
	putEntries(t, s, [2]int64{0, 4096})
	newHdr := makeHeader(t, 0, 0xc0de)
	if err := s.ReplaceRange(0, 4096, newHdr); err != nil {
		t.Fatalf("ReplaceRange: %v", err)
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
	if err := s.ReplaceRange(2048, 2048, newHdr); err != nil {
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
	if err := s.ReplaceRange(512, 1536, newHdr); err != nil { // [512, 2048)
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
	if err := s.ReplaceRange(0, 1024, []byte{1, 2, 3}); err == nil {
		t.Errorf("ReplaceRange wrong-size header: expected error")
	}
	if err := s.ReplaceRange(0, 0, makeHeader(t, 0, 0)); err == nil {
		t.Errorf("ReplaceRange length=0: expected error")
	}
	if err := s.ReplaceRange(0, -1, makeHeader(t, 0, 0)); err == nil {
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
