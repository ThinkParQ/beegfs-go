// This is a unit test. It exercises real xattr syscalls against a temp file
// (skipped if the underlying filesystem doesn't support user xattrs).
//
// Coverage: Set/Get/Remove round-trip and replace-in-place; Get/Remove of a
// missing attribute returning ErrNotFound; List; XATTR_REPLACE against a
// missing attribute; XATTR_CREATE exclusivity (second create fails EEXIST);
// probeAndGet's two-call protocol driven directly: an attribute removed
// between the size probe and the read reports ErrNotFound rather than a raw
// ENODATA, and an ERANGE between them retries instead, but a probe that never
// agrees with its read fails after a bounded number of attempts rather than
// spinning;
// and SplitNULStrings parsing of the raw listxattr byte stream (empty input,
// single/multiple NUL-terminated names, missing trailing NUL, empty segments
// dropped).
package xattr

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/internal/testxattr"
	"golang.org/x/sys/unix"
)

// makeTempFile creates a regular file in the test's temp directory and
// returns its path. Skips the test if the underlying tmp filesystem
// does not support user xattrs (e.g., tmpfs without user_xattr, or a
// system without xattr support at all).
func makeTempFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "xattr-test")
	if err := os.WriteFile(path, []byte("hi"), 0644); err != nil {
		t.Fatalf("create tempfile: %v", err)
	}
	testxattr.RequireSupport(t, path)
	return path
}

func TestSetGetRemove(t *testing.T) {
	p := makeTempFile(t)
	const name = "user.verifyio_one"
	val := []byte("hello world")

	if err := Set(p, name, val, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := Get(p, name)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get returned %q, want %q", got, val)
	}

	// Replace
	val2 := []byte("replaced")
	if err := Set(p, name, val2, 0); err != nil {
		t.Fatalf("Set replace: %v", err)
	}
	got, _ = Get(p, name)
	if !bytes.Equal(got, val2) {
		t.Errorf("after replace: got %q, want %q", got, val2)
	}

	if err := Remove(p, name); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := Get(p, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Remove: err = %v, want ErrNotFound", err)
	}
	if err := Remove(p, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("Remove of missing attr: err = %v, want ErrNotFound", err)
	}
}

func TestList(t *testing.T) {
	p := makeTempFile(t)
	names := []string{"user.verifyio_a", "user.verifyio_b", "user.verifyio_c"}
	for _, n := range names {
		if err := Set(p, n, []byte("v"), 0); err != nil {
			t.Fatalf("Set %s: %v", n, err)
		}
	}
	got, err := List(p)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, n := range names {
		if !slices.Contains(got, n) {
			t.Errorf("List missing %q (got %v)", n, got)
		}
	}
}

func TestSetReplaceMissing(t *testing.T) {
	p := makeTempFile(t)
	if err := Set(p, "user.verifyio_nope", []byte("x"), unix.XATTR_REPLACE); !errors.Is(err, ErrNotFound) {
		t.Errorf("Set REPLACE on missing: err=%v, want ErrNotFound", err)
	}
}

func TestSetCreateExclusive(t *testing.T) {
	p := makeTempFile(t)
	const name = "user.verifyio_excl"
	if err := Set(p, name, []byte("x"), unix.XATTR_CREATE); err != nil {
		t.Fatalf("first XATTR_CREATE: %v", err)
	}
	if err := Set(p, name, []byte("y"), unix.XATTR_CREATE); !errors.Is(err, unix.EEXIST) {
		t.Errorf("second XATTR_CREATE: err = %v, want EEXIST", err)
	}
}

// makeTempFileFd is makeTempFile's fd-based counterpart: returns an open
// *os.File on the same kind of probed, xattr-capable temp file, for tests
// exercising the Fd variants. The caller owns closing it.
func makeTempFileFd(t *testing.T) *os.File {
	t.Helper()
	path := makeTempFile(t)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestSetGetRemoveFd(t *testing.T) {
	f := makeTempFileFd(t)
	fd := int(f.Fd())
	const name = "user.verifyio_one_fd"
	val := []byte("hello world")

	if err := SetFd(fd, name, val, 0); err != nil {
		t.Fatalf("SetFd: %v", err)
	}
	got, err := GetFd(fd, name)
	if err != nil {
		t.Fatalf("GetFd: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("GetFd returned %q, want %q", got, val)
	}

	// Replace
	val2 := []byte("replaced")
	if err := SetFd(fd, name, val2, 0); err != nil {
		t.Fatalf("SetFd replace: %v", err)
	}
	got, _ = GetFd(fd, name)
	if !bytes.Equal(got, val2) {
		t.Errorf("after replace: got %q, want %q", got, val2)
	}

	if err := RemoveFd(fd, name); err != nil {
		t.Fatalf("RemoveFd: %v", err)
	}
	if _, err := GetFd(fd, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetFd after RemoveFd: err = %v, want ErrNotFound", err)
	}
	if err := RemoveFd(fd, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("RemoveFd of missing attr: err = %v, want ErrNotFound", err)
	}
}

func TestListFd(t *testing.T) {
	f := makeTempFileFd(t)
	fd := int(f.Fd())
	names := []string{"user.verifyio_a_fd", "user.verifyio_b_fd", "user.verifyio_c_fd"}
	for _, n := range names {
		if err := SetFd(fd, n, []byte("v"), 0); err != nil {
			t.Fatalf("SetFd %s: %v", n, err)
		}
	}
	got, err := ListFd(fd)
	if err != nil {
		t.Fatalf("ListFd: %v", err)
	}
	for _, n := range names {
		if !slices.Contains(got, n) {
			t.Errorf("ListFd missing %q (got %v)", n, got)
		}
	}
}

// TestFdVariantsSurvivePathSwap is the regression test this package's Fd
// variants exist for: once a caller holds an fd, replacing what the
// original path points to must not affect operations already using that
// fd -- unlike the path-based variants, which would silently follow the
// swap on their next call.
func TestFdVariantsSurvivePathSwap(t *testing.T) {
	f := makeTempFileFd(t)
	fd := int(f.Fd())
	const name = "user.verifyio_survives_swap"
	val := []byte("original")
	if err := SetFd(fd, name, val, 0); err != nil {
		t.Fatalf("SetFd: %v", err)
	}

	// Swap the path this fd was originally opened from to point at a
	// different file entirely.
	origPath := f.Name()
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

	// The held fd must still see the original file's attribute, unaffected
	// by the swap -- it never re-resolves origPath.
	got, err := GetFd(fd, name)
	if err != nil {
		t.Fatalf("GetFd after path swap: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("GetFd after path swap = %q, want %q (the fd must be immune to the swap)", got, val)
	}

	// The path-based Get, by contrast, now follows the swap to the other
	// file, which never had this attribute set -- demonstrating exactly
	// the exposure the Fd variants close.
	if _, err := Get(origPath, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(origPath) after swap: err = %v, want ErrNotFound (should now be resolving to the swapped-in file)", err)
	}
}

func TestSplitNULStrings(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want []string
	}{
		{"empty", nil, nil},
		{"single nul-terminated", []byte("foo\x00"), []string{"foo"}},
		{"multiple", []byte("foo\x00bar\x00baz\x00"), []string{"foo", "bar", "baz"}},
		{"missing trailing nul", []byte("foo\x00bar"), []string{"foo", "bar"}},
		{"empty segments dropped", []byte("\x00foo\x00\x00bar\x00"), []string{"foo", "bar"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitNULStrings(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProbeAndGetRemovedBetweenProbeAndRead pins that an attribute removed
// between probeAndGet's two syscalls is reported as ErrNotFound, not as a raw
// ENODATA.
//
// probeAndGet sizes the value with one getxattr and reads it with a second, so
// a concurrent removal lands in between. The size probe mapped not-found to the
// sentinel; the read did not, returning bare unix.ENODATA. Callers check the
// sentinel -- xattrstore.ForEachEntry skips records another writer shredded
// mid-iteration -- so the unmapped errno turned a benign, expected race into a
// hard error that aborted an entire verifier sweep and failed the run.
//
// The window went unnoticed while nothing removed records during a soak. It
// became reachable as soon as Writer.WriteBlock started shredding overlapping
// records, and showed up as an intermittent soak failure (roughly one run in
// four) reading "getxattr ...: no data available".
//
// Driving probeAndGet's raw closure directly makes the interleaving exact,
// rather than hoping two goroutines collide.
func TestProbeAndGetRemovedBetweenProbeAndRead(t *testing.T) {
	calls := 0
	_, err := probeAndGet(func(dest []byte) (int, error) {
		calls++
		if dest == nil {
			return 128, nil // size probe: the attribute still exists
		}
		return 0, unix.ENODATA // removed before the read landed
	})
	if calls != 2 {
		t.Fatalf("raw called %d times, want 2 (size probe then read)", calls)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v (%T), want ErrNotFound -- callers skipping a "+
			"concurrently-removed record match on the sentinel, not the errno", err, err)
	}
}

// TestProbeAndGetRetriesWhenValueGrows guards the neighbouring branch: ERANGE
// between the probe and the read means the value grew, which must retry rather
// than be mistaken for the removal case above.
func TestProbeAndGetRetriesWhenValueGrows(t *testing.T) {
	sizes := []int{4, 8}
	attempt := 0
	got, err := probeAndGet(func(dest []byte) (int, error) {
		if dest == nil {
			return sizes[min(attempt, len(sizes)-1)], nil
		}
		if attempt == 0 {
			attempt++
			return 0, unix.ERANGE // grew between probe and read
		}
		for i := range dest {
			dest[i] = byte(i)
		}
		return len(dest), nil
	})
	if err != nil {
		t.Fatalf("probeAndGet: %v", err)
	}
	if len(got) != 8 {
		t.Errorf("got %d bytes, want 8 (the grown size after retry)", len(got))
	}
}

// TestProbeAndGetBoundsAnUnstableProbe pins the loop's termination. The retry
// above is correct while each re-probe reports the grown size, but nothing makes
// a probe agree with the read that follows it: they are two syscalls, and on a
// distributed filesystem two round trips. A probe that keeps reporting a size
// the read keeps rejecting used to spin forever at full CPU with nothing logged.
// It must fail, and say so.
func TestProbeAndGetBoundsAnUnstableProbe(t *testing.T) {
	calls := 0
	_, err := probeAndGet(func(dest []byte) (int, error) {
		calls++
		if dest == nil {
			return 4, nil // probe never reports a size the read will accept
		}
		return 0, unix.ERANGE
	})
	if err == nil {
		t.Fatal("probeAndGet: got nil error, want a bounded failure rather than an endless retry")
	}
	if !errors.Is(err, errProbeUnstable) {
		t.Errorf("err = %v, want it to wrap errProbeUnstable", err)
	}
	if want := 2 * maxProbeAttempts; calls != want {
		t.Errorf("raw called %d times, want %d (a probe and a read per attempt, capped)", calls, want)
	}
}

// TestProbeAndListBoundsAnUnstableProbe is the same guard on the list path,
// which shares the shape and had the same unbounded loop.
func TestProbeAndListBoundsAnUnstableProbe(t *testing.T) {
	calls := 0
	_, err := probeAndList(func(dest []byte) (int, error) {
		calls++
		if dest == nil {
			return 4, nil
		}
		return 0, unix.ERANGE
	})
	if err == nil {
		t.Fatal("probeAndList: got nil error, want a bounded failure rather than an endless retry")
	}
	if !errors.Is(err, errProbeUnstable) {
		t.Errorf("err = %v, want it to wrap errProbeUnstable", err)
	}
	if want := 2 * maxProbeAttempts; calls != want {
		t.Errorf("raw called %d times, want %d (a probe and a read per attempt, capped)", calls, want)
	}
}
