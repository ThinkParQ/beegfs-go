// This is a unit test. It exercises real xattr syscalls against a temp file
// (skipped if the underlying filesystem doesn't support user xattrs).
//
// Coverage: Set/Get/Remove round-trip and replace-in-place; Get/Remove of a
// missing attribute returning ErrNotFound; List; XATTR_REPLACE against a
// missing attribute; XATTR_CREATE exclusivity (second create fails EEXIST);
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
	// Probe support: try setting and immediately removing a probe attr.
	if err := Set(path, "user.verifyio_probe", []byte{0}, 0); err != nil {
		t.Skipf("user xattrs not supported on tmp filesystem: %v", err)
	}
	_ = Remove(path, "user.verifyio_probe")
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
