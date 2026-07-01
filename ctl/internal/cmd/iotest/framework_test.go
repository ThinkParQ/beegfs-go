// This is a unit test.
//
// Coverage: freshCheckPath's blocklist resolution -- an ordinary path
// passes and returns its absolute form; a nonexistent path (the common
// first-run case, before start's MkdirAll creates it) also passes rather
// than erroring on a missing target; and a symlink whose target is a
// blocklisted directory is rejected, a regression test for the resolved
// vs. lexical path bug found by review (the check used to compare the
// symlink's own path against the blocklist, never the directory it
// actually pointed to).
package iotest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFreshCheckPathOrdinaryDir(t *testing.T) {
	dir := t.TempDir()
	got, err := freshCheckPath(dir)
	if err != nil {
		t.Fatalf("freshCheckPath: %v", err)
	}
	wantAbs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	// On macOS /tmp is itself a symlink to /private/tmp, so allow either the
	// absolute form or its symlink-resolved form.
	resolvedWant, err := filepath.EvalSymlinks(wantAbs)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", wantAbs, err)
	}
	if got != wantAbs && got != resolvedWant {
		t.Errorf("got %q, want %q or %q", got, wantAbs, resolvedWant)
	}
}

func TestFreshCheckPathNonexistentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist-yet")
	got, err := freshCheckPath(dir)
	if err != nil {
		t.Fatalf("freshCheckPath on a not-yet-created path (the normal first-run case): %v", err)
	}
	wantAbs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	if got != wantAbs {
		t.Errorf("got %q, want %q", got, wantAbs)
	}
}

// TestFreshCheckPathSymlinkToBlocklistedDirRejected is a regression test:
// --fresh's blocklist must reject a symlinked --path whose target is a
// blocklisted directory, not just a --path that is lexically one of the
// blocklisted strings itself.
func TestFreshCheckPathSymlinkToBlocklistedDirRejected(t *testing.T) {
	if _, err := os.Stat("/tmp"); err != nil {
		t.Skipf("/tmp not present on this system: %v", err)
	}
	link := filepath.Join(t.TempDir(), "escape-link")
	if err := os.Symlink("/tmp", link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if _, err := freshCheckPath(link); err == nil {
		t.Errorf("freshCheckPath(symlink to /tmp): got nil error, want a blocklist rejection")
	}
}
