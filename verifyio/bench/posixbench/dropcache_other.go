//go:build !linux

package posixbench

import "os"

// dropCache is a no-op on platforms without posix_fadvise(2). unix.Fadvise and
// unix.FADV_DONTNEED are Linux-only in golang.org/x/sys/unix, so calling them
// unguarded broke the build everywhere else -- and CI runs ubuntu-latest, so
// nothing here catches that.
//
// Read-phase numbers on these platforms include whatever the page cache still
// holds from the write phase, and are correspondingly optimistic. The rest of
// the tree treats non-Linux as best-effort in the same way; see
// verifier_lock_other.go.
func dropCache(f *os.File) {}
