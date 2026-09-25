// Package testxattr provides the shared "does this filesystem support user
// xattrs" probe for tests.
//
// The probe deliberately calls unix.Setxattr rather than the xattr package: the
// packages under test are the ones being probed, so using the wrapper let a
// regression in xattr.Set silently skip the suites that would have caught it --
// 121 tests across three packages, with the run still reporting ok.
package testxattr

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

// RequireSupport skips the test if the filesystem under path cannot hold user
// xattrs, and fails it on any other errno. Only EOPNOTSUPP/ENOTSUP is a skip;
// EPERM, ENOSPC, EDQUOT and an SELinux denial are real failures and must not be
// mistaken for an unsupported filesystem.
func RequireSupport(t *testing.T, path string) {
	t.Helper()
	const probe = "user.verifyio_probe"
	err := unix.Setxattr(path, probe, []byte{0}, 0)
	if err != nil {
		// unix.ENOTSUP and unix.EOPNOTSUPP are the same value on Linux; check
		// both anyway to state the intent.
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
			t.Skipf("user xattrs not supported on %s: %v", path, err)
		}
		t.Fatalf("probe Setxattr on %s: %v", path, err)
	}
	_ = unix.Removexattr(path, probe)
}
