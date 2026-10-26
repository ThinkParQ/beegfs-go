//go:build !linux

package verifier

import (
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// trySharedLock is a no-op on non-Linux platforms, which verifyio does not
// support. It always reports success so verification proceeds
// without locking guarantees -- but reports locked=false, so callers can
// tell via Span.Locked that this span's torn-write guarantee did not
// actually apply. See LockShared's doc comment in verifier.go.
func trySharedLock(s *xattrstore.Store, f *os.File, offset, length int64) (unlock func() error, contended bool, locked bool, err error) {
	return func() error { return nil }, false, false, nil
}
