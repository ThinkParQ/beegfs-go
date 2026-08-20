//go:build !linux

package verifier

import (
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// trySharedLock is a no-op on non-Linux platforms where OFD locks are
// not available. It always reports success so verification proceeds
// without locking guarantees -- see LockShared's doc comment in
// verifier.go for what this means for callers on this platform.
func trySharedLock(s *xattrstore.Store, f *os.File, offset, length int64) (func() error, bool, error) {
	return func() error { return nil }, false, nil
}
