//go:build linux

package verifier

import (
	"errors"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// trySharedLock tries to acquire a shared OFD lock over [offset,
// offset+length) using f as the lock fd. Returns:
//   - unlock: a function that releases the lock (always non-nil)
//   - contended: true if an exclusive holder blocked the attempt
//   - err: non-nil only for unexpected failures (not contention)
func trySharedLock(s *xattrstore.Store, f *os.File, offset, length int64) (unlock func() error, contended bool, err error) {
	lease, err := s.TryAcquireShared(f, offset, length)
	if err != nil {
		if errors.Is(err, xattrstore.ErrLockBusy) {
			return func() error { return nil }, true, nil
		}
		return func() error { return nil }, false, err
	}
	return lease.Release, false, nil
}
