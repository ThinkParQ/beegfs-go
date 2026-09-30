//go:build !linux

package xattrstore

import "errors"

// lockRegion is a stub on non-Linux platforms, which verifyio does not
// support; see the header comment on lock_linux.go.
func (w *Writer) lockRegion(offset, length int64) (func() error, error) {
	return func() error { return nil }, errors.New("xattrstore: range locking not supported on this platform")
}
