//go:build !linux

package xattrstore

import "errors"

// lockRegion is a stub on non-Linux platforms where OFD locks are not
// available.
func (w *Writer) lockRegion(offset, length int64) (func() error, error) {
	return func() error { return nil }, errors.New("xattrstore: OFD locking not supported on this platform")
}
