//go:build linux

package xattrstore

// lockRegion acquires an OFD range lock over [offset, offset+length)
// and returns a function that releases it.
func (w *Writer) lockRegion(offset, length int64) (func() error, error) {
	lease, err := w.store.TryAcquireExclusive(w.file.LockFd(), offset, length)
	if err != nil {
		return func() error { return nil }, err
	}
	return lease.Release, nil
}
