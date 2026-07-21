// Package fileops provides a File type that manages all file handles
// needed for different IO methods (buffered, O_DIRECT, mmap, vectored).
// Handles are lazy-initialized on first use of each IO type.
//
// File is not safe for concurrent use by multiple goroutines. The
// intended pattern is one File per worker goroutine.
package fileops

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// IOType selects the IO method used for a write operation.
type IOType int

const (
	IOTypeBuffered IOType = iota + 1 // standard pwrite(2) via os.File.WriteAt
	IOTypeODirect                    // O_DIRECT pwrite(2) — bypasses page cache
	IOTypeMmap                       // mmap(2) + memcpy into mapped region
	IOTypePwritev                    // pwritev(2) — scatter/gather vectored IO
)

// String returns the printable name of an IOType.
func (t IOType) String() string {
	switch t {
	case IOTypeBuffered:
		return "buffered"
	case IOTypeODirect:
		return "odirect"
	case IOTypeMmap:
		return "mmap"
	case IOTypePwritev:
		return "pwritev"
	default:
		return fmt.Sprintf("iotype(%d)", int(t))
	}
}

// ErrIOTypeNotSupported is returned by Write when the requested IOType
// has not been implemented yet.
var ErrIOTypeNotSupported = errors.New("fileops: IOType not supported")

// File owns all file handles required to perform IO against a single
// path using any supported IOType. Handles beyond the primary buffered
// fd are opened or mapped lazily on first use.
type File struct {
	path  string
	flags int
	perm  fs.FileMode
	f     *os.File // primary buffered fd; also used for OFD locking
}

// Open opens or creates the file at path with the given flags and
// permission bits. flags follows the same convention as os.OpenFile
// (os.O_RDWR, os.O_CREATE, os.O_TRUNC, etc.). The primary buffered fd
// is opened immediately; other handles are opened lazily.
func Open(path string, flags int, perm fs.FileMode) (*File, error) {
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, fmt.Errorf("fileops.Open: %w", err)
	}
	return &File{
		path:  path,
		flags: flags,
		perm:  perm,
		f:     f,
	}, nil
}

// Close releases all open handles and mapped regions held by the File.
func (f *File) Close() error {
	if f.f != nil {
		if err := f.f.Close(); err != nil {
			return fmt.Errorf("fileops.Close: %w", err)
		}
		f.f = nil
	}
	return nil
}

// LockFd returns the primary file description used for OFD range
// locking. Callers must not close the returned *os.File directly;
// use File.Close instead.
func (f *File) LockFd() *os.File {
	return f.f
}

// Write writes buf to the file at offset using the requested IOType.
// buf must contain exactly the bytes to be written; the caller is
// responsible for sizing it correctly.
func (f *File) Write(ioType IOType, buf []byte, offset int64) error {
	switch ioType {
	case IOTypeBuffered:
		return f.writeBuffered(buf, offset)
	case IOTypeODirect, IOTypeMmap, IOTypePwritev:
		return fmt.Errorf("%w: %s", ErrIOTypeNotSupported, ioType)
	default:
		return fmt.Errorf("%w: %s", ErrIOTypeNotSupported, ioType)
	}
}

// ReadAt reads len(buf) bytes from the file starting at offset.
func (f *File) ReadAt(buf []byte, offset int64) (int, error) {
	n, err := f.f.ReadAt(buf, offset)
	if err != nil {
		return n, fmt.Errorf("fileops.ReadAt: %w", err)
	}
	return n, nil
}

// Sync commits the file's current contents to stable storage.
func (f *File) Sync() error {
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("fileops.Sync: %w", err)
	}
	return nil
}

// writeBuffered performs a standard pwrite via os.File.WriteAt.
func (f *File) writeBuffered(buf []byte, offset int64) error {
	if _, err := f.f.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("fileops.writeBuffered: %w", err)
	}
	return nil
}
