// Package fileops provides a File type that performs IO against a single
// path using a selectable IOType.
//
// Only IOTypeBuffered is implemented. O_DIRECT, mmap and vectored IO are
// declared because a stored block header records which one wrote it, so the
// names are part of the on-disk vocabulary; Write rejects them with
// ErrIOTypeNotSupported.
//
// File is not safe for concurrent use by multiple goroutines. The
// intended pattern is one File per worker goroutine.
package fileops

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"
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

// File holds the buffered fd used for every IO against a single path, and
// for range locking. An IOType needing a second handle (an O_DIRECT fd, an
// mmap region) will open it lazily when one is implemented; none is today.
type File struct {
	path  string
	flags int
	perm  fs.FileMode
	f     *os.File // primary buffered fd; also used for range locking
	syncs atomic.Uint64
}

// Open opens the file at path with the given flags and permission bits.
// flags follows the same convention as os.OpenFile (os.O_RDWR, os.O_CREATE,
// os.O_TRUNC, etc.).
//
// Open only ever returns a regular file, reached without traversing a symlink
// at the final path component. Every caller passes a caller- or
// default-supplied path, some of them predictable fixed defaults
// (e.g. /tmp/iotest-smoke.dat) that anyone able to write the containing
// directory can pre-plant, so O_NOFOLLOW is added regardless of what the
// caller asked for -- not only on O_CREATE. A pre-planted symlink means the
// final component already exists, so O_CREATE is the one flag that attack does
// not need: opening the same planted link O_RDWR|O_TRUNC without O_NOFOLLOW
// follows the link and truncates the target.
// Unconditional also matches xattrstore.OpenStore, which is already the
// binding constraint for any tool that opens one target through both. The
// flag is forced rather than left to the caller so nothing opting out of
// O_CREATE can opt out of the guard either.
//
// A non-regular target is rejected by an Lstat before the open, because
// open(2) on a FIFO blocks indefinitely waiting for a writer -- the fstat
// below would never run. This does not cover a target replaced with a
// non-regular file in the window between the Lstat and the open; that race
// needs an active concurrent writer and is accepted deliberately.
//
// This narrows the window rather than closing it. O_NOFOLLOW guards only the
// final component, so a symlink in a parent directory still redirects the
// open, as does a hardlink planted at path. fs.protected_hardlinks=1 (the
// distro default) largely closes the hardlink case; the parent-directory
// case has no sysctl and was measured bypassing under both 1777 and 0777.
func Open(path string, flags int, perm fs.FileMode) (*File, error) {
	if info, err := os.Lstat(path); err == nil {
		if m := info.Mode(); !m.IsRegular() && m&fs.ModeSymlink == 0 {
			return nil, fmt.Errorf("fileops.Open: %s is not a regular file (mode=%s)", path, m)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("fileops.Open: stat %s: %w", path, err)
	}

	flags |= unix.O_NOFOLLOW
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, fmt.Errorf("fileops.Open: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("fileops.Open: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("fileops.Open: %s is not a regular file (mode=%s)",
			path, info.Mode())
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

// LockFd returns the primary file description used for F_SETLK range
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
	f.syncs.Add(1)
	return nil
}

// Syncs returns how many times Sync has succeeded on this File.
//
// It exists so a caller can pin the claim "this data was flushed" to the flush
// actually happening, rather than to the code that was supposed to perform it
// still being present. xattrstore's Writer stamps block.TagFsynced to tell a
// verifier that a BODY_CORRUPT block really did reach the storage servers -- so
// a stamp with no flush behind it makes the verifier blame the filesystem for
// the tool's own omission. Counting on the success path only is what gives the
// count that meaning: a failed Sync leaves it unchanged.
//
// Not a metric, and not for deciding anything at runtime: nothing in the
// production path reads it.
func (f *File) Syncs() uint64 { return f.syncs.Load() }

// Size returns the file's current length, via the held file description
// rather than the path, so it always describes the file this File reads and
// writes even if the path has since been replaced.
func (f *File) Size() (int64, error) {
	info, err := f.f.Stat()
	if err != nil {
		return 0, fmt.Errorf("fileops.Size: %w", err)
	}
	return info.Size(), nil
}

// Truncate changes the file's length to size, via the held file description
// rather than the path.
//
// This is the data half of a truncation only. Truncating away bytes that an
// xattrstore record still claims leaves that record describing data which no
// longer exists, which a verifier reports as an anomaly -- correctly, since it
// cannot tell an intentional truncate from lost data. Callers that maintain an
// xattrstore over this file must therefore go through xattrstore.Writer.Truncate,
// which reconciles both halves; reach for this method directly only for a file
// with no records.
func (f *File) Truncate(size int64) error {
	if err := f.f.Truncate(size); err != nil {
		return fmt.Errorf("fileops.Truncate: %w", err)
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
