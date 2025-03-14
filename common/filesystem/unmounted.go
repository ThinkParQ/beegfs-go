package filesystem

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// UnmountedFS can be used whenever a mounted file system may not actually be required. This allows
// use of a common Filesystem interface with callers deciding on a case-by-base basis if they can
// work with an unmounted file system. Most functions available through the Filesystem interface
// will return ErrUnmounted.
//
// Because different callers will likely want to implement different logic to determine when it is
// acceptable to fallback to an UnmountedFS, the NewFromX() functions never return an UnmountedFS.
// Instead callers should examine the errors returned by NewFromPath() to decide if the use of an
// UnmountedFS is allowed based on their use case, then initialize the struct directly. For example
// see config.BeeGFSClient().
type UnmountedFS struct {
}

func (fs UnmountedFS) GetMountPath() string {
	return ""
}

func (fs UnmountedFS) GetRelativePathWithinMount(path string) (string, error) {
	return filepath.Clean("/" + path), nil
}

func (fs UnmountedFS) Stat(path string) (os.FileInfo, error) {
	return nil, ErrUnmounted
}

func (fs UnmountedFS) Lstat(path string) (os.FileInfo, error) {
	return nil, ErrUnmounted
}

func (fs UnmountedFS) CreatePreallocatedFile(path string, size int64, overwrite bool) error {
	return ErrUnmounted
}

func (fs UnmountedFS) CreateWriteClose(path string, buf []byte) error {
	return ErrUnmounted
}

func (fs UnmountedFS) Remove(path string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) Open(path string) (io.ReadCloser, error) {
	return nil, ErrUnmounted
}

func (fs UnmountedFS) ReadFilePart(path string, offsetStart int64, offsetStop int64) (io.Reader, string, error) {

	return nil, "", ErrUnmounted
}

func (fs UnmountedFS) WriteFilePart(path string, offsetStart int64, offsetStop int64) (io.WriteCloser, error) {
	return nil, ErrUnmounted
}

func (fs UnmountedFS) WalkDir(path string, fn fs.WalkDirFunc, opts ...WalkOption) error {
	return ErrUnmounted
}

func (fs UnmountedFS) CopyXAttrsToFile(srcPath, dstPath string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) CopyContentsToFile(srcPath, dstPath string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) CopyOwnerAndMode(fromStat fs.FileInfo, dstPath string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) CopyTimestamps(fromStat fs.FileInfo, dstPath string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) OverwriteFile(srcPath, dstPath string) error {
	return ErrUnmounted
}

func (fs UnmountedFS) Readlink(path string) (string, error) {
	return "", ErrUnmounted
}
