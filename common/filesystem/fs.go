package filesystem

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// When working with extended attributes, a buffer of this size is allocated to store values
	// then dynamically grown as needed.
	initialXattrValBufferSize = 1024
)

// The use of an interface is mostly to allow file system operations to be
// mocked for tests. This could also potentially allow us to change behavior
// depending if we're working with BeeGFS or some other file system.
type Provider interface {
	// Returns the path where the Filesystem is mounted.
	GetMountPath() string
	// Given an absolute or relative path computes the relative path of the file/directory within a
	// specified mount point and returns a path that is ready to be used with any of the Filesystem
	// methods. The resulting path is prefixed with "/" and represents the location relative to the
	// mount point. If the provided path is not prefixed with the specified mount point, it is
	// presumed to already be a relative path inside the mount point and returned as is (possibly
	// with '/' added).
	GetRelativePathWithinMount(path string) (string, error)
	// Returns the equivalent of an stat(2) with -L flag. Caution: Follows symbolic links (use lstat if needed).
	Stat(name string) (os.FileInfo, error)
	// Returns the equivalent of stat(2) without the -L flag. Does not follow symbolic links.
	Lstat(name string) (os.FileInfo, error)
	// Creates a file at the specified path returning an error if the file already exists, unless
	// overwrite is true, then the file will be zeroed and overwritten. If created and supported by
	// the underlying file system, the new file will be extended to the specified size to reduce
	// fragmentation and attempt to guarantee enough space will be available for subsequent
	// operations, such as a download. Note implementations should not actual write zeros to the
	// entire file, but should use optimized methods like fallocate() or truncate(). This also means
	// it is not safe to rely on CreatePreallocatedFile to securely wipe an existing file.
	CreatePreallocatedFile(name string, size int64, overwrite bool) error
	// CreateWriteClose creates the file specified by name and immediately writes the specified buf
	// as the file contents then closes the file.
	CreateWriteClose(name string, buf []byte, mode uint32, overwrite bool) error
	// Removes the file specified by name.
	Remove(name string) error
	// Opens the file specified by name and returns it as an io.ReadCloser. The caller must close the
	// file when it is no longer required.
	Open(name string) (io.ReadCloser, error)
	// ReadFilePart reads the part of the named file from offsetStart up to and including offsetStop
	// into a new bytes buffer, calculates the sha256 checksum, then returns an io.Reader that
	// allows limited access to the file and the base64 encoded sha256 checksum of the part.
	ReadFilePart(name string, offsetStart int64, offsetStop int64) (reader io.Reader, checksumSHA256 string, err error)
	// Recursively create any missing portion of the directory structure.
	CreateDir(path string, mode uint32) error
	// WriteFilePart will open the file at the specified path for writing, and immediately return an
	// error if anything goes wrong (such as the file does not exist). Otherwise it returns a special
	// WriteCloser that will only allow the caller to write to the file between the specified offsets
	// and will return an error if the length of a requested Write() would exceed the remaining space in
	// the offset range. If this happens no data is written and the caller can optionally adjust the
	// size of their write and try again. The caller must always call Close() to cleanup when they are
	// done writing (regardless if there was a Write() error). The reason the open is split from the
	// writing is to allow the caller to skip any expensive operations that need to happen before the
	// Write (such as a download) if there is no valid file to write the results to.
	WriteFilePart(path string, offsetStart int64, offsetStop int64) (io.WriteCloser, error)
	// A wrapper around filepath.WalkDir allowing it to be used with relative paths inside a
	// Filesystem and apply different options for walking the path, for example walking in
	// lexicographical order. Note WalkDir may return an absolute path, thus the paths it returns
	// should be sanitized with GetRelativePathWithinMount() if needed.
	WalkDir(path string, fn fs.WalkDirFunc, walkOpts ...WalkOption) error
	// CopyXAttrsToFile copies the xattrs from srcPath to dstPath. If there are no xattrs or if the BeeGFS
	// instance does not have user xattrs enabled, no error is returned.
	CopyXAttrsToFile(srcPath, dstPath string) error
	CopyContentsToFile(srcPath, dstPath string) error
	CopyOwnerAndMode(fromStat fs.FileInfo, dstPath string) error
	// CopyTimestamps sets the atime/mtime in fromStat on dstPath.
	CopyTimestamps(fromStat fs.FileInfo, dstPath string) error
	// Atomically renames srcPath to dstPath overwriting the dstPath with srcPath's contents.
	OverwriteFile(srcPath, dstPath string) error
	Readlink(path string) (string, error)
}

// NewFromMountPoint accepts an exact path where a file system is mounted. It detects the file
// system type then returns an appropriately initialized Provider. If you only known a path
// somewhere inside the mount point, use NewFromPath().
func NewFromMountPoint(path string) (Provider, error) {

	if path == "" || path == "/" {
		return nil, fmt.Errorf("%w: %s", ErrInitFSClient, path)
	}
	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return nil, fmt.Errorf("unable to determine file system type for path %s using statfs: %w", path, err)
	}
	// This is the BEEGFS_MAGIC number as defined in FhgfsOpsSuper.h.
	if stat.Type == 0x19830326 {
		return BeeGFS{MountPoint: path}, nil
	}
	return nil, ErrUnsupportedFileSystem
}

// NewFromPath() automatically initializes a file system provider from the provided path. This works
// by walking the path to find its mount point then using that path with NewFromMountPoint(). The
// path will be traversed regardless of the existence of the files or directories until a mounted
// file system is determined.
func NewFromPath(path string) (Provider, error) {

	mountPath := ""
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	for {
		currentStat, err := os.Lstat(absPath)
		if err != nil {
			if errors.Is(err, os.ErrPermission) {
				return nil, fmt.Errorf("%s: %w", path, ErrInitFSClient)
			}
			parentPath := filepath.Dir(absPath)
			if parentPath == absPath {
				return nil, fmt.Errorf("%s: %w", path, ErrInitFSClient)
			}
			absPath = parentPath
			continue
		}
		parentPath := filepath.Dir(absPath)
		if parentPath == absPath {
			return nil, fmt.Errorf("%s: %w", path, ErrInitFSClient)
		}
		parentStat, err := os.Lstat(parentPath)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInitFSClient, err)
		}
		if currentStat.Sys().(*syscall.Stat_t).Dev != parentStat.Sys().(*syscall.Stat_t).Dev {
			mountPath = absPath
			break
		}
		// Otherwise move up a level.
		absPath = parentPath
	}

	return NewFromMountPoint(mountPath)
}

type BeeGFS struct {
	MountPoint string
}

func (fs BeeGFS) GetMountPath() string {
	return fs.MountPoint
}

func (fs BeeGFS) GetRelativePathWithinMount(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean("/" + strings.TrimPrefix(absPath, fs.MountPoint)), nil
}

func (fs BeeGFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(filepath.Join(fs.MountPoint, name))
}

func (fs BeeGFS) Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(filepath.Join(fs.MountPoint, name))
}

// BeeGFS does not support fallocate(), this uses truncate() instead. As a result this doesn't
// actually preallocate the chunkfiles, instead they are effectively sparse files (i.e., ls will
// show the specified size, but du will show 0). Thus it is less likely the file will occupy
// contiguous space, and there is no guarantee the disk won't run out of space when we try to write
// to the file.
func (fs BeeGFS) CreatePreallocatedFile(path string, size int64, overwrite bool) error {
	var file *os.File
	var err error
	if overwrite {
		file, err = os.OpenFile(filepath.Join(fs.MountPoint, path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	} else {
		file, err = os.OpenFile(filepath.Join(fs.MountPoint, path), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	}
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	return file.Close()
}

func (fs BeeGFS) CreateWriteClose(path string, buf []byte, mode uint32, overwrite bool) error {
	var file *os.File
	var err error
	if overwrite {
		file, err = os.OpenFile(filepath.Join(fs.MountPoint, path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(mode))
	} else {
		file, err = os.OpenFile(filepath.Join(fs.MountPoint, path), os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(mode))
	}
	if err != nil {
		return err
	}

	if _, err := file.Write(buf); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	return file.Close()
}

func (fs BeeGFS) Remove(path string) error {
	return os.Remove(filepath.Join(fs.MountPoint, path))
}

func (fs BeeGFS) Open(path string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(fs.MountPoint, path))
}

func (fs BeeGFS) ReadFilePart(path string, offsetStart int64, offsetStop int64) (io.Reader, string, error) {
	file, err := os.Open(filepath.Join(fs.MountPoint, path))
	if err != nil {
		return nil, "", err
	}
	defer file.Close()

	filePart, err := readFilePart(file, offsetStart, offsetStop)
	if err != nil {
		return nil, "", err
	}

	return bytes.NewReader(filePart), getFilePartChecksumSHA256(filePart), nil
}

func (fs BeeGFS) WriteFilePart(path string, offsetStart int64, offsetStop int64) (io.WriteCloser, error) {
	file, err := os.OpenFile(filepath.Join(fs.MountPoint, path), os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	return newLimitedFileWriter(file, offsetStart, offsetStop), nil
}

func (fs BeeGFS) CreateDir(path string, mode uint32) error {
	err := os.MkdirAll(filepath.Join(fs.MountPoint, path), os.FileMode(mode))
	if err != nil {
		return fmt.Errorf("error creating directories for path (%s): %v", path, err)
	}
	return nil
}

func (fs BeeGFS) WalkDir(path string, fn fs.WalkDirFunc, opts ...WalkOption) error {
	root := fs.MountPoint + path
	args := &WalkOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if args.Lexicographically {
		return WalkDirLexicographically(root, fn)
	}
	return filepath.WalkDir(root, fn)
}

func (fs BeeGFS) CopyXAttrsToFile(srcPath, dstPath string) error {
	srcPath = filepath.Join(fs.MountPoint, srcPath)
	dstPath = filepath.Join(fs.MountPoint, dstPath)

	// The VFS imposes a 255-byte limit on attribute names.
	// Ref: https://man7.org/linux/man-pages/man7/xattr.7.html
	buf := make([]byte, 255)
	n, err := unix.Llistxattr(srcPath, buf)
	if err != nil {
		// Testing shows if the client and/or meta don't have xattr support enabled, no error is
		// returned and the number of xattrs is simply zero. However some sources online suggest
		// file systems might return "operation not supported" when attempting to list xattrs on a
		// file system that does not support them, so we handle that error here in case BeeGFS
		// starts returning it in the future. Note BeeGFS does return ENOTSUP if a client with
		// xattrs enabled tries to talk to a meta with xattrs disabled.
		if errors.Is(err, unix.ENOTSUP) {
			return nil
		}
		return fmt.Errorf("error getting xattrs for path: %w", err)
	}

	if n == 0 {
		return nil
	}
	// Limit the buffer to what actually contains valid xattrs.
	xattrs := buf[:n]

	// Allocate a buffer for xattr values here. It will be dynamically resized if needed.
	valBuf := make([]byte, initialXattrValBufferSize)

	i := 0
	for i < len(xattrs) {
		// Xattrs are null-terminated, find the end of the current xattr name.
		end := i
		for end < len(xattrs) && xattrs[end] != 0 {
			end++
		}
		if end >= len(xattrs) {
			break
		}
		xattr := string(xattrs[i:end])

		// Query the required buffer size:
		size, err := unix.Lgetxattr(srcPath, xattr, nil)
		if err != nil {
			return fmt.Errorf("error getting size of xattr %s: %w", xattr, err)
		}
		// If needed grow the buffer:
		if size > len(valBuf) {
			valBuf = make([]byte, size)
		}
		// Get the value of this attribute:
		vlen, err := unix.Lgetxattr(srcPath, xattr, valBuf)
		if err != nil {
			return fmt.Errorf("error getting xattr %s: %w", xattr, err)
		}
		// Set the xattr on the destination path:
		err = unix.Lsetxattr(dstPath, xattr, valBuf[:vlen], 0)
		if err != nil {
			return fmt.Errorf("error setting xattr %s (value: %s): %w", xattr, valBuf[:vlen], err)
		}
		i = end + 1
	}

	return nil
}

func (fs BeeGFS) CopyContentsToFile(srcPath, dstPath string) error {
	srcPath = filepath.Join(fs.MountPoint, srcPath)
	dstPath = filepath.Join(fs.MountPoint, dstPath)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("error opening source path: %w", err)
	}
	// Handling a potential error when closing the dstFile is important because data might be lost
	// if it is cached in memory and unable to be flushed out completely when the file is closed.
	// This is not needed for srcFile since it is only opened for reading so it is closed via defer.
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dstPath, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("error opening destination path: %w", err)
	}

	// 32 KB buffer - if needed this could be optimized based on a hint like chunksize.
	buffer := make([]byte, 32*1024)
	_, err = io.CopyBuffer(dstFile, srcFile, buffer)
	if err != nil {
		if closeErr := dstFile.Close(); closeErr != nil {
			return errors.Join(
				fmt.Errorf("error copying source to destination path: %w", err),
				closeErr,
			)
		}
		return fmt.Errorf("error copying source to destination path: %w", err)
	}

	return dstFile.Close()
}

func (fs BeeGFS) CopyOwnerAndMode(fromStat fs.FileInfo, dstPath string) error {
	dstPath = filepath.Join(fs.MountPoint, dstPath)

	linuxStat, ok := fromStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unable to cast FileInfo to syscall.Stat_t (is the underlying OS Linux?)")
	}

	// Change ownership then the mode otherwise there would be a brief security issue if suid/sgid
	// bits are set.
	if err := os.Chown(dstPath, int(linuxStat.Uid), int(linuxStat.Gid)); err != nil {
		return fmt.Errorf("error changing ownership on destination path: %w", err)
	}

	if err := os.Chmod(dstPath, fromStat.Mode()); err != nil {
		return fmt.Errorf("error changing mode on destination path: %w", err)
	}

	return nil
}

// CopyTimestamps atime+mtime and the change time from `fromStat` to `dstPath` for regular files.
// This also works for symlinks, but testing shows it only seems to copy the mtime correctly for
// symlinks even though UtimesNanoAt does copy the atime for regular files.
func (fs BeeGFS) CopyTimestamps(fromStat fs.FileInfo, dstPath string) error {
	dstPath = filepath.Join(fs.MountPoint, dstPath)

	linuxStat, ok := fromStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unable to cast FileInfo to syscall.Stat_t (is the underlying OS Linux?)")
	}

	atime := time.Unix(linuxStat.Atim.Sec, linuxStat.Atim.Nsec)
	mtime := time.Unix(linuxStat.Mtim.Sec, linuxStat.Mtim.Nsec)

	// os.Chtimes does not work for symlinks because it will update the timestamps on the file the
	// link is pointing at. For symlinks use UtimesNanoAt(). That is not just also used for regular
	// files because UtimesNanoAt() does not preserve the changed timestamp.
	if linuxStat.Mode&syscall.DT_LNK != 0 {
		times := [2]unix.Timespec{
			unix.NsecToTimespec(atime.UnixNano()),
			unix.NsecToTimespec(mtime.UnixNano()),
		}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, dstPath, times[:], unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return fmt.Errorf("failed to update timestamps: %w", err)
		}
	} else {
		if err := os.Chtimes(dstPath, atime, mtime); err != nil {
			return fmt.Errorf("error copying timestamps to destination path: %w", err)
		}
	}

	return nil
}

func (fs BeeGFS) OverwriteFile(srcPath, dstPath string) error {
	srcPath = filepath.Join(fs.MountPoint, srcPath)
	dstPath = filepath.Join(fs.MountPoint, dstPath)
	// Directly use the Renameat2 syscall to atomically overwrite. If os.Remove then os.Rename was
	// used there would be a small window between removing and renaming where things could fail and
	// the srcPath is deleted before the dstPath is renamed which could cause data loss if an
	// application automatically cleans up dstPath on failure.
	err := unix.Renameat2(unix.AT_FDCWD, srcPath, unix.AT_FDCWD, dstPath, 0)
	if err != nil {
		return fmt.Errorf("failed to atomically rename %s to %s: %w", srcPath, dstPath, err)
	}
	return nil
}

func (fs BeeGFS) Readlink(path string) (string, error) {
	return os.Readlink(filepath.Join(fs.MountPoint, path))
}
