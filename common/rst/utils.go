package rst

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// appendErrors joins each nextErr onto accumulatedErr in order, keeping them all wrapped so
// errors.Is and errors.As still match any one of them. Any argument may be nil, which is what makes
// it convenient for accumulating the failures of steps that all have to run before returning.
func appendErrors(accumulatedErr error, nextErrs ...error) error {
	for _, nextErr := range nextErrs {
		if nextErr == nil {
			continue
		} else if accumulatedErr == nil {
			accumulatedErr = nextErr
		} else {
			accumulatedErr = fmt.Errorf("%w; %w", accumulatedErr, nextErr)
		}
	}
	return accumulatedErr
}

// openFileForAppend returns a handle that durably appends to path, creating it if needed and never
// truncating what is already there. Each Write is committed to stable storage before it returns, so
// an appended record survives a crash without an explicit Sync. A crash part way through a Write
// can still leave a partial record behind, so records must be fixed size or self delimiting.
func openFileForAppend(path string) (*os.File, error) {
	return createPersistentFile(path, unix.O_WRONLY|unix.O_APPEND, 0600)
}

// openFileForUpdate returns a handle that durably rewrites bytes of an existing file in place, such
// as the fixed size records WriteAt addresses by offset. Nothing is created or truncated, so
// os.ErrNotExist means the file was never created or has since been deleted, which is state a
// caller can act on instead of silently recreating.
func openFileForUpdate(path string) (*os.File, error) {
	return updatePersistentFile(path, unix.O_WRONLY)
}

// touchFile durably creates path and leaves an existing file's contents untouched, so it is safe to
// call every time state is opened. Creating state files up front is what makes their later absence
// unambiguous: it means the state was deleted, not that nothing has been written to it yet.
func touchFile(path string) error {
	f, err := createPersistentFile(path, 0, 0600)
	if err != nil {
		return err
	}

	return f.Close()
}

// createPersistentFile opens path for durable writes with the given unix.O_* mode flags, creating
// it if needed but never truncating a file that already exists. perm applies only to a file this
// call creates, and the parent directory has to exist already. Newly created files have their
// parent directory fsynced so the directory entry is persisted. The returned file uses O_DSYNC so
// successful writes are committed to stable storage before returning.
func createPersistentFile(path string, mode int, perm uint32) (*os.File, error) {
	mode |= unix.O_DSYNC | unix.O_CLOEXEC

	fd, err := unix.Open(path, mode|unix.O_CREAT|unix.O_EXCL, perm)
	created := err == nil
	if errors.Is(err, unix.EEXIST) {
		fd, err = unix.Open(path, mode, 0)
	}
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("failed to create file handle for %s", path)
	}

	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return f, nil
}

// updatePersistentFile opens an existing path for durable in-place writes with the given unix.O_*
// mode flags. Nothing is created or truncated, so opening a path that doesn't exist fails. The
// returned file uses O_DSYNC so successful writes are committed to stable storage before returning.
func updatePersistentFile(path string, mode int) (*os.File, error) {
	fd, err := unix.Open(path, mode|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("failed to create file handle for %s", path)
	}

	return f, nil
}

// writePersistentFile atomically replaces path with data, which is how a whole file's worth of
// state should be rewritten. Content is staged in a temporary file that is fsynced before being
// renamed over path, then the parent directory is fsynced so the rename is persisted. A crash
// therefore leaves path either fully replaced or untouched, never truncated part way through a
// rewrite the way an O_TRUNC write would. The parent directory has to exist already.
func writePersistentFile(path string, data []byte, perm uint32) (err error) {
	tmpPath := persistentTmpPath(path)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(perm))
	if err != nil {
		return err
	}

	closed := false
	defer func() {
		if err != nil {
			if !closed {
				_ = f.Close()
			}
			_ = removeIfExists(tmpPath)
		}
	}()

	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("failed to write %s: %w", tmpPath, err)
	}

	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", tmpPath, err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("failed to close %s: %w", tmpPath, err)
	}
	closed = true

	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, path, err)
	}

	return syncDir(filepath.Dir(path))
}

// persistentTmpPath is where writePersistentFile stages content before renaming it over path. A
// crash can leave one behind, so anything that lists a directory of persisted files must skip
// these, and anything deleting state should delete them alongside the file itself.
func persistentTmpPath(path string) string {
	return path + ".tmp"
}

// syncDir fsyncs the directory at path so entries created, renamed or removed within it are
// persisted. Only the directory's own entries are covered: file contents need their own sync.
func syncDir(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to open %s for sync: %w", path, err)
	}

	syncErr := unix.Fsync(fd)
	closeErr := unix.Close(fd)

	if syncErr != nil {
		return fmt.Errorf("failed to sync %s: %w", path, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close %s: %w", path, closeErr)
	}

	return nil
}

// removeIfExists removes path and reports success when it is already gone, for teardown paths that
// cannot assume every state file was created.
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
