// Package xattr provides ergonomic wrappers around the Linux/Darwin
// extended-attribute syscalls, in both path- and fd-based forms.
//
// The raw unix.Getxattr / unix.Listxattr calls use a two-step size-probe
// protocol (call with nil to get the needed buffer size, then call with
// a real buffer). These helpers hide that detail and return a single
// []byte or []string.
//
// Prefer the Fd variants (SetFd/GetFd/ListFd/RemoveFd) whenever the
// caller already has an open file descriptor: the path-based variants
// (Set/Get/List/Remove) re-resolve the path on every call, which means a
// path swapped out for a symlink between calls is silently followed --
// there is no way to detect or prevent that from a path string alone. An
// already-open fd was resolved once, at open time, and can't be
// redirected by a later change to what the path points to.
//
// Errors: this package normalizes the platform-specific "no such
// attribute" errno (ENODATA on Linux, ENOATTR on Darwin) to the
// exported sentinel ErrNotFound. Other syscall errors are returned
// unwrapped.
//
// Namespace note: on Linux, unprivileged processes can only touch the
// "user." namespace on regular files and directories. "trusted.*",
// "security.*", and "system.*" require capabilities. Tests should use
// "user.*" names unless specifically exercising the other namespaces.
package xattr

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// ErrNotFound is returned by Get, GetFd, Remove, and RemoveFd when the
// named attribute does not exist. The platform-specific errno (ENODATA
// on Linux, ENOATTR on Darwin) is hidden behind this sentinel so callers
// can write portable error checks with errors.Is.
var ErrNotFound = errors.New("xattr: attribute not found")

// maxProbeAttempts bounds the size-probe/read retry loop in probeAndGet and
// probeAndList.
//
// An ERANGE on the read normally means the value grew since the probe, and
// re-probing picks up the new size -- which is why the loop retries at all. But
// nothing makes the next probe report a size the following read will accept:
// the two are separate syscalls, and on a distributed filesystem separate round
// trips against state that may be cached. A probe that persistently disagrees
// with its read would spin here forever at full CPU with nothing logged, which
// is the worst way for these tools to fail. Bounding it turns a silent hang
// into an error naming the disagreement.
//
// Eight is well past any legitimate need: each retry needs the value to have
// grown again in the window between two adjacent syscalls, and growth is capped
// by the filesystem's per-inode xattr ceiling.
const maxProbeAttempts = 8

// errProbeUnstable reports that the size probe and the read never agreed within
// maxProbeAttempts. Unexported: no caller has a reason to branch on it, but the
// wrapped message has to say which call failed and at what size.
var errProbeUnstable = errors.New("xattr: size probe and read disagree")

// probeAndGet runs the standard two-step xattr read protocol (call with a
// nil buffer to size the value, then call again with a real buffer,
// retrying if the value grew between the two calls) against whatever raw
// getxattr closure the caller supplies. Shared by the path- and fd-based
// Get variants so the retry logic exists in exactly one place.
func probeAndGet(raw func(dest []byte) (int, error)) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		sz, err := raw(nil)
		if err != nil {
			if isNotFound(err) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		if sz == 0 {
			return []byte{}, nil
		}
		buf := make([]byte, sz)
		n, err := raw(buf)
		if err != nil {
			if errors.Is(err, unix.ERANGE) {
				if attempt == maxProbeAttempts {
					return nil, fmt.Errorf("%w: getxattr still ERANGE on a %d-byte buffer "+
						"after %d attempts", errProbeUnstable, sz, attempt)
				}
				continue // value grew between probe and read; retry
			}
			if isNotFound(err) {
				// Removed between the size probe and this read. Report it
				// exactly as a value already gone at probe time, so callers see
				// one condition rather than two spellings of it -- a caller
				// checking ErrNotFound (as xattrstore.ForEachEntry does, to skip
				// records another writer concurrently shredded) would otherwise
				// miss the raw errno and turn a benign race into a hard failure.
				return nil, ErrNotFound
			}
			return nil, err
		}
		return buf[:n], nil
	}
}

// probeAndList is probeAndGet's counterpart for listxattr/flistxattr,
// shared by the path- and fd-based List variants.
func probeAndList(raw func(dest []byte) (int, error)) ([]string, error) {
	for attempt := 1; ; attempt++ {
		sz, err := raw(nil)
		if err != nil {
			return nil, fmt.Errorf("size probe: %w", err)
		}
		if sz == 0 {
			return nil, nil
		}
		buf := make([]byte, sz)
		n, err := raw(buf)
		if err != nil {
			if errors.Is(err, unix.ERANGE) {
				if attempt == maxProbeAttempts {
					return nil, fmt.Errorf("%w: listxattr still ERANGE on a %d-byte buffer "+
						"after %d attempts", errProbeUnstable, sz, attempt)
				}
				continue // list grew between probe and read; retry
			}
			return nil, fmt.Errorf("read (buf=%d): %w", sz, err)
		}
		return SplitNULStrings(buf[:n]), nil
	}
}

// Set creates or replaces an extended attribute on path. Pass flags=0
// for "create or replace", unix.XATTR_CREATE to require the attribute
// not already exist, or unix.XATTR_REPLACE to require it does. With
// XATTR_REPLACE on a missing attribute, returns ErrNotFound.
func Set(path, name string, value []byte, flags int) error {
	if err := unix.Setxattr(path, name, value, flags); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// SetFd is Set's fd-based counterpart: see the package doc for why this
// is preferred over Set whenever the caller already has an open fd.
func SetFd(fd int, name string, value []byte, flags int) error {
	if err := unix.Fsetxattr(fd, name, value, flags); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Get returns the current value of an extended attribute. Returns
// ErrNotFound if the attribute does not exist.
func Get(path, name string) ([]byte, error) {
	return probeAndGet(func(dest []byte) (int, error) { return unix.Getxattr(path, name, dest) })
}

// GetFd is Get's fd-based counterpart: see the package doc for why this
// is preferred over Get whenever the caller already has an open fd.
func GetFd(fd int, name string) ([]byte, error) {
	return probeAndGet(func(dest []byte) (int, error) { return unix.Fgetxattr(fd, name, dest) })
}

// List returns the names of every extended attribute on path.
func List(path string) ([]string, error) {
	return probeAndList(func(dest []byte) (int, error) { return unix.Listxattr(path, dest) })
}

// ListFd is List's fd-based counterpart: see the package doc for why this
// is preferred over List whenever the caller already has an open fd.
func ListFd(fd int) ([]string, error) {
	return probeAndList(func(dest []byte) (int, error) { return unix.Flistxattr(fd, dest) })
}

// Remove deletes an extended attribute. Returns ErrNotFound if the
// attribute did not exist.
func Remove(path, name string) error {
	if err := unix.Removexattr(path, name); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// RemoveFd is Remove's fd-based counterpart: see the package doc for why
// this is preferred over Remove whenever the caller already has an open fd.
func RemoveFd(fd int, name string) error {
	if err := unix.Fremovexattr(fd, name); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// SplitNULStrings converts the NUL-separated byte stream that Listxattr
// returns into a slice of Go strings. Empty trailing segments are
// dropped. Exposed because it's occasionally useful when working with
// the raw unix.Listxattr output directly.
func SplitNULStrings(data []byte) []string {
	var out []string
	start := 0
	for i, b := range data {
		if b == 0 {
			if i > start {
				out = append(out, string(data[start:i]))
			}
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, string(data[start:]))
	}
	return out
}
