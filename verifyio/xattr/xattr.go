// Package xattr provides ergonomic wrappers around the Linux/Darwin
// extended-attribute syscalls.
//
// The raw unix.Getxattr / unix.Listxattr calls use a two-step size-probe
// protocol (call with nil to get the needed buffer size, then call with
// a real buffer). These helpers hide that detail and return a single
// []byte or []string.
//
// All operations target a path; if you want the fd-based variants, use
// unix.Fsetxattr / Fgetxattr / Flistxattr / Fremovexattr directly.
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

// ErrNotFound is returned by Get and Remove when the named attribute
// does not exist. The platform-specific errno (ENODATA on Linux,
// ENOATTR on Darwin) is hidden behind this sentinel so callers can
// write portable error checks with errors.Is.
var ErrNotFound = errors.New("xattr: attribute not found")

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

// Get returns the current value of an extended attribute. Returns
// ErrNotFound if the attribute does not exist.
func Get(path, name string) ([]byte, error) {
	for {
		sz, err := unix.Getxattr(path, name, nil)
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
		n, err := unix.Getxattr(path, name, buf)
		if err != nil {
			if errors.Is(err, unix.ERANGE) {
				continue // value grew between probe and read; retry
			}
			return nil, err
		}
		return buf[:n], nil
	}
}

// List returns the names of every extended attribute on path.
func List(path string) ([]string, error) {
	for {
		sz, err := unix.Listxattr(path, nil)
		if err != nil {
			return nil, fmt.Errorf("size probe: %w", err)
		}
		if sz == 0 {
			return nil, nil
		}
		buf := make([]byte, sz)
		n, err := unix.Listxattr(path, buf)
		if err != nil {
			if errors.Is(err, unix.ERANGE) {
				continue // list grew between probe and read; retry
			}
			return nil, fmt.Errorf("read (buf=%d): %w", sz, err)
		}
		return SplitNULStrings(buf[:n]), nil
	}
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
