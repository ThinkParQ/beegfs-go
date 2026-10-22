package xattr

import (
	"errors"

	"golang.org/x/sys/unix"
)

// isNotFound reports whether err is the platform's "attribute does not
// exist" errno. Linux returns ENODATA.
func isNotFound(err error) bool {
	return errors.Is(err, unix.ENODATA)
}
