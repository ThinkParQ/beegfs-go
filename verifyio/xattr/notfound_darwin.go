package xattr

import (
	"errors"

	"golang.org/x/sys/unix"
)

// isNotFound reports whether err is the platform's "attribute does not
// exist" errno. Darwin returns ENOATTR.
func isNotFound(err error) bool {
	return errors.Is(err, unix.ENOATTR)
}
