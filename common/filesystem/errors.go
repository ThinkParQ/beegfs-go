package filesystem

import "errors"

var (
	ErrNoSpaceForWrite = errors.New("insufficient space remaining in this part of the file for the requested write ")
)
