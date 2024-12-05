package filesystem

import "errors"

var (
	ErrNoSpaceForWrite       = errors.New("insufficient space remaining in this part of the file for the requested write ")
	ErrUnmounted             = errors.New("operation not possible without a mounted file system")
	ErrInitFSClient          = errors.New("unable to initialize file system client from the specified path")
	ErrUnsupportedFileSystem = errors.New("the provided path does not appear to be inside of a supported file system")
)
