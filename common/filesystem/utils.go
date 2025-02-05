package filesystem

import (
	"fmt"
	"os"
)

// Extract a user friendly string representing the file type from the mode bits. This is needed
// because the string representation of FileMode.Type().String() returns a unix style string, for
// example ` L---------` for a symlink.
func FileTypeToString(mode os.FileMode) string {
	if mode.IsDir() {
		return "directory"
	}
	if mode.IsRegular() {
		return "regular file"
	}
	switch {
	case mode&os.ModeSymlink != 0:
		return "symlink"
	case mode&os.ModeNamedPipe != 0:
		return "named pipe"
	case mode&os.ModeSocket != 0:
		return "socket"
	case mode&os.ModeDevice != 0:
		if mode&os.ModeCharDevice != 0 {
			return "character device"
		}
		return "block device"
	case mode&os.ModeIrregular != 0:
		return "irregular file"
	default:
		return fmt.Sprintf("unknown file type: %s", mode.Type())
	}
}
