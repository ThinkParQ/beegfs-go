// Package ioctl provides functions for interacting with BeeGFS ioctls.
//
// General notes on error handling:
//
// Where possible meaningful errors will be returned, but sometimes we just have to just directly
// return an error from a syscall, which can be vague, for example: "invalid argument (errno: 22)".
// To troubleshoot it can be helpful to rebuild the client module with the `BEEGFS_DEBUG=1` option.
package ioctl
