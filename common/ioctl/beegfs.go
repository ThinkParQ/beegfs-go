package ioctl

// beegfs.go is the Go equivalent of the BeeGFS user-space API defined in
// client_module/include/uapi/beegfs_client.h. This file translates the
// definitions and structures from the original C header file into Go. We do not
// export these types, but rather expose a simplified idiomatic Go API for
// external callers.

import (
	"unsafe"
)

// Constants used for arguments to various BeeGFS ioctls.
const (
	cfgMaxPath    = 4096
	entryIDMaxLen = 26
)

// beegfsIOCTypeID is the type identifier for BeeGFS ioctl operations. In ioctl
// operations, a type identifier is a unique identifier used to distinguish the
// ioctls of one driver from those of another. This constant is used as the type
// ('t') field when constructing ioctl command numbers using _ior or _iow,
// helping the kernel to route the ioctl call to the correct driver (BeeGFS in
// this case).
const beegfsIOCTypeID = uintptr('f')

// Unique command numbers for BeeGFS operations. Each ioctl operation is
// associated with a unique command number, which is used as the 'nr' (number)
// field when constructing ioctl command numbers. These numbers identify the
// specific operation or request being made to the BeeGFS driver.
//
// These are like saying "I want to perform operation X within BeeGFS".
const (
	ioctlNumGetVersionOld     = 1 // Not planning to implement in Go (value from FS_IOC_GETVERSION in linux/fs.h).
	ioctlNumGetVersion        = 3 // Not planning to implement in Go.
	ioctlNumGetCfgFile        = 20
	ioctlNumCreateFile        = 21 // Not planning to implement in Go.
	ioctlNumTestIsFhGFS       = 22 // These are intentionally the same.
	ioctlNumTestIsBeeGFS      = 22 // These are intentionally the same.
	ioctlNumGetRuntimeCfgFile = 23
	ioctlNumGetMountID        = 24
	ioctlNumGetStripeInfo     = 25
	ioctlNumGetStripeTarget   = 26
	ioctlNumMkFileStripeHints = 27
	ioctlNumCreateFileV2      = 28 // Not planning to implement in Go.
	ioctlNumCreateFileV3      = 29
	ioctlNumGetInodeID        = 30
	ioctlNumGetEntryInfo      = 31
	ioctlNumPingNode          = 32
)

// BeeGFS ioctl command numbers used in syscalls that combine the BeeGFS type identifier ('t'),
// unique command number ('nr'), and the size of the structure specific to each command. They are
// used as the third argument (a2) in the syscall.Syscall method when invoking an ioctl operation.
// These command numbers ensure that the kernel understands the exact ioctl operation and data
// structure format being requested.
//
// These are like saying "Here is the complete request for operation X, directed at BeeGFS, and
// involving data of a specific size".
//
// Example:
//
//	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, iocGetCfgFile, uintptr(unsafe.Pointer(&cfgArg)))
//
// Where fd is the file descriptor for a file or directory in BeeGFS and cfgArg is an instance of
// getCfgFileArg.
//
// Potential issues with this code: According to unsafe.go you are not supported to store an
// unsafe.Pointer that is converted to a uintptr, you should make the conversion in the syscall
// expression itself. This ensures the Go compiler recognizes it should retain and not move the
// memory associated with the unsafe.Pointer until the call completes. But there is no such
// restriction documented about storing a uintptr that reference an unsafe.Sizeof. However if there
// are ever weird segmentation fault issues this may be one area to look at.
var (
	iocGetCfgFile        = _ior(beegfsIOCTypeID, ioctlNumGetCfgFile, uintptr(unsafe.Sizeof(getCfgFileArg{})))
	iocCreateFileV3      = _iow(beegfsIOCTypeID, ioctlNumCreateFileV3, uintptr(unsafe.Sizeof(mkFileV3Arg{})))
	iocMkFileStripeHints = _iow(beegfsIOCTypeID, ioctlNumMkFileStripeHints, uintptr(unsafe.Sizeof(makeFileStripeHintsArg{})))
	iocGetEntryInfo      = _ior(beegfsIOCTypeID, ioctlNumGetEntryInfo, uintptr(unsafe.Sizeof(getEntryInfoArg{})))
)

// Argument structures used for interacting with the BeeGFS file system via each
// ioctl system call. Each structure represents a specific request to the BeeGFS
// , mirroring the parameter structures expected by the BeeGFS user-space API.
// The argument structures defined here are directly used in syscall invocations
// and are designed to match the memory layout of their C counterparts in the
// BeeGFS client API.
//
// Note that the arrangement, types, and sizes of the fields in these structures
// does matter. They must align with the BeeGFS API's expectations to ensure
// proper operation and data integrity. DO NOT MODIFY these structures including
// rearranging the order of the fields unless you understand what you are doing.
// For example with getCfgFileArg if you swapped the Path and Length fields, the
// actual BeeGFS client conf path "/etc/beegfs/beegfs-client.conf" would be
// returned as "/beegfs/beegfs-client.conf".

type getCfgFileArg struct {
	// Where the resulting path will be stored (out value).
	Path [cfgMaxPath]byte
	// Length of the path buffer. This is unused because its after a fixed-size
	// path buffer (in-value).
	Length int32
}

type mkFileV3Arg struct {
	OwnerNodeID            uint32  // Owner node of the parent directory.
	ParentParentEntryID    uintptr // Entry ID of the parent of the parent (i.e., the grandparent's ID).
	ParentParentEntryIDLen int32
	ParentEntryID          uintptr // Entry ID of the parent.
	ParentEntryIDLen       int32
	// This should be set to 0 if the parent is not buddy mirrored, 1 otherwise. Testing shows any
	// non-zero value can technically be used here to indicate true.
	ParentIsBuddyMirrored int32
	// Name of the parent directory. Testing shows this isn't actually needed so we just use an
	// empty null terminated string.
	ParentName    uintptr
	ParentNameLen int32
	// File information:
	EntryName    uintptr // Name of the file we want to create.
	EntryNameLen int32
	// The file type as defined in linux/fs.h (https://elixir.bootlin.com/linux/v4.8/source/include/linux/fs.h#L1624).
	FileType     int32
	SymlinkTo    uintptr
	SymlinkToLen int32 // Only set if we are creating a symlink. This is the name the symlink should point at.
	// The file type and mode in octal, see man inode(7) for details (`stat.st_mode`).
	// For example a regular file with 0644 permissions uses `0o0100644`.
	// Common file mask types:
	// 0o0120000 - Symbolic link.
	// 0o0100000 - Regular file.
	// 0o0040000 - Directory
	Mode           int32
	Uid            uint32 // user ID and group ID only will be used if the current user is root.
	Gid            uint32
	NumTargets     int32   // number of targets in prefTargets array (without final 0 element)
	PrefTargets    uintptr // array of preferred targets (additional last element must be 0)
	PrefTargetsLen int32   // raw byte length of prefTargets array (including final 0 element)
	StoragePoolID  uint16
}

type makeFileStripeHintsArg struct {
	Filename   uintptr // file name we want to create
	Mode       uint32  // mode (access permission) of the new file
	NumTargets uint32  // number of desired targets, 0 for directory default
	ChunkSize  uint32  // in bytes, must be 2^n >= 64Ki, 0 for directory default
}

type getEntryInfoArg struct {
	OwnerID       uint32
	ParentEntryID [entryIDMaxLen + 1]byte
	EntryID       [entryIDMaxLen + 1]byte
	EntryType     int32
	FeatureFlags  int32
}
