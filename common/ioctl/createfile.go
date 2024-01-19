package ioctl

// A more user friendly version of `IoctlTk::createFile(BeegfsIoctl_MkFileV3_Arg* fileData)` from
// ctl/source/toolkit/IoctlTk.cpp.

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// We capture the user configuration in an intermediate struct so we don't move creating the
// MakeFileV3Arg struct away from the syscall. According to unsafe.go, the conversion of an
// unsafe.Pointer to a uintptr must not be moved from the Syscall to ensure the referenced object is
// retained and not moved before the call completes. We do this a lot when building out the
// MakeFileV3Arg struct. In a few places we also need to convert user input to something else,
// notably the permissions and file type are combined into the mode.
type createFileConfig struct {
	fileType  FileType
	symlinkTo string
	// Only the file permissions (e.g., 0644) without the file type.
	permissions      int32
	uid              uint32
	gid              uint32
	preferredTargets []uint16
	storagePoolID    uint16
}

// Mask values for supported file types (e.g., S_IFREG for regular files, S_IFLNK for symbolic
// links, etc.) that are used combination with the file permission bits for the `stat.st_mode`.
// Refer to inode(7) section "The file type and mode" for details. Supported file types are
// determined by FhgfsOpsIoctl_createFile().
//
// We need these masks to generate the mkFileV3Arg.Mode field which is the result of combining the
// FileType mask with the permission bits (e.g., `cfg.permissions | int32(cfg.fileType),`), which is
// why we have the user specify the file type as a mask. Given the mask it is trivial to determine
// the file type as defined in linux/fs.h (i.e., DT_REG, DT_LNK), but harder to go the other way.
// For example to determine the file type:
//
//	stat.st_mode >> 12 & 15
//	0o0100644 >> 12 & 15 // 8
//
// For more details see: https://elixir.bootlin.com/linux/v4.8/source/include/linux/fs.h#L1624.
type FileType int32

const (
	S_REGULAR  FileType = 0o0100000 // Represents a regular file and is the equivalent of S_IFREG.
	S_SYMBOLIC FileType = 0o0120000 // Represents a symbolic link and is the equivalent of S_IFLNK.
)

// We use the functional option pattern for `CreateFile()` to provide a flexible API.
// Refer to: https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type createFileOption func(*createFileConfig)

// SetType is should never be needed until we support additional file types. By default we will
// create a regular file. If SetSymlinkTo() is specified, we will automatically set the file type to
// be a symbolic link.
func SetType(t FileType) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.fileType = t
	}
}

func SetSymlinkTo(symlinkTo string) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.symlinkTo = symlinkTo
	}
}

func SetPermissions(permissions int32) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.permissions = permissions
	}
}

func SetUID(uid uint32) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.uid = uid
	}
}

func SetGID(gid uint32) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.gid = gid
	}
}

func SetPreferredTargets(targets []uint16) createFileOption {
	return func(cfg *createFileConfig) {
		// There must be an additional last element zero. This also ensures if we are passed an
		// empty target slice things won't break.
		cfg.preferredTargets = append(targets, 0)
	}
}

func SetStoragePool(id uint16) createFileOption {
	return func(cfg *createFileConfig) {
		cfg.storagePoolID = id
	}
}

// CreateFile provides a flexible way to create file(s) in BeeGFS, The most basic use case is to
// simply provide a path where a new file should be created inside BeeGFS:
//
//	CreateFile("/mnt/beegfs/helloworld")
//
// File creation can be customized by providing one or more options:
//
//	CreateFile("/mnt/beegfs/helloworld_ln", SetSymlinkTo("/mnt/beegfs/helloworld"), SetPermissions(0777))
func CreateFile(path string, opts ...createFileOption) error {

	// Initialize the configuration with required+default values.
	cfg := &createFileConfig{
		fileType: 0,
		// File permissions without the file type.
		permissions: 0644,
		// User and group ID are only used if the current user is root. Otherwise the file will be
		// created with the uid/gid of the current user. This means its safe to use "0" as the
		// default uid/gid and avoid the overhead of figuring out what user is executing the
		// program.
		uid: 0,
		gid: 0,
		// Zero means no preferred targets are specified.
		preferredTargets: []uint16{0},
		storagePoolID:    0,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Attempt to automatically detect and set the file type depending on what the user is trying to do.
	// If the user said to create a symlink and hasn't specified the file type, assume the file type
	// should be a symbolic link:
	if cfg.symlinkTo != "" && cfg.fileType == 0 {
		cfg.fileType = S_SYMBOLIC
	} else if cfg.fileType == 0 {
		// Otherwise assume the file type should be regular:
		cfg.fileType = S_REGULAR
	}
	// If the user did specify a file type and also wants to create a symbolic link, ensure the file
	// type is correct, otherwise return an error:
	if cfg.symlinkTo != "" && cfg.fileType != S_SYMBOLIC {
		return fmt.Errorf("unable to create a symbolic link when the file type is not 'S_IFLNK' (hint: if a file type is not specified it will be detected automatically when a symbolic link is requested)")
	}

	// Get a fd for the parent and entry info for the parent and grandparent of the new entry.
	parentDirectory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parentDirectory.Close()

	parentEntryInfo, err := GetEntryInfo(filepath.Dir(path))
	if err != nil {
		return err
	}

	// The documentation for unsafe.Pointer states: "If a pointer argument must be converted to
	// uintptr for use as an argument, that conversion must appear in the call expression itself".
	// This complicates how we handle symbolic links because we either need an unsafe pointer to the
	// first byte of a byte slice, or nil. So instead of ever storing the uintptr anywhere
	// (which seems risky), we use a helper function to determine the address of a byte or nil while
	// building out the sycall and return an unsafe pointer.
	getSymlinkPtr := func() unsafe.Pointer {
		if cfg.symlinkTo != "" {
			bs := []byte(cfg.symlinkTo + "\000")
			return unsafe.Pointer(&bs[0])
		}
		return unsafe.Pointer(nil)
	}

	getSymlinkLen := func() int32 {
		if cfg.symlinkTo != "" {
			return int32(len(cfg.symlinkTo) + 1)
		}
		return 0
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(parentDirectory.Fd()), uintptr(iocCreateFileV3), uintptr(unsafe.Pointer(&mkFileV3Arg{
		OwnerNodeID:            uint32(parentEntryInfo.OwnerID),
		ParentParentEntryID:    uintptr(unsafe.Pointer(&[]byte(parentEntryInfo.ParentEntryID + "\x00")[0])),
		ParentParentEntryIDLen: int32(len(parentEntryInfo.ParentEntryID + "\x00")),
		ParentEntryID:          uintptr(unsafe.Pointer(&[]byte(parentEntryInfo.EntryID + "\x00")[0])),
		ParentEntryIDLen:       int32(len(parentEntryInfo.EntryID + "\x00")),
		ParentIsBuddyMirrored:  parentEntryInfo.FeatureFlags.BuddyMirrored,
		ParentName:             uintptr(unsafe.Pointer(&[]byte("\x00")[0])),
		ParentNameLen:          int32(len("\x00")),
		EntryName:              uintptr(unsafe.Pointer(&[]byte(filepath.Base(path) + "\x00")[0])),
		EntryNameLen:           int32(len(filepath.Base(path) + "\x00")),
		FileType:               int32(cfg.fileType >> 12 & 15),
		SymlinkTo:              uintptr(getSymlinkPtr()),
		SymlinkToLen:           getSymlinkLen(),
		Mode:                   cfg.permissions | int32(cfg.fileType),
		Uid:                    cfg.uid,
		Gid:                    cfg.gid,
		NumTargets:             int32(len(cfg.preferredTargets) - 1),
		PrefTargets:            uintptr(unsafe.Pointer(&cfg.preferredTargets[0])),
		PrefTargetsLen:         int32(len(cfg.preferredTargets) * 8),
		StoragePoolID:          cfg.storagePoolID,
	})))

	if errno != 0 {
		err := syscall.Errno(errno)
		return fmt.Errorf("error creating file: %w (errno: %d)", err, errno)
	}

	return nil
}
