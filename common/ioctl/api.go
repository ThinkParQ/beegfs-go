package ioctl

// The Go equivalent of client_devel/include/beegfs/beegfs_ioctl_functions.h and
// the beegfs-ctl's IoctlTk.cpp. This is not a direct translation, but rather
// exposes a more simplified idiomatic Go API. Larger ioctl functions may have
// their own dedicated file to organize related types.

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// GetConfigFile returns the path to the client configuration file for the provided active BeeGFS mount
// point.
func GetConfigFile(mountPoint string) (string, error) {

	mount, err := os.Open(mountPoint)
	if err != nil {
		return "", err
	}

	var cfgFile getCfgFileArg
	cfgFile.Length = cfgMaxPath

	// According to unsafe.go, the conversion of the unsafe.Pointer to a uintptr must not be moved
	// from the Syscall to ensure the referenced object is retained and not moved before the call
	// completes.
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mount.Fd()), uintptr(iocGetCfgFile), uintptr(unsafe.Pointer(&cfgFile)))
	if errno != 0 {
		err := syscall.Errno(errno)
		return "", fmt.Errorf("error getting config file: %w (errno: %d)", err, errno)
	}

	// Convert the array to a slice
	path := cfgFile.Path[:]
	return string(path[:cStringLen(path)]), nil
}

// The BeeGFS entry info for an entry inside of BeeGFS.
type EntryInfo struct {
	// The owning metadata node/group of the entry.
	OwnerID       int
	ParentEntryID string
	EntryID       string
	EntryType     BeeGFSEntryType
	FeatureFlags  EntryInfoFeatureFlags
}

// Go representation of the BeeGFS `DirEntryType` enum defined in:
//   - client_module/source/common/storage/StorageDefinitions.h
//   - common/source/common/storage/StorageDefinitions.h
type BeeGFSEntryType int

const (
	BeeGFSUnknown BeeGFSEntryType = iota
	BeeGFSDirectory
	BeeGFSRegularFile
	BeeGFSSymlink
	BeeGFSBlockDev
	BeeGFSCharDev
	BeeGFSFIFO
	BeeGFSSOCKET
)

// Go representations of the feature flags defined in:
//   - client_module/source/common/storage/EntryInfo.h
//   - common/source/common/storage/EntryInfo.h
//
// Flags are set to either 0 for false or 1 for true. We don't just use a boolean type here because
// some ioctls (such as create file) expect an int32, and Go doesn't allow you to directly cast a
// bool to an int (we'd have to have a helper function for each flag).
type EntryInfoFeatureFlags struct {
	// Indicates an inlined inode, might be outdated.
	Inlined int32
	// Indicates the file/directory metadata is buddy mirrored.
	BuddyMirrored int32
}

// GetEntryInfo returns the BeeGFS entry info for the provided path inside of an active BeeGFS mount
// point. The path can be any valid entry inside BeeGFS (i.e., file, directory, etc.)
func GetEntryInfo(path string) (EntryInfo, error) {

	entry, err := os.Open(path)
	if err != nil {
		return EntryInfo{}, err
	}
	defer entry.Close()

	var arg = &getEntryInfoArg{}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(entry.Fd()), uintptr(iocGetEntryInfo), uintptr(unsafe.Pointer(arg)))
	if errno != 0 {
		err := syscall.Errno(errno)
		return EntryInfo{}, fmt.Errorf("error getting getting entry info for path '%s': %w (errno: %d)", path, err, errno)
	}

	// Use a bitwise AND operation to check if a flag is set then convert the result to a binary
	// integer by right shifting the result based on the position of the flag:
	featureFlags := EntryInfoFeatureFlags{
		Inlined:       arg.FeatureFlags & 1 >> 0, // "1" comes from ENTRYINFO_FEATURE_INLINED in BeeGFS.
		BuddyMirrored: arg.FeatureFlags & 2 >> 1, // "2" comes from ENTRYINFO_FEATURE_BUDDYMIRRORED in BeeGFS.
	}

	return EntryInfo{
		OwnerID:       int(arg.OwnerID),
		ParentEntryID: string(arg.ParentEntryID[:]),
		EntryID:       string(arg.EntryID[:]),
		EntryType:     BeeGFSEntryType(arg.EntryType),
		FeatureFlags:  featureFlags,
	}, nil
}

// CreateFileWithStripeHints creates a file in BeeGFS with the provided striping configuration.
//
// Parameters:
//   - path: where the new file should be created inside BeeGFS.
//   - permissions: of the new file (e.g., 0644).
//   - numtargets: desired number of storage targets for striping. Use 0 for the directory default, or ^0 (bitwise NOT) to use all available targets.
//   - chunksize: in bytes, must be 2^n >= 64KiB. Use 0 for the directory default.
func CreateFileWithStripeHints(path string, permissions uint32, numTargets uint32, chunkSize uint32) error {

	parentDir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parentDir.Close()

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(parentDir.Fd()),
		uintptr(iocMkFileStripeHints),
		uintptr(unsafe.Pointer(&makeFileStripeHintsArg{
			Filename:   uintptr(unsafe.Pointer(&[]byte(filepath.Base(path) + "\x00")[0])),
			Mode:       permissions,
			NumTargets: numTargets,
			ChunkSize:  chunkSize,
		})))

	if errno != 0 {
		err := syscall.Errno(errno)
		return fmt.Errorf("error creating file: %w (errno: %d)", err, errno)
	}

	return nil
}
