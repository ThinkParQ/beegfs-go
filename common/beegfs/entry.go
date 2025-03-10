package beegfs

import (
	"fmt"
	"strings"
)

// Go representation of the BeeGFS `DirEntryType` enum defined in:
//   - client_module/source/common/storage/StorageDefinitions.h
//   - common/source/common/storage/StorageDefinitions.h
type EntryType uint32

const (
	EntryUnknown EntryType = iota
	EntryDirectory
	EntryRegularFile
	EntrySymlink
	EntryBlockDev
	EntryCharDev
	EntryFIFO
	EntrySOCKET
)

// Is file returns true for any kind of file, including symlinks and special files.
func (t EntryType) IsFile() bool {
	return t >= 2 && t <= 7
}

func (t EntryType) String() string {
	switch t {
	case EntryDirectory:
		return "directory"
	case EntryRegularFile:
		return "file"
	case EntrySymlink:
		return "symlink"
	case EntryBlockDev:
		return "block device node"
	case EntryCharDev:
		return "character device node"
	case EntryFIFO:
		return "pipe"
	case EntrySOCKET:
		return "unix domain socket"
	default:
		return "invalid"
	}
}

// Equivalent of StripePatternType in C++.
type StripePatternType uint32

const (
	StripePatternInvalid StripePatternType = iota
	StripePatternRaid0
	StripePatternRaid10
	StripePatternBuddyMirror
)

func (p StripePatternType) String() string {
	switch p {
	case StripePatternRaid0:
		return "RAID0"
	case StripePatternRaid10:
		return "RAID10"
	case StripePatternBuddyMirror:
		return "Buddy Mirror"
	default:
		return "invalid"
	}
}

type EntryFeatureFlags int32

const (
	// Equivalent of ENTRYINFO_FEATURE_INLINED in C++.
	entryFeatureFlagInlined EntryFeatureFlags = 1
	// Equivalent of ENTRYINFO_FEATURE_BUDDYMIRRORED in C++.
	entryFeatureFlagBuddyMirrored EntryFeatureFlags = 2
)

func (f EntryFeatureFlags) IsInlined() bool {
	return f&entryFeatureFlagInlined != 0
}

func (f *EntryFeatureFlags) SetInlined() {
	*f |= entryFeatureFlagInlined
}

func (f EntryFeatureFlags) IsBuddyMirrored() bool {
	return f&entryFeatureFlagBuddyMirrored != 0
}

func (f *EntryFeatureFlags) SetBuddyMirrored() {
	*f |= entryFeatureFlagBuddyMirrored
}

// FileDataState represents the possible states for file data tiering.
// Equivalent to FileDataState in C++.
type FileDataState uint8

const (
	// FileDataStateLocal indicates data is stored within BeeGFS (default).
	FileDataStateLocal FileDataState = 0x00
	// FileDataStateOffloaded indicates data is stored in external storage
	// e.g. S3, tape (i.e. file on metadata server is a stub).
	FileDataStateOffloaded FileDataState = 0x01
	// Special value to indicate that data state inode feature flag should be
	// cleared from the inode feature flags and data state should be reset to local.
	FileDataStateUnset FileDataState = 0xff
)

// FileDataStateToString converts a FileDataState to a human-readable string.
func FileDataStateToString(state FileDataState) string {
	switch state {
	case FileDataStateLocal:
		return "Local"
	case FileDataStateOffloaded:
		return "Offloaded"
	case FileDataStateUnset:
		return "None"
	default:
		// For unknown values, just return the numeric representation
		return fmt.Sprintf("unknown(%d)", state)
	}
}

// ParseFileDataState converts a string to a FileDataState.
func ParseFileDataState(s string) (FileDataState, error) {
	switch strings.ToLower(s) {
	case "local":
		return FileDataStateLocal, nil
	case "offloaded":
		return FileDataStateOffloaded, nil
	case "none":
		return FileDataStateUnset, nil
	default:
		return 0, fmt.Errorf("invalid file data state: %s (valid values: local, offloaded, none)", s)
	}
}
