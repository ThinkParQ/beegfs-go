package beegfs

import (
	"encoding/json"
	"fmt"
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

// String returns the entry type using the same vocabulary as the "type ==" clause of --filter-files
// (see common/filesystem), so a type printed by `entry info` can be pasted into a filter.
func (t EntryType) String() string {
	switch t {
	case EntryDirectory:
		return "directory"
	case EntryRegularFile:
		return "file"
	case EntrySymlink:
		return "symlink"
	case EntryBlockDev:
		return "block"
	case EntryCharDev:
		return "char"
	case EntryFIFO:
		return "fifo"
	case EntrySOCKET:
		return "socket"
	case EntryUnknown:
		return "unknown"
	default:
		// The type is read off the wire from meta, so a value no constant covers keeps its number
		// instead of being reported as the sentinel a local caller would have set.
		return fmt.Sprintf("unknown(%d)", uint32(t))
	}
}

// MarshalJSON encodes the entry type as its human-readable string.
func (t EntryType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
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
		return "raid0"
	case StripePatternRaid10:
		return "raid10"
	case StripePatternBuddyMirror:
		return "buddy-mirror"
	case StripePatternInvalid:
		return "invalid"
	default:
		// Also read off the wire, so an unmatched pattern stays diagnosable. See EntryType.String().
		return fmt.Sprintf("unknown(%d)", uint32(p))
	}
}

// MarshalJSON encodes the stripe pattern type as its human-readable string.
func (p StripePatternType) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
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

// IsBuddyMirroredI32() is needed for use with C++ code that expects 0 for false and 1 for true.
func (f EntryFeatureFlags) IsBuddyMirroredI32() int32 {
	if f&entryFeatureFlagBuddyMirrored != 0 {
		return 1
	}
	return 0
}

func (f *EntryFeatureFlags) SetBuddyMirrored() {
	*f |= entryFeatureFlagBuddyMirrored
}

// FileState represents the combined AccessFlag (lower 5 bits) and DataState (upper 3 bits).
type FileState uint8

// AccessFlags represents the access control settings in the lower 5 bits of FileState
type AccessFlags uint8

// Constants for AccessFlags
const (
	// No access restrictions (default access mode)
	AccessFlagUnlocked AccessFlags = 0x00
	// Block read operations (file is "write-only")
	AccessFlagReadLock AccessFlags = 0x01
	// Block write operations (file is "read-only")
	AccessFlagWriteLock AccessFlags = 0x02
	// Reserved for future use
	AccessFlagReserved3 AccessFlags = 0x04
	// Reserved for future use
	AccessFlagReserved4 AccessFlags = 0x08
	// Reserved for future use
	AccessFlagReserved5 AccessFlags = 0x10
)

const (
	// LockedContentAccessFlags defines the access flags applied when locking or unlocking job
	// request and stub files. Always use this constant when managing RST locks.
	LockedContentAccessFlags AccessFlags = AccessFlagReadLock | AccessFlagWriteLock
)

// DataState represents an user/application defined data state (upper 3 bits of FileState)
type DataState uint8

const (
	DataStateAvailable = iota
	DataStateManualRestore
	DataStateAutoRestore
	DataStateDelayedRestore
	DataStateUnavailable
	// DataStateReserved5
	// DataStateReserved6
	// DataStateReserved7
)

// IsDataStateOffloaded returns true when the data state indicates the file contents have been
// offloaded to a remote storage target (stub file). Note this indicates if the contents are not
// available in BeeGFS, not necessarily if they are still available elsewhere.
func IsDataStateOffloaded(state DataState) bool {
	return state != DataStateAvailable
}

// Masks for extracting parts of the state
const (
	// Mask for extracting access flags (lower 5 bits)
	AccessFlagMask FileState = 0x1F
	// Mask for extracting data state (upper 3 bits)
	DataStateMask FileState = 0xE0
	// Number of bits to shift for data state
	DataStateShift = 5
)

// NewFileState combines AccessFlags and DataState into a single byte
// Lower 5 bits represent access flags, upper 3 bits represent data state
func NewFileState(AccessFlag AccessFlags, dataState DataState) FileState {
	return (FileState(AccessFlag) & AccessFlagMask) |
		(FileState(dataState<<DataStateShift) & DataStateMask)
}

// GetAccessFlags returns the access flags part of the file state.
func (fs FileState) GetAccessFlags() AccessFlags {
	return AccessFlags(fs & AccessFlagMask)
}

// GetDataState returns the data state part of the file state.
func (fs FileState) GetDataState() DataState {
	return DataState((fs & DataStateMask) >> DataStateShift) // Shift right to get the original value
}

// Helper methods for checking access restrictions
func (fs FileState) IsUnlocked() bool {
	return fs.GetAccessFlags() == AccessFlagUnlocked
}

func (fs FileState) IsReadLocked() bool {
	return (fs.GetAccessFlags() & AccessFlagReadLock) != 0
}

func (fs FileState) IsWriteLocked() bool {
	return (fs.GetAccessFlags() & AccessFlagWriteLock) != 0
}

func (fs FileState) IsReadWriteLocked() bool {
	return fs.IsReadLocked() && fs.IsWriteLocked()
}

// GetRawValue returns the raw byte value of the file state
func (fs FileState) GetRawValue() uint8 {
	return uint8(fs)
}

// String returns a human-readable representation of the file state as its two component enums
// separated by a slash, for example "unlocked/available".
func (state FileState) String() string {
	return fmt.Sprintf("%s/%s", state.GetAccessFlags(), state.GetDataState())
}

// MarshalJSON encodes the file state as its human-readable string so the packed byte is never
// exposed as an integer.
func (state FileState) MarshalJSON() ([]byte, error) {
	return json.Marshal(state.String())
}

// Helper function to convert access flags to a string.
func (f AccessFlags) String() string {
	switch f {
	case AccessFlagUnlocked:
		return "unlocked"
	case AccessFlagReadLock:
		return "locked-read" // Indicates READ operations are blocked (write-only)
	case AccessFlagWriteLock:
		return "locked-write" // Indicates WRITE operations are blocked (read-only)
	case AccessFlagReadLock | AccessFlagWriteLock:
		return "locked-read-write" // All access blocked
	default:
		// For combinations with reserved bits
		return fmt.Sprintf("unknown(%d)", f)
	}
}

// MarshalJSON encodes the access flags as their human-readable string.
func (f AccessFlags) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.String())
}

func (s DataState) String() string {
	switch s {
	case DataStateAvailable:
		return "available"
	case DataStateManualRestore:
		return "manual-restore"
	case DataStateAutoRestore:
		return "auto-restore"
	case DataStateDelayedRestore:
		return "delayed-restore"
	case DataStateUnavailable:
		return "unavailable"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// MarshalJSON encodes the data state as its human-readable string.
func (s DataState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// WithDataState returns a copy of the FileState with the updated data state.
func (fs FileState) WithDataState(state DataState) FileState {
	return (fs & AccessFlagMask) | ((FileState(state) << DataStateShift) & DataStateMask)
}

// WithAccessFlags returns a copy of the FileState with the specified access flags set.
func (fs FileState) WithAccessState(flags AccessFlags) FileState {
	return (fs & DataStateMask) | (FileState(flags) & AccessFlagMask)
}

// WithoutAccessState returns a copy of the FileState with the specified access flags cleared.
func (fs FileState) WithoutAccessState(flags AccessFlags) FileState {
	return (fs & DataStateMask) | ((fs & AccessFlagMask) &^ FileState(flags))
}
