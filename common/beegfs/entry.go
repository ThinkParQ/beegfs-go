package beegfs

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
	StripePatternPatternRaid0
	StripePatternRaid10
	StripePatternBuddyMirror
)

func (p StripePatternType) String() string {
	switch p {
	case StripePatternPatternRaid0:
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

func (f EntryFeatureFlags) String() string {
	flagsSet := ""
	if f.IsInlined() {
		flagsSet += "Inlined inode: yes, "
	} else {
		flagsSet += "Inlined inode: no, "
	}

	if f.IsBuddyMirrored() {
		flagsSet += "Buddy Mirrored: yes"
	} else {
		flagsSet += "Buddy Mirrored: no"
	}

	return flagsSet
}

func (f EntryFeatureFlags) IsInlined() bool {
	return f&entryFeatureFlagInlined != 0
}

func (f EntryFeatureFlags) IsBuddyMirrored() bool {
	return f&entryFeatureFlagBuddyMirrored != 0
}

func (f *EntryFeatureFlags) SetBuddyMirrored() {
	*f |= entryFeatureFlagBuddyMirrored
}
