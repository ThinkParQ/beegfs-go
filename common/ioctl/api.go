package ioctl

// The Go equivalent of client_devel/include/beegfs/beegfs_ioctl_functions.h and
// the beegfs-ctl's IoctlTk.cpp. This is not a direct translation, but rather
// exposes a more simplified idiomatic Go API. Larger ioctl functions may have
// their own dedicated file to organize related types.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
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

type getEntryInfoCfg struct {
	trimNullBytes bool
}

type getEntryInfoOpt (func(*getEntryInfoCfg))

func TrimEntryInfoNullBytes() getEntryInfoOpt {
	return func(cfg *getEntryInfoCfg) {
		cfg.trimNullBytes = true
	}
}

// GetEntryInfo returns the BeeGFS entry info for the provided file descriptor inside of an active
// BeeGFS mount point. The path can be any valid entry inside BeeGFS (i.e., file, directory, etc.).
// This returns the same EntryInfo type as used by BeeMsg RPCs for compatibility.
//
// USAGE NOTES:
//
//   - This function DOES NOT set the FileName in EntryInfo consistent with the the behavior
//     of the equivalent IoctlTk::getEntryInfo() in the C++ implementation.
//   - When using EntryInfo from the ioctl as input to BeeMsg network serializers you typically want to
//     set the TrimEntryInfoNullBytes() option. Otherwise you may see errors like unknown or invalid
//     entry, notably when using a LookupIntentRequest with the root directory and the EntryID is root.
//     This option should not typically be used when the EntryInfo is used as input to other ioctls.
func GetEntryInfo(fd uintptr, opts ...getEntryInfoOpt) (msg.EntryInfo, error) {
	cfg := &getEntryInfoCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	var arg = &getEntryInfoArg{}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(iocGetEntryInfo), uintptr(unsafe.Pointer(arg)))
	if errno != 0 {
		err := syscall.Errno(errno)
		return msg.EntryInfo{}, fmt.Errorf("unable to get entry info for fd %d via ioctl: %w (errno: %d)", fd, err, errno)
	}

	// Use bitwise AND operations to check if feature flags are set:
	var featFlags beegfs.EntryFeatureFlags
	// "1" comes from ENTRYINFO_FEATURE_INLINED in BeeGFS.
	if arg.FeatureFlags&1>>0 == 1 {
		featFlags.SetInlined()
	}
	// "2" comes from ENTRYINFO_FEATURE_BUDDYMIRRORED in BeeGFS.
	if arg.FeatureFlags&2>>1 == 1 {
		featFlags.SetBuddyMirrored()
	}

	if cfg.trimNullBytes {
		return msg.EntryInfo{
			OwnerID:       arg.OwnerID,
			ParentEntryID: bytes.TrimRight(arg.ParentEntryID[:], "\x00"),
			EntryID:       bytes.TrimRight(arg.EntryID[:], "\x00"),
			// The equivalent IoctlTk::getEntryInfo() in the C++ does not set the FileName.
			FileName:     []byte{},
			EntryType:    beegfs.EntryType(arg.EntryType),
			FeatureFlags: featFlags,
		}, nil
	}
	return msg.EntryInfo{
		OwnerID:       arg.OwnerID,
		ParentEntryID: arg.ParentEntryID[:],
		EntryID:       arg.EntryID[:],
		// The equivalent IoctlTk::getEntryInfo() in the C++ does not set the FileName.
		FileName:     []byte{},
		EntryType:    beegfs.EntryType(arg.EntryType),
		FeatureFlags: featFlags,
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
	fileName := []byte(filepath.Base(path) + "\x00")
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(parentDir.Fd()),
		uintptr(iocMkFileStripeHints),
		uintptr(unsafe.Pointer(&makeFileStripeHintsArg{
			Filename:   uintptr(unsafe.Pointer(&fileName[0])),
			Mode:       permissions,
			NumTargets: numTargets,
			ChunkSize:  chunkSize,
		})))

	// Ensure the slice is not reclaimed by the GC before the syscall completes.
	runtime.KeepAlive(fileName)

	if errno != 0 {
		err := syscall.Errno(errno)
		return fmt.Errorf("error creating file: %w (errno: %d)", err, errno)
	}

	return nil
}

func PingNode(mountpoint string, nodeID beegfs.LegacyId, count uint32, interval time.Duration) (*pingNodeArgResults, error) {
	mp, err := os.Open(mountpoint)
	if err != nil {
		return nil, err
	}
	defer mp.Close()

	if count > pingMaxCount {
		return nil, fmt.Errorf("count exceeds the maximum allowed ping count (%d)", pingMaxCount)
	}

	if interval > pingMaxInterval*time.Millisecond {
		return nil, fmt.Errorf("interval exceeds the maximum allowed ping count (%dms)", pingMaxInterval)
	}
	intervalMS := uint32(interval.Milliseconds())

	arg := pingNodeArg{
		Params: pingNodeArgParams{
			NodeID:   uint32(nodeID.NumId),
			Count:    count,
			Interval: intervalMS,
		},
		Results: pingNodeArgResults{
			OutNode:     [pingNodeBufLen]byte{},
			OutPingTime: [pingMaxCount]uint32{},
			OutPingType: [pingMaxCount][pingSockTypeBufLen]byte{},
		},
	}
	copy(arg.Params.NodeType[:], strings.ToLower(nodeID.NodeType.String())+"\x00")

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(mp.Fd()),
		uintptr(iocPingNode),
		uintptr(unsafe.Pointer(&arg)))

	if errno != 0 {
		err := syscall.Errno(errno)
		return nil, fmt.Errorf("error pinging node: %w (errno: %d)", err, errno)
	}

	return &arg.Results, nil
}

func SetFileState(path string, fileState beegfs.FileState) error {

	fileNameStr := filepath.Base(path)
	// Subtract one here to account for the null byte added below.
	if len(fileNameStr) > filenameMaxLen-1 {
		return fmt.Errorf("path %q has a file name %q longer than the maximum allowed length %d", path, fileNameStr, filenameMaxLen-1)
	}

	parentDir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parentDir.Close()

	var fileName [filenameMaxLen]byte
	copy(fileName[:], []byte(fileNameStr+"\x00"))
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(parentDir.Fd()),
		uintptr(iocSetFileState),
		uintptr(unsafe.Pointer(&setFileStateArg{
			Filename:  fileName,
			FileState: fileState.GetRawValue(),
		})),
	)
	runtime.KeepAlive(fileName)

	if errno != 0 {
		err := syscall.Errno(errno)
		return fmt.Errorf("error setting file state: %w (errno: %d)", err, errno)
	}

	return nil
}

// GetEntryInfoV2 is an improved version of GetEntryInfo that works on a path instead of a file
// descriptor. Notable improvements include:
//
//   - It returns the EntryInfo and detailed EntryInfoResponse, avoiding the need for the caller to
//     collect the latter separately using an RPC.
//   - If the specified path is a file, it does not open the file directly but instead opens the
//     parent directory and passes the file name as an argument to the ioctl. This allows it to work on
//     files that are locked, either by DMAPI access flags or files in the inode lock store (e.g., if
//     they are being rebalanced). This also ensures it does not trigger OPEN_BLOCKED events which could
//     trigger an automatic file restore by some consumers of the data management API. It also allows it
//     to work on special file types that cannot be opened directly (a limitation of the old ioctl).
//
// It automatically trims null bytes from entry IDs meaning the output is not safe to use directly
// with other ioctls, however it is always safe to use with RPCs. See TrimEntryInfoNullBytes() on
// the original GetEntryInfo() for more details.
//
// It also does not populate the StripePattern "length" field which serves no purpose in the Go
// code. A StripePattern returned by the ioctl is still safe to use when serializing BeeMsges
// because the Length field in the GetEntryInfoResponse struct is only populated during
// deserialization and ignored for serialization as the length is calculated by the serializer.
func GetEntryInfoV2(path string) (msg.EntryInfo, msg.GetEntryInfoResponse, error) {

	var arg = &getEntryInfoV2Arg{}
	var fileName [filenameMaxLen]byte
	var dir *os.File
	lstat, err := os.Lstat(path)
	if err != nil {
		return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, fmt.Errorf("unable to get entry info for %q via ioctl: %w", path, err)
	}
	if !lstat.IsDir() {
		// If this is a file open the parent directory then include the filename in the ioctl args.
		fileNameStr := filepath.Base(path)
		// Subtract one here to account for the null byte added below.
		if len(fileNameStr) > filenameMaxLen-1 {
			return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, fmt.Errorf("path %q has a file name %q longer than the maximum allowed length %d", path, fileNameStr, filenameMaxLen-1)
		}
		copy(fileName[:], []byte(fileNameStr+"\x00"))
		arg.Filename = fileName
		dir, err = os.Open(filepath.Dir(path))
		if err != nil {
			return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, err
		}
	} else {
		// If this is a directory open it directly.
		dir, err = os.Open(path)
		if err != nil {
			return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, err
		}
	}
	defer dir.Close()

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, dir.Fd(), uintptr(iocGetEntryInfoV2), uintptr(unsafe.Pointer(arg)))
	if errno != 0 {
		err := syscall.Errno(errno)
		return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, fmt.Errorf("unable to get entry info for %q via ioctl: %w (errno: %d)", path, err, errno)
	}

	// Use bitwise AND operations to check if feature flags are set:
	var featFlags beegfs.EntryFeatureFlags
	// "1" comes from ENTRYINFO_FEATURE_INLINED in BeeGFS.
	if arg.FeatureFlags&1>>0 == 1 {
		featFlags.SetInlined()
	}
	// "2" comes from ENTRYINFO_FEATURE_BUDDYMIRRORED in BeeGFS.
	if arg.FeatureFlags&2>>1 == 1 {
		featFlags.SetBuddyMirrored()
	}

	entryInfo := msg.EntryInfo{
		OwnerID:       arg.OwnerID,
		ParentEntryID: bytes.TrimRight(arg.ParentEntryID[:], "\x00"),
		EntryID:       bytes.TrimRight(arg.EntryID[:], "\x00"),
		FileName:      bytes.TrimRight(arg.Filename[:], "\x00"),
		EntryType:     beegfs.EntryType(arg.EntryType),
		FeatureFlags:  featFlags,
	}

	// arg.BasicOnly is set by the kernel when the inode is held in the global lock store (e.g.
	// during chunk rebalancing). The ioctl still succeeds and the basic entry info fields populated
	// above are valid, but stripe pattern, PathInfo, RST, and session fields are not populated.
	// Return OpsErr_INODELOCKED in the response so callers can distinguish a partial result.
	if arg.BasicOnly != 0 {
		return entryInfo, msg.GetEntryInfoResponse{Result: beegfs.OpsErr_INODELOCKED}, nil
	}

	// This matches the same checks done in common/beemsg/msg/entry.go if we were assembling
	// GetEntryInfoResponse from an RPC response.
	if arg.RSTMajorVersion > 1 || arg.RSTMinorVersion != 0 {
		return msg.EntryInfo{}, msg.GetEntryInfoResponse{}, fmt.Errorf("unsupported RST format (major: %d, minor: %d)", arg.RSTMajorVersion, arg.RSTMinorVersion)
	}

	entryInfoResp := msg.GetEntryInfoResponse{
		Result: beegfs.OpsErr_SUCCESS,
		Pattern: msg.StripePattern{
			Length:            0, // Intentionally unset, see common on the function description.
			Type:              beegfs.StripePatternType(arg.PatternType),
			HasPoolID:         true,
			Chunksize:         arg.ChunkSize,
			StoragePoolID:     uint16(arg.StoragePoolID),
			DefaultNumTargets: arg.DefaultNumTargets,
			TargetIDs:         arg.StripeTargetIDs[0:arg.NumTargets],
		},
		Path: msg.PathInfo{
			// Flags in the PathInfo set with the GetEntryInfoMsgEx is defined as an int32_t in the
			// C++ server code, however in the client code it is defined and deserialized as a
			// unsigned integer (PathInfo_deserialize). Casting back to an int32 here.
			Flags:             int32(arg.PathInfoFlags),
			OrigParentUID:     arg.OrigParentUID,
			OrigParentEntryID: bytes.TrimRight(arg.OrigParentEntryID[:], "\x00"),
		},
		RST: msg.RemoteStorageTarget{
			CoolDownPeriod: arg.RSTCooldown,
			FilePolicies:   arg.RSTFilePolicies,
			RSTIDs:         nil,
			// RSTIDs defaults to nil and is set below only if there are RST IDs configured. This
			// preserves legacy behavior when RST IDs were returned by a server GetEntryInfo RPC.
			// Notably in case any clients rely on a nil check to detect presence of RST config.
		},
		MirrorNodeID:     0, // No longer in use and always set to 0.
		NumSessionsRead:  arg.NumSessionsRead,
		NumSessionsWrite: arg.NumSessionsWrite,
		FileState:        beegfs.FileState(arg.FileDataState),
	}
	if arg.NumRSTIDs != 0 {
		entryInfoResp.RST.RSTIDs = arg.RSTIDs[0:arg.NumRSTIDs]
	}
	return entryInfo, entryInfoResp, nil

}
