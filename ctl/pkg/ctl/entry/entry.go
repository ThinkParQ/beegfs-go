package entry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/ioctl"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type GetEntriesCfg struct {
	Verbose        bool
	IncludeOrigMsg bool
	// Parallel allows results to be returned in any order (instead of lexicographical order). This
	// allows multiple goroutines to be used to fetch entry info, typically improving performance.
	Parallel   bool
	NoIoctl    bool
	FilterExpr string
}

// GetEntryCombinedInfo returns all information needed to print details about an entry in BeeGFS.
type GetEntryCombinedInfo struct {
	// The relative path inside BeeGFS for this entry.
	Path string
	// Details about the entry indicated by GetEntryConfig.Path.
	Entry Entry
	// Details about the parent of GetEntryConfig.Path. Only populated if GetEntryConfig.Verbose.
	// IMPORTANT: Currently not all fields are populated on the parent entry including Pattern, RST,
	// and Verbose. These fields are not currently needed to print verbose details about the entry,
	// and would require a second RPC to collect.
	Parent Entry
}

// Entry combines details from the BeeMsg EntryInfo and GetEntryInfoResponse messages along with
// details about the owner node and verbose details about the entry (if requested). It is typically
// created using newEntry().
type Entry struct {
	// The metadata node that owns this entry (i.e., the ownerNode). When the metadata for the entry
	// is mirrored, this is always whichever node is the current primary.
	MetaOwnerNode  beegfs.Node
	MetaBuddyGroup int
	ParentEntryID  string
	EntryID        string
	FileName       string
	Type           beegfs.EntryType
	FeatureFlags   beegfs.EntryFeatureFlags
	Pattern        patternConfig
	Remote         remoteConfig
	// NumSessionsRead is only applicable for regular files. Note sessions is the number of clients
	// that have this file open for reading at least once. If the same file has the a file opened
	// multiple times, it is still treated as a single session for that client.
	NumSessionsRead uint32
	// NumSessionsWrite is only applicable for regular files.  Note sessions is the number of clients
	// that have this file open for writing at least once. If the same file has the a file opened
	// multiple times, it is still treated as a single session for that client.
	NumSessionsWrite uint32
	// FileState is only applicable for regular files. It stores access flags in the lower 5 bits and
	// data state in the upper 3 bits. The access flags determine if the file is accessible for read/write
	// operations or if access is restricted. The data state value is arbitrary and meaningful only to the
	// data management application (or users).
	FileState beegfs.FileState
	// Only populated if GetEntryConfig.Verbose.
	Verbose Verbose
	// Only populated if getEntries() is called with includeOrigMsg. This is mostly useful for other
	// modes like set pattern that need to include the EntryInfo message so they don't need to recreate
	// the message from scratch.
	origEntryInfoMsg *msg.EntryInfo
}

// patternConfig embeds the BeeMsg defined stripe pattern alongside fields that map various
// details from the pattern like the storage pool ID to the user friendly alias (description).
type patternConfig struct {
	msg.StripePattern
	StoragePoolName string
	// Map of legacy storage target IDs to the entity ID set for the storage node that owns that
	// target. Only populated if StripePatternType is StripePatternRaid0. If this entry is buddy
	// mirrored look at TargetIDs instead. In the future we could consider adding a field to map
	// buddy groups to targets then to nodes, but this did not exist in the old CTL.
	StorageTargets map[beegfs.NumId]*beegfs.EntityIdSet
}

type remoteConfig struct {
	msg.RemoteStorageTarget
	// Map of RST IDs to their full configuration details.
	Targets map[uint32]*flex.RemoteStorageTarget
}

func (entryInfo *GetEntryCombinedInfo) GetOrigEntryInfo() *msg.EntryInfo {
	return entryInfo.Entry.origEntryInfoMsg
}

// newEntry is used to assemble an entry from BeeMsgs. If user friendly names of the various IDs
// should be set, the caller should first initialize the various mappers. If the mappers are not
// available value types will be set to logical defaults (like <unknown>) and reference types will
// be nil. If mappings is nil then the cached mappings will be used along with a single attempt to
// update the cached mappings on a failure to retrieve an expected value.
func newEntry(ctx context.Context, mappings *util.Mappings, entry msg.EntryInfo, ownerNode beegfs.Node, entryInfo msg.GetEntryInfoResponse) Entry {

	e := Entry{
		MetaOwnerNode: ownerNode,
		ParentEntryID: string(entry.ParentEntryID),
		EntryID:       string(entry.EntryID),
		FileName:      string(entry.FileName),
		Type:          entry.EntryType,
		FeatureFlags:  entry.FeatureFlags,
		Pattern: patternConfig{
			StripePattern:   entryInfo.Pattern,
			StoragePoolName: "<unknown>",
			StorageTargets:  make(map[beegfs.NumId]*beegfs.EntityIdSet),
		},
		Remote: remoteConfig{
			RemoteStorageTarget: entryInfo.RST,
			Targets:             make(map[uint32]*flex.RemoteStorageTarget),
		},
		NumSessionsRead:  entryInfo.NumSessionsRead,
		NumSessionsWrite: entryInfo.NumSessionsWrite,
		FileState:        entryInfo.FileState,
	}

	if entry.FeatureFlags.IsBuddyMirrored() {
		e.MetaBuddyGroup = int(entry.OwnerID)
	}

	fetchedMappings := false
	if mappings == nil {
		mappings, _ = util.GetCachedMappings(ctx, false)
		fetchedMappings = true
	}

	mappingsUpdated := false
	updateMappings := func() {
		if mappingsUpdated {
			return
		}
		mappings, _ = util.GetCachedMappings(ctx, true)
		mappingsUpdated = true
	}

	pool, err := mappings.StoragePoolToConfig.Get(beegfs.LegacyId{NumId: beegfs.NumId(entryInfo.Pattern.StoragePoolID), NodeType: beegfs.Storage})
	if fetchedMappings && errors.Is(err, util.ErrMapperNotFound) {
		updateMappings()
		pool, err = mappings.StoragePoolToConfig.Get(beegfs.LegacyId{NumId: beegfs.NumId(entryInfo.Pattern.StoragePoolID), NodeType: beegfs.Storage})
	}
	if err == nil {
		e.Pattern.StoragePoolName = pool.Pool.Alias.String()
	}

	if entryInfo.Pattern.Type == beegfs.StripePatternRaid0 {
		for _, tgt := range entryInfo.Pattern.TargetIDs {
			node, err := mappings.TargetToNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(tgt), NodeType: beegfs.Storage})
			if fetchedMappings && errors.Is(err, util.ErrMapperNotFound) {
				updateMappings()
				node, err = mappings.TargetToNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(tgt), NodeType: beegfs.Storage})
			}
			if err != nil {
				e.Pattern.StorageTargets[beegfs.NumId(tgt)] = nil
			} else {
				e.Pattern.StorageTargets[beegfs.NumId(tgt)] = &node
			}
		}
	}

	if len(entryInfo.RST.RSTIDs) != 0 {
		for _, id := range entryInfo.RST.RSTIDs {
			rst, ok := mappings.RstIdToConfig[id]
			if fetchedMappings && errors.Is(err, util.ErrMapperNotFound) {
				updateMappings()
				rst, ok = mappings.RstIdToConfig[id]
			}
			if !ok {
				e.Remote.Targets[id] = nil
			} else {
				e.Remote.Targets[id] = rst
			}
		}
	}

	return e
}

// Verbose carries extra details about a BeeGFS entry and is typically created with newVerbose().
type Verbose struct {
	// Any errors that happen generating verbose details about the entry are recorded here so the
	// caller can decide how to handle them. Often the best course is to simply warn and print as
	// much detail as possible.
	Err        types.MultiError
	ChunkPath  string
	DentryPath string
	HashPath   string
}

func newVerbose(pathInfo msg.PathInfo, entry Entry, parent Entry) Verbose {

	var getEntryVerbose Verbose
	getEntryVerbose.Err = types.MultiError{}

	// Compute chunk path for any kind of file:
	if entry.Type.IsFile() {
		var err error
		getEntryVerbose.ChunkPath, err = getFileChunkPath(pathInfo.OrigParentUID, string(pathInfo.OrigParentEntryID), entry.EntryID)
		if err != nil {
			getEntryVerbose.Err.Errors = append(getEntryVerbose.Err.Errors, err)
		}
	}

	// Compute and include dentry info for files and directories other than root:
	if entry.EntryID != "root" {
		getEntryVerbose.DentryPath = getMetaDirEntryPath("", parent.EntryID)
	}
	// Include inode info for non-inlined inodes:
	if !entry.FeatureFlags.IsInlined() {
		getEntryVerbose.HashPath = getMetaInodePath("", entry.EntryID)
	}
	return getEntryVerbose
}

// GetEntries accepts a context, an input method that controls how paths are provided, and a
// GetEntriesCfg that determines what detail about each entry should be returned. If anything
// goes wrong during the initial setup an error will be returned, otherwise GetEntries will
// immediately return channels where results and errors are written asynchronously. The entriesChan
// will return entry info and be closed once the info for all requested entries is returned or after
// an error. The error channel returns any errors walking the directory or getting the entry info.
// This approach allows callers to decide when there is an error if they should immediately
// terminate, or continue writing out the remaining entries before handling the error.
func GetEntries(ctx context.Context, pm util.PathInputMethod, cfg GetEntriesCfg) (<-chan *GetEntryCombinedInfo, func() error, error) {
	log, _ := config.GetLogger()

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, nil, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
		log.Debug("remote storage mappings are not available (ignoring)", zap.Any("error", err))
	}

	processEntry := func(path string) (*GetEntryCombinedInfo, error) {
		return GetEntry(ctx, mappings, cfg, path)
	}

	return util.ProcessPaths(ctx, pm, !cfg.Parallel, processEntry, util.FilterExpr(cfg.FilterExpr))
}

// GetEntry retrieves GetEntryCombinedInfo based on the provided information. If mappings is nil
// then GetCachedMappings() will be used.
func GetEntry(ctx context.Context, mappings *util.Mappings, cfg GetEntriesCfg, path string) (*GetEntryCombinedInfo, error) {
	entry, ownerNode, err := GetEntryAndOwnerFromPath(ctx, mappings, path, setGetEntryNoIoctl(cfg.NoIoctl))
	if err != nil {
		return nil, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("error accessing the node store: %w", err)
	}

	request := &msg.GetEntryInfoRequest{
		EntryInfo: entry,
	}
	var resp = &msg.GetEntryInfoResponse{}

	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		return nil, fmt.Errorf("error getting entry info from node: %w", err)
	}

	var entryWithParent = GetEntryCombinedInfo{
		Path: path,
	}
	entryWithParent.Entry = newEntry(ctx, mappings, entry, ownerNode, *resp)
	if cfg.IncludeOrigMsg {
		entryWithParent.Entry.origEntryInfoMsg = &entry
	}

	if cfg.Verbose {
		if path != "/" {
			// Unless the searchPath is the root directory, always drop the trailing slash to allow
			// filepath.Dir to determine the parent of the file/directory indicated by searchPath.
			parentSearchPath := filepath.Dir(strings.TrimSuffix(path, "/"))
			// We have to make another RPC to get the parent details needed for verbose output. We can't
			// just cache them the first time around because some of the path components may be skipped
			// if we were able to take any shortcuts during the search.
			parentEntry, parentOwner, err := GetEntryAndOwnerFromPath(ctx, mappings, parentSearchPath)
			if err != nil {
				entryWithParent.Entry.Verbose = Verbose{
					Err: types.MultiError{Errors: []error{err}},
				}
			} else {
				entryWithParent.Parent = newEntry(ctx, mappings, parentEntry, parentOwner, msg.GetEntryInfoResponse{})
				entryWithParent.Entry.Verbose = newVerbose(resp.Path, entryWithParent.Entry, entryWithParent.Parent)
				if cfg.IncludeOrigMsg {
					entryWithParent.Parent.origEntryInfoMsg = &parentEntry
				}
			}
		} else {
			// If the searchPath is root there is no parent, just use an empty Entry as the parent.
			entryWithParent.Entry.Verbose = newVerbose(resp.Path, entryWithParent.Entry, Entry{})
		}
	}

	return &entryWithParent, nil
}

type getEntryCfg struct {
	noIoctl bool
}

type getEntryOpt func(*getEntryCfg)

func setGetEntryNoIoctl(noIoctl bool) getEntryOpt {
	return func(cfg *getEntryCfg) {
		cfg.noIoctl = noIoctl
	}
}

// GetEntryAndOwnerFromPath retrieves entry and owning node information for the provided relative
// path inside BeeGFS. The mappings will be acquired if needed when mappings is nil. It requires the
// relative path inside BeeGFS for which entry info should be collected.
//
// If BeeGFS is mounted this function will fetch entry info using an ioctl, otherwise it will
// fallback to the (more inefficient) RPC path. For the automatic detection to work, the global
// BeeGFS client accessed via config.BeeGFSClient() must have already been initialized for either a
// valid BeeGFS mount point (to use the ioctl path) or an unmounted client (to fallback to an RPC).
// This function cannot initialize config.BeeGFSClient() itself since it works on a relative path.
func GetEntryAndOwnerFromPath(ctx context.Context, mappings *util.Mappings, path string, opts ...getEntryOpt) (msg.EntryInfo, beegfs.Node, error) {
	cfg := &getEntryCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	if !cfg.noIoctl {
		beegfsClient, err := config.BeeGFSClient(path)
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to get a valid BeeGFS client (this is likely a bug): %w", err)
		}
		// Default to using an ioctl to get entry info if BeeGFS is mounted.
		if beegfsClient.GetMountPath() != "" {
			entry, ownerNode, err := getEntryAndOwnerFromPathViaIoctl(ctx, mappings, beegfsClient, path)
			if err != nil {
				return msg.EntryInfo{}, beegfs.Node{}, err
			}
			return entry, ownerNode, nil
		}
	}

	if syscall.Geteuid() != 0 {
		return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("only root can get entry info without using an ioctl")
	}
	entry, ownerNode, err := getEntryAndOwnerFromPathViaRPC(ctx, mappings, path)
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, err
	}
	return entry, ownerNode, nil
}

// getEntryAndOwnerFromPathViaIoctl is the equivalent of the useMountedPath code path from the C++
// getEntryAndOwnerFromPath() implementation.
func getEntryAndOwnerFromPathViaIoctl(ctx context.Context, mappings *util.Mappings, beegfsClient filesystem.Provider, searchPath string) (msg.EntryInfo, beegfs.Node, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, err
	}

	// This mode is highly optimized and doesn't always use the beegfsClient wrapper functions.
	absPath := filepath.Clean(beegfsClient.GetMountPath() + "/" + searchPath)

	// Entry types other than regular files and directories require special handling. This behavior
	// was recreated based on the v7 CTL implementation. The reason is because special file types
	// cannot always be opened directly and used with the GetEntryInfo ioctl.
	lookupSpecialFileType := false

	// Most entries in BeeGFS are regular files or directories and this code is optimized for that.
	// If this is a special entry type we might get an error trying to open it with these flags. For
	// example ELOOP likely indicates a symlink and ENXIO is common if this is a char/block device.
	// Rather than try and handle all possible error types, this falls back to using Lstat to verify
	// if we're working with a special file type then using LookupIntentRequest with the parent.
	fd, err := unix.Open(absPath, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		log, _ := config.GetLogger()
		log.Debug("unable to open entry, checking on fallback options", zap.String("searchPath", searchPath), zap.Error(err))

		// Bail out immediately on any permissions errors.
		if errors.Is(err, syscall.EPERM) {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to open entry %q: %w", searchPath, err)
		}
		// If the entry doesn't exist there is no reason to call lstat(), return immediately.
		if errors.Is(err, syscall.ENOENT) {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to open entry %q: %w", searchPath, err)
		}

		// Stat the file so we can check its type and if the user has permissions:
		stat, statErr := beegfsClient.Lstat(searchPath)
		if statErr != nil {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to lstat entry %q: %w", searchPath, statErr)
		}

		// If this is a regular file and the open failed due to EWOULDBLOCK/EAGAIN, probably the
		// BeeGFS file access flags blocked the open. Try to fetch entry info using the RPC instead.
		if stat.Mode().IsRegular() && errors.Is(err, syscall.EWOULDBLOCK) {
			log.Debug("entry is temporarily unavailable, possibly the contents are locked, trying to fetch entry info via rpc", zap.String("searchPath", searchPath))
			return getEntryAndOwnerFromPathViaRPC(ctx, mappings, searchPath)
		}

		// Otherwise if we could stat but this is not a regular file or directory we know this is
		// likely a special file type.
		if !(stat.Mode().IsRegular() || stat.Mode().IsDir()) {
			lookupSpecialFileType = true
		} else {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to open entry %q: %w", searchPath, err)
		}
	} else {
		// Always check the entry type. Notably opening pipes and device entries may succeed but the
		// resulting file descriptors will work with ioctl syscalls.
		stat := &unix.Stat_t{}
		// Testing shows unix.Fstat with an existing fd is faster than calling lstat() then open().
		if err := unix.Fstat(fd, stat); err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to fstat entry %q: %w", searchPath, err)
		}
		fileType := stat.Mode & unix.S_IFMT
		lookupSpecialFileType = !(fileType == unix.S_IFREG || fileType == unix.S_IFDIR)
	}

	if lookupSpecialFileType {
		if err == nil {
			// If the original open succeeded but we ended up with an fd for a special file type,
			// close it before replacing it with the parent fd.
			unix.Close(fd)
		}
		fd, err = unix.Open(filepath.Dir(absPath), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to open parent directory %q: %w", filepath.Dir(absPath), err)
		}
		log, _ := config.GetLogger()
		log.Debug("encountered an entry that is not a regular file or directory, using the parent to get entry info", zap.String("searchPath", searchPath))
	}

	// Now we know its safe to defer closing the fd.
	defer unix.Close(fd)

	entryInfo, err := ioctl.GetEntryInfo(uintptr(fd), ioctl.TrimEntryInfoNullBytes())
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to get entry info for %q: %w", searchPath, err)
	}

	metaNodeNumID, err := getPrimaryMetaNode(ctx, mappings, entryInfo)
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, err
	}

	currentNode, err := store.GetNode(beegfs.LegacyId{
		NumId:    metaNodeNumID,
		NodeType: beegfs.Meta,
	})
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, err
	}

	// If this is a special file type use a LookupIntent to get the entryInfo of the actual entry.
	if lookupSpecialFileType {
		request := &msg.LookupIntentRequest{
			ParentInfo: entryInfo,
			EntryName:  []byte(filepath.Base(searchPath)),
		}
		resp := &msg.LookupIntentResponse{}
		if err = store.RequestTCP(ctx, currentNode.Uid, request, resp); err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, err
		}

		if resp.LookupResult != beegfs.OpsErr_SUCCESS {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unexpected lookup result for %q: %w", filepath.Base(searchPath), resp.LookupResult)
		}

		// Return the entryInfo from the LookupIntentResponse.
		entryInfo = resp.EntryInfo
		metaNodeNumID, err = getPrimaryMetaNode(ctx, mappings, entryInfo)
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, err
		}

		currentNode, err = store.GetNode(beegfs.LegacyId{
			NumId:    metaNodeNumID,
			NodeType: beegfs.Meta,
		})
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, err
		}
	}

	return entryInfo, currentNode, nil
}

// getEntryAndOwnerFromPathViaRPC() is the Go equivalent of the use unmounted code path from the C++
// getEntryAndOwnerFromPath().
func getEntryAndOwnerFromPathViaRPC(ctx context.Context, mappings *util.Mappings, searchPath string) (msg.EntryInfo, beegfs.Node, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return msg.EntryInfo{}, beegfs.Node{}, err
	}

	var resp = &msg.FindOwnerResponse{}
	// If the root is buddy mirrored this always returns the current primary.
	metaRoot := store.GetMetaRootNode()
	if metaRoot == nil {
		return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to proceed without a working root metadata server")
	}
	// Start search at root metadata node:
	currentNode := *metaRoot
	var searchDepth uint32
	pathComponents := strings.Split(searchPath, string(filepath.Separator))
	for _, c := range pathComponents {
		if c != "" {
			searchDepth++
		}
	}

	// Start at the root to look for searchPath (which may be the root itself):
	request := &msg.FindOwnerRequest{
		Path: msg.Path{
			PathStr: []byte(searchPath),
		},
		SearchDepth:  searchDepth,
		CurrentDepth: 0,
		EntryInfo: msg.EntryInfo{
			OwnerID:       uint32(currentNode.Id.NumId),
			ParentEntryID: []byte(""),
			EntryID:       []byte("root"),
			FileName:      []byte(filepath.Base(searchPath)),
			EntryType:     1,
			FeatureFlags: func() beegfs.EntryFeatureFlags {
				var ff beegfs.EntryFeatureFlags
				if store.HasMetaRootBuddyGroup() {
					ff.SetBuddyMirrored()
				}
				return ff
			}(),
		},
	}

	// Then walk through each of the path components (taking shortcuts when possible). Shortcuts are
	// possible when the same meta owns multiple adjacent directories in the path, the meta server
	// will search as far as possible before returning a response (see FindOwnerMsgEx::findOwner()).
	for numSearchSteps := 0; numSearchSteps < 128; numSearchSteps++ {

		err = store.RequestTCP(ctx, currentNode.Uid, request, resp)
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, err
		} else if resp.Result != 0 {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unexpected search result for %q: %w", filepath.Base(searchPath), resp.Result)
		}

		metaNodeNumID, err := getPrimaryMetaNode(ctx, mappings, resp.EntryInfoWithDepth.EntryInfo)
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, err
		}

		currentNode, err = store.GetNode(beegfs.LegacyId{
			NumId:    metaNodeNumID,
			NodeType: beegfs.Meta,
		})
		if err != nil {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to retrieve metadata node %d from the node store: %w", metaNodeNumID, err)
		}

		// Return on successful end of search:
		if resp.EntryInfoWithDepth.EntryDepth == searchDepth {
			return resp.EntryInfoWithDepth.EntryInfo, currentNode, nil
		} else if resp.EntryInfoWithDepth.EntryDepth <= request.CurrentDepth {
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("current depth unexpectedly exceeded entry depth (probably there was concurrent access)")
		}

		// Otherwise the search is not finished, proceed to the next node:
		request.EntryInfo = resp.EntryInfoWithDepth.EntryInfo
		request.CurrentDepth = resp.EntryInfoWithDepth.EntryDepth
	}

	return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("max search steps exceeded for path: %s", searchPath)
}

// getPrimaryMetaNode is a helper function used to determine the metadata node that owns the
// specified entry. If the entry is buddy mirrored this handles converting the OwnerID in the entry
// info to the NumID of the current primary node. Otherwise it just directly returns the OwnerID. If
// mappings are not provided it will use cached mappings.
func getPrimaryMetaNode(ctx context.Context, mappings *util.Mappings, entryInfo msg.EntryInfo) (beegfs.NumId, error) {
	if !entryInfo.FeatureFlags.IsBuddyMirrored() {
		return beegfs.NumId(entryInfo.OwnerID), nil
	}
	fetchedMappings := false
	if mappings == nil {
		var err error
		mappings, err = util.GetCachedMappings(ctx, false)
		if err != nil {
			return 0, err
		}
		fetchedMappings = true
	}

	primaryMetaNode, err := mappings.MetaBuddyGroupToPrimaryNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(entryInfo.OwnerID), NodeType: beegfs.Meta})
	if fetchedMappings && errors.Is(err, util.ErrMapperNotFound) {
		mappings, err = util.GetCachedMappings(ctx, true)
		if err != nil {
			return 0, err
		}
		primaryMetaNode, err = mappings.MetaBuddyGroupToPrimaryNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(entryInfo.OwnerID), NodeType: beegfs.Meta})
	}
	if err != nil {
		return 0, fmt.Errorf("unable to determine primary metadata node from buddy mirror ID %d: %w", entryInfo.OwnerID, err)
	}
	return primaryMetaNode.LegacyId.NumId, err
}

func GetFileDataState(ctx context.Context, path string) (beegfs.DataState, error) {
	state, err := getFileState(ctx, path)
	if err != nil {
		return beegfs.DataStateMask.GetDataState(), fmt.Errorf("failed to get file data state: %w", err)
	}
	return state.GetDataState(), nil
}

func SetFileDataState(ctx context.Context, path string, state beegfs.DataState) error {
	entry, ownerNode, err := GetEntryAndOwnerFromPath(ctx, nil, path)
	if err != nil {
		return fmt.Errorf("unable to retrieve entry info: %w", err)
	}
	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	info := &msg.GetEntryInfoResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, &msg.GetEntryInfoRequest{EntryInfo: entry}, info)
	if err != nil {
		return fmt.Errorf("unable to get data state for path, %s: %w", path, err)
	}

	fs := info.FileState.WithDataState(state)
	if fs == info.FileState {
		return nil
	}

	response := &msg.SetFileStateResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, &msg.SetFileStateRequest{EntryInfo: entry, FileState: fs}, response)
	if err != nil {
		return err
	}
	if response.Result != beegfs.OpsErr_SUCCESS {
		// It is possible for multiple requests to attempt to set the same file's data state
		// concurrently. In these cases, OpsErr_INODELOCKED is returned and will be retried once
		// more.
		if response.Result == beegfs.OpsErr_INODELOCKED {
			err = store.RequestTCP(ctx, ownerNode.Uid, &msg.GetEntryInfoRequest{EntryInfo: entry}, info)
			if err != nil {
				return fmt.Errorf("unable to get data state for path, %s: %w", path, err)
			}

			fs := info.FileState.WithDataState(state)
			if fs == info.FileState {
				return nil
			}

			err = store.RequestTCP(ctx, ownerNode.Uid, &msg.SetFileStateRequest{EntryInfo: entry, FileState: fs}, response)
			if err != nil {
				return err
			}

			if response.Result == beegfs.OpsErr_SUCCESS {
				return nil
			}
		}

		return fmt.Errorf("server returned an error setting data state for path, %s: %w", path, response.Result)
	}
	return nil
}

func GetFileAccessFlags(ctx context.Context, path string) (beegfs.AccessFlags, error) {
	state, err := getFileState(ctx, path)
	if err != nil {
		return beegfs.AccessFlagMask.GetAccessFlags(), fmt.Errorf("failed to get file access flags: %w", err)
	}
	return state.GetAccessFlags(), nil
}

func SetAccessFlags(ctx context.Context, path string, flags beegfs.AccessFlags) error {
	if err := setAccessFlags(ctx, path, flags, false); err != nil {
		return fmt.Errorf("failed to set file access flags: %w", err)
	}
	return nil
}

func ClearAccessFlags(ctx context.Context, path string, flags beegfs.AccessFlags) error {
	if err := setAccessFlags(ctx, path, flags, true); err != nil {
		return fmt.Errorf("failed to clear file access flags: %w", err)
	}
	return nil
}

func getFileState(ctx context.Context, path string) (beegfs.FileState, error) {
	entry, ownerNode, err := GetEntryAndOwnerFromPath(ctx, nil, path)
	if err != nil {
		mask := beegfs.NewFileState(beegfs.AccessFlagMask.GetAccessFlags(), beegfs.AccessFlagMask.GetDataState())
		return mask, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		mask := beegfs.NewFileState(beegfs.AccessFlagMask.GetAccessFlags(), beegfs.AccessFlagMask.GetDataState())
		return mask, err
	}

	request := &msg.GetEntryInfoRequest{EntryInfo: entry}
	resp := &msg.GetEntryInfoResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		mask := beegfs.NewFileState(beegfs.AccessFlagMask.GetAccessFlags(), beegfs.AccessFlagMask.GetDataState())
		return mask, err
	}

	return resp.FileState, nil
}

func setAccessFlags(ctx context.Context, path string, flags beegfs.AccessFlags, clearFlags bool) error {
	entry, ownerNode, err := GetEntryAndOwnerFromPath(ctx, nil, path)
	if err != nil {
		return err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	info := &msg.GetEntryInfoResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, &msg.GetEntryInfoRequest{EntryInfo: entry}, info)
	if err != nil {
		return err
	}

	fs := info.FileState
	if clearFlags {
		fs = fs.WithoutAccessState(flags)
	} else {
		fs = fs.WithAccessState(flags)
	}

	if fs == info.FileState {
		return nil
	}

	response := &msg.SetFileStateResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, &msg.SetFileStateRequest{EntryInfo: entry, FileState: fs}, response)
	if err != nil {
		return err
	}
	if response.Result != beegfs.OpsErr_SUCCESS {
		// It is possible for multiple requests to attempt to set the same file's data state
		// concurrently. In these cases, OpsErr_INODELOCKED is returned and will be retried once
		// more.
		if response.Result == beegfs.OpsErr_INODELOCKED {
			info := &msg.GetEntryInfoResponse{}
			err = store.RequestTCP(ctx, ownerNode.Uid, &msg.GetEntryInfoRequest{EntryInfo: entry}, info)
			if err != nil {
				return err
			}

			if fs == info.FileState {
				return nil
			}

			err = store.RequestTCP(ctx, ownerNode.Uid, &msg.SetFileStateRequest{EntryInfo: entry, FileState: fs}, response)
			if err != nil {
				return err
			}
			if response.Result == beegfs.OpsErr_SUCCESS {
				return nil
			}

		}
		return fmt.Errorf("server returned an error setting file access flags, %s: %w", path, response.Result)
	}
	return nil
}

func SetFileRstIds(ctx context.Context, entry msg.EntryInfo, ownerNode beegfs.Node, path string, rstIds []uint32) error {
	if rstIds == nil {
		return nil
	}
	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	rstIdRequest := &msg.SetFilePatternRequest{EntryInfo: entry, RST: msg.RemoteStorageTarget{RSTIDs: rstIds}}
	rstIdResp := &msg.SetFilePatternResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, rstIdRequest, rstIdResp)
	if err != nil {
		return err
	}
	if rstIdResp.Result != beegfs.OpsErr_SUCCESS {
		return fmt.Errorf("server returned an error configuring file targets, %s: %w", path, rstIdResp.Result)
	}

	return nil
}
