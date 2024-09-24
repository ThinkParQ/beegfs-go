package entry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// GetEntriesConfig is used to determine how paths are provided to GetEntries() and control what
// output is returned for each entry. The PathsViaX fields are mutually exclusive, and if multiple
// are specified they are prioritized from top to bottom.
type GetEntriesConfig struct {
	// The stdin mechanism allows the caller to provide multiple paths over a channel. This allows
	// paths to be sent to GetEntries() while simultaneously returning results for each path. This
	// is useful when GetEntries() is used as part of a larger pipeline. The caller should close the
	// channel once all paths are done to cleanup.
	PathsViaChan <-chan string
	// Provide a single path to walking a directory tree and return all sub-entries.
	PathsViaRecursion string
	// Specify one or more paths.
	PathsViaList []string
	// Return extra detail about each entry.
	Verbose bool
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

// newEntry is used to assemble an entry from BeeMsgs. If user friendly names of the various IDs
// should be set, the caller should first initialize the various mappers. If the mappers are not
// available value types will be set to logical defaults (like <unknown>) and reference types will
// be nil.
func newEntry(mappings *util.Mappings, entry msg.EntryInfo, ownerNode beegfs.Node, entryInfo msg.GetEntryInfoResponse) Entry {
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
	}
	if entry.FeatureFlags.IsBuddyMirrored() {
		e.MetaBuddyGroup = int(entry.OwnerID)
	}

	pool, err := mappings.StoragePoolToConfig.Get(beegfs.LegacyId{NumId: beegfs.NumId(entryInfo.Pattern.StoragePoolID), NodeType: beegfs.Storage})
	if err == nil {
		e.Pattern.StoragePoolName = pool.Pool.Alias.String()
	}

	if entryInfo.Pattern.Type == beegfs.StripePatternRaid0 {
		for _, tgt := range entryInfo.Pattern.TargetIDs {
			node, err := mappings.TargetToNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(tgt), NodeType: beegfs.Storage})
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

// GetEntries accepts a context and a GetEntriesConfig that controls how paths are provided and what
// detail about each entry should be returned.
//
// If anything goes wrong during the initial setup an error will be returned, otherwise GetEntries
// will immediately return channels where results and errors are written asynchronously. The
// entriesChan will return entry info and be closed once the info for all requested entries is
// returned or after an error. The error channel returns any errors walking the directory or getting
// the entry info. This approach allows callers to decide when there is an error if they should
// immediately terminate, or continue writing out the remaining entries before handling the error.
func GetEntries(ctx context.Context, cfg GetEntriesConfig) (<-chan *GetEntryCombinedInfo, <-chan error, error) {
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", "getEntries"))

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, nil, err
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
		log.Debug("remote storage mappings are not available (ignoring)", zap.Any("error", err))
	}

	var entriesChan <-chan *GetEntryCombinedInfo
	// At most two errors are expected. Increase if additional writers are added to the channel.
	errChan := make(chan error, 2)

	if cfg.PathsViaChan != nil {
		// Read paths from the provided channel:
		entriesChan = getEntries(ctx, mappings, cfg.PathsViaChan, cfg.Verbose, errChan)
	} else if cfg.PathsViaRecursion != "" {
		pathsChan, err := walkDir(ctx, cfg.PathsViaRecursion, errChan)
		if err != nil {
			return nil, nil, err
		}
		entriesChan = getEntries(ctx, mappings, pathsChan, cfg.Verbose, errChan)
	} else {
		pathsChan := make(chan string, 1024)
		entriesChan = getEntries(ctx, mappings, pathsChan, cfg.Verbose, errChan)
		for _, e := range cfg.PathsViaList {
			pathsChan <- e
		}
		close(pathsChan)
	}
	return entriesChan, errChan, nil
}

// Returns a channel where the entry info for each path will be sent. It asynchronously processes
// paths as they are sent to the provided channel and sends the results to the returned channel. If
// there are any issues getting an entry they are returned on the provided errChan and no more
// entries are processed.
func getEntries(ctx context.Context, mappings *util.Mappings, paths <-chan string, verbose bool, errChan chan<- error) <-chan *GetEntryCombinedInfo {

	entriesChan := make(chan *GetEntryCombinedInfo, 1024)
	var beegfsClient filesystem.Provider
	var err error
	go func() {
		defer close(entriesChan)
		for {
			select {
			case path, ok := <-paths:
				if !ok {
					return
				}
				// Automatically initialize the BeeGFS client with the first path.
				if beegfsClient == nil {
					beegfsClient, err = config.BeeGFSClient(path)
					if err != nil {
						errChan <- err
						return
					}
				}
				searchPath, err := beegfsClient.GetRelativePathWithinMount(path)
				if err != nil {
					errChan <- err
					return
				}
				result, err := GetEntry(ctx, mappings, searchPath, verbose, false)
				if err != nil {
					errChan <- err
					return
				}
				entriesChan <- &result
			case <-ctx.Done():
				return
			}
		}
	}()

	return entriesChan
}

func GetEntry(ctx context.Context, mappings *util.Mappings, searchPath string, verbose bool, includeOrigMsg bool) (GetEntryCombinedInfo, error) {
	// TODO: https://github.com/thinkparq/ctl/issues/54
	// Add the ability to get the entry via ioctl. Note, here we don't need to get RST info from the
	// ioctl path. The old CTL can use an ioctl or RPC to get the entry but the actual info is
	// always retrieved using the RPC.
	entry, ownerNode, err := getEntryAndOwnerFromPathViaRPC(ctx, mappings, searchPath)
	if err != nil {
		return GetEntryCombinedInfo{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return GetEntryCombinedInfo{}, fmt.Errorf("error accessing the node store: %w", err)
	}

	request := &msg.GetEntryInfoRequest{
		EntryInfo: entry,
	}
	var resp = &msg.GetEntryInfoResponse{}

	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		return GetEntryCombinedInfo{}, fmt.Errorf("error getting entry info from node: %w", err)
	}

	var entryWithParent = GetEntryCombinedInfo{
		Path: searchPath,
	}
	entryWithParent.Entry = newEntry(mappings, entry, ownerNode, *resp)
	if includeOrigMsg {
		entryWithParent.Entry.origEntryInfoMsg = &entry
	}

	if verbose {
		if searchPath != "/" {
			// Unless the searchPath is the root directory, always drop the trailing slash to allow
			// filepath.Dir to determine the parent of the file/directory indicated by searchPath.
			parentSearchPath := filepath.Dir(strings.TrimSuffix(searchPath, "/"))
			// We have to make another RPC to get the parent details needed for verbose output. We can't
			// just cache them the first time around because some of the path components may be skipped
			// if we were able to take any shortcuts during the search.
			parentEntry, parentOwner, err := getEntryAndOwnerFromPathViaRPC(ctx, mappings, parentSearchPath)
			if err != nil {
				entryWithParent.Entry.Verbose = Verbose{
					Err: types.MultiError{Errors: []error{err}},
				}
			} else {
				entryWithParent.Parent = newEntry(mappings, parentEntry, parentOwner, msg.GetEntryInfoResponse{})
				entryWithParent.Entry.Verbose = newVerbose(resp.Path, entryWithParent.Entry, entryWithParent.Parent)
				if includeOrigMsg {
					entryWithParent.Parent.origEntryInfoMsg = &parentEntry
				}
			}
		} else {
			// If the searchPath is root there is no parent, just use an empty Entry as the parent.
			entryWithParent.Entry.Verbose = newVerbose(resp.Path, entryWithParent.Entry, Entry{})
		}
	}

	return entryWithParent, nil
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
			// TODO: https://github.com/thinkparq/ctl/issues/55
			// Correctly set the FeatureFlags if the root metadata node is mirrored. Technically
			// this doesn't matter, but may in the future if things change.
			FeatureFlags: 0,
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
			return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unexpected search result for '%s': %s", searchPath, resp.Result)
		}

		// If the directory is buddy mirrored, the owner ID will be a buddy group ID. We have to map
		// that to the ID of the current primary metadata node.
		var metaNodeNumID beegfs.NumId
		if resp.EntryInfoWithDepth.EntryInfo.FeatureFlags.IsBuddyMirrored() {
			primaryMetaNode, err := mappings.MetaBuddyGroupToPrimaryNode.Get(beegfs.LegacyId{NumId: beegfs.NumId(resp.EntryInfoWithDepth.EntryInfo.OwnerID), NodeType: beegfs.Meta})
			if err != nil {
				return msg.EntryInfo{}, beegfs.Node{}, fmt.Errorf("unable to determine primary metadata node from buddy mirror ID %d: %w", resp.EntryInfoWithDepth.EntryInfo.OwnerID, err)
			}
			metaNodeNumID = primaryMetaNode.LegacyId.NumId
		} else {
			metaNodeNumID = beegfs.NumId(resp.EntryInfoWithDepth.EntryInfo.OwnerID)
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
