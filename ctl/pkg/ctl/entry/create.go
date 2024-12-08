package entry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

// CreateEntryCfg contains all configuration that is required when creating new entries in BeeGFS.
// The specific type of entry to create is determined depending if FileCfg or DirCfg is set. If both
// are set, FileCfg will take precedence.
type CreateEntryCfg struct {
	Paths []string
	// Fields that must always be specified by the user:
	Permissions *int32
	UserID      *uint32
	GroupID     *uint32
	FileCfg     *CreateFileCfg
	DirCfg      *CreateDirCfg
}

// CreateFileCfg contains optional configuration that can be set when creating a new file. By
// default configuration is inherited from the parent directory.
type CreateFileCfg struct {
	Force              bool
	StripePattern      *beegfs.StripePatternType
	Chunksize          *uint32
	DefaultNumTargets  *uint32
	Pool               *beegfs.EntityId
	TargetIDs          []beegfs.EntityId
	RemoteTargets      []uint32
	RemoteCooldownSecs *uint16
}

// CreateDirCfg contains optional configuration that can be set when creating a new directory.
type CreateDirCfg struct {
	Nodes    []beegfs.EntityId
	NoMirror bool
}

type CreateEntryResult struct {
	Path         string
	Status       beegfs.OpsErr
	RawEntryInfo msg.EntryInfo
}

// generateAndVerifyMakeFileReq merges the provided user and parent entry config into a base
// MakeFileWithPatternRequest that can be used for creating multiple entries under the same
// directory. It handles verifying the configuration for the new file is valid, notably ensuring the
// stripe pattern is compatible with the available targets/buddy groups determined by the pool or
// direct user configuration.
//
// IMPORTANT: It DOES NOT set NewFileName, relying on the caller to set this when making individual
// requests for each new entry.
func generateAndVerifyMakeFileReq(userCfg *CreateEntryCfg, parent *GetEntryCombinedInfo, mappings *util.Mappings) (*msg.MakeFileWithPatternRequest, error) {

	if userCfg.FileCfg == nil {
		return nil, errors.New("user config does not contain file configuration (probably this is a bug)")
	}

	// Verify/set basic settings that must be set with all requests:
	if userCfg.UserID == nil {
		return nil, fmt.Errorf("user ID must be set")
	}
	if userCfg.GroupID == nil {
		return nil, fmt.Errorf("group ID must be set")
	}
	if userCfg.Permissions == nil {
		return nil, fmt.Errorf("permissions must be set")
	}

	var err error
	request := &msg.MakeFileWithPatternRequest{
		UserID:  *userCfg.UserID,
		GroupID: *userCfg.GroupID,
		// Trim invalid flags from mode and ensure it contains the "file" flag (otherwise you will
		// get a vague internal error).
		Mode:       (*userCfg.Permissions & 07777) | syscall.S_IFREG,
		Umask:      0000,
		ParentInfo: *parent.Entry.origEntryInfoMsg,
		// Start with the parent pattern and RST config.
		Pattern: parent.Entry.Pattern.StripePattern,
		RST:     parent.Entry.Remote.RemoteStorageTarget,
		// NewFileName must be set by the caller.
	}

	// Specific checks are performed based on the pattern, set that first:
	if userCfg.FileCfg.StripePattern != nil {
		request.Pattern.Type = *userCfg.FileCfg.StripePattern
	}

	// Set the chunksize if requested:
	if userCfg.FileCfg.Chunksize != nil {
		request.Pattern.Chunksize = *userCfg.FileCfg.Chunksize
	}

	// Set the default number of targets if requested:
	if userCfg.FileCfg.DefaultNumTargets != nil {
		request.Pattern.DefaultNumTargets = *userCfg.FileCfg.DefaultNumTargets
	}

	// Set the pool if requested:
	var storagePool pool.GetStoragePools_Result
	if userCfg.FileCfg.Pool != nil {
		storagePool, err = mappings.StoragePoolToConfig.Get(*userCfg.FileCfg.Pool)
		if err != nil {
			return nil, fmt.Errorf("error looking up the specified pool %s: %w", *userCfg.FileCfg.Pool, err)
		}
		request.Pattern.StoragePoolID = uint16(storagePool.Pool.LegacyId.NumId)
	} else {
		// Otherwise still get the pool assigned to the parent so it can be potentially checked
		// below to ensure it is valid for the configured stripe pattern.
		storagePool, err = mappings.StoragePoolToConfig.Get(beegfs.LegacyId{
			NodeType: beegfs.Storage,
			NumId:    beegfs.NumId(parent.Entry.Pattern.StoragePoolID),
		})
		if err != nil {
			return nil, fmt.Errorf("error looking up pool for parent entry %s: %w", *userCfg.FileCfg.Pool, err)
		}
		// No need to do anything else, the parent pool is already set above on the request.
	}

	// Set specific targets/buddy groups if requested:
	if len(userCfg.FileCfg.TargetIDs) > 0 {
		if userCfg.FileCfg.DefaultNumTargets != nil && int(*userCfg.FileCfg.DefaultNumTargets) > len(userCfg.FileCfg.TargetIDs) {
			return nil, fmt.Errorf("requested number of targets is greater than the number of targets/buddy groups that were specified")
		}
		// Make sure the targets or buddy groups specified by the user are actually valid and
		// compatible with the configured stripe pattern:
		request.Pattern.TargetIDs, err = checkAndGetTargets(userCfg.FileCfg.Force, mappings, storagePool, request.Pattern.Type, userCfg.FileCfg.TargetIDs)
		if err != nil {
			return nil, err
		}
	} else {
		// If the user did not explicitly specify anything we should send an empty slice and let
		// targets be automatically determined by the metadata service based on the storage pool.
		request.Pattern.TargetIDs = []uint16{}
		// If the user did not specify specific target IDs, make sure the configured storage pool
		// actually contains targets or buddy groups depending on the stripe pattern.
		err = checkPoolForPattern(storagePool, request.Pattern.Type)
		if err != nil {
			return nil, err
		}
	}

	// Set specific RST config if requested:
	if len(userCfg.FileCfg.RemoteTargets) > 0 {
		request.RST.RSTIDs = userCfg.FileCfg.RemoteTargets
	}
	if userCfg.FileCfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *userCfg.FileCfg.RemoteCooldownSecs
	}

	// Return the request
	return request, nil
}

func checkPoolForPattern(storagePool pool.GetStoragePools_Result, pattern beegfs.StripePatternType) error {
	if pattern == beegfs.StripePatternRaid0 && len(storagePool.Targets) == 0 {
		return fmt.Errorf("the specified pool %s does not contain any targets for the configured stripe pattern %s", storagePool.Pool, pattern)
	} else if pattern == beegfs.StripePatternBuddyMirror && len(storagePool.BuddyGroups) == 0 {
		return fmt.Errorf("the specified pool %s does not contain any buddy groups for the configured stripe pattern %s", storagePool.Pool, pattern)
	} else if pattern != beegfs.StripePatternRaid0 && pattern != beegfs.StripePatternBuddyMirror {
		return fmt.Errorf("unknown stripe pattern: %s", pattern)
	}
	return nil
}

// checkAndGetTargets accepts a slice of targets or buddy groups and verifies they exist otherwise
// an error is returned. Unless force is set, it also verifies they are all in the specified
// storagePool. If the checks pass, a slice containing the IDs of the targets/groups is returned.
//
// Note the checking differs slightly from the v7 CTL. The old tool just verified all the specified
// targets/buddy groups were in the same storage pool. Now we also check the specified targets are
// in the pool that will be assigned to the new entry. This also has the same effect of not allowing
// files to be created using targets in different pools unless forced.
func checkAndGetTargets(force bool, mappings *util.Mappings, storagePool pool.GetStoragePools_Result, pattern beegfs.StripePatternType, targetsOrBuddies []beegfs.EntityId) ([]uint16, error) {
	ids := []uint16{}
	if pattern == beegfs.StripePatternRaid0 {
		targetMap := map[beegfs.EntityIdSet]struct{}{}
		for _, t := range storagePool.Targets {
			targetMap[t] = struct{}{}
		}
		for _, userTgt := range targetsOrBuddies {
			if t, err := mappings.TargetToEntityIdSet.Get(userTgt); err != nil {
				return nil, fmt.Errorf("unable to look up specified target: %w", err)
			} else {
				if _, ok := targetMap[t]; !ok && !force {
					return nil, fmt.Errorf("specified target %s is not in the pool %s assigned to the new entry (use force to override)", t, storagePool.Pool)
				}
				ids = append(ids, uint16(t.LegacyId.NumId))
			}
		}
	} else if pattern == beegfs.StripePatternBuddyMirror {
		buddyMap := map[beegfs.EntityIdSet]struct{}{}
		for _, b := range storagePool.BuddyGroups {
			buddyMap[b] = struct{}{}
		}
		for _, userBuddy := range targetsOrBuddies {
			if b, err := mappings.StorageBuddyToEntityIdSet.Get(userBuddy); err != nil {
				return nil, fmt.Errorf("unable to look up specified buddy group: %w", err)
			} else {
				if _, ok := buddyMap[b]; !ok && !force {
					return nil, fmt.Errorf("specified buddy group %s is not in the pool %s assigned to the new entry (use force to override)", b, storagePool.Pool)
				}
				ids = append(ids, uint16(b.LegacyId.NumId))
			}
		}
	} else {
		return nil, fmt.Errorf("unknown stripe pattern: %s", pattern)
	}

	return ids, nil
}

func generateAndVerifyMkDirRequest(userCfg *CreateEntryCfg, parent *GetEntryCombinedInfo, nodeStore *beemsg.NodeStore, mappings *util.Mappings) (*msg.MkDirRequest, error) {
	if userCfg.DirCfg == nil {
		return nil, errors.New("user config does not contain directory configuration (probably this is a bug)")
	}

	// Verify/set basic settings that must be set with all requests:
	if userCfg.UserID == nil {
		return nil, fmt.Errorf("user ID must be set")
	}
	if userCfg.GroupID == nil {
		return nil, fmt.Errorf("group ID must be set")
	}
	if userCfg.Permissions == nil {
		return nil, fmt.Errorf("permissions must be set")
	}

	var err error
	request := &msg.MkDirRequest{
		UserID:  *userCfg.UserID,
		GroupID: *userCfg.GroupID,
		// Trim invalid flags from mode and ensure it contains the "dir" flag (otherwise you will
		// get a vague internal error).
		Mode:           (*userCfg.Permissions & 07777) | syscall.S_IFDIR,
		Umask:          0000,
		ParentInfo:     *parent.Entry.origEntryInfoMsg,
		NoMirror:       userCfg.DirCfg.NoMirror,
		PreferredNodes: make([]uint16, 0, len(userCfg.DirCfg.Nodes)),
	}

	if len(userCfg.DirCfg.Nodes) > 0 {
		for _, entityID := range userCfg.DirCfg.Nodes {
			// If the parent is mirrored and the user has not explicitly set no mirror, then the
			// Node list is actually a list of buddy groups. If the parent is not mirrored or the
			// user has set no mirror, these are meta node IDs instead.
			if parent.Entry.FeatureFlags.IsBuddyMirrored() && !userCfg.DirCfg.NoMirror {
				buddyGroup, err := mappings.MetaBuddyGroupToPrimaryNode.Get(entityID)
				if err != nil {
					return nil, fmt.Errorf("unable to look up specified buddy group: %w", err)
				}
				if buddyGroup.LegacyId.NodeType != beegfs.Meta {
					return nil, fmt.Errorf("requested buddy group %s is not a metadata buddy group", entityID)
				}
				request.PreferredNodes = append(request.PreferredNodes, uint16(buddyGroup.LegacyId.NumId))
			} else {
				node, err := nodeStore.GetNode(entityID)
				if err != nil {
					return nil, fmt.Errorf("unable to look up requested node in the node store: %w", err)
				}
				if node.Id.NodeType != beegfs.Meta {
					return nil, fmt.Errorf("requested node %s is not a metadata node", entityID)
				}
				request.PreferredNodes = append(request.PreferredNodes, uint16(node.Id.NumId))
			}
		}
	}
	return request, err
}

func CreateEntry(ctx context.Context, cfg CreateEntryCfg) ([]CreateEntryResult, error) {
	results := make([]CreateEntryResult, 0, len(cfg.Paths))
	if os.Geteuid() != 0 {
		return results, errors.New("only root may use this mode")
	}
	if len(cfg.Paths) == 0 {
		return results, fmt.Errorf("unable to create entry (no path specified)")
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return results, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
	}

	// Sort the slice so when creating multiple entries we can get the parent info and generate the
	// base request once for all files created in a specific directory.
	sort.StringSlice(cfg.Paths).Sort()

	// Initialize the BeeGFS client from the parent directory of the first path provided.
	client, err := config.BeeGFSClient(filepath.Dir(cfg.Paths[0]))
	if err != nil {
		if !errors.Is(err, filesystem.ErrUnmounted) {
			return results, err
		}
	}

	nodeStore, err := config.NodeStore(ctx)
	if err != nil {
		return results, err
	}

	// baseRequest is generated once for all files created in a particular directory.
	var baseRequest msg.SerializableMsg
	// currentParent is the path to the parent directory that baseRequest is valid for.
	// When the parent of the path we're handling does not match the currentParent,
	// we must get entry info for the new parent and regenerate the base request.
	var currentParent string
	var parentEntry *GetEntryCombinedInfo

	for i := range cfg.Paths {
		path, err := client.GetRelativePathWithinMount(cfg.Paths[i])
		if err != nil {
			return results, err
		}

		parent := filepath.Dir(path)
		if currentParent != parent {
			currentParent = parent
			parentEntry, err = GetEntry(ctx, mappings, GetEntriesCfg{Verbose: false, IncludeOrigMsg: true}, parent)
			if err != nil {
				return results, fmt.Errorf("unable to get entry info for parent path %s: %w", parent, err)
			}

			if cfg.FileCfg != nil {
				baseRequest, err = generateAndVerifyMakeFileReq(&cfg, parentEntry, mappings)
			} else if cfg.DirCfg != nil {
				baseRequest, err = generateAndVerifyMkDirRequest(&cfg, parentEntry, nodeStore, mappings)
			} else {
				return nil, errors.New("user request does not contain file or directory configuration (probably this is a bug)")
			}
			if err != nil {
				return results, fmt.Errorf("invalid configuration for path %s: %w", path, err)
			}
		}

		var resp msg.DeserializableMsg
		switch req := baseRequest.(type) {
		case *msg.MakeFileWithPatternRequest:
			req.NewFileName = []byte(filepath.Base(path))
			resp = &msg.MakeFileWithPatternResponse{}
		case *msg.MkDirRequest:
			req.NewDirName = []byte(filepath.Base(path))
			resp = &msg.MkDirResp{}
		default:
			return nil, fmt.Errorf("unknown request type (this is probably a bug): %t", baseRequest)
		}

		err = nodeStore.RequestTCP(ctx, parentEntry.Entry.MetaOwnerNode.Uid, baseRequest, resp)
		if err != nil {
			return results, err
		}

		switch resp := resp.(type) {
		case *msg.MakeFileWithPatternResponse:
			results = append(results, CreateEntryResult{
				Path:         path,
				Status:       resp.Result,
				RawEntryInfo: resp.EntryInfo,
			})
		case *msg.MkDirResp:
			results = append(results, CreateEntryResult{
				Path:         path,
				Status:       resp.Result,
				RawEntryInfo: resp.EntryInfo,
			})
		}
	}

	return results, nil
}
