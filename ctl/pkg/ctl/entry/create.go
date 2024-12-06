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
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

type CreateFileCfg struct {
	Paths []string
	Force bool
	// Fields that must always be specified by the user:
	Permissions *int32
	UserID      *uint32
	GroupID     *uint32
	// Fields that may be inherited from the parent directory if not explicitly set:
	StripePattern      *beegfs.StripePatternType
	Chunksize          *uint32
	DefaultNumTargets  *uint32
	Pool               *beegfs.EntityId
	TargetIDs          []beegfs.EntityId
	RemoteTargets      []uint32
	RemoteCooldownSecs *uint16
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
func generateAndVerifyMakeFileReq(userCfg *CreateFileCfg, parent *GetEntryCombinedInfo, mappings *util.Mappings) (*msg.MakeFileWithPatternRequest, error) {

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
		// The mode must contain the "file" flag (otherwise you will get a vague internal error).
		Mode:       *userCfg.Permissions | syscall.S_IFREG,
		Umask:      0000,
		ParentInfo: *parent.Entry.origEntryInfoMsg,
		// Start with the parent pattern and RST config.
		Pattern: parent.Entry.Pattern.StripePattern,
		RST:     parent.Entry.Remote.RemoteStorageTarget,
		// NewFileName must be set by the caller.
	}

	// Specific checks are performed based on the pattern, set that first:
	if userCfg.StripePattern != nil {
		request.Pattern.Type = *userCfg.StripePattern
	}

	// Set the chunksize if requested:
	if userCfg.Chunksize != nil {
		request.Pattern.Chunksize = *userCfg.Chunksize
	}

	// Set the default number of targets if requested:
	if userCfg.DefaultNumTargets != nil {
		request.Pattern.DefaultNumTargets = *userCfg.DefaultNumTargets
	}

	// Set the pool if requested:
	var storagePool pool.GetStoragePools_Result
	if userCfg.Pool != nil {
		storagePool, err = mappings.StoragePoolToConfig.Get(*userCfg.Pool)
		if err != nil {
			return nil, fmt.Errorf("error looking up the specified pool %s: %w", *userCfg.Pool, err)
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
			return nil, fmt.Errorf("error looking up pool for parent entry %s: %w", *userCfg.Pool, err)
		}
		// No need to do anything else, the parent pool is already set above on the request.
	}

	// Set specific targets/buddy groups if requested:
	if len(userCfg.TargetIDs) > 0 {
		if userCfg.DefaultNumTargets != nil && int(*userCfg.DefaultNumTargets) > len(userCfg.TargetIDs) {
			return nil, fmt.Errorf("requested number of targets is greater than the number of targets/buddy groups that were specified")
		}
		// Make sure the targets or buddy groups specified by the user are actually valid and
		// compatible with the configured stripe pattern:
		request.Pattern.TargetIDs, err = checkAndGetTargets(userCfg.Force, mappings, storagePool, request.Pattern.Type, userCfg.TargetIDs)
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
	if len(userCfg.RemoteTargets) > 0 {
		request.RST.RSTIDs = userCfg.RemoteTargets
	}
	if userCfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *userCfg.RemoteCooldownSecs
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

func CreateFile(ctx context.Context, cfg CreateFileCfg) ([]CreateEntryResult, error) {
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
	var baseRequest *msg.MakeFileWithPatternRequest
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
			baseRequest, err = generateAndVerifyMakeFileReq(&cfg, parentEntry, mappings)
			if err != nil {
				return results, fmt.Errorf("invalid configuration for path %s: %w", path, err)
			}
		}
		if baseRequest == nil {
			return results, fmt.Errorf("base request is unexpectedly nil (this is probably a bug)")
		}
		if parentEntry == nil {
			return results, fmt.Errorf("parent entry is unexpectedly nil (this is probably a bug)")
		}

		baseRequest.NewFileName = []byte(filepath.Base(path))
		var resp = &msg.MakeFileWithPatternResponse{}
		err = nodeStore.RequestTCP(ctx, parentEntry.Entry.MetaOwnerNode.Uid, baseRequest, resp)
		if err != nil {
			return results, err
		}

		results = append(results, CreateEntryResult{
			Path:         path,
			Status:       resp.Result,
			RawEntryInfo: resp.EntryInfo,
		})
	}

	return results, nil
}
