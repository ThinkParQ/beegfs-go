package resync

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	buddygroup "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

type StorageResyncStats_Result struct {
	State           string
	StartTime       int64
	EndTime         int64
	DiscoveredFiles uint64
	DiscoveredDirs  uint64
	MatchedFiles    uint64
	MatchedDirs     uint64
	SyncedFiles     uint64
	SyncedDirs      uint64
	ErrorFiles      uint64
	ErrorDirs       uint64
}

type MetaResyncStats_Result struct {
	State             string
	StartTime         int64
	EndTime           int64
	DiscoveredDirs    uint64
	GatherErrors      uint64
	SyncedDirs        uint64
	SyncedFiles       uint64
	ErrorDirs         uint64
	ErrorFiles        uint64
	SessionsToSync    uint64
	SyncedSessions    uint64
	SessionSyncErrors bool
	ModObjectsSynced  uint64
	ModSyncErrors     uint64
}

// Retrieves resync statistics for metadata targets.
func GetMetaResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (MetaResyncStats_Result, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return MetaResyncStats_Result{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return MetaResyncStats_Result{}, err
	}

	resp := msg.GetMetaResyncStatsResp{}
	err = store.RequestTCP(ctx, node.Uid, &msg.GetMetaResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp)
	if err != nil {
		return MetaResyncStats_Result{}, err
	}

	result := MetaResyncStats_Result{
		State:             resp.State.String(),
		StartTime:         resp.StartTime,
		EndTime:           resp.EndTime,
		DiscoveredDirs:    resp.DiscoveredDirs,
		GatherErrors:      resp.GatherErrors,
		SyncedDirs:        resp.SyncedDirs,
		SyncedFiles:       resp.SyncedFiles,
		ErrorDirs:         resp.ErrorDirs,
		ErrorFiles:        resp.ErrorFiles,
		SessionsToSync:    resp.SessionsToSync,
		SyncedSessions:    resp.SyncedSessions,
		SessionSyncErrors: resp.SessionSyncErrors != 0,
		ModObjectsSynced:  resp.ModObjectsSynced,
		ModSyncErrors:     resp.ModSyncErrors,
	}

	return result, err
}

// Retrieves resync statistics for storage targets.
func GetStorageResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (StorageResyncStats_Result, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return StorageResyncStats_Result{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return StorageResyncStats_Result{}, err
	}

	resp := msg.GetStorageResyncStatsResp{}
	err = store.RequestTCP(ctx, node.Uid, &msg.GetStorageResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp)
	if err != nil {
		return StorageResyncStats_Result{}, err
	}

	result := StorageResyncStats_Result{
		State:           resp.State.String(),
		StartTime:       resp.StartTime,
		EndTime:         resp.EndTime,
		DiscoveredFiles: resp.DiscoveredFiles,
		DiscoveredDirs:  resp.DiscoveredDirs,
		MatchedFiles:    resp.MatchedFiles,
		MatchedDirs:     resp.MatchedDirs,
		SyncedFiles:     resp.SyncedFiles,
		SyncedDirs:      resp.SyncedDirs,
		ErrorFiles:      resp.ErrorFiles,
		ErrorDirs:       resp.ErrorDirs,
	}

	return result, err
}

// Query the node for the given target
func getNode(ctx context.Context, pTarget beegfs.EntityIdSet) (beegfs.EntityIdSet, beegfs.EntityIdSet, error) {
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return beegfs.EntityIdSet{}, beegfs.EntityIdSet{}, err
		}
	}

	node, err := mappings.TargetToNode.Get(pTarget.Alias)
	if err != nil {
		return beegfs.EntityIdSet{}, beegfs.EntityIdSet{}, err
	}

	return node, pTarget, nil
}

// Query primary target for the buddy group
func GetPrimaryTarget(ctx context.Context, buddyGroup beegfs.EntityId) (beegfs.EntityIdSet, error) {
	groups, err := buddygroup.GetBuddyGroups(ctx)
	if err != nil {
		return beegfs.EntityIdSet{}, err
	}

	for _, g := range groups {
		switch buddyGroup.(type) {
		case beegfs.Uid:
			if buddyGroup == g.BuddyGroup.Uid {
				return g.PrimaryTarget, nil
			}
		case beegfs.Alias:
			if buddyGroup == g.BuddyGroup.Alias {
				return g.PrimaryTarget, nil
			}
		case beegfs.LegacyId:
			if buddyGroup.ToProto().LegacyId.NumId == g.BuddyGroup.LegacyId.ToProto().LegacyId.NumId &&
				buddyGroup.ToProto().LegacyId.NodeType == g.BuddyGroup.LegacyId.ToProto().LegacyId.NodeType {
				return g.PrimaryTarget, nil
			}
		default:
			return beegfs.EntityIdSet{}, fmt.Errorf("invalid entity ID")
		}
	}

	return beegfs.EntityIdSet{}, fmt.Errorf("buddy group %s not found", buddyGroup)
}
