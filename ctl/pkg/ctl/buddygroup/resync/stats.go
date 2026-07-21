package resync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	buddygroup "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

// MetaResyncStats is the projection of a metadata target's resync statistics. It is the type
// returned to callers so they render display/JSON output without touching the BeeMsg wire type
// (msg.GetMetaResyncStatsResp) or repeating conversions such as timestamp formatting.
type MetaResyncStats struct {
	TargetType               string `json:"targetType"`
	State                    string `json:"state"`
	StartTime                string `json:"startTime,omitempty"`
	EndTime                  string `json:"endTime,omitempty"`
	DiscoveredDirs           uint64 `json:"discoveredDirs"`
	DiscoveryErrors          uint64 `json:"discoveryErrors"`
	SyncedDirs               uint64 `json:"syncedDirs"`
	SyncedFiles              uint64 `json:"syncedFiles"`
	ErrorDirs                uint64 `json:"errorDirs"`
	ErrorFiles               uint64 `json:"errorFiles"`
	SessionsToSync           uint64 `json:"sessionsToSync"`
	SyncedSessions           uint64 `json:"syncedSessions"`
	SessionSyncErrors        bool   `json:"sessionSyncErrors"`
	ModifiedObjectsSynced    uint64 `json:"modifiedObjectsSynced"`
	ModifiedObjectSyncErrors uint64 `json:"modifiedObjectSyncErrors"`
}

func newMetaResyncStats(r msg.GetMetaResyncStatsResp) MetaResyncStats {
	return MetaResyncStats{
		TargetType:               "meta",
		State:                    r.State.String(),
		StartTime:                resyncTime(r.StartTime),
		EndTime:                  resyncTime(r.EndTime),
		DiscoveredDirs:           r.DiscoveredDirs,
		DiscoveryErrors:          r.GatherErrors,
		SyncedDirs:               r.SyncedDirs,
		SyncedFiles:              r.SyncedFiles,
		ErrorDirs:                r.ErrorDirs,
		ErrorFiles:               r.ErrorFiles,
		SessionsToSync:           r.SessionsToSync,
		SyncedSessions:           r.SyncedSessions,
		SessionSyncErrors:        r.SessionSyncErrors != 0,
		ModifiedObjectsSynced:    r.ModObjectsSynced,
		ModifiedObjectSyncErrors: r.ModSyncErrors,
	}
}

// StorageResyncStats is the projection of a storage target's resync statistics (see MetaResyncStats).
type StorageResyncStats struct {
	TargetType      string `json:"targetType"`
	State           string `json:"state"`
	StartTime       string `json:"startTime,omitempty"`
	EndTime         string `json:"endTime,omitempty"`
	DiscoveredFiles uint64 `json:"discoveredFiles"`
	DiscoveredDirs  uint64 `json:"discoveredDirs"`
	MatchedFiles    uint64 `json:"matchedFiles"`
	MatchedDirs     uint64 `json:"matchedDirs"`
	SyncedFiles     uint64 `json:"syncedFiles"`
	SyncedDirs      uint64 `json:"syncedDirs"`
	ErrorFiles      uint64 `json:"errorFiles"`
	ErrorDirs       uint64 `json:"errorDirs"`
}

func newStorageResyncStats(r msg.GetStorageResyncStatsResp) StorageResyncStats {
	return StorageResyncStats{
		TargetType:      "storage",
		State:           r.State.String(),
		StartTime:       resyncTime(r.StartTime),
		EndTime:         resyncTime(r.EndTime),
		DiscoveredFiles: r.DiscoveredFiles,
		DiscoveredDirs:  r.DiscoveredDirs,
		MatchedFiles:    r.MatchedFiles,
		MatchedDirs:     r.MatchedDirs,
		SyncedFiles:     r.SyncedFiles,
		SyncedDirs:      r.SyncedDirs,
		ErrorFiles:      r.ErrorFiles,
		ErrorDirs:       r.ErrorDirs,
	}
}

// resyncTime formats a resync unix timestamp as RFC3339, returning "" for a zero (unset) time so the
// field is omitted from JSON and skipped in the human report.
func resyncTime(unix int64) string {
	if unix == 0 {
		return ""
	}
	return time.Unix(unix, 0).Format(time.RFC3339)
}

// GetMetaResyncStats retrieves resync statistics for a metadata target. Should be used with the
// primary target.
func GetMetaResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (MetaResyncStats, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return MetaResyncStats{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return MetaResyncStats{}, err
	}

	resp := msg.GetMetaResyncStatsResp{}
	if err := store.RequestTCP(ctx, node.Uid, &msg.GetMetaResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp); err != nil {
		return MetaResyncStats{}, err
	}

	return newMetaResyncStats(resp), nil
}

// GetStorageResyncStats retrieves resync statistics for a storage target. Should be used with the
// primary target.
func GetStorageResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (StorageResyncStats, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return StorageResyncStats{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return StorageResyncStats{}, err
	}

	resp := msg.GetStorageResyncStatsResp{}
	if err := store.RequestTCP(ctx, node.Uid, &msg.GetStorageResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp); err != nil {
		return StorageResyncStats{}, err
	}

	return newStorageResyncStats(resp), nil
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
