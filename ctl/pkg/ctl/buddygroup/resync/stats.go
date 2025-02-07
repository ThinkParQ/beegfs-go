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

// Retrieves resync statistics for metadata targets. Should be used with the primary target.
func GetMetaResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (msg.GetMetaResyncStatsResp, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return msg.GetMetaResyncStatsResp{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return msg.GetMetaResyncStatsResp{}, err
	}

	resp := msg.GetMetaResyncStatsResp{}
	err = store.RequestTCP(ctx, node.Uid, &msg.GetMetaResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp)
	if err != nil {
		return msg.GetMetaResyncStatsResp{}, err
	}

	return resp, err
}

// Retrieves resync statistics for storage targets. Should be used with the primary target.
func GetStorageResyncStats(ctx context.Context, pTarget beegfs.EntityIdSet) (msg.GetStorageResyncStatsResp, error) {
	node, target, err := getNode(ctx, pTarget)
	if err != nil {
		return msg.GetStorageResyncStatsResp{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return msg.GetStorageResyncStatsResp{}, err
	}

	resp := msg.GetStorageResyncStatsResp{}
	err = store.RequestTCP(ctx, node.Uid, &msg.GetStorageResyncStats{TargetID: uint16(target.LegacyId.NumId)}, &resp)
	if err != nil {
		return msg.GetStorageResyncStatsResp{}, err
	}

	return resp, err
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
