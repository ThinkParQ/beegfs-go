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
	"go.uber.org/zap"
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
	SessionSyncErrors uint8
	ModObjectsSynced  uint64
	ModSyncErrors     uint64
}

func ResyncStats(ctx context.Context, buddyGroup beegfs.Alias, primary bool, nt beegfs.NodeType) error {
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", "storageBench"))

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return err
		}
		log.Debug("remote storage target mappings are not available (ignoring)", zap.Any("error", err))
	}

	p, s, err := findTarget(ctx, buddyGroup, nt)
	if err != nil {
		return err
	}

	var srcTarget beegfs.EntityIdSet
	if primary {
		srcTarget = s
	} else {
		srcTarget = p
	}

	node, err := mappings.TargetToNode.Get(srcTarget.Alias)
	if err != nil {
		return err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	switch nt {
	case beegfs.Meta:
		resp := msg.GetMetaResyncStatsResp{}
		err = store.RequestTCP(ctx, node.Uid, &msg.GetMetaResyncStats{TargetID: uint16(srcTarget.LegacyId.NumId)}, &resp)
		if err != nil {
			return err
		}
		fmt.Println(resp)
	case beegfs.Storage:
		resp := msg.GetStorageResyncStatsResp{}
		err = store.RequestTCP(ctx, node.Uid, &msg.GetStorageResyncStats{TargetID: uint16(srcTarget.LegacyId.NumId)}, &resp)
		if err != nil {
			return err
		}
		fmt.Println(resp)
	default:
		fmt.Errorf("invalid node-type %s, only meta and storage are supported", nt)
	}

	return nil
}

// Find primary and secondary targets from the buddy group alias
func findTarget(ctx context.Context, buddyGroup beegfs.Alias, nt beegfs.NodeType) (beegfs.EntityIdSet, beegfs.EntityIdSet, error) {
	groups, err := buddygroup.GetBuddyGroups(ctx)
	if err != nil {
		return beegfs.EntityIdSet{}, beegfs.EntityIdSet{}, err
	}

	for _, g := range groups {
		if buddyGroup == g.BuddyGroup.Alias && g.NodeType == nt {
			return g.PrimaryTarget, g.SecondaryTarget, nil
		}
	}

	return beegfs.EntityIdSet{}, beegfs.EntityIdSet{}, fmt.Errorf("buddy group %s not found", buddyGroup)

}
