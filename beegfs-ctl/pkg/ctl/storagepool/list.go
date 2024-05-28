package storagepool

import (
	"context"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/gobee/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type GetStoragePools_Result struct {
	Pool        beegfs.EntityIdSet
	Targets     []beegfs.EntityIdSet
	BuddyGroups []beegfs.EntityIdSet
}

// Get the complete list of storage pools from the mananagement
func GetStoragePools(ctx context.Context) ([]GetStoragePools_Result, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	pools, err := mgmtd.GetStoragePools(ctx, &pm.GetStoragePoolsRequest{})
	if err != nil {
		return nil, err
	}

	res := make([]GetStoragePools_Result, 0, len(pools.Pools))
	for _, p := range pools.Pools {
		pool, err := beegfs.EntityIdSetFromProto(p.Id)
		if err != nil {
			return nil, err
		}

		targets := make([]beegfs.EntityIdSet, 0, len(p.Targets))
		for _, t := range p.Targets {
			target, err := beegfs.EntityIdSetFromProto(t)
			if err != nil {
				return nil, err
			}

			targets = append(targets, target)
		}

		buddy_groups := make([]beegfs.EntityIdSet, 0, len(p.BuddyGroups))
		for _, t := range p.BuddyGroups {
			bg, err := beegfs.EntityIdSetFromProto(t)
			if err != nil {
				return nil, err
			}

			buddy_groups = append(buddy_groups, bg)
		}

		res = append(res, GetStoragePools_Result{
			Pool:        pool,
			Targets:     targets,
			BuddyGroups: buddy_groups,
		})
	}

	return res, nil
}
