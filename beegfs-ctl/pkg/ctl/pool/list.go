package pool

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
	// -1 means unlimited
	UserSpaceLimit *int64
	// -1 means unlimited
	UserInodeLimit *int64
	// -1 means unlimited
	GroupSpaceLimit *int64
	// -1 means unlimited
	GroupInodeLimit *int64
}

type GetStoragePools_Config struct {
	WithLimits bool
}

// Get the complete list of storage pools from the mananagement
func GetStoragePools(ctx context.Context, cfg GetStoragePools_Config) ([]GetStoragePools_Result, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	pools, err := mgmtd.GetPools(ctx, &pm.GetPoolsRequest{WithQuotaLimits: cfg.WithLimits})
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
			Pool:            pool,
			Targets:         targets,
			BuddyGroups:     buddy_groups,
			UserSpaceLimit:  p.UserSpaceLimit,
			UserInodeLimit:  p.UserInodeLimit,
			GroupSpaceLimit: p.GroupSpaceLimit,
			GroupInodeLimit: p.GroupInodeLimit,
		})
	}

	return res, nil
}
