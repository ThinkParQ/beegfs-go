package target

import (
	"context"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/gobee/beegfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
)

type GetTargets_Result struct {
	Target            beegfs.EntityIdSet
	NodeType          beegfs.NodeType
	Node              beegfs.EntityIdSet
	StoragePool       *beegfs.EntityIdSet
	ReachabilityState string
	ConsistencyState  string
	LastContactS      *uint64
	FreeSpaceBytes    *uint64
	FreeInodes        *uint64
	TotalSpaceBytes   *uint64
	TotalInodes       *uint64
	CapacityPool      string
}

// Get the complete list of targets from the mananagement
func GetTargets(ctx context.Context) ([]GetTargets_Result, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	targets, err := mgmtd.GetTargets(ctx, &pb.GetTargetsRequest{})
	if err != nil {
		return nil, err
	}

	res := make([]GetTargets_Result, 0, len(targets.Targets))
	for _, t := range targets.Targets {

		rbs := ""
		switch t.ReachabilityState {
		case pb.ReachabilityState_ONLINE:
			rbs = "Online"
		case pb.ReachabilityState_POFFLINE:
			rbs = "Probably offline"
		case pb.ReachabilityState_OFFLINE:
			rbs = "Offline"
		}

		cs := ""
		switch t.ConsistencyState {
		case pb.ConsistencyState_GOOD:
			cs = "Good"
		case pb.ConsistencyState_NEEDS_RESYNC:
			cs = "Needs resync"
		case pb.ConsistencyState_BAD:
			cs = "Bad"
		}

		cp := ""
		switch t.CapPool {
		case pb.CapacityPool_NORMAL:
			cp = "Normal"
		case pb.CapacityPool_LOW:
			cp = "Low"
		case pb.CapacityPool_EMERGENCY:
			cp = "Emergency"
		}

		target, err := beegfs.EntityIdSetFromProto(t.Id)
		if err != nil {
			return nil, err
		}

		node, err := beegfs.EntityIdSetFromProto(t.Node)
		if err != nil {
			return nil, err
		}

		r := GetTargets_Result{
			Target:            target,
			Node:              node,
			ReachabilityState: rbs,
			ConsistencyState:  cs,
			LastContactS:      t.LastContactS,
			FreeSpaceBytes:    t.FreeSpaceBytes,
			FreeInodes:        t.FreeInodes,
			TotalSpaceBytes:   t.TotalSpaceBytes,
			TotalInodes:       t.TotalInodes,
			CapacityPool:      cp,
		}

		if t.StoragePool != nil {
			pool, err := beegfs.EntityIdSetFromProto(t.StoragePool)
			if err != nil {
				return nil, err
			}

			r.StoragePool = &pool
		}

		res = append(res, r)
	}

	return res, nil
}
