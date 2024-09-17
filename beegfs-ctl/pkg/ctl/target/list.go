package target

import (
	"context"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
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

// Defined as constants for reuse elsewhere, notably the health checks.
const (
	ReachabilityOnline          = "Online"
	ReachabilityProbablyOffline = "Probably offline"
	ReachabilityOffline         = "Offline"

	ConsistencyGood        = "Good"
	ConsistencyNeedsResync = "Needs resync"
	ConsistencyBad         = "Bad"

	CapacityNormal    = "Normal"
	CapacityLow       = "Low"
	CapacityEmergency = "Emergency"
)

// Get the complete list of targets from the mananagement
func GetTargets(ctx context.Context) ([]GetTargets_Result, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	targets, err := mgmtd.GetTargets(ctx, &pm.GetTargetsRequest{})
	if err != nil {
		return nil, err
	}

	res := make([]GetTargets_Result, 0, len(targets.Targets))
	for _, t := range targets.Targets {

		rbs := ""
		switch t.ReachabilityState {
		case pb.ReachabilityState_ONLINE:
			rbs = ReachabilityOnline
		case pb.ReachabilityState_POFFLINE:
			rbs = ReachabilityProbablyOffline
		case pb.ReachabilityState_OFFLINE:
			rbs = ReachabilityOffline
		}

		cs := ""
		switch t.ConsistencyState {
		case pb.ConsistencyState_GOOD:
			cs = ConsistencyGood
		case pb.ConsistencyState_NEEDS_RESYNC:
			cs = ConsistencyNeedsResync
		case pb.ConsistencyState_BAD:
			cs = ConsistencyBad
		}

		cp := ""
		switch t.CapPool {
		case pb.CapacityPool_NORMAL:
			cp = CapacityNormal
		case pb.CapacityPool_LOW:
			cp = CapacityLow
		case pb.CapacityPool_EMERGENCY:
			cp = CapacityEmergency
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
			NodeType:          beegfs.NodeTypeFromProto(t.NodeType),
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
