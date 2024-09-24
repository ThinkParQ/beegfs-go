package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type GetBuddyGroups_Result struct {
	BuddyGroup                beegfs.EntityIdSet
	NodeType                  beegfs.NodeType
	PrimaryTarget             beegfs.EntityIdSet
	SecondaryTarget           beegfs.EntityIdSet
	PrimaryConsistencyState   string
	SecondaryConsistencyState string
}

// Get the complete list of buddy groups from the mananagement
func GetBuddyGroups(ctx context.Context) ([]GetBuddyGroups_Result, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	groups, err := mgmtd.GetBuddyGroups(ctx, &pm.GetBuddyGroupsRequest{})
	if err != nil {
		return nil, err
	}

	res := make([]GetBuddyGroups_Result, 0, len(groups.BuddyGroups))
	for _, t := range groups.BuddyGroups {
		bg, err := beegfs.EntityIdSetFromProto(t.Id)
		if err != nil {
			return nil, err
		}

		primary, err := beegfs.EntityIdSetFromProto(t.PrimaryTarget)
		if err != nil {
			return nil, err
		}

		secondary, err := beegfs.EntityIdSetFromProto(t.SecondaryTarget)
		if err != nil {
			return nil, err
		}

		primary_cs := ""
		switch t.PrimaryConsistencyState {
		case pb.ConsistencyState_GOOD:
			primary_cs = "Good"
		case pb.ConsistencyState_NEEDS_RESYNC:
			primary_cs = "Needs resync"
		case pb.ConsistencyState_BAD:
			primary_cs = "Bad"
		}

		secondary_cs := ""
		switch t.SecondaryConsistencyState {
		case pb.ConsistencyState_GOOD:
			secondary_cs = "Good"
		case pb.ConsistencyState_NEEDS_RESYNC:
			secondary_cs = "Needs resync"
		case pb.ConsistencyState_BAD:
			secondary_cs = "Bad"
		}

		res = append(res, GetBuddyGroups_Result{
			BuddyGroup:                bg,
			NodeType:                  bg.LegacyId.NodeType,
			PrimaryTarget:             primary,
			SecondaryTarget:           secondary,
			PrimaryConsistencyState:   primary_cs,
			SecondaryConsistencyState: secondary_cs,
		})
	}

	return res, nil
}
