package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
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
	// Empty for buddy groups that don't have a quota accounting mode (e.g. meta groups).
	QuotaAccounting string
}

// Defined as constants for reuse elsewhere, notably the --quota-accounting flag which accepts the
// same values that are printed here.
const (
	QuotaAccountingPrimary = "primary"
	QuotaAccountingBoth    = "both"
)

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
			primary_cs = target.ConsistencyGood
		case pb.ConsistencyState_NEEDS_RESYNC:
			primary_cs = target.ConsistencyNeedsResync
		case pb.ConsistencyState_BAD:
			primary_cs = target.ConsistencyBad
		}

		secondary_cs := ""
		switch t.SecondaryConsistencyState {
		case pb.ConsistencyState_GOOD:
			secondary_cs = target.ConsistencyGood
		case pb.ConsistencyState_NEEDS_RESYNC:
			secondary_cs = target.ConsistencyNeedsResync
		case pb.ConsistencyState_BAD:
			secondary_cs = target.ConsistencyBad
		}

		quota_accounting := ""
		switch t.GetOptions().GetQuotaAccounting() {
		case pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_PRIMARY:
			quota_accounting = QuotaAccountingPrimary
		case pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_BOTH:
			quota_accounting = QuotaAccountingBoth
		}

		res = append(res, GetBuddyGroups_Result{
			BuddyGroup:                bg,
			NodeType:                  bg.LegacyId.NodeType,
			PrimaryTarget:             primary,
			SecondaryTarget:           secondary,
			PrimaryConsistencyState:   primary_cs,
			SecondaryConsistencyState: secondary_cs,
			QuotaAccounting:           quota_accounting,
		})
	}

	return res, nil
}
