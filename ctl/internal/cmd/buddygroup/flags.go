package buddygroup

import (
	"fmt"
	"strings"

	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	pm "github.com/thinkparq/protobuf/go/management"
)

// Shared by the create and modify commands, which each append a sentence about their own default.
const quotaAccountingFlagHelp = "Set how the space and inodes used on the targets of this group are accounted " +
	"towards quota. 'primary' only accounts the usage on the primary target, ignoring potential unmirrored data on " +
	"the secondary. 'both' accounts the usage on both targets, meaning mirrored data is counted twice. Only " +
	"valid for storage buddy groups. Consult the documentation for more info."

// quotaAccountingFlag implements pflag.Value for the quota accounting mode of a buddy group. The
// target pointer is only allocated once the user provides the flag so callers can distinguish
// between "not specified" and an actual mode and leave the field unset in the request.
type quotaAccountingFlag struct {
	p **pm.BuddyGroupOptions_BuddyGroupQuotaAccounting
}

func newQuotaAccountingFlag(p **pm.BuddyGroupOptions_BuddyGroupQuotaAccounting) *quotaAccountingFlag {
	return &quotaAccountingFlag{p: p}
}

func (f *quotaAccountingFlag) String() string {
	if *f.p == nil {
		return ""
	}

	switch **f.p {
	case pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_PRIMARY:
		return backend.QuotaAccountingPrimary
	case pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_BOTH:
		return backend.QuotaAccountingBoth
	default:
		return fmt.Sprintf("Unknown(%d)", **f.p)
	}
}

func (f *quotaAccountingFlag) Type() string {
	return fmt.Sprintf("<%s|%s>", backend.QuotaAccountingPrimary, backend.QuotaAccountingBoth)
}

func (f *quotaAccountingFlag) Set(value string) error {
	var mode pm.BuddyGroupOptions_BuddyGroupQuotaAccounting

	switch strings.ToLower(strings.TrimSpace(value)) {
	case backend.QuotaAccountingPrimary:
		mode = pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_PRIMARY
	case backend.QuotaAccountingBoth:
		mode = pm.BuddyGroupOptions_BUDDY_GROUP_QUOTA_ACCOUNTING_BOTH
	default:
		return fmt.Errorf("invalid quota accounting mode '%s' - accepted are '%s', '%s'",
			value, backend.QuotaAccountingPrimary, backend.QuotaAccountingBoth)
	}

	*f.p = &mode

	return nil
}
