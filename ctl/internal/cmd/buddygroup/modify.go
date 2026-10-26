package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	pm "github.com/thinkparq/protobuf/go/management"
)

type modifyBuddyGroup_Config struct {
	group           beegfs.EntityId
	quotaAccounting *pm.BuddyGroupOptions_BuddyGroupQuotaAccounting
}

func newModifyBuddyGroupCmd() *cobra.Command {
	cfg := modifyBuddyGroup_Config{group: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "modify <group>",
		Short: "Modify an existing buddy group",
		Long:  `Modify an existing buddy group. At least one of the optional flags must be given.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			group, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}
			cfg.group = group

			if cfg.quotaAccounting == nil {
				return fmt.Errorf("no modification requested - specify at least one flag")
			}

			return runModifyBuddyGroupCmd(cmd, cfg)
		},
	}

	cmd.Flags().Var(newQuotaAccountingFlag(&cfg.quotaAccounting), "quota-accounting",
		quotaAccountingFlagHelp+" Left unchanged if unspecified.")

	return cmd
}

func runModifyBuddyGroupCmd(cmd *cobra.Command, cfg modifyBuddyGroup_Config) error {
	_, err := backend.Modify(cmd.Context(), &pm.ModifyBuddyGroupRequest{
		Group: cfg.group.ToProto(),
		Options: &pm.BuddyGroupOptions{
			QuotaAccounting: cfg.quotaAccounting,
		},
	})
	if err != nil {
		return err
	}

	cmdfmt.Printf("Buddy group modified: %s\n", cfg.group)

	return nil
}
