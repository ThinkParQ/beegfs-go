package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
)

type list_Config struct {
	NodeType beegfs.NodeType
}

func newListCmd() *cobra.Command {
	cfg := list_Config{}

	cmd := &cobra.Command{
		Use:         "list",
		Short:       "List buddy groups",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg)
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"Filter by node type.")

	return cmd
}

func runListCmd(cmd *cobra.Command, cfg list_Config) error {
	groups, err := buddygroup.GetBuddyGroups(cmd.Context())
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return fmt.Errorf("no mirrors configured")
	}

	defaultColumns := []string{"alias", "id", "type", "primary_target", "primary_consistency", "secondary_target", "secondary_consistency"}
	allColumns := append([]string{"uid"}, defaultColumns...)

	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	}
	tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
	defer tbl.PrintRemaining()

	for _, t := range groups {
		if cfg.NodeType != beegfs.InvalidNodeType && t.NodeType != cfg.NodeType {
			continue
		}
		primaryTarget := ""
		secondaryTarget := ""
		if viper.GetBool(config.DebugKey) {
			primaryTarget = fmt.Sprintf("%v", t.PrimaryTarget)
			secondaryTarget = fmt.Sprintf("%v", t.SecondaryTarget)
		} else {
			primaryTarget = t.PrimaryTarget.LegacyId.String()
			secondaryTarget = t.SecondaryTarget.LegacyId.String()
		}
		tbl.AddItem(
			beegfs.Uid(t.BuddyGroup.Uid),
			t.BuddyGroup.Alias,
			t.BuddyGroup.LegacyId,
			t.BuddyGroup.LegacyId.NodeType,
			primaryTarget,
			t.PrimaryConsistencyState,
			secondaryTarget,
			t.SecondaryConsistencyState,
		)
	}
	return nil
}
