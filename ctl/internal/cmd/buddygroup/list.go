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

	defaultColumns := []string{"alias", "id", "primary", "secondary"}
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
			primaryTarget = fmt.Sprintf("%v (%s)", t.PrimaryTarget, t.PrimaryConsistencyState)
			secondaryTarget = fmt.Sprintf("%v (%s)", t.SecondaryTarget, t.SecondaryConsistencyState)
		} else {
			primaryTarget = fmt.Sprintf("%s (%s)", t.PrimaryTarget.Alias, t.PrimaryConsistencyState)
			secondaryTarget = fmt.Sprintf("%s (%s)", t.SecondaryTarget.Alias, t.SecondaryConsistencyState)
		}
		tbl.AddItem(
			beegfs.Uid(t.BuddyGroup.Uid),
			t.BuddyGroup.Alias,
			t.BuddyGroup.LegacyId,
			primaryTarget,
			secondaryTarget,
		)
	}
	return nil
}
