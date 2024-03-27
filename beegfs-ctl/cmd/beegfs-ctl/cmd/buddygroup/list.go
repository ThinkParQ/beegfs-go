package buddygroup

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/gobee/beegfs"
)

type list_Config struct {
	NodeType beegfs.NodeType
}

func newListCmd() *cobra.Command {
	cfg := list_Config{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List buddy groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg)
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "nodeType",
		"Filter by node type")

	return cmd
}

func runListCmd(cmd *cobra.Command, cfg list_Config) error {
	groups, err := buddygroup.GetBuddyGroups(cmd.Context())
	if err != nil {
		return err
	}

	w := cmdfmt.NewTableWriter(os.Stdout)
	defer w.Flush()

	if config.Get().Debug {
		fmt.Fprint(&w, "UID\t")
	}
	fmt.Fprint(&w, "Alias\tID\tPrimary\tSecondary\t")

	fmt.Fprintln(&w)

	for _, t := range groups {
		if cfg.NodeType != beegfs.InvalidNodeType && t.NodeType != cfg.NodeType {
			continue
		}

		if config.Get().Debug {
			fmt.Fprintf(&w, "%d\t", beegfs.Uid(t.BuddyGroup.Uid))
		}

		fmt.Fprintf(&w, "%s\t%s\t", t.BuddyGroup.Alias, t.BuddyGroup.LegacyId)

		if config.Get().Debug {
			fmt.Fprintf(&w, "%v (%s)\t", t.PrimaryTarget, t.PrimaryConsistencyState)
			fmt.Fprintf(&w, "%v (%s)\t", t.SecondaryTarget, t.SecondaryConsistencyState)
		} else {
			fmt.Fprintf(&w, "%s (%s)\t", t.PrimaryTarget.Alias, t.PrimaryConsistencyState)
			fmt.Fprintf(&w, "%s (%s)\t", t.SecondaryTarget.Alias, t.SecondaryConsistencyState)
		}

		fmt.Fprintln(&w)
	}

	return nil
}
