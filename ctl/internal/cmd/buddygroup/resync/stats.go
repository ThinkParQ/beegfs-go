package resync

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

type resycStats_config struct {
	buddyGroup beegfs.Alias
	nodeType   beegfs.NodeType
	primary    bool
}

func newResycStatsCmd() *cobra.Command {
	cfg := resycStats_config{}

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Resync stats",
		Long:  "Resync stats",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			alias, err := beegfs.AliasFromString(args[0])
			if err != nil {
				return err
			}
			cfg.buddyGroup = alias

			return runResyncStatsCmd(cmd, &cfg)
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.nodeType, beegfs.Meta, beegfs.Storage),
		"node-type", "Node type of the new buddy group")
	cmd.MarkFlagRequired("node-type")
	cmd.Flags().BoolVarP(&cfg.primary, "primary", "p", false, "Print resync stats for primary target")

	return cmd
}

func runResyncStatsCmd(cmd *cobra.Command, cfg *resycStats_config) error {
	err := backend.ResyncStats(cmd.Context(), cfg.buddyGroup, cfg.primary, cfg.nodeType)

	if err != nil {
		return err
	}

	return nil
}
