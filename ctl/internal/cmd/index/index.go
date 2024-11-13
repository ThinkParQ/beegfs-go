package index

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Create/update and run operations on the the file system index",
		Long:  "Create/update and run different metadata operations against the the file system index.",
	}

	cmd.AddCommand(newCreateCmd())
	cmd.AddCommand(newUpdateCmd())
	cmd.AddCommand(newFindCmd())
	cmd.AddCommand(newLsCmd())
	cmd.AddCommand(newStatCmd())
	cmd.AddCommand(newStatsCmd())
	cmd.AddCommand(newQueryCmd())
	cmd.AddCommand(newUpgradeCmd())

	return cmd
}
