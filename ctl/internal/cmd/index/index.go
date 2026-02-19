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
	cmd.PersistentFlags().StringVar(&indexAddr, "index-addr", indexAddrDefault, "Index backend address. Use \"local\" or \"ssh:<host>[:port]\".")
	cmd.PersistentFlags().StringVar(&indexRoot, "index-root", "", "Override index root path (absolute). Used to resolve index paths locally or with --index-addr=ssh.")

	cmd.AddCommand(newCreateCmd())
	cmd.AddCommand(newFindCmd())
	cmd.AddCommand(newLsCmd())
	cmd.AddCommand(newStatCmd())
	cmd.AddCommand(newStatsCmd())
	cmd.AddCommand(newQueryCmd())
	cmd.AddCommand(newRescanCmd())
	//cmd.AddCommand(newUpgradeCmd()) Shall not be included in V8 Release

	return cmd
}
