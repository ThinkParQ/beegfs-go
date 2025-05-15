package stats

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Query and monitor IO statistics",
		Long:  `Display IO statistics for BeeGFS servers, clients and users.`,
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(newServerStatsCmd())
	cmd.AddCommand(newClientStatsCmd())
	cmd.AddCommand(newUserStatsCmd())

	return cmd
}
