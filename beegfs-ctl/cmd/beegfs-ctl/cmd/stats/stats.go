package stats

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Stats",
		Long:  "Get stats",
	}

	cmd.AddCommand(newServerStatsCmd())

	return cmd
}
