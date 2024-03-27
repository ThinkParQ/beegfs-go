package stats

import (
	"github.com/spf13/cobra"
)

func NewStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Stats",
		Long:  "Get stats",
	}

	cmd.AddCommand(newServerStatsCmd())

	return cmd
}
