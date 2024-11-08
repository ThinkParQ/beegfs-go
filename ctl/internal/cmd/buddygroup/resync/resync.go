package resync

import (
	"github.com/spf13/cobra"
)

func NewResyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resync",
		Short: "Manage and query resyncing buddy mirrors",
		Long:  "Manage and query resyncing buddy mirrors",
	}

	cmd.AddCommand(newResyncStatsCmd())
	cmd.AddCommand(newStartResyncCmd())
	cmd.AddCommand(newRestartCmd())

	return cmd
}
