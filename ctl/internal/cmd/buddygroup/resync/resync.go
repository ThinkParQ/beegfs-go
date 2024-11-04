package resync

import (
	"github.com/spf13/cobra"
)

func NewResyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resync",
		Short: "Manage and query resync of a  target from its buddy.",
		Long:  "Manage and query resync of a  target from its buddy.",
	}

	cmd.AddCommand(newResyncStatsCmd())
	cmd.AddCommand(newStartCmd())
	cmd.AddCommand(newRestartCmd())

	return cmd
}
