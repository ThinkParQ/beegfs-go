package pool

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pool",
		Short: "Query and manage storage pools",
		Long:  "Contains subcommands related to storage pool management",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetAliasCmd())
	cmd.AddCommand(newCreatePoolCmd())
	cmd.AddCommand(newAssignPoolCmd())
	cmd.AddCommand(newDeletePoolCmd())

	return cmd
}
