package storagepool

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pool",
		Short: "Query and manage storage pools",
		Long:  "Contains subcommands related to storage pool management",
	}

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetAliasCmd())

	return cmd
}
