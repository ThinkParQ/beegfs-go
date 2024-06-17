package target

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "target",
		Short: "Query and manage targets.",
		Long:  "Contains commands related to target management.",
	}

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetAliasCmd())
	cmd.AddCommand(newDeleteCmd())

	return cmd
}
