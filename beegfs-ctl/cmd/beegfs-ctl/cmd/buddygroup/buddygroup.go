package buddygroup

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mirror",
		Short: "Query and manage mirroring and buddy groups",
		Long:  "Contains subcommands related to mirroring and buddy group management",
	}

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetAliasCmd())

	return cmd
}
