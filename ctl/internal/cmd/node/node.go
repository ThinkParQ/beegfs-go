package node

import (
	"github.com/spf13/cobra"
)

// Creates new "node" command
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Query and manage nodes",
		Long:  "Contains commands related to node management.",
	}

	// Add all the subcommands. If they are actual commands doing work, they should be placed in this same package / in the same folder
	// along this file. If they are only containing further subcommands, they should go into their
	// own subpackage (as this package is a subpackage of the root cmd package).

	// This is the recommended structure by the cobra user guide:
	// https://github.com/spf13/cobra/blob/main/site/content/user_guide.md

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetAliasCmd())
	cmd.AddCommand(newDeleteCmd())

	return cmd
}
