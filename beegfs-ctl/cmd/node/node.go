package node

import (
	"github.com/spf13/cobra"
)

var NodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Query and manage nodes",
	Long:  "Contains subcommands related to node management",
}

func init() {
	NodeCmd.AddCommand(listCmd)
	NodeCmd.AddCommand(infoCmd)
}
