package node

import (
	"fmt"

	"github.com/spf13/cobra"
)

var NodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Query and manage nodes",
	Long:  "Contains subcommands related to node management",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Hello World")
	},
}

func init() {
	NodeCmd.AddCommand(listCmd)
}
