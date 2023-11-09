package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints the tools version",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO
		fmt.Println("TODO")
	},
}
