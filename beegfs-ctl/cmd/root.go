package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/node"
)

var rootCmd = &cobra.Command{
	Use:   "beegfs-ctl",
	Short: "The BeeGFS commandline control tool",
	Long: `The BeeGFS commandline control tool blablablabla ablablablabalablabl
	blabla`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Hello World")
	},
}

func init() {
	rootCmd.AddCommand(node.NodeCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
