package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/node"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/config"
)

// Main entry point of the tool
func Execute() {
	// The root command
	cmd := &cobra.Command{
		Use:   "beegfs-ctl",
		Short: "The BeeGFS command line control tool",
		Long: `The BeeGFS command line control tool

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`,
		SilenceUsage: true,
	}

	// Initialize global config
	// Can be accessed at config.Config and passed to the ctl API
	config.Init(cmd)

	// Add subcommands
	cmd.AddCommand(versionCmd)
	cmd.AddCommand(node.NewNodeCmd())

	// Parse the given parameters and execute the selected command
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		os.Exit(1)
	}
}
