package cmd

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/node"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/config"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/util"
)

// Main entry point of the tool
func Execute() int {
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

	// Normalize flags to lowercase - makes the program accept case insensitive flags
	cmd.SetGlobalNormalizationFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		lowercaseFlagName := strings.ToLower(name)
		return pflag.NormalizedName(lowercaseFlagName)
	})

	// Initialize global config
	// Can be accessed at config.Config and passed to the ctl API
	config.Init(cmd)
	defer config.Cleanup()

	// Add subcommands
	cmd.AddCommand(versionCmd)
	cmd.AddCommand(node.NewNodeCmd())

	// Parse the given parameters and execute the selected command
	err := cmd.ExecuteContext(context.Background())

	if err != nil {
		// If the command returned a util.CtlError with an included exit code, use this to exit the
		// program
		ctlError, ok := err.(util.CtlError)
		if ok {
			return ctlError.GetExitCode()
		}

		return 1
	}

	return 0
}
