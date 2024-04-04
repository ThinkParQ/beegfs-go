package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/buddygroup"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/node"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/rst"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/stats"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/storagepool"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/target"
	cmdConfig "github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/config"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/util"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
)

// Main entry point of the tool
func Execute() int {
	// The root command
	cmd := &cobra.Command{
		Use:   "beegfs-ctl",
		Short: "The BeeGFS command line control tool",
		Long: `The BeeGFS command line control tool (http://www.beegfs.com)
		`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if config.Get().NumWorkers < 1 {
				return fmt.Errorf("the number of workers must be at least 1")
			}
			return nil
		},
	}

	// Normalize flags to lowercase - makes the program accept case insensitive flags
	cmd.SetGlobalNormalizationFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		lowercaseFlagName := strings.ToLower(name)
		return pflag.NormalizedName(lowercaseFlagName)
	})

	// Initialize global config
	// Can be accessed at config.Config and passed to the ctl API
	cmdConfig.Init(cmd)
	defer cmdConfig.Cleanup()

	// Add subcommands
	cmd.AddCommand(versionCmd)
	cmd.AddCommand(node.NewCmd())
	cmd.AddCommand(target.NewCmd())
	cmd.AddCommand(storagepool.NewCmd())
	cmd.AddCommand(buddygroup.NewCmd())
	cmd.AddCommand(stats.NewCmd())
	cmd.AddCommand(rst.NewRSTCmd())

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
