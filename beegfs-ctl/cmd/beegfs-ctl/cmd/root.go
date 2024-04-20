package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/buddygroup"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/entry"
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
	// This is the first line of the root help message. This is generated/stored here to allow the
	// number of characters separating the header with the rest of the help text to be determined
	// dynamically since the version width may vary.
	longHelpHeader := fmt.Sprintf("BeeGFS Command Line Tool: %s", Version)
	// The root command.
	cmd := &cobra.Command{
		Use:   BinaryName,
		Short: "The BeeGFS command line control tool.",
		Long: fmt.Sprintf(`%s
%s
This tool allows you to inspect, configure, and monitor BeeGFS.

* View help for specific commands with "<command> help".
* For full product documentation, visit: https://doc.beegfs.io/.
* Questions?
  - If you have an active support contract, please visit: https://www.beegfs.io/c/enterprise/.
  - For community support, check out the mailing list at: https://groups.google.com/g/fhgfs-user/.

BeeGFS is crafted with 💛 by contributors worldwide.
Thank you for using BeeGFS and supporting its ongoing development! 🐝
		`, longHelpHeader, strings.Repeat("=", len(longHelpHeader))),
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
	cmd.AddCommand(entry.NewEntryCmd())

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
