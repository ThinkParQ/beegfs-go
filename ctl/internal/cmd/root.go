package cmd

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/benchmark"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/copy"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/entry"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/health"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/index"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/license"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/node"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/pool"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/quota"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/stats"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/target"
	cmdConfig "github.com/thinkparq/beegfs-go/ctl/internal/config"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
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
		Short: "The BeeGFS command line control tool",
		Long: fmt.Sprintf(`%s
%s
This tool allows you to inspect, configure, and monitor BeeGFS.

* View help for specific commands with "<command> help".
* For full product documentation, visit: https://doc.beegfs.io/.
* Questions?
  - If you have an active support contract, please visit: https://www.beegfs.io/c/enterprise/
  - For community support, check out: https://github.com/ThinkParQ/beegfs/blob/master/SUPPORT.md

BeeGFS is crafted with 💛 by contributors worldwide.
Thank you for using BeeGFS and supporting its ongoing development! 🐝
		`, longHelpHeader, strings.Repeat("=", len(longHelpHeader))),
		SilenceUsage: true,
		// PersistentPreRunE: // Do not set here, see attachPersistentPreRunE() to modify instead.
	}

	// Normalize flags to lowercase - makes the program accept case insensitive flags
	cmd.SetGlobalNormalizationFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		lowercaseFlagName := strings.ToLower(name)
		return pflag.NormalizedName(lowercaseFlagName)
	})

	// Initialize global config
	// Can be accessed at config.Config and passed to the ctl API
	cmdConfig.InitGlobalFlags(cmd)
	defer cmdConfig.Cleanup()

	// Add subcommands
	cmd.AddCommand(versionCmd)
	cmd.AddCommand(license.NewCmd())
	cmd.AddCommand(node.NewCmd())
	cmd.AddCommand(target.NewCmd())
	cmd.AddCommand(pool.NewCmd())
	cmd.AddCommand(buddygroup.NewCmd())
	cmd.AddCommand(stats.NewCmd())
	cmd.AddCommand(rst.NewRSTCmd())
	cmd.AddCommand(entry.NewEntryCmd())
	cmd.AddCommand(quota.NewCmd())
	cmd.AddCommand(health.NewHealthCmd())
	cmd.AddCommand(benchmark.NewBenchmarkCmd())
	cmd.AddCommand(index.NewCmd())
	cmd.AddCommand(copy.NewCopyCmd())

	// This must run AFTER all commands are added.
	for _, child := range cmd.Commands() {
		attachPersistentPreRunE(child)
	}

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

// attachPersistentPreRunE ensures checkCommand() is executed before running all commands. It is not
// sufficient to simply set checkCommand as the PersistentPreRunE of the root command, because it
// would be overridden if a command defined its own PersistentPreRunE. This approach ensures
// checkCommand always runs before the PersistentPreRunE defined on each sub-command.
func attachPersistentPreRunE(cmd *cobra.Command) {
	original := cmd.PersistentPreRunE
	cmd.PersistentPreRunE = func(c *cobra.Command, args []string) error {
		if err := checkCommand(c); err != nil {
			return err
		}
		if original != nil {
			return original(c, args)
		}
		return nil
	}
}

// checkCommand() implements any checks that should run for all commands after all configuration is
// known and the only thing remaining is to execute the command.
func checkCommand(cmd *cobra.Command) error {
	if viper.GetInt(config.NumWorkersKey) < 1 {
		return fmt.Errorf("the number of workers must be at least 1")
	}
	return isCommandAuthorized(cmd)
}

// isCommandAuthorized enforces "opt-out" user authorization requiring commands to explicitly
// declare using an annotation they can be run by users that do not have root privileges.
func isCommandAuthorized(cmd *cobra.Command) error {
	if _, ok := cmd.Annotations["authorization.AllowAllUsers"]; ok {
		if mount := viper.GetString(config.BeeGFSMountPointKey); mount == config.BeeGFSMountPointNone {
			// By forcing non-root users to interact with BeeGFS through a mount point Linux will
			// handle verifying users have permissions for the entries they want to interact with.
			// Otherwise users could guess file names and use CTL to see if those files exist.
			return fmt.Errorf("only root can interact with an unmounted file system")
		}
		return nil
	}
	euid := syscall.Geteuid()
	if euid != 0 {
		return fmt.Errorf("only root can use this command")
	}
	return nil
}
