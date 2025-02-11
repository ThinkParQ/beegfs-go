package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/benchmark"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/copy"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/debug"
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

	// Initialize global config that can be later accessed through Viper.
	cmdConfig.InitGlobalFlags(cmd)
	defer cmdConfig.Cleanup()
	// WARNING: Configuration in Viper is not set at this point based on user provided flags. This
	// does not happen until after a command is executed and all configuration is merged together.
	// To add functionality that should run before all commands that depends on global configuration
	// this must be added elsewhere, typically globalPersistentPreRunE().

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
	cmd.AddCommand(debug.NewCmd())

	// This must run AFTER all commands are added.
	for _, child := range cmd.Commands() {
		attachPersistentPreRunE(child)
	}

	// Override the help template to allow the output to be customized including skipping printing
	// persistent (global) flags so they are only printed for the root "beegfs" command.
	cmd.SetHelpTemplate(helpTemplate)

	// By default there is no explicit signal handler and the Go runtime will immediately terminate
	// on a interrupt signal (Ctrl+C). To allow for more graceful handling/cleanup including the
	// opportunity for additional logging and errors to be returned, setup the context passed to all
	// commands to be cancelled by an interrupt and automatically cancel when the program exits.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Parse the given parameters and execute the selected command
	err := cmd.ExecuteContext(ctx)
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
		if err := globalPersistentPreRunE(c); err != nil {
			return err
		}
		if original != nil {
			return original(c, args)
		}
		return nil
	}
}

// globalPersistentPreRunE() implements any functionality that should run before all commands after
// all configuration is known and the only thing remaining is to execute the command.
func globalPersistentPreRunE(cmd *cobra.Command) error {
	// Enable performance profiling via pprof if requested:
	if viper.GetString(config.PprofAddress) != "" {
		go func() {
			err := http.ListenAndServe(viper.GetString(config.PprofAddress), nil)
			if err != nil {
				panic("unable to setup pprof HTTP server: " + err.Error())
			}
		}()
	}
	// Global configuration checks:
	if viper.GetInt(config.NumWorkersKey) < 1 {
		return fmt.Errorf("the number of workers must be at least 1")
	}
	return isCommandAuthorized(cmd)
}

// isCommandAuthorized enforces "opt-out" user authorization requiring commands to explicitly
// declare using an annotation they can be run by users that do not have root privileges.
func isCommandAuthorized(cmd *cobra.Command) error {
	euid := syscall.Geteuid()
	if _, ok := cmd.Annotations["authorization.AllowAllUsers"]; ok {
		if mount := viper.GetString(config.BeeGFSMountPointKey); mount == config.BeeGFSMountPointNone && euid != 0 {
			// By forcing non-root users to interact with BeeGFS through a mount point Linux will
			// handle verifying users have permissions for the entries they want to interact with.
			// Otherwise users could guess file names and use CTL to see if those files exist.
			return fmt.Errorf("only root can interact with an unmounted file system")
		}
		return nil
	}
	if euid != 0 {
		return fmt.Errorf("only root can use this command")
	}
	return nil
}

var helpTemplate = fmt.Sprintf(`{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}{{end}}

Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

Available Commands:{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

Additional Commands:{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

{{if (eq .Name "beegfs")}}
Global Flags:
{{ else }}
Flags:
{{end}}
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}{{end}}
{{if (eq .Name "beegfs")}}
Global flags also apply to all sub-commands and can be set persistently using environment variables:

  export BEEGFS_MGMTD_ADDR=hostname:port

To persist configuration across sessions/reboots set it in your .bashrc file or similar.

Exit Codes:

  %d - %s
  %d - %s
  %d - %s
{{ else }}
See "beegfs help" for a list of global flags that also apply to this command.
{{end}}
`, util.Success, util.Success, util.GeneralError, util.GeneralError, util.PartialSuccess, util.PartialSuccess)
