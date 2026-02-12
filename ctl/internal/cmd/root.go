package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/mitchellh/go-wordwrap"
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
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	cmdConfig "github.com/thinkparq/beegfs-go/ctl/internal/config"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	lic "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/license"
	pl "github.com/thinkparq/protobuf/go/license"
	"golang.org/x/term"
)

const (
	helpMinTermWidth = 80
	helpMaxTermWidth = 150
)

var (
	helpBulletRegex = regexp.MustCompile(`^\s*([-*•]\s+|\d+\.\s+)`)
	paragraphRegex  = regexp.MustCompile("\n\n+")
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
		Long: fmt.Sprintf(`%s\
%s
This tool allows you to inspect, configure, and monitor BeeGFS.

* View help for specific commands with "<command> --help".
* For full product documentation, visit: https://doc.beegfs.io/.
* Questions?
  - If you have an active support contract, please visit: https://www.beegfs.io/c/enterprise/
  - For community support, check out: https://github.com/ThinkParQ/beegfs/blob/master/SUPPORT.md

BeeGFS is crafted with 💛 by contributors worldwide.
Thank you for using BeeGFS and supporting its ongoing development! 🐝
		`, longHelpHeader, strings.Repeat("=", len(longHelpHeader))),
		SilenceUsage:  true,
		SilenceErrors: true,
		// These are inherited by ALL commands even if they also define there own PersistentPreRunE
		// or PersistentPostRunE. See attachPersistentPreRunE() for more details.
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			return globalPersistentPreRunE(c)
		},
		PersistentPostRunE: func(c *cobra.Command, args []string) error {
			return globalPersistentPostRunE(c)
		},
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
	wrapAllCommands(cmd)

	// Override the help template to allow the output to be customized including skipping printing
	// persistent (global) flags so they are only printed for the root "beegfs" command and wrapping
	// long help text and arguments automatically.
	cobra.AddTemplateFunc("wrapFlagUsages", wrapFlagUsages)
	cobra.AddTemplateFunc("wrapHelpText", wrapHelpText)
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
		// SilenceErrors is set on the root command so we can add custom context to errors.
		if errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "Error: command execution was interrupted: %s\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		}
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

// wrapAllCommands applies any customizations that should be made to all commands. Notably
// customizing error handling around positional arguments and executing any extra functionality that
// should run before all commands.
func wrapAllCommands(cmd *cobra.Command) {
	if cmd.Args != nil {
		origArgs := cmd.Args
		cmd.Args = attachCustomArgsErr(origArgs)
	}
	attachPersistentPreRunE(cmd)
	attachPersistentPostRunE(cmd)
	for _, subCmd := range cmd.Commands() {
		wrapAllCommands(subCmd) // Recursively wrap subcommands
	}
}

// attachCustomArgsErr prints the usage text along with the error when the positional arguments are
// specified incorrectly. For example if required positional arguments are omitted Cobra prints a
// generic error "requires at least N arg(s), only received N" which does not make it obvious what
// went wrong and how to fix the issue.
func attachCustomArgsErr(argsFn cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := argsFn(cmd, args); err != nil {
			return fmt.Errorf("%w\nUsage: %s --help", err, cmd.Use)
		}
		return nil
	}
}

// attachPersistentPreRunE ensures checkCommand() is executed before running all commands. It is not
// sufficient to simply set checkCommand as the PersistentPreRunE of the root command, because it
// would be overridden if a command defined its own PersistentPreRunE. This approach ensures the
// globalPersistentPreRunE always runs before the PersistentPreRunE defined by any sub-command.
func attachPersistentPreRunE(cmd *cobra.Command) {
	// Do not wrap the root command. It already calls globalPersistentPreRunE and wrapping it would
	// invoke the global handler twice when executing the root command or any command that inherits
	// its PersistentPreRunE.
	if cmd.Parent() == nil {
		return
	}
	if cmd.PersistentPreRunE != nil {
		origFunc := cmd.PersistentPreRunE
		newFunc := func(c *cobra.Command, args []string) error {
			if err := globalPersistentPreRunE(c); err != nil {
				return err
			}
			return origFunc(c, args)
		}
		cmd.PersistentPreRunE = newFunc
	}
	// Commands with a nil PersistentPreRunE will inherit their parent's commands PersistentPreRunE
	// (if any) via Cobra's lazy inheritance. We do not assign a new function to these commands to
	// preserve that behavior, otherwise sub-commands of a command with a PersistentPreRunE defined
	// would only use the globalPersistentPreRunE, not their parents wrapped PersistentPreRunE func.
}

// attachPersistentPostRunE does the same as attachPersistentPreRunE for PersistentPostRunE.
func attachPersistentPostRunE(cmd *cobra.Command) {
	if cmd.Parent() == nil {
		return
	}
	if cmd.PersistentPostRunE != nil {
		origFunc := cmd.PersistentPostRunE
		newFunc := func(c *cobra.Command, args []string) error {
			if err := globalPersistentPostRunE(c); err != nil {
				return err
			}
			return origFunc(c, args)
		}
		cmd.PersistentPostRunE = newFunc
	}
}

// pprofStarted ensures pprof only starts once as globalPersistentPreRunE is executed for all
// defined commands (not just the one that is executed).
var pprofStarted bool

// globalPersistentPreRunE() implements any functionality that should run before all commands after
// all configuration is known and the only thing remaining is to execute the command.
func globalPersistentPreRunE(cmd *cobra.Command) error {
	// Enable performance profiling via pprof if requested:
	if viper.GetString(config.PprofAddress) != "" && !pprofStarted {
		pprofStarted = true
		log, _ := config.GetLogger()
		log.Debug("starting pprof")
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

func globalPersistentPostRunE(cmd *cobra.Command) error {
	// Printing license warnings on the license command results in redundant output.
	if cmd.Name() != "license" {
		license, err := lic.GetLicense(cmd.Context(), false)
		if err != nil {
			return err
		}

		if license.Result != pl.VerifyResult_VERIFY_VALID {
			cmdfmt.Printf("WARNING: this system does not have a valid license installed.\n")
			cmdfmt.Printf("To avoid disruptions, run 'beegfs license' and follow the required steps.\n")
			cmdfmt.Printf("Reason: %s\n", license.Message)
			return nil
		}

		remaining, message := lic.GetTimeToExpiration(license)
		// Warn by default 90 days before expiry.
		renewalWindow := 90 * 24 * time.Hour
		if license.Data.Type == pl.CertType_CERT_TYPE_TEMPORARY {
			// For temp licenses only warn 14 days before.
			renewalWindow = 14 * 24 * time.Hour
		}
		if remaining <= renewalWindow {
			cmdfmt.Printf("WARNING: License %s, run 'beegfs license' for more details.\n", message)
		}

		for _, f := range license.Data.DnsNames {
			if after, ok := strings.CutPrefix(f, lic.PrefixCapacity); ok {
				if err := lic.CheckIfOverStorageCapacityLimit(cmd.Context(), after); err != nil {
					cmdfmt.Printf("WARNING: The %s.\n", err)
				}
			}
		}
	}

	return nil
}

// isCommandAuthorized enforces "opt-out" user authorization requiring commands to explicitly
// declare using an annotation they can be run by users that do not have root privileges.
func isCommandAuthorized(cmd *cobra.Command) error {
	// The completion commands are auto generated so we cannot easily add an annotation, but they
	// should be runnable by all users. However in case we add a command called completion this need
	// to only apply to the top-level completion command.
	if cmd.Parent().Name() == "completion" && cmd.Parent().Parent().Name() == BinaryName {
		return nil
	}

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

// Cobra does not directly support automatically wrapping flags with long descriptions but instead
// recommends this approach: https://github.com/spf13/cobra/issues/1805#issuecomment-1246192724
// https://github.com/vmware-tanzu/community-edition/blob/138acbf49d492815d7f72055db0186c43888ae15/cli/cmd/plugin/unmanaged-cluster/cmd/utils.go#L74-L113
// We follow a similar approach defining our own custom template functions so we can control the
// width of flags and other text in the help output of each command.
var helpTemplate = fmt.Sprintf(`{{with (or .Long .Short)}}{{wrapHelpText . | trimTrailingWhitespaces}}{{end}}

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
{{wrapFlagUsages .LocalFlags | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}{{end}}
{{if (eq .Name "beegfs")}}
Global flags apply to all commands and can all be set persistently using environment variables.
To specify configuration using environment variables specify BEEGFS_KEY=VALUE where KEY is the 
flag name you want to specify in all capitals replacing hyphens (-) with an underscore (_).
For example to set the --mgmtd-addr persistently for this shell session run: 

  export BEEGFS_MGMTD_ADDR=hostname:port

To persist across sessions/reboots set it in your .bashrc file or similar.

Exit Codes:

  %d - %s
  %d - %s
  %d - %s
{{ else }}
See "beegfs --help" for a list of global flags that also apply to this command.
{{end}}
`, util.Success, util.Success, util.GeneralError, util.GeneralError, util.PartialSuccess, util.PartialSuccess)

// getTermWidthForHelp attempts to determine the width of the terminal, falling back to
// helpMinTermWidth if there is an error (i.e., stdout is not a terminal). It also enforces a
// helpMaxTermWidth and helpMinTermWidth to ensure readability.
func getTermWidthForHelp() int {
	termWidth, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return helpMinTermWidth
	}
	if termWidth > helpMaxTermWidth {
		return helpMaxTermWidth
	} else if termWidth < helpMinTermWidth {
		return helpMinTermWidth
	}
	return termWidth
}

// getParagraphsForHelp() allows help text to be defined using markdown like formatting where line
// breaks can be inserted anywhere to help code readability that are ignored when rendering. It
// preserve line breaks when typically desired, for example when items are specified as a list, or
// for indented example that should be rendered exactly as specified. See below for details.
func getParagraphsForHelp(input string) []string {
	paragraphs := paragraphRegex.Split(input, -1)
	for i, paragraph := range paragraphs {

		lines := strings.Split(paragraph, "\n")
		for i := range lines {
			// We don't want to trim leading spaces as they are needed to preserve indentation.
			// However trailing spaces should not need to be preserved and make the result ugly.
			lines[i] = strings.TrimRight(lines[i], " ")
		}

		if helpBulletRegex.MatchString(strings.TrimSpace(lines[0])) {
			// Handle bullet blocks:
			paragraphs[i] = mergeBulletsForHelp(lines)
		} else if strings.HasSuffix(lines[0], ":") {
			// Handle header blocks.
			var adjustedLines []string
			// Keep the header:
			adjustedLines = append(adjustedLines, lines[0])
			if len(lines) > 1 {
				if helpBulletRegex.MatchString(strings.TrimSpace(lines[1])) {
					// If its a bullet block merge each bullet maintaining newlines between bullets.
					// Handle if this is a bullet block:
					bulletParagraph := mergeBulletsForHelp(lines[1:])
					adjustedLines = append(adjustedLines, bulletParagraph)
				} else {
					// Handle if its a normal paragraph block, placing it right after the header:
					adjustedLines = append(adjustedLines, strings.Join(lines[1:], " "))
				}
			}
			paragraphs[i] = strings.Join(adjustedLines, "\n")
		} else if strings.HasPrefix(lines[0], " ") {
			// Handle code/example blocks indicated by indentation:
			paragraphs[i] = strings.Join(lines, "\n")
		} else if strings.HasSuffix(lines[0], "\\") {
			// Handle preserving line breaks when lines end in a backslash:
			lines[0] = strings.TrimSuffix(lines[0], "\\")
			paragraphs[i] = strings.Join(lines, "\n")
		} else {
			// Otherwise collapse the block into a single line with spaces:
			paragraphs[i] = strings.Join(lines, " ")
		}
	}
	return paragraphs
}

// mergeBulletsForHelp allows bullets to be specified using line breaks for readability that are
// merged while maintaining new line between each bullet.
func mergeBulletsForHelp(lines []string) string {
	var bulletLines []string
	var currentBullet string

	for _, line := range lines {
		if helpBulletRegex.MatchString(strings.TrimSpace(line)) {
			// If we were collecting a previous bullet, add it to the list
			if currentBullet != "" {
				bulletLines = append(bulletLines, currentBullet)
			}
			// Start a new bullet, keeping its original indentation
			currentBullet = line
		} else {
			// If this line is NOT a new bullet, append it to the current bullet
			currentBullet += " " + strings.TrimSpace(line)
		}
	}
	// Add the last bullet to the list (if one exists)
	if currentBullet != "" {
		bulletLines = append(bulletLines, currentBullet)
	}
	// Join bullets together maintaining newlines between bullets.
	return strings.Join(bulletLines, "\n")
}

// wrapFlagUsages allows flag usage to be defined using markdown like formatting where line breaks
// and tabs can be inserted anywhere to help code readability that are then ignored when rendering.
// Ignoring tabs is helpful so help text can be defined without having to unindent the second line
// to the left column to avoid insertion of a tab:
//
//	cmd.Flags().BoolVar(&someFlag, "some-flag", false, `Some very long help text
//	that should wrap on another line for readability.`)
//
// The resulting text will gracefully wrap at the terminal width, or at the defined min/max widths.
func wrapFlagUsages(cmd *pflag.FlagSet) string {
	cmd.VisitAll(func(flag *pflag.Flag) {
		usage := strings.Join(getParagraphsForHelp(flag.Usage), "\n\n")
		usage = strings.Replace(usage, "\t", "", -1)
		flag.Usage = usage
	})
	return cmd.FlagUsagesWrapped(getTermWidthForHelp() - 1)
}

// wrapFlagUsages allows flag usage to be defined using markdown like formatting where line breaks
// and tabs can be inserted anywhere to help code readability that are ignored when rendering.
//
// The resulting text will gracefully wrap at the terminal width, or at the defined min/max widths.
func wrapHelpText(input string) string {
	paragraphs := getParagraphsForHelp(input)
	width := getTermWidthForHelp()
	lines := strings.Split(strings.Join(paragraphs, "\n\n"), "\n")
	var wrappedLines []string
	for _, line := range lines {
		// Get leading spaces for indentation
		trimmed := strings.TrimLeft(line, " ")
		// Calculate indentation length.
		indent := len(line) - len(trimmed)
		// Wrap the line at the prescribed width, accounting for indentation. Subtract 1 from the
		// available width to prevent edge-case wrapping issues where text might exceed the terminal
		// width by one character. This helps avoid quirks in terminal rendering and ensures more
		// consistent wrapping behavior. This is a fancy way of saying ¯\_(ツ)_/¯ but FWIW I also
		// see this done in other implementations that auto-wrap text based on the terminal width.
		wrapped := wordwrap.WrapString(trimmed, uint(width-indent-1))

		// Re-add indentation to wrapped lines
		for i, wrappedLine := range strings.Split(wrapped, "\n") {
			// If the line is a bullet then account for the bullet when indenting wrapped lines.
			if i > 0 && helpBulletRegex.MatchString(line) {
				// This approach allows for variable width bullets but will indent like this:
				//
				// 1. long
				//    line
				// 12. long
				//     line
				//
				// It seems unlikely we'll have lists this large anyway, but if needed we could
				// align these lists differently but this was the most straightforward approach.
				wrappedLines = append(wrappedLines, strings.Repeat(" ", len(helpBulletRegex.FindString(line)))+wrappedLine)
			} else {
				wrappedLines = append(wrappedLines, strings.Repeat(" ", indent)+wrappedLine)
			}
		}
	}
	return strings.Join(wrappedLines, "\n")
}
