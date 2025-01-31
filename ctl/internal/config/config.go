package config

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

// This package handles the global command line tool config - the global flags, environment
// variable bindings and config file handling.

// Defines all the global flags and binds them to the backends config singleton
func InitGlobalFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(config.DebugKey, false, "Print additional details that are normally hidden.")

	cmd.PersistentFlags().Bool(config.RawKey, false, "Print raw values without SI or IEC prefixes (except durations).")

	cmd.PersistentFlags().String(config.ManagementAddrKey, config.BeeGFSMgmtdAddrAuto, `The network address and gRPC port of the management node.
	By default determined automatically when BeeGFS is mounted and all mount points are for the same file system.`)

	cmd.PersistentFlags().String(config.BeeRemoteAddrKey, "127.0.0.1:9010", "The gRPC network address and port of the BeeRemote node.")

	cmd.PersistentFlags().String(config.BeeGFSMountPointKey, "auto", fmt.Sprintf(`Generally the path where BeeGFS is mounted is determined automatically from the provided path(s).
	Both absolute and relative paths inside BeeGFS are supported (e.g., "./myfile" if the cwd is somewhere in BeeGFS or "/mnt/beegfs/myfile").
	Optionally specify the absolute path where BeeGFS is mounted to also be able to use paths relative to the BeeGFS root directory.
	Alternatively set this option to '%s' if BeeGFS is not mounted locally or you want to interact with BeeGFS directly.
	This will skip all local path resolution logic and require paths to be specified relative to the BeeGFS root directory.
	Not all modes (such as migrate) and functionality (such as path recursion) is available using option 'none'.
	Some modes require specifying '%s', for example to interact with paths that no longer exist in BeeGFS.`, config.BeeGFSMountPointNone, config.BeeGFSMountPointNone))

	cmd.PersistentFlags().Bool(config.DisableEmojisKey, false, "If emojis should be omitted throughout various output.")

	cmd.PersistentFlags().Int(config.NumWorkersKey, runtime.GOMAXPROCS(0), "The maximum number of workers to use when a command can complete work in parallel (default: number of CPUs).")

	cmd.PersistentFlags().Bool(config.TlsDisableKey, false, fmt.Sprintf("Disable TLS for gRPC communication (ignores %s).", config.TlsCertFile))

	cmd.PersistentFlags().String(config.TlsCertFile, "/etc/beegfs/cert.pem", `Use the specified certificate to verify and encrypt gRPC traffic. Leave empty to use the system's default certificate pool.
	To allow use by non-root users, ensure the file is owned by group 'beegfs' and has group read permissions.`)

	cmd.PersistentFlags().Bool(config.TlsDisableVerificationKey, false, "Disable TLS server verification")

	cmd.PersistentFlags().Bool(config.AuthDisableKey, false, fmt.Sprintf("Disable authentication (ignores %s).", config.AuthFileKey))
	cmd.PersistentFlags().String(config.AuthFileKey, "/etc/beegfs/conn.auth", `The file containing the authentication secret. 
	To allow use by non-root users, ensure the file is owned by group 'beegfs' and has group read permissions.`)

	cmd.PersistentFlags().Duration(config.ConnTimeoutKey, time.Millisecond*500, "Maximum time to attempt establishing non-gRPC connections.")

	cmd.PersistentFlags().Int8(config.LogLevelKey, 0, fmt.Sprintf(`By default all logging is disabled example for fatal errors. 
	Optionally additional logging to stderr can be enabled to assist with debugging (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).
	When enabling logging you may wish to set --%s=0 to ensure output and log messages are synchronized.`, config.PageSizeKey))

	cmd.PersistentFlags().Bool(config.LogDeveloperKey, false, "Enable logging at DebugLevel and above and print stack traces at WarnLevel and above.")
	cmd.PersistentFlags().MarkHidden(config.LogDeveloperKey)

	cmd.PersistentFlags().String(config.PprofAddress, "", "Start the pprof HTTP server at this address:port for performance debugging (e.g., localhost:6060 or :9999).")
	cmd.PersistentFlags().MarkHidden(config.PprofAddress)

	cmd.PersistentFlags().StringSlice(config.ColumnsKey, []string{}, `When printing structured data, the columns/fields to include (use 'all' to include everything).
	Currently does not automatically set potential flags required to actually fetch the data for some non-default fields.
	Refer to the help for each command to see what additional flags may be needed.`)
	cmd.PersistentFlags().Uint(config.PageSizeKey, 100, `The number of rows/elements to print before output is flushed to stdout.
	When set to 0, output is flushed immediately: When printing tables columns may not be aligned. When printing JSON the output switches to NDJSON.`)
	// TODO (https://github.com/ThinkParQ/beegfs-go/issues/30): Unmark experimental. This is mainly
	// considered experimental because not all modes return structured output yet. While the output
	// for modes that already return structured output is unlikely to change, there is always the
	// possibility we need to make slight adjustments to the output based on user feedback. Thus it
	// is better to consider this experimental until it is completely finalized.
	cmd.PersistentFlags().Var(util.ValidatedStringFlag(config.OutputOptions, config.OutputTable), "output",
		fmt.Sprintf(`Controls how structured output is printed to stdout. Valid options: %v (experimental).
	When printing using a table, the header will be repeated after printing %s rows (set to 0 to omit printing table headers).
	When printing JSON or pretty JSON, if the number of elements to print is greater than %s, prints multiple JSON lists separated by newlines.
	When printing an unknown or large number of elements it is recommended to use NDJSON (Newline-Delimited JSON).`, config.OutputOptions, config.PageSizeKey, config.PageSizeKey))
	// Environment variables should start with BEEGFS_
	viper.SetEnvPrefix("beegfs")
	// Environment variables cannot use "-", replace with "_"
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	os.Setenv("BEEGFS_BINARY_NAME", "beegfs")

	// Bind all persistent pflags to viper
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		viper.BindEnv(flag.Name)
		viper.BindPFlag(flag.Name, flag)
	})
}

func Cleanup() {
	config.Cleanup()
}
