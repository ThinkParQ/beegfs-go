package config

import (
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
)

// This package handles the global command line tool config - the global flags, environment
// variable bindings and config file handling.

// Defines all the global flags and binds them to the backends config singleton
func InitGlobalFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(config.DebugKey, false, "Print additional details that are normally hidden.")
	cmd.PersistentFlags().MarkHidden(config.DebugKey)

	cmd.PersistentFlags().Bool(config.RawKey, false, "Print raw values without SI or IEC prefixes (except durations).")

	cmd.PersistentFlags().String(config.ManagementAddrKey, "127.0.0.1:8010", "The gRPC network address and port of the management node.")

	cmd.PersistentFlags().String(config.BeeRemoteAddrKey, "127.0.0.1:9010", "The gRPC network address and port of the BeeRemote node.")

	cmd.PersistentFlags().String(config.BeeGFSMountPointKey, "", "The path to the BeeGFS mount point. Only required to work with relative paths when the current working directory is outside BeeGFS.")

	cmd.PersistentFlags().Bool(config.DisableEmojisKey, false, "If emojis should be omitted throughout various output.")
	cmd.PersistentFlags().MarkHidden(config.DisableEmojisKey)

	cmd.PersistentFlags().Int(config.NumWorkersKey, runtime.GOMAXPROCS(0), "The maximum number of workers to use when a command can complete work in parallel (default: number of CPUs).")
	cmd.PersistentFlags().MarkHidden(config.NumWorkersKey)

	cmd.PersistentFlags().Bool(config.TlsDisableKey, false, "Disable TLS for gRPC communication")

	cmd.PersistentFlags().String(config.TlsCaCertKey, "/etc/beegfs/cert.pem", "Use a custom CA certificate for server verification")

	cmd.PersistentFlags().Bool(config.TlsDisableVerificationKey, false, "Disable TLS server verification")

	cmd.PersistentFlags().String(config.AuthFileKey, "/etc/beegfs/conn.auth", "The file containing the authentication secret")

	cmd.PersistentFlags().Duration(config.ConnTimeoutKey, time.Millisecond*500, "Maximum time for each BeeMsg TCP connection attempt")

	// Environment variables should start with BEEGFS_
	viper.SetEnvPrefix("beegfs")
	// Environment variables cannot use "-", replace with "_"
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// Bind all persistent pflags to viper
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		viper.BindEnv(flag.Name)
		viper.BindPFlag(flag.Name, flag)
	})
}

func Cleanup() {
	config.Cleanup()
}
