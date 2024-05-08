package config

import (
	"runtime"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
)

// This package handles the global command line tool config - the global flags, environment
// variable bindings and config file handling.

// Defines all the global flags and binds them to the backends config singleton
func Init(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&config.Get().ManagementAddr, "mgmtd-addr", "127.0.0.1:8010",
		"The gRPC network address and port of the management node.")
	viper.BindEnv("mgmtd-addr", "BEEGFS_MGMTD_ADDR")
	viper.BindPFlag("mgmtd-addr", cmd.PersistentFlags().Lookup("mgmtd-addr"))

	cmd.PersistentFlags().BoolVar(&config.Get().Debug, "debug", false,
		"Print additional details that are normally hidden.")
	cmd.PersistentFlags().MarkHidden("debug")
	viper.BindEnv("debug", "BEEGFS_DEBUG")
	viper.BindPFlag("debug", cmd.PersistentFlags().Lookup("debug"))

	cmd.PersistentFlags().BoolVar(&config.Get().Raw, "raw", false,
		"Print raw values without SI or IEC prefixes (except durations).")
	viper.BindEnv("raw", "BEEGFS_RAW")
	viper.BindPFlag("raw", cmd.PersistentFlags().Lookup("raw"))

	cmd.PersistentFlags().StringVar(&config.Get().BeeRemoteAddr, "bee-remote-addr", "127.0.0.1:9010", "The gRPC network address and port of the BeeRemote node.")
	viper.BindEnv("bee-remote-addr", "BEEGFS_BEEREMOTE_ADDR")
	viper.BindPFlag("bee-remote-addr", cmd.PersistentFlags().Lookup("bee-remote-addr"))

	cmd.PersistentFlags().StringVar(&config.Get().BeeGFSMountPoint, "mount-point", "", "The path to the BeeGFS mount point. Only required to work with relative paths when the current working directory is outside BeeGFS.")
	viper.BindEnv("mount-point", "BEEGFS_MOUNT_POINT")
	viper.BindPFlag("mount-point", cmd.PersistentFlags().Lookup("mount-point"))

	cmd.PersistentFlags().BoolVar(&config.Get().DisableEmojis, "disable-emojis", false, "If emojis should be omitted throughout various output.")
	cmd.PersistentFlags().Lookup("disable-emojis").Hidden = true
	viper.BindEnv("disable-emojis", "BEEGFS_DISABLE_EMOJIS")
	viper.BindPFlag("disable-emojis", cmd.PersistentFlags().Lookup("disable-emojis"))

	cmd.PersistentFlags().IntVar(&config.Get().NumWorkers, "num-workers", runtime.GOMAXPROCS(0), "The maximum number of workers to use when a command can complete work in parallel (default: number of CPUs).")
	cmd.PersistentFlags().Lookup("num-workers").Hidden = true
	viper.BindEnv("num-workers", "BEEGFS_NUM_WORKERS")
	viper.BindPFlag("num-workers", cmd.PersistentFlags().Lookup("num-workers"))

	// TODO authenticationSecret need custom action to parse it (must be loaded from file)
	// See here: https://github.com/ThinkParQ/beegfs-ctl/issues/5
}

func Cleanup() {
	config.Cleanup()
}
