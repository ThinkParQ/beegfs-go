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
	cmd.PersistentFlags().StringVar(&config.Get().ManagementAddr, "mgmtdAddr", "127.0.0.1:8010",
		"The gRPC network address and port of the management node")
	viper.BindEnv("mgmtdAddr", "BEEGFS_MGMTD_ADDR")
	viper.BindPFlag("mgmtdAddr", cmd.PersistentFlags().Lookup("mgmtdAddr"))

	cmd.PersistentFlags().BoolVar(&config.Get().Debug, "debug", false,
		"Prints some additional info that is normally hidden, depending on the command.")
	cmd.PersistentFlags().MarkHidden("debug")
	viper.BindEnv("debug", "BEEGFS_DEBUG")
	viper.BindPFlag("debug", cmd.PersistentFlags().Lookup("debug"))

	cmd.PersistentFlags().BoolVar(&config.Get().Raw, "raw", false,
		"Prints raw values without SI or IEC prefixes (except durations)")
	viper.BindEnv("raw", "BEEGFS_RAW")
	viper.BindPFlag("raw", cmd.PersistentFlags().Lookup("raw"))

	cmd.PersistentFlags().StringVar(&config.Get().BeeRemoteAddr, "beeRemoteAddr", "127.0.0.1:9010", "The gRPC network address and port of the BeeRemote node")
	viper.BindEnv("beeRemoteAddr", "BEEGFS_BEEREMOTE_ADDR")
	viper.BindPFlag("beeRemoteAddr", cmd.PersistentFlags().Lookup("beeRemoteAddr"))

	cmd.PersistentFlags().StringVar(&config.Get().BeeGFSMountPoint, "mountPoint", "", "The path to the BeeGFS mount point (only required to work with relative paths when the current working directory is outside BeeGFS).")
	viper.BindEnv("mountPoint", "BEEGFS_MOUNT_POINT")
	viper.BindPFlag("mountPoint", cmd.PersistentFlags().Lookup("mountPoint"))

	cmd.PersistentFlags().BoolVar(&config.Get().DisableEmojis, "disableEmojis", false, "If emojis or basic unicode characters are used for visual representations throughout various output.")
	cmd.PersistentFlags().Lookup("disableEmojis").Hidden = true
	viper.BindEnv("disableEmojis", "BEEGFS_DISABLE_EMOJIS")
	viper.BindPFlag("disableEmojis", cmd.PersistentFlags().Lookup("disableEmojis"))

	cmd.PersistentFlags().IntVar(&config.Get().NumWorkers, "numWorkers", runtime.GOMAXPROCS(0), "The maximum number of workers to use when a command can complete work in parallel (default: number of CPUs).")
	cmd.PersistentFlags().Lookup("numWorkers").Hidden = true
	viper.BindEnv("numWorkers", "BEEGFS_NUM_WORKERS")
	viper.BindPFlag("numWorkers", cmd.PersistentFlags().Lookup("numWorkers"))

	// TODO authenticationSecret need custom action to parse it (must be loaded from file)
	// See here: https://github.com/ThinkParQ/beegfs-ctl/issues/5
}

func Cleanup() {
	config.Cleanup()
}
