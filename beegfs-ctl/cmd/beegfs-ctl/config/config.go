package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
)

// This package handles the global command line tool config - the global flags, environment
// variable bindings and config file handling.

// Defines all the global flags and binds them to the backends config singleton
func Init(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&config.Get().ManagementAddr, "mgmtdAddr", "127.0.0.1:8010", "The gRPC network address and port of the management node")
	viper.BindEnv("mgmtdAddr", "BEEGFS_MGMTD_ADDR")
	viper.BindPFlag("mgmtdAddr", cmd.PersistentFlags().Lookup("mgmtdAddr"))
}
