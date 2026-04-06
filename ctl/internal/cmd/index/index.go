package index

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Create/update and run operations on the the file system index",
		Long:  "Create/update and run different metadata operations against the the file system index.",
	}

	// Persistent flags available to all index subcommands.
	// viper.BindPFlag only activates when the flag is explicitly set by the user;
	// when unset, viper falls through to the defaults loaded by LoadGUFIConfig().
	pf := cmd.PersistentFlags()

	pf.String("index-addr", "", "Index execution mode: local (default) or ssh:<host>. If unset, read from .beegfs.index at the BeeGFS mount root.")
	viper.BindPFlag(indexPkg.IndexAddrKey, pf.Lookup("index-addr")) //nolint:errcheck

	pf.String("index-root", "", "GUFI index root path (overrides IndexRoot in /etc/GUFI/config).")
	viper.BindPFlag(indexPkg.IndexRootKey, pf.Lookup("index-root")) //nolint:errcheck

	pf.Int("index-threads", 0, "Number of threads for GUFI operations (0 = use value from /etc/GUFI/config or CPU count).")
	viper.BindPFlag(indexPkg.ThreadsKey, pf.Lookup("index-threads")) //nolint:errcheck

	cmd.AddCommand(newCreateCmd())
	cmd.AddCommand(newFindCmd())
	cmd.AddCommand(newInfoCmd())
	cmd.AddCommand(newLsCmd())
	cmd.AddCommand(newStatCmd())
	cmd.AddCommand(newStatsCmd())
	cmd.AddCommand(newQueryCmd())
	cmd.AddCommand(newRescanCmd())
	cmd.AddCommand(newMigrateCmd())
	//cmd.AddCommand(newUpgradeCmd()) Shall not be included in V8 Release

	return cmd
}
