package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func NewCmd() *cobra.Command {
	globalCfg := indexPkg.GlobalCfg{}

	cmd := &cobra.Command{
		Use:   "index",
		Short: "Create/update and run operations on the the file system index",
		Long:  "Create/update and run different metadata operations against the the file system index.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			mgmtdClient, err := config.ManagementClient()
			if err != nil {
				return err
			}
			detail, err := mgmtdClient.VerifyLicense(cmd.Context(), "io.beegfs.index")
			if err != nil {
				return err
			}
			if log, lerr := config.GetLogger(); lerr == nil {
				log.Debug("verified feature io.beegfs.index is licensed", zap.Any("licenseDetail", detail))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&globalCfg.IndexAddr, "index-addr", "", "Index execution mode: local (default) or ssh:<host>. If unset, read from .beegfs.index at the BeeGFS mount root.")
	cmd.Flags().StringVar(&globalCfg.IndexRoot, "index-root", "", "Root path of the index tree. Required: pass this flag or set it in .beegfs.index at the BeeGFS mount root (no default).")

	cmd.AddCommand(newCreateCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newFindCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newInfoCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newLsCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newStatCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newStatsCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newQueryCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newRescanCmd(&globalCfg, cmd.Flags()))
	cmd.AddCommand(newMigrateCmd(&globalCfg, cmd.Flags()))

	return cmd
}
