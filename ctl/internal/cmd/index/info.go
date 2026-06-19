package index

import (
	"fmt"
	osexec "os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newInfoCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	var runTreesummary bool
	var raw bool

	cmd := &cobra.Command{
		Use:         "info [path]",
		Short:       "Display index configuration and tree summary statistics.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Show the active index configuration and summary statistics from
the tree summary table at the index root.

If a .beegfs.index file exists at the BeeGFS mount root, configuration
is auto-discovered. Use --columns all for extended statistics including
UID/GID ranges, size ranges, and modification time ranges.

Example: show info for the whole index

  beegfs index info

Example: scope info to a subtree

  beegfs index info /mnt/beegfs/data

Example: show all columns as JSON

  beegfs index info --output json-pretty --columns all /mnt/beegfs/data
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()
			cfg, infoArgs := resolveCfgAndPaths(cmd.Context(), globalCfg, args)
			indexPath, err := resolveAndCheckIndexPath(cfg, infoArgs)
			if err != nil {
				return err
			}
			exec, err := indexPkg.NewExecutor(cfg)
			if err != nil {
				return err
			}

			if runTreesummary {
				treesumArgs := []string{indexPath}
				if cfg.Threads > 0 {
					treesumArgs = []string{"-n", fmt.Sprint(cfg.Threads), indexPath}
				}
				bin, treesumArgs, err := indexPkg.WrapForRemote(indexPkg.TreesumBin, treesumArgs, cfg.IndexAddr)
				if err != nil {
					return err
				}
				treesumCmd := osexec.CommandContext(cmd.Context(), bin, treesumArgs...)
				treesumCmd.SysProcAttr = indexPkg.CallerSysProcAttr()
				treesumCmd.Stdout = cmd.OutOrStdout()
				treesumCmd.Stderr = cmd.ErrOrStderr()
				if err := treesumCmd.Run(); err != nil {
					return fmt.Errorf("tree summary: %w", err)
				}
			}

			infoCfg := indexPkg.InfoCfg{
				GlobalCfg: cfg,
				IndexPath: indexPath,
			}

			log.Debug("running beegfs index info",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", infoCfg),
			)

			rows, errWait, err := indexPkg.Info(cmd.Context(), exec, infoCfg)
			if err != nil {
				return err
			}

			allColumns := []string{
				"index_root", "index_addr", "last_updated",
				"total_files", "total_dirs", "total_links", "total_size", "depth",
				"min_uid", "max_uid", "min_gid", "max_gid",
				"min_size", "max_size", "min_mtime", "max_mtime",
				"zero_files", "total_blocks",
			}
			defaultColumns := []string{
				"index_root", "index_addr", "last_updated",
				"total_files", "total_dirs", "total_links", "total_size", "depth",
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

			return drain(cmd.Context(), rows, errWait, func(row []string) {
				tbl.AddItem(toAny(formatInfoRow(row, raw))...)
			})
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().BoolVar(&runTreesummary, "treesummary", false,
		"Compute tree summary on the index path before displaying stats.")
	cmd.Flags().BoolVar(&raw, "raw", false,
		"Print raw values instead of human-readable sizes and timestamps.")

	return cmd
}
