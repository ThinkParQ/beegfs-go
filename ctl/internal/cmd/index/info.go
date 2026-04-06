package index

import (
	"fmt"
	osexec "os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newInfoCmd() *cobra.Command {
	var runTreesummary bool

	cmd := &cobra.Command{
		Use:         "info [path]",
		Short:       "Display index configuration and tree summary statistics.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Show the active index configuration and summary statistics from
the GUFI treesummary table at the index root.

If a .beegfs.index file exists at the BeeGFS mount root, configuration
is auto-discovered. Use --columns all for extended statistics including
UID/GID ranges, size ranges, and modification time ranges.

Example: show index info for current directory

  beegfs index info

Example: show index info for a specific path

  beegfs index info /mnt/beegfs/data

Example: show all columns as JSON

  beegfs index info --output json-pretty --columns all /mnt/beegfs/data
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			exec, err := newExecutor()
			if err != nil {
				return err
			}
			indexPath, err := resolveIndexPath(args)
			if err != nil {
				return err
			}

			if err := checkIndexExists(indexPath); err != nil {
				return err
			}

			if runTreesummary {
				treesumBin := viper.GetString(indexPkg.TreesumBinKey)
				threads := viper.GetInt(indexPkg.ThreadsKey)
				treesumCmd := osexec.CommandContext(cmd.Context(), treesumBin,
					"-n", fmt.Sprint(threads), indexPath)
				treesumCmd.Stdout = cmd.OutOrStdout()
				treesumCmd.Stderr = cmd.ErrOrStderr()
				if err := treesumCmd.Run(); err != nil {
					return fmt.Errorf("gufi_treesummary: %w", err)
				}
			}

			cfg := indexPkg.InfoCfg{
				IndexRoot: indexPath,
				IndexAddr: viper.GetString(indexPkg.IndexAddrKey),
			}

			log.Debug("running beegfs index info",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", cfg),
			)

			rows, errWait, err := indexPkg.Info(cmd.Context(), exec, cfg)
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

		run:
			for {
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case row, ok := <-rows:
					if !ok {
						break run
					}
					tbl.AddItem(toAny(row)...)
				}
			}

			return errWait()
		},
	}

	cmd.Flags().BoolVar(&runTreesummary, "treesummary", false,
		"Run gufi_treesummary on the index path before displaying stats.")

	return cmd
}
