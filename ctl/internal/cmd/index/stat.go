package index

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newStatCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.StatCfg{}

	cmd := &cobra.Command{
		Use:         "stat <path>...",
		Short:       "Displays file or directory metadata information.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument")
			}
			return nil
		},
		Long: `Retrieve metadata for one or more files or directories from the index.

Example: display metadata for a file

  beegfs index stat /mnt/beegfs/data/README.md

Example: display BeeGFS-specific metadata

  beegfs index stat --beegfs /mnt/beegfs/data/README.md

Example: display metadata for multiple files

  beegfs index stat /mnt/beegfs/data/file1 /mnt/beegfs/data/file2
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			cfg, args := resolveCfgAndPaths(cmd.Context(), globalCfg, args)
			backendCfg.GlobalCfg = cfg
			paths, err := resolveIndexPaths(cmd.Context(), cfg, args)
			if err != nil {
				return err
			}
			exec, err := indexPkg.NewExecutor(cfg)
			if err != nil {
				return err
			}

			statColumns := []string{"name", "type", "size", "blocks", "inode", "links", "mode", "uid", "gid", "atime", "mtime", "ctime"}
			allColumns := slices.Clone(statColumns)
			defaultColumns := slices.Clone(statColumns)
			if backendCfg.BeeGFS {
				allColumns = append(allColumns,
					"owner_id", "parent_entry_id", "entry_id",
					"stripe_pattern_type", "stripe_chunk_size", "stripe_num_targets",
				)
				defaultColumns = append(defaultColumns, "entry_id", "owner_id", "stripe_num_targets")
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

			for _, rp := range paths {
				// checkIndexExists is a no-op for a remote index; StatIndexDir's local
				// stat there just yields the parent, which is then ignored.
				if err := checkIndexExists(cfg, indexPkg.StatIndexDir(rp.indexPath)); err != nil {
					return err
				}

				log.Debug("running beegfs index stat",
					zap.String("indexPath", rp.indexPath),
					zap.Any("cfg", backendCfg),
				)

				rows, errWait, err := indexPkg.Stat(cmd.Context(), exec, backendCfg, rp.walkRoot, rp.glob)
				if err != nil {
					return err
				}

				rowCount := 0
				if err := drain(cmd.Context(), rows, errWait, func(row []string) {
					tbl.AddItem(toAny(formatStatRow(row, backendCfg.BeeGFS))...)
					rowCount++
				}); err != nil {
					return err
				}
				if rowCount == 0 {
					return fmt.Errorf("no index entry found for %q", rp.fsPath)
				}
			}
			return nil
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS-specific metadata.")

	return cmd
}

// formatStatRow renders one stat result row for display: symbolic mode and
// stat(1)-style timestamps; remaining fields pass through raw.
func formatStatRow(raw []string, beegfs bool) []string {
	r := indexPkg.ParseStatRow(raw, beegfs)

	out := []string{
		r.Name,
		r.Type,
		r.Size,
		r.Blocks,
		r.Inode,
		r.Nlink,
		modeString(r.Mode, r.Type),
		r.Uid,
		r.Gid,
		fmtStatTime(r.Atime),
		fmtStatTime(r.Mtime),
		fmtStatTime(r.Ctime),
	}
	if beegfs {
		out = append(out, r.OwnerID, r.ParentEntryID, r.EntryID,
			r.StripePatternType, r.StripeChunkSize, r.StripeNumTargets)
	}
	return out
}
