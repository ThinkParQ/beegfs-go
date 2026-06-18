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

func newStatsCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.StatsCfg{}

	cmd := &cobra.Command{
		Use:         "stats <stat-name> [path]",
		Short:       "Calculate statistics of the index directory.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <stat-name> argument")
			}
			return nil
		},
		Long: `Compute aggregate statistics over the index tree.

Global stats (output: single number):
  total-filecount     total number of regular files
  total-dircount      total number of directories
  total-filesize      total bytes used by regular files
  total-linkcount     total number of symbolic links
  total-leaf-files    total regular files in leaf directories
  total-leaf-links    total symlinks in leaf directories

Per-directory stats (output: path, value):
  filecount           regular file count
  filesize            total file size
  depth               directory depth
  linkcount           symlink count
  dircount            immediate subdirectory count
  leaf-dirs           leaf directory names (no value column)
  leaf-depth          leaf directory depth
  leaf-files          leaf directory regular file count
  leaf-links          leaf directory symlink count

Per-level stats (output: level, count):
  files-per-level     regular file count per depth level
  links-per-level     symlink count per depth level
  dirs-per-level      directory count per depth level

Aggregate leaf stats (output: single number):
  average-leaf-files  average regular files per leaf directory
  average-leaf-links  average symlinks per leaf directory
  average-leaf-size   average total file size per leaf directory
  median-leaf-files   median regular files per leaf directory
  median-leaf-links   median symlinks per leaf directory
  median-leaf-size    median total file size per leaf directory

Cross-directory stats:
  duplicate-names     names appearing in more than one directory

Per-file listing stats (output: id, size, path):
  uid-size            all regular files listed by uid then size
  gid-size            all regular files listed by gid then size

Extension histogram (output: extension, count):
  extensions          file extension frequency histogram

File-size bin stats (output: bin, count):
  filesize-log2-bins    base-2 logarithmic histogram of file sizes
  filesize-log1024-bins base-1024 logarithmic histogram of file sizes

Directory file-count bin stats (output: bin, count):
  dirfilecount-log2-bins    base-2 logarithmic histogram of per-directory file counts
  dirfilecount-log1024-bins base-1024 logarithmic histogram of per-directory file counts

Age-bin stats (output: bucket, count). Use --reftime <epoch> to control the
reference point for age computation (defaults to now if unset):
  file-age-atime      regular-file atime age buckets
  file-age-mtime      regular-file mtime age buckets
  file-age-ctime      regular-file ctime age buckets
  dir-age-atime       directory atime age buckets
  dir-age-mtime       directory mtime age buckets
  dir-age-ctime       directory ctime age buckets

Global stats (total-*) always traverse the full tree.
Per-directory stats (filecount, filesize, depth, linkcount, dircount) process
the root directory only by default; use --recursive to include subdirectories.
Use --cumulative to merge subdirectory data into a single set instead of
grouping per-path. Applies to age-* and bin metrics.

Example: count all files under /mnt/beegfs

  beegfs index stats total-filecount /mnt/beegfs

Example: per-directory file counts, sorted descending, top 20

  beegfs index stats filecount --order DESC --num-results 20 /mnt/beegfs

Example: total filesize owned by uid 1000

  beegfs index stats total-filesize --uid 1000 /mnt/beegfs
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			statName := indexPkg.StatName(args[0])
			cfg, pathArgs := resolveCfgAndPaths(cmd.Context(), globalCfg, args[1:])
			backendCfg.GlobalCfg = cfg
			indexPath, err := resolveAndCheckIndexPath(cfg, pathArgs)
			if err != nil {
				return err
			}
			exec, err := indexPkg.NewExecutor(cfg)
			if err != nil {
				return err
			}

			backendCfg.Stat = statName

			log.Debug("running beegfs index stats",
				zap.String("stat", string(statName)),
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			columns, rows, errWait, err := indexPkg.Stats(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			allColumns := columns
			defaultColumns := slices.Clone(columns)
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

			return drain(cmd.Context(), rows, errWait, func(row []string) {
				tbl.AddItem(toAny(padRow(row, len(allColumns)))...)
			})
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().IntVar(&backendCfg.Uid, "uid", -1, "Filter by numeric user ID (-1 = all users). Applies to total-* stats.")
	cmd.Flags().BoolVarP(&backendCfg.Recursive, "recursive", "r", false, "Include all subdirectories (for filecount, filesize, depth stats).")
	cmd.Flags().BoolVarP(&backendCfg.Cumulative, "cumulative", "c", false, "Merge subdirectory data into a single combined result. Applies to age-* and bin metrics.")
	cmd.Flags().IntVarP(&backendCfg.NumResults, "num-results", "n", 0, "Limit per-directory results to N (0 = unlimited).")
	cmd.Flags().StringVar(&backendCfg.Order, "order", "", "Sort per-directory results: ASC or DESC.")
	cmd.Flags().Int64Var(&backendCfg.Reftime, "reftime", 0, "Reference epoch (seconds) for age-* metrics. 0 = use current time.")

	return cmd
}
