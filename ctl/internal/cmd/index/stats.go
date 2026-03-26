package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newStatsCmd() *cobra.Command {
	backendCfg := indexPkg.StatsCfg{Uid: -1}

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
		Long: `Compute aggregate statistics over the GUFI index tree.

Stat names:
  total-filecount   total number of regular files
  total-dircount    total number of directories
  total-filesize    total bytes used by regular files
  total-linkcount   total number of symbolic links
  filecount         per-directory file count
  filesize          per-directory total file size
  depth             per-directory depth

Global stats (total-*) always traverse the full tree.
Per-directory stats (filecount, filesize, depth) process the root directory
only by default; use --recursive to include all subdirectories.

Example: count all files under /data

  beegfs index stats total-filecount /data

Example: per-directory file counts, sorted descending, top 20

  beegfs index stats filecount --order DESC --num-results 20 /data

Example: total filesize owned by uid 1000

  beegfs index stats total-filesize --uid 1000 /data
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			statName := indexPkg.StatName(args[0])
			pathArgs := args[1:]

			exec, err := newExecutor()
			if err != nil {
				return err
			}
			indexPath, err := resolveIndexPath(pathArgs)
			if err != nil {
				return err
			}

			backendCfg.Stat = statName

			log.Debug("running beegfs index stats",
				zap.String("stat", string(statName)),
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Stats(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			allColumns := []string{"value"}
			defaultColumns := []string{"value"}
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

	cmd.Flags().IntVar(&backendCfg.Uid, "uid", -1, "Filter by numeric user ID (-1 = all users). Applies to total-* stats.")
	cmd.Flags().BoolVarP(&backendCfg.Recursive, "recursive", "r", false, "Include all subdirectories (for filecount, filesize, depth stats).")
	cmd.Flags().IntVarP(&backendCfg.NumResults, "num-results", "n", 0, "Limit per-directory results to N (0 = unlimited).")
	cmd.Flags().StringVar(&backendCfg.Order, "order", "", "Sort per-directory results: ASC or DESC.")

	return cmd
}
