package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newQueryCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.QueryCfg{}

	cmd := &cobra.Command{
		Use:         "query [path]",
		Short:       "Run an SQL query against the index",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if backendCfg.SQLEntries == "" && backendCfg.SQLSummary == "" &&
				backendCfg.SQLAggregate == "" && backendCfg.SQLIntermed == "" {
				return fmt.Errorf("at least one of --sql-entries, --sql-summary, --sql-intermed, or --sql-aggregate is required")
			}
			if backendCfg.AddUp < 0 || backendCfg.AddUp > 2 {
				return fmt.Errorf("--add-up must be 0, 1, or 2 (got %d)", backendCfg.AddUp)
			}
			return nil
		},
		Long: `Execute raw SQL against the index tree.

Use --sql-entries (-E) for per-entry queries and --sql-summary (-S) for
per-directory summary queries. Aggregation flags (-I/-K/-J/-G/-F) are
also supported for multi-stage pipelines.

Example: list all .c files from the index

  beegfs index query -E "SELECT rpath(sname,sroll,name) FROM vrpentries WHERE name GLOB '*.c'"
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			cfg, args := resolveCfgAndPaths(cmd.Context(), globalCfg, args)
			backendCfg.GlobalCfg = cfg
			indexPath, err := resolveAndCheckIndexPath(cfg, args)
			if err != nil {
				return err
			}
			exec, err := indexPkg.NewExecutor(cfg)
			if err != nil {
				return err
			}

			log.Debug("running beegfs index query",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Query(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			var tbl *cmdfmt.Printomatic
			var nCols int
			var arityErr error
			defer func() {
				if tbl != nil {
					tbl.PrintRemaining()
				}
			}()

			if err := drain(cmd.Context(), rows, errWait, func(row []string) {
				if tbl == nil {
					nCols = len(row)
					cols := genericColumns(nCols)
					p := cmdfmt.NewPrintomatic(cols, cols)
					tbl = &p
				}
				if len(row) != nCols {
					if arityErr == nil {
						arityErr = fmt.Errorf("query returned a row with %d columns but the first had %d; mixed-arity output is not supported (check your SQL)", len(row), nCols)
					}
					return
				}
				tbl.AddItem(toAny(padRow(row, nCols))...)
			}); err != nil {
				return err
			}
			return arityErr
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().StringVarP(&backendCfg.SQLInit, "sql-init", "I", "", "SQL init: run once on the aggregate DB before any directory queries.")
	cmd.Flags().StringVarP(&backendCfg.SQLTreeSum, "sql-treesum", "T", "", "SQL for tree-summary table (per-directory aggregate row).")
	cmd.Flags().StringVarP(&backendCfg.SQLSummary, "sql-summary", "S", "", "SQL for summary table (per-directory).")
	cmd.Flags().StringVarP(&backendCfg.SQLEntries, "sql-entries", "E", "", "SQL for entries table (per-file).")
	cmd.Flags().StringVarP(&backendCfg.SQLIntermed, "sql-intermed", "J", "", "SQL for intermediate per-thread results.")
	cmd.Flags().StringVarP(&backendCfg.SQLAggInit, "sql-agg-init", "K", "", "SQL to create the final aggregation table.")
	cmd.Flags().StringVarP(&backendCfg.SQLAggregate, "sql-aggregate", "G", "", "SQL for aggregated results (streams to stdout).")
	cmd.Flags().StringVarP(&backendCfg.SQLFinal, "sql-final", "F", "", "SQL cleanup: runs on the aggregate DB after -G; output is not surfaced.")
	cmd.Flags().IntVarP(&backendCfg.AddUp, "add-up", "a", 0, "AFlag mode mirroring gufi_query -a: 0=RUN_ON_ROW (default; skip -E when -S returns no rows), 1=RUN_SE (run -S and -E unconditionally), 2=RUN_TSE (run -T, -S, and -E unconditionally). Required when both -S and -E are INSERTs.")

	return cmd
}
