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

func newQueryCmd() *cobra.Command {
	backendCfg := indexPkg.QueryCfg{}

	cmd := &cobra.Command{
		Use:         "query [path]",
		Short:       "Run an SQL query against the GUFI index.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if backendCfg.SQLEntries == "" && backendCfg.SQLSummary == "" {
				return fmt.Errorf("at least one of --sql-entries or --sql-summary is required")
			}
			return nil
		},
		Long: `Execute raw SQL against the GUFI index tree.

Use --sql-entries (-E) for per-entry queries and --sql-summary (-S) for
per-directory summary queries. Aggregation flags (-I/-K/-J/-G/-F) are
also supported for multi-stage pipelines.

Example: list all .c files from the index

  beegfs index query -E "SELECT rpath(sname,sroll,name) FROM vrpentries WHERE name GLOB '*.c'"
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

			log.Debug("running beegfs index query",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Query(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			allColumns := []string{"result"}
			defaultColumns := []string{"result"}
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

	cmd.Flags().StringVarP(&backendCfg.SQLEntries, "sql-entries", "E", "", "SQL to run against each entries table.")
	cmd.Flags().StringVarP(&backendCfg.SQLSummary, "sql-summary", "S", "", "SQL to run against each summary table.")
	cmd.Flags().StringVarP(&backendCfg.SQLInit, "sql-init", "I", "", "SQL to initialise the aggregate DB.")
	cmd.Flags().StringVarP(&backendCfg.SQLAggInit, "sql-agg-init", "K", "", "SQL to initialise per-thread aggregate table.")
	cmd.Flags().StringVarP(&backendCfg.SQLIntermed, "sql-intermed", "J", "", "SQL to insert directory result into per-thread aggregate.")
	cmd.Flags().StringVarP(&backendCfg.SQLAggregate, "sql-aggregate", "G", "", "SQL to merge per-thread aggregate into global aggregate.")
	cmd.Flags().StringVarP(&backendCfg.SQLFinal, "sql-final", "F", "", "Final SELECT from aggregate DB.")

	return cmd
}
