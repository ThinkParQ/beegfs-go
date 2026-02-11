package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"go.uber.org/zap"
)

func newGenericQueryCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, sqlite3Binary); err != nil {
				return err
			}
			dbPath, err := cmd.Flags().GetString("db-path")
			if err != nil {
				return err
			}
			if dbPath != "" {
				resolved, err := resolveIndexPath(dbPath)
				if err != nil {
					return err
				}
				if err := cmd.Flags().Set("db-path", resolved); err != nil {
					return err
				}
			}
			return runPythonQueryIndex(bflagSet, backend)
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("db-path", "I", "Path to the directory containing the database (.bdm.db file)", "-I", ""),
		bflag.Flag("sql-query", "s", "Provide sql query", "-s", ""),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

	return cmd
}

func newQueryCmd() *cobra.Command {
	s := newGenericQueryCmd()
	s.Use = "query"
	s.Short = "Run an SQL query on a database file."

	s.Long = `Execute an SQL query on a single-level Hive database file (.bdm.db) within a specified directory.

You can provide multiple SQL statements in a single string, separated by semicolons. Only the output 
of the last SQL statement in the string will be displayed. This allows you to 
perform complex operations, such as attaching an input database to join data across queries or applying 
queries at various levels within a directory structure.

Example:

beegfs index query --db-path /index/dir1/ --sql-query "SELECT * FROM entries;"
`
	return s
}

func runPythonQueryIndex(bflagSet *bflag.FlagSet, backend indexBackend) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs))
	allArgs = append(allArgs, wrappedArgs...)
	return runIndexCommandWithPrint(backend, sqlite3Binary, allArgs, "Running GUFI query command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("allArgs", allArgs),
	)
}
