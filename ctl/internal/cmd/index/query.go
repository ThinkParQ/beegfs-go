package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

const queryCmd = "query-index"

func newGenericQueryCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonQueryIndex(bflagSet)
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

func runPythonQueryIndex(bflagSet *bflag.FlagSet) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+1)
	allArgs = append(allArgs, queryCmd)
	allArgs = append(allArgs, wrappedArgs...)
	log.Debug("Running BeeGFS Hive Index query command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("queryCmd", queryCmd),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(beeBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing index command: %w", err)
	}
	return nil
}
