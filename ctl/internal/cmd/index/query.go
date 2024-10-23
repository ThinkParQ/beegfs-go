package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
)

const queryCmd = "query-index"

func newGenericQueryCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonQueryIndex(bflagSet)
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("db-path", "", "path to dir containing .bdm.db", "-I", ""),
		bflag.Flag("sql-query", "", "Provide sql query", "-s", ""),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

	return cmd
}

func newQueryCmd() *cobra.Command {
	s := newGenericQueryCmd()
	s.Use = "query"
	s.Short = "Query a database file"
	s.Long = `Run an SQL query against a table in a single level directory
Hive db - just point it at the directory containing the database(.bdm.db).

When  providing  SQL  statements to bee you can put more than one SQL
statement in the same string using semicolons at the end of each statement, 
however the only SQL statement that will have output displayed if you have
chosen to display output is the last SQL statement in the string. 
This enables complex things like attaching an  input  database  to join with
on each query (issued at ever level/directory found), 
or other highly powerful but complex things.

Example:
beegfs index query --db-path /index/dir1/ --sql-query "select * from entries"
`
	return s
}

func runPythonQueryIndex(bflagSet *bflag.FlagSet) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+1)
	allArgs = append(allArgs, queryCmd)
	allArgs = append(allArgs, wrappedArgs...)
	cmd := exec.Command(beeBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error starting command: %v", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing beeBinary: %v", err)
	}
	return nil
}
