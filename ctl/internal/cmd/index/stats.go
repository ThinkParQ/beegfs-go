package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"go.uber.org/zap"
)

func newGenericStatsCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, statsBinary); err != nil {
				return err
			}
			if len(args) < 1 {
				return fmt.Errorf("stat argument is required")
			}
			statArg := args[0]
			var pathArgs []string
			if len(args) > 1 {
				pathArgs = []string{args[1]}
			}
			path, err := defaultIndexPath(backend, pathArgs)
			if err != nil {
				return err
			}
			return runPythonExecStats(bflagSet, backend, statArg, path)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("recursive", "r", "Run command recursively", "-r", false),
		bflag.Flag("cumulative", "c", "Return cumulative values", "-c", false),
		bflag.Flag("order", "", "Sort output (if applicable)", "--order", "ASC"),
		bflag.Flag("num-results", "n", "Limit the number of results", "--num-results", 0),
		bflag.Flag("uid", "", "Restrict to a user uid", "--uid", ""),
		bflag.Flag("version", "v", "Version of the find command.", "--version", false),
		bflag.Flag("in-memory-name", "", "In-memory name for processing.", "--in-memory-name", "out"),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newStatsCmd() *cobra.Command {
	s := newGenericStatsCmd()
	s.Use = "stats"
	s.Short = "Calculate statistics of the index directory."

	s.Long = `Generates statistics by traversing the index directory hierarchy.

The stats subcommand provides various filesystem statistics, such as the total 
count of files, directories, or links, as well as distribution per level, maximum 
and minimum file sizes, and other key metrics.

Example: Get the total file count in a directory

$ beegfs index stats total-filecount

Positional arguments:
  {depth, filesize, filecount, linkcount, dircount, leaf-dirs, leaf-depth, 
   leaf-files, leaf-links, total-filesize, total-filecount, total-linkcount, 
   total-dircount, total-leaf-files, total-leaf-links, files-per-level, 
   links-per-level, dirs-per-level, average-leaf-files, average-leaf-links, 
   median-leaf-files, duplicate-names}
`
	return s
}

func runPythonExecStats(bflagSet *bflag.FlagSet, backend indexBackend, stat, path string) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+3)
	allArgs = append(allArgs, stat, path)
	allArgs = append(allArgs, wrappedArgs...)
	return runIndexCommandWithPrint(backend, statsBinary, allArgs, "Running GUFI stats command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("stat", stat),
		zap.String("path", path),
		zap.Any("allArgs", allArgs),
	)
}
