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

const statsCmd = "stats"

var stat string

func newGenericStatsCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			if len(args) < 1 {
				return fmt.Errorf("stat argument is required")
			}
			stat = args[0]
			if len(args) > 1 {
				path = args[1]
			} else {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				beegfsClient, err := config.BeeGFSClient(cwd)
				if err != nil {
					return err
				}
				path = beegfsClient.GetMountPath()
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonExecStats(bflagSet, stat, path)
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

func runPythonExecStats(bflagSet *bflag.FlagSet, stat, path string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+3)
	allArgs = append(allArgs, statsCmd, stat, path)
	allArgs = append(allArgs, wrappedArgs...)
	log.Debug("running index stats", zap.Any("args", allArgs))
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
