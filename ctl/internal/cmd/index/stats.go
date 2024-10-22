package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const statsCmd = "stats"

var path, stat string

type createStatsConfig struct {
	delim string
}

func newGenericStatsCmd() *cobra.Command {
	cfg := createStatsConfig{}
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
			return runPythonExecStats(bflagSet, &cfg, stat, path)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("recursive", "r", "Run command recursively", "-r", false),
		bflag.Flag("cumulative", "c", "Return cumulative values", "-c", false),
		bflag.Flag("order", "", "Sort output (if applicable)", "--order", "ASC"),
		bflag.Flag("num-results", "", "Limit the number of results", "--num-results", 0),
		bflag.Flag("uid", "", "Restrict to user", "--uid", ""),
		bflag.Flag("user", "", "Restrict to user", "--user", ""),
		bflag.Flag("version", "v", "Version of the find command.", "--version", false),
		bflag.Flag("in-memory-name", "", "In-memory name for processing.", "--in-memory-name", "out"),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().StringVar(&cfg.delim, "delim", " ", "Delimiter separating output columns")
	cmd.MarkFlagsMutuallyExclusive("uid", "user")
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newStatsCmd() *cobra.Command {
	s := newGenericStatsCmd()
	s.Use = "stats"
	s.Short = "Calculates stats of index directory"
	s.Long = `Generate the index by walking the source directory.

The stats subcommand allows users to obtain filesystem statistics
like the total number of files, directories or links in the directory hierarchy,
files, directories, or links per level, maximum and minimum file sizes..

Example: Get the total number of files under a directory
$ beegfs index stats total-filecount

positional arguments:
  {depth,filesize,filecount,linkcount,dircount,leaf-dirs,leaf-depth,leaf-files,leaf-links,total-filesize,total-filecount,total-linkcount,total-dircount,total-leaf-files,total-leaf-links,files-per-level,links-per-level,dirs-per-level,average-leaf-files,average-leaf-links,median-leaf-files,duplicate-names}
`
	return s

}

func runPythonExecStats(bflagSet *bflag.FlagSet, cfg *createStatsConfig, stat,
	path string) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+4)
	allArgs = append(allArgs, statsCmd, stat, path)
	if cfg.delim != "" {
		allArgs = append(allArgs, "--delim", cfg.delim)
	}
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
