package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericLsCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			paths := args
			if len(paths) == 0 {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				paths = []string{cwd}
			}

			return runPythonLsIndex(bflagSet, paths)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("all", "a", "Do not ignore entries starting with .", "-a", false),
		bflag.Flag("almost-all", "A", "Do not list implied . and ..", "-A", false),
		bflag.Flag("block-size", "", "With -l, scale sizes by SIZE when printing them", "--block-size", ""),
		bflag.Flag("ignore-backups", "B", "Do not list implied entries ending with ~", "--ignore-backups", false),
		bflag.Flag("full-time", "", "Like -l --time-style=full-iso", "--full-time", false),
		bflag.Flag("no-group", "G", "In a long listing, don't print group names", "--no-group", false),
		bflag.Flag("human-readable", "h", "With -l and -s, print sizes like 1K 234M 2G etc.", "-h", false),
		bflag.Flag("inode", "i", "Print the index number of each file", "-i", false),
		bflag.Flag("long-listing", "l", "Use a long listing format", "-l", false),
		bflag.Flag("reverse", "r", "Reverse order while sorting", "-r", false),
		bflag.Flag("recursive", "R", "List subdirectories recursively", "-R", false),
		bflag.Flag("size", "s", "Print the allocated size of each file, in blocks", "-s", false),
		bflag.Flag("sort-largest", "S", "Sort by file size, largest first", "-S", false),
		bflag.Flag("version", "v", "BeeGFS Hive Index Version", "--version", false),
		bflag.Flag("time-style", "", "Time/date format with -l", "--time-style", ""),
		bflag.Flag("mtime", "t", "Sort by modification time, newest first", "-t", false),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
		bflag.Flag("in-memory-name", "", "In-memory name", "--in-memory-name", "out"),
		bflag.Flag("aggregate-name", "", "Name of final database when aggregation is performed", "--aggregate-name", ""),
		bflag.Flag("skip-file", "", "Name of file containing directory basenames to skip", "--skip-file", ""),
		bflag.Flag("verbose", "V", "Show the gufi_query being executed", "--verbose", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.PersistentFlags().BoolP("help", "", false, "Help for ls")
	cmd.Flags().MarkHidden("in-memory-name")
	return cmd
}

func newLsCmd() *cobra.Command {
	s := newGenericLsCmd()
	s.Use = "ls"
	s.Short = "Lists the contents of the index directory."

	s.Long = `Displays the contents of the index directory.

This command works similarly to the standard "ls" command, supporting both absolute and relative paths. 
You can use it from within the index directory or from a filesystem directory when specifying relative paths.

Example: List the contents of the index directory at /mnt/index.

$ beegfs index ls /mnt/index
`
	return s
}

func runPythonLsIndex(bflagSet *bflag.FlagSet, paths []string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths)+2)
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, wrappedArgs...)
	outputFormat := viper.GetString(config.OutputKey)
	if outputFormat != "" && outputFormat != config.OutputTable.String() {
		allArgs = append(allArgs, "-Q", outputFormat)
	}
	log.Debug("Running BeeGFS Hive Index ls command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("lsBinary", lsBinary),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(lsBinary, allArgs...)

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
