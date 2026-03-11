package index

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericLsCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			beegfsEnabled, err := cmd.Flags().GetBool("beegfs")
			if err != nil {
				return err
			}
			binaries := []string{lsBinary}
			if beegfsEnabled {
				binaries = append(binaries, queryBinary)
			}
			if err := checkIndexConfig(backend, binaries...); err != nil {
				return err
			}
			delim, err := cmd.Flags().GetString("delim")
			if err != nil {
				return err
			}
			if beegfsEnabled {
				delim, err = normalizeBeeGFSLsDelim(delim)
				if err != nil {
					return err
				}
			}
			recursive, err := cmd.Flags().GetBool("recursive")
			if err != nil {
				return err
			}
			paths, err := defaultBeeGFSPaths(backend, args)
			if err != nil {
				return err
			}
			return runPythonLsIndex(bflagSet, backend, paths, beegfsEnabled, delim, recursive)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("all", "a", "Do not ignore entries starting with .", "-a", false),
		bflag.Flag("almost-all", "A", "Do not list implied . and ..", "-A", false),
		bflag.Flag("block-size", "", "{K,KB,KiB,M,MB,MiB,G,GB,GiB,T,TB,TiB,P,PB,PiB,E,EB,EiB,Z,ZB,ZiB,Y,YB,YiB}, With -l, scale sizes by SIZE when printing them", "--block-size", ""),
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
		bflag.Flag("no-sort", "U", "Do not sort; list entries in directory order", "-U", false),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", "\t"),
		bflag.Flag("in-memory-name", "", "In-memory name", "--in-memory-name", "out"),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().BoolP("beegfs", "b", false, "Print BeeGFS metadata for the file(s)")
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

func runPythonLsIndex(bflagSet *bflag.FlagSet, backend indexBackend, paths []string, beegfsEnabled bool, delim string, recursive bool) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths))
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, wrappedArgs...)
	log.Debug("Running GUFI ls command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	if !beegfsEnabled {
		tbl := newIndexLinePrintomatic("ls_line")
		return runIndexCommand(backend, lsBinary, allArgs, func(r io.Reader) error {
			return streamIndexLines(r, &tbl)
		})
	}
	indexPaths := paths
	tbl := newIndexLinePrintomatic("ls_line")
	queryStart := time.Now()
	meta, err := fetchBeeGFSMetadata(backend, indexPaths, indexPaths, delim, recursive)
	if err != nil {
		return err
	}
	queryDur := time.Since(queryStart)

	processStart := time.Now()
	err = runIndexCommand(backend, lsBinary, allArgs, func(r io.Reader) error {
		return mergeBeeGFSLsOutput(r, meta, delim, indexPaths, &tbl)
	})
	if err != nil {
		return err
	}
	processDur := time.Since(processStart)
	log.Debug("BeeGFS metadata timings",
		zap.Duration("queryDuration", queryDur),
		zap.Duration("mergeDuration", processDur),
	)
	return nil
}

func normalizeBeeGFSLsDelim(delim string) (string, error) {
	if delim == "" {
		return "", fmt.Errorf("--delim cannot be empty when --beegfs is set")
	}
	if delim == "\t" {
		return delim, nil
	}
	if strings.ContainsAny(delim, " \t\r\n") {
		return "", fmt.Errorf("--delim must not contain whitespace when --beegfs is set")
	}
	return delim, nil
}
