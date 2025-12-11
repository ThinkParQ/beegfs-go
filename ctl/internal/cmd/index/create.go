package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericCreateCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonCreateIndex(bflagSet)
		},
	}

	bflagSet = bflag.NewFlagSet(commonIndexFlags, cmd)
	applyIndexRootDefault(cmd)
	cmd.MarkFlagRequired("fs-path")
	cmd.MarkFlagRequired("index-path")
	cmd.MarkFlagsMutuallyExclusive("summary", "only-summary")
	return cmd
}

func newCreateCmd() *cobra.Command {
	s := newGenericCreateCmd()
	s.Use = "create"
	s.Short = "Generates or updates the index for the specified file system."
	s.Long = `Generate or updates the index by traversing the source directory.

The index can exist within the source directory or in a separate index directory.
The program performs a breadth-first readdirplus traversal to list the contents, or it creates
an output database and/or files listing directories and files it encounters. This program serves two main purposes:

1. To identify directories with changes, allowing incremental updates to a Hive index from changes in the source file system.
2. To create a comprehensive dump of directories, files, and links. You can choose to output in traversal order
   (each directory followed by its files) or to stride inodes across multiple files for merging with inode-strided attribute lists.

Example: Create or update the index for the file system at /mnt/fs, limiting memory usage to 8GB:

$ beegfs index create --target-memory 8GB --fs-path /mnt/fs --index-path /mnt/index_parent
`
	return s
}

func runPythonCreateIndex(bflagSet *bflag.FlagSet) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	var fsPath string
	var indexPath string
	var summary bool
	var onlySummary bool
	// Convert legacy -F/-I flags into positional arguments expected by gufi_dir2index.
	filteredArgs := make([]string, 0, len(wrappedArgs))
	treeArgs := make([]string, 0, len(wrappedArgs))
	for i := 0; i < len(wrappedArgs); i++ {
		switch wrappedArgs[i] {
		case "-F":
			if i+1 >= len(wrappedArgs) {
				return fmt.Errorf("missing value for --fs-path")
			}
			fsPath = wrappedArgs[i+1]
			i++
		case "-I":
			if i+1 >= len(wrappedArgs) {
				return fmt.Errorf("missing value for --index-path")
			}
			indexPath = wrappedArgs[i+1]
			i++
		case "-s":
			summary = true
		case "-S":
			onlySummary = true
		case "-n":
			if i+1 >= len(wrappedArgs) {
				return fmt.Errorf("missing value for --threads/-n")
			}
			filteredArgs = append(filteredArgs, wrappedArgs[i], wrappedArgs[i+1])
			treeArgs = append(treeArgs, wrappedArgs[i], wrappedArgs[i+1])
			i++
		case "--debug", "-H":
			filteredArgs = append(filteredArgs, wrappedArgs[i])
			treeArgs = append(treeArgs, wrappedArgs[i])
		case "-v", "--version":
			filteredArgs = append(filteredArgs, wrappedArgs[i])
			treeArgs = append(treeArgs, wrappedArgs[i])
		default:
			filteredArgs = append(filteredArgs, wrappedArgs[i])
		}
	}

	if onlySummary {
		return runTreeSummary(log, treeArgs, indexPath, wrappedArgs)
	}

	if err := runDir2Index(log, filteredArgs, fsPath, indexPath, wrappedArgs); err != nil {
		return err
	}
	if summary {
		return runTreeSummary(log, treeArgs, indexPath, wrappedArgs)
	}
	return nil
}

func runDir2Index(log *logger.Logger, args []string, fsPath, indexPath string, wrappedArgs []string) error {
	allArgs := make([]string, 0, len(args)+2)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, fsPath, indexPath)
	log.Debug("Running BeeGFS Hive Index create command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.String("fsPath", fsPath),
		zap.String("indexPath", indexPath),
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

func runTreeSummary(log *logger.Logger, args []string, indexPath string, wrappedArgs []string) error {
	allArgs := make([]string, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, indexPath)
	log.Debug("Running BeeGFS Hive Tree Summary command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.String("indexPath", indexPath),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(treeSummaryBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start tree summary command: %w", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing tree summary command: %w", err)
	}
	return nil
}
