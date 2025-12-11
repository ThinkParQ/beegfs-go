package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericRescanCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var recurse bool
	var cmd = &cobra.Command{
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runRescanIndex(bflagSet, recurse)
		},
	}
	rescanFlags := []bflag.FlagWrapper{
		bflag.Flag("fs-path", "F", "File system path for which index will be updated.", "-F", ""),
		bflag.Flag("index-path", "I", "File system path at which the index is stored.", "-I", ""),
		bflag.Flag("index-xattrs", "x", "Index xattrs", "-x", false),
		bflag.Flag("plugin", "", "Plugin library for modifying database entries", "--plugin", ""),
		bflag.Flag("swap-prefix", "", "File name prefix for swap files", "--swap-prefix", ""),
		bflag.Flag("compress", "", "Compress work items", "--compress", false),
		bflag.Flag("validate-external-dbs", "q", "Check that external databases are valid before tracking during indexing", "-q", false),
		bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
		bflag.GlobalFlag(config.NumWorkersKey, "-n"),
		bflag.GlobalFlag(config.DebugKey, "--debug"),
	}
	bflagSet = bflag.NewFlagSet(rescanFlags, cmd)
	applyIndexRootDefault(cmd)
	cmd.MarkFlagRequired("fs-path")
	cmd.MarkFlagRequired("index-path")
	cmd.Flags().BoolVar(&recurse, "recurse", false, "Recursively rescan all directories beneath the specified path.")

	return cmd
}

func newRescanCmd() *cobra.Command {
	s := newGenericRescanCmd()
	s.Use = "rescan"
	s.Short = "Updates the index for a specific subdirectory of a previously indexed filesystem."
	s.Long = `The rescan command allows users to refresh the metadata for a subdirectory, ensuring that newly created files and directories are indexed, and stale entries (files or directories deleted from the filesystem but still present in the index) are removed.

Two modes are supported:

1. Rescan (non-recursive)
   - Updates metadata for the specified subdirectory.
   - Indexes newly created files within the subdirectory.
   - Detects and indexes newly created immediate child subdirectories.
   - Deletes stale immediate child subdirectories from the index.
   - Does not update existing child subdirectories that were already indexed.

2. Rescan with recursion
   - Recursively updates the index for the entire subdirectory tree.
   - Updates metadata for all files and subdirectories, including existing indexed subdirectories.
   - Detects and indexes newly created files and directories at all levels.
   - Removes stale directories and files from the index.

Example: Rescan the Index for contents in a subdirectory.

$ beegfs index rescan --fs-path /mnt/fs/sub-dir1 --index-path /mnt/index_root --recurse
After rescanning, a tree summary is generated for the index root.
`

	return s
}

func runRescanIndex(bflagSet *bflag.FlagSet, recurse bool) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	var fsPath string
	var indexPath string
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

	if fsPath == "" {
		return fmt.Errorf("missing value for --fs-path")
	}
	if indexPath == "" {
		return fmt.Errorf("missing value for --index-path")
	}

	cfg, err := loadIndexConfig()
	if err != nil {
		return fmt.Errorf("loading index config: %w", err)
	}

	if !recurse {
		filteredArgs = append(filteredArgs, "--max-level", "0")
	}

	log.Debug("Running BeeGFS Hive Index rescan command",
		zap.String("fsPath", fsPath),
		zap.String("indexPath", indexPath),
		zap.Bool("recurse", recurse),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("filteredArgs", filteredArgs),
	)

	if err := runDir2Index(log, filteredArgs, fsPath, indexPath, wrappedArgs); err != nil {
		return err
	}

	var summaryPath string
	if cfg != nil {
		summaryPath = cfg.IndexRoot
	}
	if summaryPath == "" {
		log.Info("Skipping tree summary because index root is not configured")
		return nil
	}

	return runTreeSummary(log, treeArgs, summaryPath, wrappedArgs)
}
