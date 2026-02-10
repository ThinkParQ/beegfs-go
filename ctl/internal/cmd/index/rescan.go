package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericRescanCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var recurse bool
	var cmd = &cobra.Command{
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, beeBinary); err != nil {
				return err
			}
			if err := checkIndexConfig(backend, treeSummaryBinary); err != nil {
				return err
			}
			indexPath, ok := getGUFIConfigValue("IndexRoot")
			if !ok || indexPath == "" {
				return fmt.Errorf("IndexRoot not found in %s", indexConfig)
			}
			paths, err := resolvePaths(args)
			if err != nil {
				return err
			}
			return runPythonRescanIndex(paths, bflagSet, recurse, backend, indexPath)
		},
	}
	rescanFlags := []bflag.FlagWrapper{
		bflag.Flag("max-memory", "X", "Target memory utilization (soft limit)", "--target-memory", ""),
		bflag.Flag("xattrs", "x", "Pull xattrs from source", "-x", false),
		bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
		bflag.GlobalFlag(config.NumWorkersKey, "-n"),
		bflag.GlobalFlag(config.DebugKey, "-H"),
		bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
	}
	bflagSet = bflag.NewFlagSet(rescanFlags, cmd)
	cmd.Flags().BoolVar(&recurse, "recurse", false, "Recursively rescan all directories beneath the specified path.")

	return cmd
}

func newRescanCmd() *cobra.Command {
	s := newGenericRescanCmd()
	s.Use = "rescan <directory-path>"
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

$ beegfs index rescan sub-dir1/ sub-dir2/
`

	return s
}

func runPythonRescanIndex(paths []string, bflagSet *bflag.FlagSet, recurse bool, backend indexBackend, indexPath string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	baseArgs := buildDir2IndexBaseArgs(wrappedArgs)
	if !recurse {
		baseArgs = append(baseArgs, "--max-level", "1")
	}
	tbl := newIndexLinePrintomatic("line")
	allArgs := make([]string, 0, len(baseArgs)+len(paths)+1)
	allArgs = append(allArgs, baseArgs...)
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, indexPath)
	log.Debug("Running GUFI dir2index command",
		zap.Bool("recurse", recurse),
		zap.String("indexAddr", indexAddr),
		zap.String("indexPath", indexPath),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	if err := runIndexCommandPrintLines(backend, beeBinary, allArgs, &tbl); err != nil {
		return err
	}
	treeArgs, err := buildTreeSummaryArgs(indexPath, treeSummaryOptions{
		threads: viper.GetInt(config.NumWorkersKey),
		debug:   viper.GetBool(config.DebugKey),
	})
	if err != nil {
		return err
	}
	log.Debug("Running GUFI tree summary command",
		zap.String("indexAddr", indexAddr),
		zap.Any("Args", treeArgs),
	)
	return runIndexCommandPrintLines(backend, treeSummaryBinary, treeArgs, &tbl)
}
