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

func newGenericRescanCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var recurse bool
	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				args = append([]string{cwd}, args...)
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonRescanIndex(args, bflagSet, recurse)
		},
	}
	rescanFlags := []bflag.FlagWrapper{
		bflag.Flag("max-memory", "X", "Max memory usage (e.g. 8GB, 1G)", "-X", ""),
		bflag.Flag("xattrs", "x", "Pull xattrs from source", "-x", false),
		bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
		bflag.GlobalFlag(config.NumWorkersKey, "-n"),
		bflag.GlobalFlag(config.DebugKey, "-V=1"),
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

func runPythonRescanIndex(paths []string, bflagSet *bflag.FlagSet, recurse bool) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	for _, path := range paths {
		allArgs := make([]string, 0, len(wrappedArgs)+4)
		allArgs = append(allArgs, createCmd, "-F", path)
		allArgs = append(allArgs, wrappedArgs...)
		if recurse {
			allArgs = append(allArgs, "-U")
		} else {
			allArgs = append(allArgs, "-k")
		}
		log.Debug("Running BeeGFS Hive Index rescan command",
			zap.String("path", path),
			zap.Bool("recurse", recurse),
			zap.Any("wrappedArgs", wrappedArgs),
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
	}
	treeArgs := []string{createCmd, "-S"}
	requiredFlags := map[string]bool{"-X": true, "-n": true}
	for i := 0; i < len(wrappedArgs); i++ {
		if requiredFlags[wrappedArgs[i]] && i+1 < len(wrappedArgs) {
			treeArgs = append(treeArgs, wrappedArgs[i], wrappedArgs[i+1])
			i++
		}
	}
	log.Debug("Running BeeGFS Hive Index Tree-Summary command",
		zap.Any("Args", treeArgs),
	)
	cmd := exec.Command(beeBinary, treeArgs...)
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
