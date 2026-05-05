package index

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newRescanCmd() *cobra.Command {
	backendCfg := indexPkg.RescanCfg{}
	var indexPath string
	var indexRoot string

	cmd := &cobra.Command{
		Use:   "rescan <directory-path> [<directory-path>...]",
		Short: "Rebuilds the index for a directory using gufi_dir2index with the BeeGFS plugin.",
		Long: `Rebuild the GUFI index for one or more directories using gufi_dir2index.

Two modes are supported:

  rescan (non-recursive, default)
    Reindexes only the specified directory (--max-level 0).
    Files and subdirectory entries in that single directory are updated.

  rescan --recurse
    Reindexes the full subtree rooted at the specified directory.

When multiple paths are given they must be siblings at the same depth
within the index tree (i.e. share the same parent directory in the index).

Example: rescan a single subdirectory (non-recursive)

  beegfs index rescan /mnt/fs/subdir

Example: recursively rescan a subtree

  beegfs index rescan --recurse /mnt/fs/subdir

Example: rescan the entire filesystem index

  beegfs index rescan --recurse /mnt/fs
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			if indexRoot != "" {
				viper.Set(indexPkg.IndexRootKey, indexRoot)
			}

			paths := args
			if len(paths) == 0 {
				if indexPath != "" {
					// Derive the BeeGFS FS path from the explicitly provided index path.
					fsPath, err := fsDirFromIndexPath(indexPath)
					if err != nil {
						return err
					}
					paths = []string{fsPath}
				} else {
					cwd, err := os.Getwd()
					if err != nil {
						return fmt.Errorf("getting working directory: %w", err)
					}
					paths = []string{cwd}
				}
			}
			backendCfg.Paths = paths
			applyDotIndexDefaults(paths[0])

			if indexPath == "" {
				idx, err := resolveIndexPath(paths)
				if err != nil {
					return err
				}
				indexPath = idx
			}

			if err := checkIndexExists(indexPath); err != nil {
				return err
			}

			log.Debug("running beegfs index rescan",
				zap.Strings("paths", paths),
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			lines, errWait, err := indexPkg.Rescan(cmd.Context(), backendCfg, indexPath)
			if err != nil {
				return err
			}

		run:
			for {
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case line, ok := <-lines:
					if !ok {
						break run
					}
					fmt.Println(line)
				}
			}

			return errWait()
		},
	}

	cmd.Flags().StringVarP(&indexPath, "index-path", "I", "", "GUFI index path for the target directory (default: derived from path + index-root config).")
	cmd.Flags().StringVar(&indexRoot, "index-root", "", "Root path of the GUFI index tree (overrides .beegfs.index and config defaults).")
	cmd.Flags().BoolVar(&backendCfg.Recurse, "recurse", false, "Recursively rescan all subdirectories beneath the specified path.")
	cmd.Flags().IntVarP(&backendCfg.Threads, "threads", "n", 0, "Number of threads (default: index-threads from config).")
	cmd.Flags().BoolVar(&backendCfg.SkipTreesummary, "skip-treesummary", false, "Skip running gufi_treesummary after rescan.")
	cmd.Flags().BoolVarP(&backendCfg.Xattrs, "xattrs", "x", false, "Index extended attributes.")

	return cmd
}
