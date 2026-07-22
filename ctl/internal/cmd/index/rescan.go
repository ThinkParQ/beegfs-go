package index

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newRescanCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.RescanCfg{}

	cmd := &cobra.Command{
		Use:   "rescan <directory-path> [<directory-path>...]",
		Short: "Rebuild the index for one or more directories",
		Long: `Rebuild the index for one or more directories.

All paths must lie within the single indexed BeeGFS mount. The index has one
root, and every path is resolved beneath it; the tree summary exists only at
that index root, so one refresh after the rescan covers every path updated in
the same invocation.

Two modes are supported:

  rescan (non-recursive, default)
    Reindexes only the specified directory (--max-level 0).
    Files and subdirectory entries in that single directory are updated.

  rescan --recurse
    Reindexes the full subtree rooted at the specified directory.

Multiple paths may be passed in a single invocation; each must be within the
indexed mount and is reindexed in turn.

Example: rescan a single subdirectory (non-recursive)

  beegfs index rescan /mnt/beegfs/subdir

Example: recursively rescan a subtree

  beegfs index rescan --recurse /mnt/beegfs/subdir

Example: rescan multiple unrelated directories

  beegfs index rescan /mnt/beegfs/projA/data /mnt/beegfs/projB/logs

Example: rescan the entire filesystem index

  beegfs index rescan --recurse /mnt/beegfs
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			paths := args
			if len(paths) == 0 {
				cwd, err := os.Getwd()
				if err != nil {
					return fmt.Errorf("getting working directory: %w", err)
				}
				paths = []string{cwd}
			}

			cfg := resolveGlobalCfg(cmd.Context(), *globalCfg, paths[0])
			backendCfg.GlobalCfg = cfg

			targets := make([]indexPkg.RescanTarget, 0, len(paths))
			for _, p := range paths {
				idx, err := resolveFSPathToIndex(cfg, p)
				if err != nil {
					return fmt.Errorf("resolving index path for %q: %w", p, err)
				}
				if err := checkIndexExists(cfg, idx); err != nil && !errors.Is(err, errLegacyIndex) {
					return err
				}
				targets = append(targets, indexPkg.RescanTarget{FSPath: p, IndexPath: idx})
			}
			backendCfg.Targets = targets

			log.Debug("running beegfs index rescan",
				zap.Any("targets", targets),
				zap.Any("cfg", backendCfg),
			)

			lines, errWait, err := indexPkg.Rescan(cmd.Context(), backendCfg)
			if err != nil {
				return err
			}

			return drain(cmd.Context(), lines, errWait, func(line string) {
				fmt.Println(line)
			})
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().BoolVar(&backendCfg.Recurse, "recurse", false, "Recursively rescan all subdirectories beneath the specified path.")
	cmd.Flags().BoolVar(&backendCfg.SkipTreesummary, "skip-treesummary", false, "Skip computing tree summary after rescan.")
	cmd.Flags().BoolVarP(&backendCfg.Xattrs, "xattrs", "x", false, "Index extended attributes.")

	return cmd
}
