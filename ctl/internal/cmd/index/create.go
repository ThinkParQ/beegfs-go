package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newCreateCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.CreateCfg{}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Generates or updates the index for the specified file system.",
		Args:  cobra.NoArgs,
		Long: `Generate a new index by traversing the source directory.

The index is created at <index-root>/<basename(fs-path)>. Tree summary
is computed automatically after indexing unless --skip-treesummary is set.

The filesystem path, index root and address come from the
.beegfs.index entry when --fs-path/--index-root/--index-addr are not given.

Example: create an index for /mnt/beegfs at /mnt/index
  (index lands at /mnt/index/beegfs)

  beegfs index create --fs-path /mnt/beegfs --index-root /mnt/index
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()
			if backendCfg.FSPath == "" {
				backendCfg.FSPath = indexPkg.DotIndexPath(discoverBeeGFSMountPath())
			}
			if backendCfg.FSPath == "" {
				return fmt.Errorf("no filesystem path to index: pass --fs-path or configure a path in %s at the BeeGFS mount root", indexPkg.DotIndexFileName)
			}
			backendCfg.GlobalCfg = resolveGlobalCfg(cmd.Context(), *globalCfg, backendCfg.FSPath)
			log.Debug("running beegfs index create", zap.Any("cfg", backendCfg))

			lines, errWait, err := indexPkg.Create(cmd.Context(), backendCfg)
			if err != nil {
				return err
			}

			if err := drain(cmd.Context(), lines, errWait, func(line string) {
				fmt.Println(line)
			}); err != nil {
				return err
			}
			fmt.Printf("Indexed filesystem path: %s\n", backendCfg.FSPath)
			fmt.Printf("Index root:              %s\n", backendCfg.IndexRoot)
			return nil
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().StringVarP(&backendCfg.FSPath, "fs-path", "F", "", "Source filesystem path to index.")
	cmd.Flags().BoolVar(&backendCfg.SkipTreesummary, "skip-treesummary", false, "Skip computing tree summary after indexing.")
	cmd.Flags().BoolVarP(&backendCfg.Xattrs, "xattrs", "x", false, "Index extended attributes.")
	cmd.Flags().BoolVarP(&backendCfg.NoMetadata, "no-beegfs-metadata", "B", false, "Skip BeeGFS plugin (do not collect BeeGFS metadata).")

	return cmd
}
