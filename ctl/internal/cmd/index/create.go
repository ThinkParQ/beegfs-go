package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newCreateCmd() *cobra.Command {
	backendCfg := indexPkg.CreateCfg{}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Generates or updates the index for the specified file system.",
		Args: func(cmd *cobra.Command, args []string) error {
			if backendCfg.FSPath == "" {
				return fmt.Errorf("--fs-path is required")
			}
			if backendCfg.IndexPath == "" {
				return fmt.Errorf("--index-path is required")
			}
			return nil
		},
		Long: `Generate a new GUFI index by traversing the source directory.

The index is created at --index-path. Optionally run gufi_treesummary
after indexing for improved query performance.

Example: create an index for /mnt/fs at /mnt/index

  beegfs index create --fs-path /mnt/fs --index-path /mnt/index
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()
			applyDotIndexDefaults(backendCfg.FSPath)
			log.Debug("running beegfs index create", zap.Any("cfg", backendCfg))

			lines, errWait, err := indexPkg.Create(cmd.Context(), backendCfg)
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

	cmd.Flags().StringVarP(&backendCfg.FSPath, "fs-path", "F", "", "Source filesystem path to index. (required)")
	cmd.Flags().StringVarP(&backendCfg.IndexPath, "index-path", "I", "", "Destination path for the GUFI index. (required)")
	cmd.Flags().IntVarP(&backendCfg.Threads, "threads", "n", 0, "Number of indexing threads (default: index-threads from config).")
	cmd.Flags().BoolVarP(&backendCfg.Summary, "summary", "s", false, "Run gufi_treesummary on the index root after indexing.")
	cmd.Flags().BoolVarP(&backendCfg.Xattrs, "xattrs", "x", false, "Index extended attributes.")
	cmd.Flags().BoolVarP(&backendCfg.NoMetadata, "no-metadata", "B", false, "Skip BeeGFS plugin (do not collect BeeGFS metadata).")

	return cmd
}
