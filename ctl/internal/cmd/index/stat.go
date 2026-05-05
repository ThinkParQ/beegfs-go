package index

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newStatCmd() *cobra.Command {
	backendCfg := indexPkg.StatCfg{}

	cmd := &cobra.Command{
		Use:         "stat <path>",
		Short:       "Displays file or directory metadata information.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument")
			}
			return nil
		},
		Long: `Retrieve metadata for a specific file or directory from the GUFI index.

Example: display metadata for a file

  beegfs index stat /mnt/beegfs/data/README.md

Example: display BeeGFS-specific metadata

  beegfs index stat --beegfs /mnt/beegfs/data/README.md
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			exec, err := newExecutor()
			if err != nil {
				return err
			}
			// resolveIndexPath converts the full path to an index path;
			// Stat() will split it into parent dir + basename internally.
			indexPath, err := resolveIndexPath(args)
			if err != nil {
				return err
			}
			if err := checkIndexExists(filepath.Dir(indexPath)); err != nil {
				return err
			}

			log.Debug("running beegfs index stat",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Stat(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			allColumns := []string{"name", "type", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink"}
			defaultColumns := []string{"name", "type", "size", "mtime", "mode", "uid", "gid"}
			if backendCfg.BeeGFS {
				allColumns = append(allColumns,
					"owner_id", "parent_entry_id", "entry_id",
					"stripe_pattern_type", "stripe_chunk_size", "stripe_num_targets",
				)
				defaultColumns = append(defaultColumns, "entry_id", "owner_id", "stripe_num_targets")
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

		run:
			for {
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case row, ok := <-rows:
					if !ok {
						break run
					}
					tbl.AddItem(toAny(row)...)
				}
			}

			return errWait()
		},
	}

	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS-specific metadata.")

	return cmd
}
