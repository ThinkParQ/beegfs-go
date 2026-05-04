package index

import (
	"fmt"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

func newMigrateCmd() *cobra.Command {
	cfg := indexPkg.MigrateIndexCfg{}
	var skipTreesummary bool

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate old .bdm.db index databases to the new GUFI format.",
		Args: func(cmd *cobra.Command, args []string) error {
			if cfg.IndexRoot == "" {
				return fmt.Errorf("--index-root is required (or set via config)")
			}
			return nil
		},
		Long: `Migrate old BeeGFS Hive index databases (.bdm.db) to the new GUFI format (db.db).

This command walks the index tree starting at --index-root, finds all old
database files, detects the schema version, and applies the appropriate
migration SQL. BeeGFS metadata columns are extracted into separate plugin
tables (beegfs_entries, beegfs_stripe_targets, beegfs_rst_targets) with views
for querying.

Note: fields introduced in newer plugin versions (stripe_default_num_targets,
storage_pool_id, path_info_flags, orig_parent_uid, orig_parent_entry_id,
file_data_state, and the RST columns) will be NULL for entries migrated from
old indexes. These are populated only after re-indexing with the current
BeeGFS plugin (beegfs index rescan or create).

The treesummary table is dropped during migration and is automatically rebuilt
by running gufi_treesummary on the index root afterward. Use --skip-treesummary
to skip this step.

Databases that already have a db.db file are skipped unless --force is set,
in which case the existing db.db is backed up to db.db.bak before migration.

Example: migrate all databases under /mnt/index

  beegfs index migrate --index-root /mnt/index
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			ch, errWait, err := indexPkg.MigrateIndex(cmd.Context(), log.Logger, cfg)
			if err != nil {
				return err
			}

			stats, results := indexPkg.CollectStats(ch)

			// Print per-database errors.
			for _, r := range results {
				if r.Error != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "ERROR %s: %v\n", r.Dir, r.Error)
				}
			}

			// Print summary.
			fmt.Fprintf(cmd.OutOrStdout(), "\nMigration complete.\n")
			fmt.Fprintf(cmd.OutOrStdout(), "  Total databases found: %d\n", stats.Total)
			fmt.Fprintf(cmd.OutOrStdout(), "  Migrated (full):       %d\n", stats.MigratedFull)
			fmt.Fprintf(cmd.OutOrStdout(), "  Migrated (simple):     %d\n", stats.MigratedSimple)
			fmt.Fprintf(cmd.OutOrStdout(), "  Skipped:               %d\n", stats.Skipped)
			fmt.Fprintf(cmd.OutOrStdout(), "  Failed:                %d\n", stats.Failed)

			if waitErr := errWait(); waitErr != nil {
				return waitErr
			}

			if stats.Failed > 0 {
				return fmt.Errorf("%d database(s) failed to migrate", stats.Failed)
			}

			if (stats.MigratedFull > 0 || stats.MigratedSimple > 0) && !skipTreesummary {
				treesumBin := viper.GetString(indexPkg.TreesumBinKey)
				threads := viper.GetInt(indexPkg.ThreadsKey)
				fmt.Fprintf(cmd.OutOrStdout(), "\nRebuilding tree summaries with gufi_treesummary...\n")
				treesumCmd := exec.CommandContext(cmd.Context(), treesumBin,
					"-n", fmt.Sprint(threads), cfg.IndexRoot)
				treesumCmd.Stdout = cmd.OutOrStdout()
				treesumCmd.Stderr = cmd.ErrOrStderr()
				if err := treesumCmd.Run(); err != nil {
					return fmt.Errorf("gufi_treesummary: %w", err)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&cfg.IndexRoot, "index-root", "", "Root directory of the GUFI index tree to migrate. (required)")
	cmd.Flags().BoolVarP(&cfg.Force, "force", "f", false, "Backup and overwrite existing db.db files.")
	cmd.Flags().StringVar(&cfg.OldFileName, "old-filename", ".bdm.db", "Old database filename to look for.")
	cmd.Flags().StringVar(&cfg.NewFileName, "new-filename", "db.db", "New database filename after migration.")
	cmd.Flags().IntVarP(&cfg.Workers, "workers", "n", 0, "Number of databases to migrate in parallel (default: number of CPUs).")
	cmd.Flags().BoolVar(&cfg.SkipVacuum, "skip-vacuum", false, "Skip VACUUM after full migration (faster, but leaves reclaimed space on disk).")
	cmd.Flags().BoolVar(&skipTreesummary, "skip-treesummary", false, "Skip rebuilding tree summaries after migration.")

	return cmd
}
