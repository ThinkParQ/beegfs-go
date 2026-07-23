package index

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

func newMigrateCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	cfg := indexPkg.MigrateIndexCfg{}
	var skipTreesummary bool
	var verbose bool
	var mountPathArg string

	cmd := &cobra.Command{
		Use:   "migrate [path]",
		Short: "Migrate old .bdm.db index databases to the new format",
		Args:  cobra.MaximumNArgs(1),
		Long: `Migrate legacy BeeGFS Index databases (.bdm.db) to the
new BeeGFS Index format (db.db) introduced in 8.4.


This command walks the index tree at the index root, finds all old database
files, detects the schema version, and applies the appropriate migration SQL.
BeeGFS metadata columns are extracted into separate plugin tables
(beegfs_entries, beegfs_stripe_targets, beegfs_rst_targets) with views for
querying.

The index root comes from --index-root, or from the matching .beegfs.index
entry at the BeeGFS mount root when the flag is omitted; an optional [path]
selects which entry to use (defaulting to the current directory).

Note: fields introduced in newer plugin versions (stripe_default_num_targets,
storage_pool_id, path_info_flags, orig_parent_uid, orig_parent_entry_id,
file_data_state, and the RST columns) will be NULL for entries migrated from
old indexes. These are populated only after re-indexing with the current
BeeGFS plugin (beegfs index rescan or create).

The migrated index is placed under the nested layout
<index-root>/<mount-name>/..., where <mount-name> is the basename of the BeeGFS
mount path. The mount is detected automatically when BeeGFS is mounted; when it
is not, pass --mount-path <mount-path> and its basename is used (so
"/mnt/beegfs" yields "beegfs"). Migrate aborts up front if the mount name
cannot be determined.

The treesummary table is dropped during migration and is automatically
rebuilt afterward. Use --skip-treesummary to skip this step.

Databases that already have a db.db file are skipped unless --force is set,
in which case the existing db.db is moved aside to db.db.bak before migration.
On success the backup is removed; if the migration fails it is restored to
db.db so the index is never left without a database.

Example: migrate all databases under /mnt/index

  beegfs index migrate --index-root /mnt/index

Example: migrate using the index root configured in .beegfs.index

  beegfs index migrate /mnt/beegfs
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			resolved, args := resolveCfgAndPaths(cmd.Context(), globalCfg, args)
			if resolved.IndexRoot == "" {
				return indexPkg.ErrIndexRootNotSet
			}
			cfg.IndexRoot = resolved.IndexRoot
			cfg.IndexAddr = resolved.IndexAddr

			var mountBase string
			if mountPath := resolveMountPath(primeFSPath(args)); mountPath != "" {
				mountBase = filepath.Base(mountPath)
			} else if mountPathArg != "" {
				mountBase = filepath.Base(mountPathArg)
				if mountBase == "" || mountBase == "." || mountBase == ".." || mountBase == string(filepath.Separator) {
					return fmt.Errorf("--mount-path %q does not yield a valid mount name (basename %q)", mountPathArg, mountBase)
				}
			} else {
				return fmt.Errorf("BeeGFS is not mounted; pass --mount-path <mount-path> so the migrated index can be placed under <index-root>/<mount-name>")
			}

			ch, errWait, err := indexPkg.MigrateIndex(cmd.Context(), log.Logger, cfg)
			if err != nil {
				return err
			}

			stats, results := indexPkg.CollectStats(ch)

			for _, r := range results {
				switch {
				case r.Error != nil:
					fmt.Fprintf(cmd.ErrOrStderr(), "FAILED %s: %v\n", r.Dir, r.Error)
				case r.Type == indexPkg.OutcomeSkipped && verbose:
					fmt.Fprintf(cmd.ErrOrStderr(), "SKIPPED %s: a db.db already exists (use --force to overwrite)\n", r.Dir)
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "\nMigration complete.\n")
			fmt.Fprintf(cmd.OutOrStdout(), "  Total databases found: %d\n", stats.Total)
			fmt.Fprintf(cmd.OutOrStdout(), "  Migrated:              %d\n", stats.MigratedFull+stats.MigratedSimple)
			fmt.Fprintf(cmd.OutOrStdout(), "  Skipped:               %d\n", stats.Skipped)
			fmt.Fprintf(cmd.OutOrStdout(), "  Failed:                %d\n", stats.Failed)

			if waitErr := errWait(); waitErr != nil {
				return waitErr
			}

			if stats.Failed > 0 {
				return fmt.Errorf("%d database(s) failed to migrate", stats.Failed)
			}
			treesumTarget := cfg.IndexRoot
			if stats.MigratedFull > 0 || stats.MigratedSimple > 0 {
				relocated, err := indexPkg.RelocateToNested(cfg.IndexRoot, mountBase, cfg.NewFileName)
				if err != nil {
					return fmt.Errorf("relocating migrated index into the nested layout: %w", err)
				}
				if relocated {
					treesumTarget = filepath.Join(cfg.IndexRoot, mountBase)
					fmt.Fprintf(cmd.OutOrStdout(), "Moved migrated index to %s\n", treesumTarget)
				}
			}

			if (stats.MigratedFull > 0 || stats.MigratedSimple > 0) && !skipTreesummary {
				threads := indexPkg.DefaultThreads(cmd.Context(), "")
				fmt.Fprintf(cmd.OutOrStdout(), "\nRebuilding tree summaries...\n")
				treesumCmd := exec.CommandContext(cmd.Context(), indexPkg.TreesumBin,
					"-n", fmt.Sprint(threads), treesumTarget)
				treesumCmd.SysProcAttr = indexPkg.CallerSysProcAttr()
				treesumCmd.Stdout = cmd.OutOrStdout()
				treesumCmd.Stderr = cmd.ErrOrStderr()
				if err := treesumCmd.Run(); err != nil {
					return fmt.Errorf("tree summary: %w", err)
				}
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().BoolVarP(&cfg.Force, "force", "f", false, "Backup and overwrite existing db.db files.")
	cmd.Flags().StringVar(&cfg.OldFileName, "old-filename", ".bdm.db", "Old database filename to look for.")
	cmd.Flags().StringVar(&cfg.NewFileName, "new-filename", "db.db", "New database filename after migration.")
	cmd.Flags().IntVarP(&cfg.Workers, "workers", "n", 0, "Number of databases to migrate in parallel (default: number of CPUs).")
	cmd.Flags().BoolVar(&cfg.SkipVacuum, "skip-vacuum", false, "Skip VACUUM after full migration (faster, but leaves reclaimed space on disk).")
	cmd.Flags().BoolVar(&skipTreesummary, "skip-treesummary", false, "Skip rebuilding tree summaries after migration.")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "List each skipped and failed directory with the reason.")
	cmd.Flags().StringVar(&mountPathArg, "mount-path", "", "Mount path whose basename names the nested layout (<index-root>/<mount-name>); used only when BeeGFS is not mounted.")

	return cmd
}
