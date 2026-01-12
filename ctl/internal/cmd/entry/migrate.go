package entry

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	fUtil "github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

type migrateCfg struct {
	recurse            bool
	stdinDelimiter     string
	confirmBulkUpdates bool
	verbose            bool
}

func newMigrateCmd() *cobra.Command {
	frontendCfg := migrateCfg{}
	backendCfg := entry.MigrateCfg{
		DstPool: beegfs.InvalidEntityId{},
	}

	cmd := &cobra.Command{
		Use:   "migrate <path> [<path>] ...",
		Short: "Migrate files and directories to the specified storage pool, targets, or groups",
		Long: fmt.Sprintf(`Migrate files and directories to the specified storage pool, targets, or groups.
Files are only migrated if they have chunks on one or more of the specified targets. Destination 
targets and buddy groups can be directly specified, or a pool can be specified and all targets and groups 
in that pool are are potential candidates to migrate each file. Files cannot be migrated if the number of 
remaining targets/groups in the stripe pattern and destination targets/groups is less than the current 
stripe width. When the number of destination targets/groups is greater than the number of targets/groups 
needed to migrate a particular file, the destination IDs will be selected randomly to balance migrated files 
across the destination targets/groups. Note files cannot be assigned to the same target/group multiple times.

There are two available migration modes:

(1) Migrate using background data rebalancing (requires an enterprise license).

This mode was introduced in BeeGFS %s and handles migrating data from the specified target(s) to new 
target(s) by starting background tasks on the metadata and storage nodes that handle syncing data directly 
between the source/destination storage nodes and updating the stripe pattern on the metadata node. Because
data is rebalanced in the background, the migrate command may complete before files are fully migrated.
Files are locked and inaccessible to clients until they are rebalanced. Attempting to access files while 
background rebalancing is in progress will return an "in use" error.

This mode is generally more efficient, especially when data only needs to be migrated from some of the
targets assigned to a file. This mode also supports migrating hard links as it does not require the
inode be recreated, as is the case when migrating using a temporary file.

Note: When the number of targets/buddy groups in the destination pool are greater than the number of
targets/buddy groups a particular entry is migrated away from, the destination targets/buddy groups
will be randomly picked for each entry to more evenly redistribute file data.

(2) Migrate using a temporary file (default).

This is the legacy approach to migrating data between storage pools/targets in BeeGFS.
With this mode, a temporary file is created with new targets from the specified storage pool.
Then the contents of the original file are copied to the new file, and the temporary file atomically 
renamed overwriting the original. This mode handles migrating extended attributes, user/group ownership 
and permissions, and will set the new file to have the same access/modification timestamps as the original.

This mode does not lock the files or directories while the migration is in progress.
Thus migration should not be done while applications are modifying files in the directory tree being migrated.

If applications are only creating files you can first use the "entry set" mode to update the storage pool
for all directories in the tree being migrated to one that does not contain the targets you wish to free,
then run the migrate mode. Alternatively if applications are not creating files in the directory tree, you 
can use the migrate mode to update the storage pool for all directories it encounters while migrating files.

Symlinks are supported but the migrated links will look slightly different than regular files:

* A destination pool must always be specified. Specifying individual targets and buddy groups is not supported.
* The original number of targets is not preserved and instead inherited from the parent directory. This is not 
  important as the contents of a symlink in BeeGFS are simply the path the link is pointing at, which will only 
  ever be stored on a single target as the max path length is 4096 and the minimum chunk size in BeeGFS is 64KiB. 
* Timestamps on the link itself may not be preserved correctly.
* The link will always be moved to targets or storage buddy mirrors in the specified destination pool, even though
  the link will always inherit its storage pool assignment from its parent directory (which may differ).
  
These differences should never be problematic as typically the link itself is not important and most commands will
actually redirect and return information from the linked file (i.e., stat, open, etc).`, msg.StartChunkBalanceMsgVersions),
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			} else if len(args) > 1 && frontendCfg.recurse {
				return fmt.Errorf("only one path can be specified when recursively updating entries")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateRunner(cmd.Context(), args, frontendCfg, backendCfg)
		},
	}

	// Frontend / display configuration options:
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, `When <path> is a single directory recursively migrate all entries beneath the path.
CAUTION: this may migrate many entries, for example if the BeeGFS root is the provided path.`)
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\\n", `Change the string delimiter used to determine individual paths when read from stdin.
For example use --stdin-delimiter=\"\\x00\" for NULL.`)
	cmd.Flags().BoolVar(&frontendCfg.confirmBulkUpdates, "yes", false, "Use to acknowledge when running this command may migrate a large number of entries.")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print details about each entry being migrated.")

	// Backend
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.SrcTargets, 16, beegfs.Storage), "from-targets", "Migrate files from the specified targets.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.SrcNodes, 16, beegfs.Storage), "from-nodes", "Migrate files from the specified storage nodes.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.SrcPools, 16, beegfs.Storage), "from-pools", "Migrate files from the specified storage pools.")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&backendCfg.DstPool, 16, beegfs.Storage), "pool", "Migrate files to the targets and buddy groups in this storage pool.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.DstTargets, 16, beegfs.Storage), "targets", "Migrate files to the specified targets.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.DstGroups, 16, beegfs.Storage), "groups", "Migrate files to the specified buddy groups.")
	cmd.Flags().BoolVar(&backendCfg.UpdateDirs, "update-directories", false, "Update directories to use the specified storage pool.")
	cmd.Flags().BoolVar(&backendCfg.SkipMirrors, "skip-mirrors", false, "Migrate only files that are not buddy mirrored.")
	cmd.Flags().BoolVar(&backendCfg.DryRun, "dry-run", false, "Print out what migrations would happen but don't actually migrate the files.")
	cmd.Flags().StringVar(&backendCfg.FilterExpr, "filter-files", "", filesystem.FilterFilesHelp)
	cmd.Flags().BoolVar(&backendCfg.UseRebalancing, "rebalance", false, fmt.Sprintf("Use background data rebalancing instead of temporary files to migrate data between targets (%s).", msg.StartChunkBalanceMsgVersions))
	cmd.Flags().IntVar(&backendCfg.Retries, "retries", 10, "When using background data rebalancing, the number of times a request is retried if the metadata chunk balance queue is full (-1 = infinite, 0 = no retries).")
	cmd.MarkFlagsOneRequired("from-targets", "from-nodes", "from-pools")
	cmd.MarkFlagsOneRequired("pool", "targets", "groups")
	// On the backend all the entity IDs are collected into slices of destination targets and
	// groups, either implicitly from the specified pool or explicitly based on the target and
	// groups specified by the user. This means it would be possible to allow users to specify
	// candidate entity IDs using any mix of these three methods, then choose the entity ID at
	// random. We don't do this to maintain the legacy behavior of temp file migrations where
	// specifying a pool results in the meta choosing the destination target/group IDs, which also
	// allows it to respect the tuneTargetChooser settings. This also leaves us a path to update the
	// rebalancing mode to have the meta choose target/groups when a pool is specified.
	cmd.MarkFlagsMutuallyExclusive("pool", "targets")
	cmd.MarkFlagsMutuallyExclusive("pool", "groups")
	return cmd
}

func migrateRunner(ctx context.Context, args []string, frontendCfg migrateCfg, backendCfg entry.MigrateCfg) error {

	// Setup the method for sending paths to the backend:
	if frontendCfg.recurse {
		if !frontendCfg.confirmBulkUpdates {
			return fmt.Errorf("the recurse mode migrates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
		}
	}
	method, err := util.DeterminePathInputMethod(args, frontendCfg.recurse, frontendCfg.stdinDelimiter)
	if err != nil {
		return err
	}

	results, errWait, err := entry.MigrateEntries(ctx, method, backendCfg)
	if err != nil {
		return err
	}

	allColumns := []string{"path", "entry_id", "status", "original_ids", "migration_type", "source_ids", "destination_ids", "message"}
	tbl := cmdfmt.NewPrintomatic(allColumns, allColumns)
	var migrateStats = &entry.MigrateStats{}

	migrateErr := false
	printVerbosely := frontendCfg.verbose || viper.GetBool(config.DebugKey)
run:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-results:
			if !ok {
				break run
			}
			if result.Status == entry.MigrateError {
				migrateErr = true
				tbl.AddItem(result.Path, result.EntryID, result.Status, result.StartingIDs, result.IDType, result.SourceIDs, result.DestinationIDs, result.Message)
			} else if printVerbosely {
				tbl.AddItem(result.Path, result.EntryID, result.Status, result.StartingIDs, result.IDType, result.SourceIDs, result.DestinationIDs, result.Message)
			}
			migrateStats.Update(result)
		}
	}
	if migrateErr || printVerbosely {
		tbl.PrintRemaining()
	}
	cmdfmt.Printf("Summary: %+v\n", *migrateStats)
	if backendCfg.UseRebalancing && migrateStats.StartedRebalancingFiles > 0 {
		cmdfmt.Printf("Hint: Files are being migrated in the background, to monitor progress run 'beegfs stats rebalance'.\n")
	}

	if err = errWait(); err != nil {
		return err
	}
	if migrateErr {
		return fUtil.NewCtlError(fmt.Errorf("some entries could not be migrated"), fUtil.PartialSuccess)
	}

	return nil
}
