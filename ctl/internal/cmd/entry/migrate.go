package entry

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
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
		Use:   "migrate",
		Short: "Migrate files and directories to the specified storage pool",
		Long: `Migrate files and directories to the specified storage pool.
Files are only migrated if they have chunks on one or more of the specified targets.
Migration occurs by creating a temporary file with new targets from the specified storage pool.
Then the contents of the original file are copied to the new file, and the temporary file atomically 
renamed overwriting the original. This mode handles migrating extended attributes, user/group ownership 
and permissions, and will set the new file to have the same access/modification timestamps as the original.

This mode does not lock the files or directories while the migration is in progress.
Thus migration should not be done while applications are modifying files in the directory tree being migrated.

If applications are only creating files you can first use the "entry set" mode to update the storage pool
for all directories in the tree being migrated to one that does not contain the targets you wish to free,
then run the migrate mode. Alternatively if applications are not creating files in the directory tree, you 
can use the migrate mode to update the storage pool for all directories it encounters while migrating files.

Symlinks are supported with a few limitations:

* The original number of targets is not preserved and instead inherited from the parent directory. This is not 
  important as the contents of a symlink in BeeGFS are simply the path the link is pointing at, which will only 
  ever be stored on a single target as the max path length is 4096 and the minimum chunk size in BeeGFS is 64KiB. 
* Timestamps on the link itself may not be preserved correctly.
* The link entry will be moved to targets or storage mirrors in the specified pool, but the link entry may appear
  to be assigned to the same pool as the parent directory unless the directory's pool is also updated.`,
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
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, `When  <path> is a single directory recursively migrate all entries beneath the path.
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
	cmd.Flags().BoolVar(&backendCfg.UpdateDirs, "update-directories", false, "Update directories to use the specified storage pool.")
	cmd.Flags().BoolVar(&backendCfg.SkipMirrors, "skip-mirrors", false, "Migrate only files that are not buddy mirrored.")
	cmd.Flags().BoolVar(&backendCfg.DryRun, "dry-run", false, "Print out what migrations would happen but don't actually migrate the files.")
	cmd.MarkFlagsOneRequired("from-targets", "from-nodes", "from-pools")
	cmd.MarkFlagRequired("pool")
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

	results, errs, err := entry.MigrateEntries(ctx, method, backendCfg)
	if err != nil {
		return err
	}

	allColumns := []string{"path", "status", "original ids", "errors"}
	tbl := cmdfmt.NewPrintomatic(allColumns, allColumns)
	var multiErr types.MultiError
	var migrateStats = &entry.MigrateStats{}

	printVerbosely := frontendCfg.verbose || viper.GetBool(config.DebugKey)
run:
	for {
		select {
		case result, ok := <-results:
			if !ok {
				break run
			}
			if printVerbosely {
				if result.Err != nil {
					tbl.AddItem(result.Path, result.Status, result.StartingIDs, result.Err)
				} else {
					tbl.AddItem(result.Path, result.Status, result.StartingIDs, "none")
				}
			}
			migrateStats.Update(result.Status)

		case err, ok := <-errs:
			if ok {
				multiErr.Errors = append(multiErr.Errors, err)
			}
		}
	}
	if printVerbosely {
		tbl.PrintRemaining()
	}
	cmdfmt.Printf("Summary: %+v\n", *migrateStats)
	// We may have still processed some entries so wait to print an error until the end.
	if len(multiErr.Errors) != 0 {
		return &multiErr
	}

	return nil
}
