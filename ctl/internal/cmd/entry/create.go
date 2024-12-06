package entry

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

func newCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create files and directories in BeeGFS with specific configuration",
	}
	cmd.AddCommand(
		newCreateFileCmd(),
	)
	return cmd
}

func newCreateFileCmd() *cobra.Command {
	backendCfg := entry.CreateFileCfg{}
	cmd := &cobra.Command{
		Use:   "file <path> [<path>] ...",
		Short: "Create a file in BeeGFS with specific configuration",
		Long: `Create a file in BeeGFS with specific configuration.
Unless specified, striping configuration is inherited from the parent directory.
WARNING: Files and directories created using this mode do not trigger file system modification events.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			backendCfg.Paths = args
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			results, err := entry.CreateFile(cmd.Context(), backendCfg)
			if err != nil {
				return err
			}
			return PrintCreateEntryResult(results)
		},
	}
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Ignore if the specified targets/buddy groups are not in the same pool as the entry.")
	cmd.Flags().Var(newChunksizeFlag(&backendCfg.Chunksize), "chunksize", "Block size for striping (per storage target). Suffixes 'Ki' (Kibibytes) and 'Mi` (Mebibytes) are allowed.")
	cmd.Flags().Var(newNumTargetsFlag(&backendCfg.DefaultNumTargets), "num-targets", `Number of targets to stripe each file across.
	If the stripe pattern is 'buddymirror' this is the number of mirror groups.`)
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.TargetIDs, 16, beegfs.Storage), "targets", `Comma-separated list of targets to use for the new file (only applies to new files).
	The number of targets may be longer than the num-targets parameter, but cannot be less.
	When the stripe pattern is set to buddy mirror, this argument expects buddy group IDs instead of target IDs.
	If the targets/groups are not in the same storage pool that will be assigned to the new file, the force flag must be set.`)
	cmd.Flags().Var(newPoolFlag(&backendCfg.Pool), "pool", `Use targets from this storage pool when creating the file. 
	Can be specified as the alias, numerical ID, or unique ID of the pool.
	NOTE: This is an enterprise feature. See end-user license agreement for definition and usage.`)
	cmd.Flags().Var(newPermissionsFlag(&backendCfg.Permissions), "permissions", "The octal access permissions for user, group, and others.")
	cmd.Flags().Var(newUserFlag(&backendCfg.UserID), "uid", "User ID of the file owner. Defaults to the current effective user ID.")
	cmd.Flags().Var(newGroupFlag(&backendCfg.GroupID), "gid", "Group ID of the file owner. Defaults to the current effective group ID.")
	cmd.Flags().Var(newStripePatternFlag(&backendCfg.StripePattern), "pattern", fmt.Sprintf(`Set the stripe pattern type to use. Valid patterns: %s.
	When the pattern is set to "buddymirror", each target will be mirrored on a corresponding mirror target.
	NOTE: Buddy mirroring is an enterprise feature. See end-user license agreement for definition and usage.`, strings.Join(validStripePatternKeys(), ", ")))
	cmd.Flags().VarP(newRstsFlag(&backendCfg.RemoteTargets), "remote-targets", "r", `Comma-separated list of Remote Storage Target IDs.`)
	cmd.Flags().Var(newRstCooldownFlag(&backendCfg.RemoteCooldownSecs), "remote-cooldown", "Time to wait after a file is closed before replication begins. Accepts a duration such as 1s, 1m, or 1h. The max duration is 65,535 seconds.")
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/18
	// Unmark this as hidden once automatic uploads are supported.
	cmd.Flags().MarkHidden("remote-cooldown")
	cmd.MarkFlagsMutuallyExclusive("pool", "targets")

	return cmd
}

func PrintCreateEntryResult(entries []entry.CreateEntryResult) error {

	columns := []string{"name", "status", "entry id", "type"}
	tbl := cmdfmt.NewTableWrapper(columns, columns)
	anyErrors := false

	for _, r := range entries {
		if r.Status == beegfs.OpsErr_SUCCESS {
			tbl.Row(r.Path, r.Status, string(r.RawEntryInfo.EntryID), r.RawEntryInfo.EntryType)
		} else {
			anyErrors = true
			tbl.Row(r.Path, r.Status, "n/a", "n/a")
		}
	}
	tbl.PrintRemaining()
	if anyErrors {
		return fmt.Errorf("unable to create one or more entries (see individual results for details)")
	}
	return nil
}
