package entry

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	iUtil "github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

func newCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create files and directories in BeeGFS with specific configuration",
	}
	cmd.AddCommand(
		newCreateFileCmd(),
		newCreateDirCmd(),
	)
	return cmd
}

func newCreateFileCmd() *cobra.Command {
	backendCfg := entry.CreateEntryCfg{
		FileCfg: &entry.CreateFileCfg{},
	}
	cmd := &cobra.Command{
		Use:   "file <path> [<path>] ...",
		Short: "Create a file in BeeGFS with specific configuration",
		Long: `Create a file in BeeGFS with specific configuration.
Unless specified, striping configuration is inherited from the parent directory.
WARNING: Files created using this mode do not trigger file system modification events.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			backendCfg.Paths = args
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			results, err := entry.CreateEntry(cmd.Context(), backendCfg)
			if err != nil {
				return err
			}
			return PrintCreateEntryResult(results)
		},
	}
	cmd.Flags().BoolVar(&backendCfg.FileCfg.Force, "force", false, "Ignore if the specified targets/buddy groups are not in the same pool as the entry.")
	cmd.Flags().Var(newChunksizeFlag(&backendCfg.FileCfg.Chunksize), "chunk-size", "Block size for striping (per storage target). Suffixes 'Ki' (Kibibytes) and 'Mi` (Mebibytes) are allowed.")
	cmd.Flags().Var(newNumTargetsFlag(&backendCfg.FileCfg.DefaultNumTargets), "num-targets", `Number of targets or mirror groups to stripe each file across.`)
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.FileCfg.TargetIDs, 16, beegfs.Storage), "targets", `Comma-separated list of targets to use for the new file (stripe pattern will always be RAID0).
	The number of targets may be longer than the num-targets parameter, but cannot be less.
	If the targets are not in the same storage pool that will be assigned to the new file, the force flag must be set.`)
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.FileCfg.BuddyGroups, 16, beegfs.Storage), "buddy-groups", `Comma-separated list of buddy groups to use for the new file (stripe pattern will always be mirrored).
	The number of buddy groups may be longer than the num-targets parameter, but cannot be less.
	If the groups are not in the same storage pool that will be assigned to the new file, the force flag must be set.`)
	cmd.Flags().Var(newPoolFlag(&backendCfg.FileCfg.Pool), "pool", `Use targets or buddy groups from this storage pool when creating the file. 
	Can be specified as the alias, numerical ID, or unique ID of the pool.
	NOTE: This is an enterprise feature. See end-user license agreement for definition and usage.`)
	cmd.Flags().Var(newPermissionsFlag(&backendCfg.Permissions, 0644), "permissions", "The octal access permissions for user, group, and others.")
	cmd.Flags().Var(newUserFlag(&backendCfg.UserID), "uid", "User ID of the file owner. Defaults to the current effective user ID.")
	cmd.Flags().Var(newGroupFlag(&backendCfg.GroupID), "gid", "Group ID of the file owner. Defaults to the current effective group ID.")
	cmd.Flags().Var(newStripePatternFlag(&backendCfg.FileCfg.StripePattern), "pattern", fmt.Sprintf(`Set the stripe pattern type to use. Valid patterns: %s.
	When the pattern is set to "buddymirror", each target will be mirrored on a corresponding mirror target.
	NOTE: Buddy mirroring is an enterprise feature. See end-user license agreement for definition and usage.`, strings.Join(validStripePatternKeys(), ", ")))
	cmd.Flags().VarP(iUtil.NewRemoteTargetsFlag(&backendCfg.FileCfg.RemoteTargets), "remote-targets", "r", `Comma-separated list of Remote Storage Target IDs.`)
	cmd.Flags().Var(newRstCooldownFlag(&backendCfg.FileCfg.RemoteCooldownSecs), "remote-cooldown", "Time to wait after a file is closed before replication begins. Accepts a duration such as 1s, 1m, or 1h. The max duration is 65,535 seconds.")
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/18
	// Unmark this as hidden once automatic uploads are supported.
	cmd.Flags().MarkHidden("remote-cooldown")
	cmd.MarkFlagsMutuallyExclusive("pool", "targets", "buddy-groups")
	cmd.MarkFlagsMutuallyExclusive("pattern", "targets", "buddy-groups")

	return cmd
}

func newCreateDirCmd() *cobra.Command {
	backendCfg := entry.CreateEntryCfg{
		DirCfg: &entry.CreateDirCfg{},
	}

	cmd := &cobra.Command{
		Use:     "directory <path> [<path>] ...",
		Aliases: []string{"dir"},
		Short:   "Create a directory in BeeGFS with specific configuration",
		Long: `Create a directory in BeeGFS with specific configuration.
By default if the parent directory is mirrored, the new directory will also be mirrored.
Optionally the new directory can always be unmirrored or created on specific metadata node(s).
WARNING: Directories created using this mode do not trigger file system modification events.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			backendCfg.Paths = args
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			results, err := entry.CreateEntry(cmd.Context(), backendCfg)
			if err != nil {
				return err
			}
			return PrintCreateEntryResult(results)
		},
	}
	cmd.Flags().Var(newPermissionsFlag(&backendCfg.Permissions, 0755), "permissions", "The octal access permissions for user, group, and others.")
	cmd.Flags().Var(newUserFlag(&backendCfg.UserID), "uid", "User ID of the directory owner. Defaults to the current effective user ID.")
	cmd.Flags().Var(newGroupFlag(&backendCfg.GroupID), "gid", "Group ID of the directory owner. Defaults to the current effective group ID.")
	cmd.Flags().BoolVar(&backendCfg.DirCfg.NoMirror, "no-mirror", false, "Do not mirror metadata, even if the parent directory has mirroring enabled.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.DirCfg.Nodes, 16, beegfs.Meta), "nodes", `Comma-separated list of metadata nodes to choose from for the new directory.
	When using mirroring, this is the buddy mirror group ID, rather than a node ID.`)

	return cmd
}

func PrintCreateEntryResult(entries []entry.CreateEntryResult) error {

	columns := []string{"name", "status", "entry id", "type"}
	tbl := cmdfmt.NewPrintomatic(columns, columns)
	anyErrors := false

	for _, r := range entries {
		if r.Status == beegfs.OpsErr_SUCCESS {
			tbl.AddItem(r.Path, r.Status, string(r.RawEntryInfo.EntryID), r.RawEntryInfo.EntryType)
		} else {
			anyErrors = true
			tbl.AddItem(r.Path, r.Status, "n/a", "n/a")
		}
	}
	tbl.PrintRemaining()
	if anyErrors {
		return fmt.Errorf("unable to create one or more entries (see individual results for details)")
	}
	return nil
}
