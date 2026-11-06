package rst

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func newTargetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "target",
		Short: "Create, modify, delete and list Remote Storage Targets",
		Long: `Create, modify, delete and list Remote Storage Targets.

Remote applies every change at once, to itself and to every sync node, without a restart.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(newListCmd(), newTargetCreateCmd(), newTargetModifyCmd(), newTargetDeleteCmd())
	return cmd
}

func parseTargetID(arg string) (uint32, error) {
	id, err := strconv.ParseUint(arg, 10, 32)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid remote target ID %q (it must be a whole number above 0)", arg)
	}
	return uint32(id), nil
}

func newTargetCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <type>",
		Short: "Create a Remote Storage Target",
		Long: `Create a Remote Storage Target of the given type.

--name, --backend, --policies and --id apply to every type. When --id is omitted, Remote assigns the next ID that was never used, counting deleted targets.

With --backend, every setting the target does not set is taken from the backend, which must have the same type. The target stores only what it sets, so a later change to the backend reaches the target. Settings a new target needs, such as the endpoint, may come from the backend. Manage backends with "remote backend".

The ID of a deleted target is refused unless --force is given. Stubs, file metadata and jobs may still name that ID, and each of them then refers to the new target. The deleted target's unfinished jobs are no longer cancelled, and continue on the new target.`,
		Args: cobra.NoArgs,
	}
	for _, t := range cRst.TargetTypes() {
		cmd.AddCommand(newTargetCreateTypeCmd(t))
	}
	return cmd
}

// newTargetCreateTypeCmd returns the create subcommand of type t. Its flags come from the type's
// declaration. The flags every type has are registered here too, rather than inherited from create,
// because the CLI's help does not list inherited flags.
func newTargetCreateTypeCmd(t cRst.TargetType) *cobra.Command {
	flags := newTargetFlags()
	var id uint32
	var force bool
	cmd := &cobra.Command{
		Use:     t.Name(),
		Short:   fmt.Sprintf("Create an %s target", t.Name()),
		Example: t.Example(),
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			target, err := rst.NewTarget(t.Name(), id)
			if err != nil {
				return err
			}
			target, err = rst.ApplyTargetChanges(target, changes)
			if err != nil {
				return err
			}
			if !cmd.Flags().Changed("backend") {
				if err := checkRequiredFlags(cmd, t, isRequiredOnCreate, "or give a --backend that sets them"); err != nil {
					return err
				}
			}
			resp, err := rst.CreateTarget(cmd.Context(), target, force)
			if err != nil {
				return err
			}
			shown := shownAfterChange(cmd.Context(), resp.GetEffective(), resp.GetRst())
			reused := ""
			if resp.GetReusedUnfinishedJobs() > 0 {
				reused = fmt.Sprintf("Warning: %d unfinished jobs of the deleted target with ID %d now continue on this target.\n", resp.GetReusedUnfinishedJobs(), resp.GetRst().GetId())
			}
			if jsonOutput() {
				cmdfmt.Printf("%s", reused)
				if err := cmdfmt.PrintJSON(createResult(shown, resp)); err != nil {
					return err
				}
				return applyError(resp.GetApply())
			}
			fmt.Printf("Created remote target %d (%s).\n", resp.GetRst().GetId(), resp.GetRst().GetName())
			fmt.Print(reused)
			printTargetDetail(shown, false)
			return printApplyResult(resp.GetApply())
		},
	}
	flags.registerCommon(cmd.Flags(), "target")
	flags.registerBackend(cmd.Flags())
	cmd.Flags().Uint32Var(&id, "id", 0, "The ID of the new target. Omit it to let Remote assign one.")
	cmd.Flags().BoolVar(&force, "force", false, "Reuse the --id of a deleted target. Stubs, file metadata and unfinished jobs that name it then refer to the new target.")
	// Settings a target needs are not marked required, because a backend may set them. RunE checks
	// them when no backend is given, and Remote checks the effective configuration either way.
	flags.registerTypes(cmd.Flags(), []cRst.TargetType{t}, false, false)
	return cmd
}

// checkRequiredFlags returns an error when cmd lacks a flag for a setting of type t that required
// selects. A target create selects what every target needs. A backend create selects what its type
// requires of a backend. A secret setting may be given as --<key>-file instead, which is why
// cobra's own required flags cannot express this. The error has the form of cobra's own, with hint
// added when it is not "".
func checkRequiredFlags(cmd *cobra.Command, t cRst.TargetType, required func(cRst.Setting) bool, hint string) error {
	missing := []string{}
	for _, s := range t.Settings() {
		given := cmd.Flags().Changed(s.Key) || (s.Secret && cmd.Flags().Changed(s.Key+"-file"))
		if required(s) && !given {
			missing = append(missing, strconv.Quote(s.Key))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	err := fmt.Errorf("required flag(s) %s not set", strings.Join(missing, ", "))
	if hint != "" {
		err = fmt.Errorf("%w (hint: %s)", err, hint)
	}
	return err
}

func isRequiredOnCreate(s cRst.Setting) bool { return s.RequiredOnCreate }

// shownAfterChange returns how to show a target that was just created or modified. effective and
// definition come from Remote's response, or from the request when Remote returned no effective
// configuration. The backends are read again to name the backend of each inherited value. That read
// is for output only, and the change already happened. So when it fails, a warning is printed and
// the target is shown without those names.
func shownAfterChange(ctx context.Context, effective, definition *flex.RemoteStorageTarget) shownTarget {
	if effective == nil {
		effective = definition
	}
	backends := map[string]*flex.RemoteStorageTarget{}
	if definition.GetBackend() != "" {
		targets, err := rst.GetTargets(ctx)
		if err == nil {
			backends = targets.Backends
		} else {
			cmdfmt.Printf("Warning: unable to read the backends, so inherited values are not marked: %s\n", err)
		}
	}
	shown, err := newShownTarget(effective, definition, backends)
	if err != nil {
		return shownTarget{effective: effective, backend: definition.GetBackend(), inherited: []string{}}
	}
	return shown
}

func newTargetModifyCmd() *cobra.Command {
	flags := newTargetFlags()
	var force bool
	cmd := &cobra.Command{
		Use:   "modify <id>",
		Short: "Modify a Remote Storage Target",
		Long: `Modify a Remote Storage Target.

Only the settings given are changed, and every other setting keeps its value. A setting given as empty, such as --region=, is inherited again from the target's backend.

--backend=<name> makes the target inherit from another backend of the same type. --backend= removes the backend, and the target keeps every value it inherited as its own. A list entry, such as a storage class, given by its key is added when it is new, and only its given settings change when it exists. Use the --remove and --clear flags to remove entries. A flag that belongs to another type is refused. The type of a target cannot change.

Changing where a target keeps its objects, such as its endpoint, region, bucket or partition, is refused while unfinished jobs use the target, because they may refer to objects the change moves. Use --force to change it anyway.`,
		Example: `  # Add an archival storage class, keeping the existing ones
  beegfs remote target modify 1 --storage-class name=GLACIER_IR,retrieval-tier=Expedited,retention-days=1,check-time=5m,recheck-time=1m

  # Change one setting of a storage class
  beegfs remote target modify 1 --storage-class name=GLACIER,recheck-time=30m

  # Rotate credentials
  beegfs remote target modify 1 --access-key=AKIANEW --secret-key-file=/etc/beegfs/archive.secret.new`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseTargetID(args[0])
			if err != nil {
				return err
			}
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			updated, resp, err := rst.UpdateTarget(cmd.Context(), id, changes, force)
			if err != nil {
				return err
			}
			shown := shownAfterChange(cmd.Context(), resp.GetEffective(), updated)
			if jsonOutput() {
				if err := cmdfmt.PrintJSON(modifyResult(shown, resp)); err != nil {
					return err
				}
				return applyError(resp.GetApply())
			}
			fmt.Printf("Modified remote target %d (%s).\n", id, updated.GetName())
			printTargetDetail(shown, false)
			return printApplyResult(resp.GetApply())
		},
	}
	flags.registerCommon(cmd.Flags(), "target")
	flags.registerBackend(cmd.Flags())
	// The target's type is only known once it is read, so modify takes the flags of every type, and
	// ApplyTargetChanges refuses one the target's type does not have.
	flags.registerTypes(cmd.Flags(), cRst.TargetTypes(), true, false)
	cmd.Flags().BoolVar(&force, "force", false, "Change where the target keeps its objects even while unfinished jobs use it.")
	return cmd
}

func newTargetDeleteCmd() *cobra.Command {
	var force bool
	var message string
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a Remote Storage Target",
		Long: `Delete a Remote Storage Target.

Without --force nothing is deleted, and the number of unfinished jobs that use the target is printed.

A deleted target serves no new jobs. Its unfinished jobs are not stopped at once: each one is cancelled the next time Remote or a sync node would use the target for it, and Remote undoes its changes, for example by restoring a partly downloaded file. The --message is recorded with each such job and returned to every new request that names the target.

Files and stubs may still name the target's ID. Remote cannot find them, so it does not give the ID to another target unless "create --id --force" asks for it.`,
		Example: `  beegfs remote target delete 10 --force --message="Tape library decommissioned"`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseTargetID(args[0])
			if err != nil {
				return err
			}
			resp, err := rst.DeleteTarget(cmd.Context(), id, message, !force)
			if err != nil {
				return err
			}
			if jsonOutput() {
				if force {
					cmdfmt.Printf("Warning: files and stubs may still name remote target %d.\n", id)
				}
				if err := cmdfmt.PrintJSON(deleteResult(id, resp, force)); err != nil {
					return err
				}
				if !force {
					return fmt.Errorf("nothing was deleted (hint: rerun with --force to delete remote target %d)", id)
				}
				return applyError(resp.GetApply())
			}
			if !force {
				fmt.Printf("Remote target %d has %d unfinished jobs. They will be cancelled as they progress.\n", id, resp.GetUnfinishedJobs())
				return fmt.Errorf("nothing was deleted (hint: rerun with --force to delete remote target %d)", id)
			}
			fmt.Printf("Deleted remote target %d.\n", id)
			if resp.GetUnfinishedJobs() > 0 {
				fmt.Printf("%d unfinished jobs will be cancelled as they progress.\n", resp.GetUnfinishedJobs())
			}
			fmt.Printf("Warning: files and stubs may still name remote target %d.\n", id)
			return printApplyResult(resp.GetApply())
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Delete the target. Without it nothing is deleted.")
	cmd.Flags().StringVar(&message, "message", "", "Why the target is deleted. It is recorded with every job the delete cancels, and returned to new requests that name the target.")
	return cmd
}

// createResult returns the JSON output of create: the target as Remote stored it, how the change
// reached each sync node, and how many unfinished jobs of a deleted target now continue on it.
func createResult(shown shownTarget, resp *beeremote.CreateRSTResponse) map[string]any {
	return map[string]any{
		"target":                 targetView(shown, false),
		"apply":                  applyView(resp.GetApply()),
		"reused-unfinished-jobs": resp.GetReusedUnfinishedJobs(),
	}
}

// modifyResult returns the JSON output of modify: the target as it was stored, and how the change
// reached each sync node.
func modifyResult(shown shownTarget, resp *beeremote.UpdateRSTResponse) map[string]any {
	return map[string]any{
		"target": targetView(shown, false),
		"apply":  applyView(resp.GetApply()),
	}
}

// deleteResult returns the JSON output of delete. deleted is false for a dry run, which counts the
// unfinished jobs and changes nothing.
func deleteResult(id uint32, resp *beeremote.DeleteRSTResponse, deleted bool) map[string]any {
	return map[string]any{
		"id":              id,
		"deleted":         deleted,
		"unfinished-jobs": resp.GetUnfinishedJobs(),
		"apply":           applyView(resp.GetApply()),
	}
}
