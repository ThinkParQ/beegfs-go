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

--name, --backend, --policies and --id apply to every type. When --id is omitted, Remote assigns the next ID that was never used, counting deleted targets. When --name is omitted, Remote names the target rst-<id>-<unique suffix>. The name is only how BeeGFS refers to the target.

With --backend, the target takes its whole connection to the store from the backend, which must have the same type. The connection is the endpoint, region, keys and the like, and the target cannot set any of it itself. Without --backend, the target sets its connection itself. Other settings and list entries the backend holds, such as storage classes, are defaults the target may override. The target stores only what it sets, so a later change to the backend reaches the target. Manage backends with "remote backend".

For a type that keeps its objects in a bucket, such as s3, create also makes the bucket. --bucket is the bucket's real name in the store. The bucket is made in the target's store: the backend's, or the one the target's own connection reaches. A bucket that already exists is refused, because it may hold another target's objects. The bucket cannot change once the target exists.

With --adopt, create uses the existing bucket --bucket names instead, and changes nothing in the store.

With --backend and --adopt, every setting the backend does not provide is optional. Without --bucket, create then makes one target for every bucket of the backend's store that no live or deleted target uses. Each one gets the next ID, a generated name, and every other flag given. --dry-run shows them without creating any.

When the target cannot be stored after its bucket was made, Remote deletes the bucket again.

The ID of a deleted target is refused unless --force is given. Stubs, file metadata and jobs may still name that ID, and each of them then refers to the new target. The deleted target's unfinished jobs are no longer cancelled, and continue on the new target. To bring a deleted target back with its objects, give its --id, --force, --adopt and its --bucket.`,
		Args: cobra.NoArgs,
	}
	for _, t := range cRst.TargetTypes() {
		cmd.AddCommand(newTargetCreateTypeCmd(t))
	}
	return cmd
}

// newTargetCreateTypeCmd returns the create subcommand of type t. Its flags come from the type's
// declaration. The flags every type has are registered here too, rather than inherited from create,
// because the CLI's help does not list inherited flags. A type that can manage buckets also gets
// --adopt, --dry-run and the --bucket-* flags.
func newTargetCreateTypeCmd(t cRst.TargetType) *cobra.Command {
	flags := newTargetFlags()
	var id uint32
	var force, adopt, dryRun bool
	var bucketFlags bucketSettingFlags
	bucketSetting, hasBuckets := cRst.BucketSettingOf(t)
	cmd := &cobra.Command{
		Use:     t.Name(),
		Short:   fmt.Sprintf("Create an %s target", t.Name()),
		Example: createExample(t),
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			bucketChanges, err := bucketFlags.changes()
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
			backend := target.GetBackend()
			if err := checkCreateFlags(cmd, t, backend, adopt); err != nil {
				return err
			}
			if adopt && bucketFlags.given(cmd) {
				return fmt.Errorf("--adopt uses an existing bucket as it is, so it takes no --bucket-* flags")
			}
			adoptAll := hasBuckets && adopt && backend != "" && !settingGiven(cmd, bucketSetting)
			if dryRun && !adoptAll {
				return fmt.Errorf("--dry-run only previews --backend --adopt without --%s", bucketSetting.Key)
			}
			if adoptAll {
				for _, name := range []string{"name", "id", "force"} {
					if cmd.Flags().Changed(name) {
						return fmt.Errorf("--%s names one target, but --adopt without --%s creates one target per bucket", name, bucketSetting.Key)
					}
				}
				return runAdoptAll(cmd, target, bucketSetting, dryRun)
			}

			resp, err := rst.CreateTarget(cmd.Context(), target, rst.CreateTargetOptions{Force: force, Adopt: adopt, Bucket: bucketChanges})
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
			printBucketResult(resp.GetBucket())
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
	if hasBuckets {
		cmd.Flags().BoolVar(&adopt, "adopt", false, fmt.Sprintf("Use the existing bucket --%[1]s names instead of creating it. With --backend and no --%[1]s, create a target for every unused bucket of the backend's store.", bucketSetting.Key))
		cmd.Flags().BoolVar(&dryRun, "dry-run", false, fmt.Sprintf("With --backend --adopt and no --%s, show the targets that would be created, and create none.", bucketSetting.Key))
		bucketFlags.register(cmd.Flags(), false)
	}
	// Settings a target needs are not marked required, because a backend may provide them, and
	// adopt may leave out the bucket. RunE checks them (checkCreateFlags), and Remote checks the
	// definition either way.
	flags.registerTypes(cmd.Flags(), []cRst.TargetType{t}, false, true)
	return cmd
}

// createExample returns the help example of the create subcommand of type t: the type's own
// example, and for a type with buckets, the ways to use a backend's buckets.
func createExample(t cRst.TargetType) string {
	bucket, ok := cRst.BucketSettingOf(t)
	if !ok {
		return t.Example()
	}
	return fmt.Sprintf(`%[1]s

  # Create the bucket archive in the store of backend site, and a target that uses it
  beegfs remote target create %[2]s --backend=site --%[3]s=archive

  # Use the existing bucket archive instead
  beegfs remote target create %[2]s --backend=site --adopt --%[3]s=archive

  # Show the targets that adopting every unused bucket of backend site would create
  beegfs remote target create %[2]s --backend=site --adopt --dry-run`, t.Example(), t.Name(), bucket.Key)
}

// runAdoptAll creates one target from template for every unused bucket of the store of template's
// backend, and prints them. With dryRun it creates none. bucket is the setting that names a
// target's bucket.
func runAdoptAll(cmd *cobra.Command, template *flex.RemoteStorageTarget, bucket cRst.Setting, dryRun bool) error {
	resp, err := rst.AdoptBuckets(cmd.Context(), template, dryRun)
	if err != nil {
		return err
	}
	if jsonOutput() {
		if err := cmdfmt.PrintJSON(adoptResult(resp, bucket, dryRun)); err != nil {
			return err
		}
		return applyError(resp.GetApply())
	}
	printAdoptResult(resp, bucket, template.GetBackend(), dryRun)
	if dryRun {
		return nil
	}
	return printApplyResult(resp.GetApply())
}

// checkCreateFlags returns an error when the flags of cmd break the owner rules of type t (see
// cRst.CheckDefinitionSettings). backend is the backend the target names, or "". adopt is whether
// --adopt was given. Remote applies the same rules, so this only fails early, before the bucket
// step. The error for a missing flag has the form of cobra's own, with a hint for the case:
//   - Without a backend, a backend could provide the connection.
//   - With a backend, only the bucket can be missing. --adopt without it adopts every unused
//     bucket instead.
func checkCreateFlags(cmd *cobra.Command, t cRst.TargetType, backend string, adopt bool) error {
	missing, conflicting := cRst.CheckDefinitionSettings(t, cRst.TargetRole(backend != "", adopt), func(s cRst.Setting) bool { return settingGiven(cmd, s) })
	if len(conflicting) > 0 {
		flags := []string{}
		for _, s := range conflicting {
			flags = append(flags, "--"+s.Key)
		}
		return fmt.Errorf("%s cannot be given with --backend, because backend %q provides the connection (hint: drop them, or drop --backend)", strings.Join(flags, ", "), backend)
	}
	if len(missing) == 0 {
		return nil
	}
	hint := "or give a --backend that provides the connection"
	if backend != "" {
		bucket, _ := cRst.BucketSettingOf(t)
		hint = fmt.Sprintf("give --%s to create a bucket, or add --adopt to import every unused bucket of backend %q as a target", bucket.Key, backend)
	}
	return missingFlagsError(missing, hint)
}

// settingGiven reports whether cmd has a flag for setting s. A secret setting may be given as
// --<key>-file instead.
func settingGiven(cmd *cobra.Command, s cRst.Setting) bool {
	return cmd.Flags().Changed(s.Key) || (s.Secret && cmd.Flags().Changed(s.Key+"-file"))
}

// missingFlagsError returns an error in the form of cobra's own for missing required flags, with
// hint added when it is not "".
func missingFlagsError(missing []cRst.Setting, hint string) error {
	keys := []string{}
	for _, s := range missing {
		keys = append(keys, strconv.Quote(s.Key))
	}
	err := fmt.Errorf("required flag(s) %s not set", strings.Join(keys, ", "))
	if hint != "" {
		err = fmt.Errorf("%w (hint: %s)", err, hint)
	}
	return err
}

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

--backend=<name> makes the target take its connection from another backend of the same type. A target with a backend cannot set a connection setting itself. So when a target that sets its own connection gets a backend, the same modify must clear those settings, such as --backend=site --endpoint-url= --region=. --backend= removes the backend, and the target keeps every value it inherited as its own. A list entry, such as a storage class, given by its key is added when it is new, and only its given settings change when it exists. Use the --remove and --clear flags to remove entries. A flag that belongs to another type is refused. The type of a target cannot change.

Changing where a target keeps its objects, such as its endpoint, region or partition, is refused while unfinished jobs use the target, because they may refer to objects the change moves. Use --force to change it anyway.

The bucket cannot change, because create made or checked it. To move a target to another bucket, delete it, then create it again with its --id, --force, --adopt and the new --bucket.`,
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

Files and stubs may still name the target's ID. Remote cannot find them, so it does not give the ID to another target unless "create --id --force" asks for it.

The target's bucket and its objects stay in the store. "create --id --force --adopt" brings the target back in the same bucket.`,
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
// reached each sync node, how many unfinished jobs of a deleted target now continue on it, and what
// happened to its bucket (null for a type without one).
func createResult(shown shownTarget, resp *beeremote.CreateRSTResponse) map[string]any {
	return map[string]any{
		"target":                 targetView(shown, false),
		"apply":                  applyView(resp.GetApply()),
		"reused-unfinished-jobs": resp.GetReusedUnfinishedJobs(),
		"bucket":                 bucketResultView(resp.GetBucket()),
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
