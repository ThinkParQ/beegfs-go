package rst

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// This file defines "beegfs remote target parent". A parent is a named, partial configuration of
// one RST type that targets inherit unset values from (see common/rst/inherit.go). A parent names
// no parent, so its commands have no --parent flag. The commands reuse the target flags (targetflags.go) and output
// (targetformat.go), because a parent has the same settings as a target of its type.

func newTargetParentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parent",
		Short: "Create, modify, delete and list parents that Remote Storage Targets inherit settings from",
		Long: `Create, modify, delete and list parents that Remote Storage Targets inherit settings from.

A parent is a named set of settings of one target type, such as the endpoint and credentials many buckets share. It is not a target: no job uses it, and it needs no setting a target needs. A target names it with --parent, and every setting the target does not set itself is taken from the parent. A parent cannot name a parent.

Remote stores only what each target and parent sets. A change to a parent reaches every target below it at once, as a modify of each target would. Settings a target sets, and list entries with the same key, such as a storage class name, override the parent's.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(newParentListCmd(), newParentCreateCmd(), newParentModifyCmd(), newParentDeleteCmd())
	return cmd
}

func newParentCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <type>",
		Short: "Create a parent",
		Long: `Create a parent of the given type.

--name is required, and must not belong to another parent. Every other setting is optional.

A list entry, such as a storage class, must be complete, with every field a target would need. Targets inherit an entry whole, and never merge its fields with their own.`,
		Args: cobra.NoArgs,
	}
	for _, t := range cRst.TargetTypes() {
		cmd.AddCommand(newParentCreateTypeCmd(t))
	}
	return cmd
}

func newParentCreateTypeCmd(t cRst.TargetType) *cobra.Command {
	flags := newTargetFlags()
	cmd := &cobra.Command{
		Use:   t.Name(),
		Short: fmt.Sprintf("Create an %s parent", t.Name()),
		Example: fmt.Sprintf(`  # Settings every %s target of one endpoint shares
  beegfs remote target parent create %s --name=site \
    --endpoint-url=https://s3.example.com --access-key=AKIAEXAMPLE \
    --secret-key-file=/etc/beegfs/site.secret

  # A target that only adds its bucket
  beegfs remote target create %s --name=archive --parent=site --bucket=beegfs-archive`, t.Name(), t.Name(), t.Name()),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			parent, err := backend.NewParent(t.Name())
			if err != nil {
				return err
			}
			parent, err = backend.ApplyTargetChanges(parent, changes)
			if err != nil {
				return err
			}
			resp, err := backend.CreateParent(cmd.Context(), parent)
			if err != nil {
				return err
			}
			shown, err := shownParentAfterChange(cmd.Context(), resp.GetParent().GetName())
			if err != nil {
				return err
			}
			if jsonOutput() {
				return cmdfmt.PrintJSON(map[string]any{"parent": targetView(shown, false)})
			}
			fmt.Printf("Created remote target parent %s.\n", shown.effective.GetName())
			printTargetDetail(shown, false)
			return nil
		},
	}
	flags.registerCommon(cmd.Flags())
	cmd.MarkFlagRequired("name")
	flags.registerTypes(cmd.Flags(), []cRst.TargetType{t}, false)
	return cmd
}

func newParentModifyCmd() *cobra.Command {
	flags := newTargetFlags()
	var force bool
	cmd := &cobra.Command{
		Use:   "modify <name>",
		Short: "Modify a parent",
		Long: `Modify a parent.

Settings change as they do for "remote target modify". --name renames the parent, and every target that names it is changed to the new name.

The change reaches every target that names the parent. It is checked for each of them as a modify of that target would be. Changing where a target keeps its objects, such as its endpoint or bucket, is refused while unfinished jobs use the target, unless --force is given.`,
		Example: `  # Rotate the credentials of every target that inherits them
  beegfs remote target parent modify site --access-key=AKIANEW --secret-key-file=/etc/beegfs/site.secret.new`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			updated, resp, err := backend.UpdateParent(cmd.Context(), args[0], changes, force)
			if err != nil {
				return err
			}
			shown, err := shownParentAfterChange(cmd.Context(), updated.GetName())
			if err != nil {
				return err
			}
			if jsonOutput() {
				if err := cmdfmt.PrintJSON(parentChangeResult("parent", targetView(shown, false), resp.GetAffectedTargets(), resp.GetApply())); err != nil {
					return err
				}
				return applyError(resp.GetApply())
			}
			fmt.Printf("Modified remote target parent %s. %s\n", updated.GetName(), describeAffected(resp.GetAffectedTargets()))
			printTargetDetail(shown, false)
			return printApplyResult(resp.GetApply())
		},
	}
	flags.registerCommon(cmd.Flags())
	flags.registerTypes(cmd.Flags(), cRst.TargetTypes(), true)
	cmd.Flags().BoolVar(&force, "force", false, "Apply the change even when it moves where a target that names the parent keeps its objects while unfinished jobs use it.")
	return cmd
}

func newParentDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a parent",
		Long: `Delete a parent.

The delete is refused while any target names the parent, and the error lists those targets. First give each one another parent with "remote target modify <id> --parent=<name>", or remove its parent with "remote target modify <id> --parent=", which keeps the values it inherited as its own. So a delete never changes a target.`,
		Example: `  # Keep the values target 3 inherits, then delete the parent it named
  beegfs remote target modify 3 --parent=
  beegfs remote target parent delete team`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := backend.DeleteParent(cmd.Context(), name); err != nil {
				return err
			}
			if jsonOutput() {
				return cmdfmt.PrintJSON(map[string]any{"name": name})
			}
			fmt.Printf("Deleted remote target parent %s.\n", name)
			return nil
		},
	}
	return cmd
}

func newParentListCmd() *cobra.Command {
	var showSecrets bool
	var verbose bool
	cmd := &cobra.Command{
		Use:   "list [<name>...]",
		Short: "List parents and their settings",
		Long: `List parents and their settings.

By default one row is printed per parent. Each row shows the parent's settings and the targets that name it. Use --verbose, or name one or more parents, to print every setting of each parent. With --output json, --verbose or names print every column instead of the detail view.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := backend.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			names := args
			if len(names) == 0 {
				names = slices.Sorted(maps.Keys(targets.Parents))
			}
			shown := []shownTarget{}
			for _, name := range names {
				s, err := newShownParent(targets, name)
				if err != nil {
					return err
				}
				shown = append(shown, s)
			}
			printShownTargets(shown, newTargetColumns(true), verbose || len(args) > 0, showSecrets)
			return nil
		},
	}
	cmd.Flags().BoolVar(&showSecrets, "show-secrets", false, "Print secret keys in cleartext instead of masked.")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print every setting of each parent.")
	return cmd
}

// shownParentAfterChange reads the parents again and returns the parent called name for output,
// with the targets that name it.
func shownParentAfterChange(ctx context.Context, name string) (shownTarget, error) {
	targets, err := backend.GetTargets(ctx)
	if err != nil {
		return shownTarget{}, fmt.Errorf("the change was made, but the parent could not be read back: %w", err)
	}
	return newShownParent(targets, name)
}

// parentChangeResult returns the JSON output of a parent modify: what was changed, under
// key, the targets whose effective configuration changed, and how the change reached each sync
// node.
func parentChangeResult(key string, value any, affected []uint32, apply *beeremote.ApplyResult) map[string]any {
	if affected == nil {
		affected = []uint32{}
	}
	return map[string]any{
		key:                value,
		"affected-targets": affected,
		"apply":            applyView(apply),
	}
}

// describeAffected says which targets a parent change reached, as a sentence.
func describeAffected(affected []uint32) string {
	if len(affected) == 0 {
		return "No target changed."
	}
	ids := []string{}
	for _, id := range affected {
		ids = append(ids, strconv.FormatUint(uint64(id), 10))
	}
	return fmt.Sprintf("Changed remote targets %s.", strings.Join(ids, ", "))
}
