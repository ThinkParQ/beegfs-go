package rst

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/viper"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file formats RST configs for every command that prints one: the list views and the output
// of create, modify and delete. The settings and lists of a target come from its type's declaration
// (see rst.TargetType in common/rst), so a new type prints without code of its own here.
//
// Each command prints text in the table output mode, and one JSON object in the JSON modes (see
// config.OutputType). The JSON object uses the same names as the flags and list columns, such as
// "endpoint-url" and "storage-classes", so a script can map a flag to a field without a table.
//
// A target or parent may inherit values from a parent (see common/rst/inherit.go). Output shows its
// effective values, and marks each inherited one with the parent that sets it.

const maskedSecret = "*****"

// shownTarget is a target or parent as the CLI shows it.
type shownTarget struct {
	// effective has every value filled in, including what is inherited.
	effective *flex.RemoteStorageTarget
	// parent is the parent the definition names, or "". Always "" for a parent.
	parent string
	// inherited holds the keys of the values taken from parent, sorted and named as rst.Inherited
	// names them. Always empty for a parent.
	inherited []string
	// isParent is set when this is a parent. A parent has no ID, and shows its children.
	isParent bool
	// childTargets holds the IDs of the targets that name this parent. Only set for a parent.
	childTargets []uint32
}

// newShownTarget returns a target for output. effective comes from Remote for a target, or is
// resolved from parents for a parent. definition is the stored form, with only its own values.
// parents maps each parent's name to its definition.
func newShownTarget(effective, definition *flex.RemoteStorageTarget, parents map[string]*flex.RemoteStorageTarget) (shownTarget, error) {
	inherited, err := cRst.Inherited(definition, parents)
	if err != nil {
		return shownTarget{}, err
	}
	return shownTarget{effective: effective, parent: definition.GetParent(), inherited: inherited}, nil
}

// newShownParent returns the parent called name from targets for output. A parent names no parent,
// so it is shown as stored, with nothing inherited.
func newShownParent(targets backend.Targets, name string) (shownTarget, error) {
	definition, ok := targets.Parents[name]
	if !ok {
		return shownTarget{}, fmt.Errorf("remote target parent %q does not exist", name)
	}
	return shownTarget{
		effective:    definition,
		inherited:    []string{},
		isParent:     true,
		childTargets: targets.Children(name),
	}, nil
}

// title returns how a heading names the target, such as "Remote target 1 (archive)".
func (s shownTarget) title() string {
	if s.isParent {
		return fmt.Sprintf("Remote target parent %s", s.effective.GetName())
	}
	return fmt.Sprintf("Remote target %d (%s)", s.effective.GetId(), s.effective.GetName())
}

// from returns " (from <parent>)" when key is inherited, and "" otherwise.
func (s shownTarget) from(key string) string {
	if slices.Contains(s.inherited, key) {
		return " (from " + s.parent + ")"
	}
	return ""
}

// inheritedKeys returns the inherited keys joined, or "-" when there are none.
func (s shownTarget) inheritedKeys() string {
	return orDash(strings.Join(s.inherited, ", "))
}

// children returns the targets that name a parent as text, such as "targets 1, 2", or "-".
func (s shownTarget) children() string {
	if len(s.childTargets) == 0 {
		return "-"
	}
	ids := []string{}
	for _, id := range s.childTargets {
		ids = append(ids, strconv.FormatUint(uint64(id), 10))
	}
	return "targets " + strings.Join(ids, ", ")
}

// jsonOutput reports whether --output asks for JSON rather than text.
func jsonOutput() bool {
	return config.OutputType(viper.GetString(config.OutputKey)).IsJSON()
}

func formatPolicies(policies *flex.RemoteStorageTarget_Policies) string {
	return "fast-start-max-size=" + util.I64FormatPrefixWithUnlimited(policies.GetFastStartMaxSize(), unitconv.IEC, 0, true)
}

// policiesView returns policies for JSON output. Sizes are in bytes, so scripts need not parse a
// unit.
func policiesView(policies *flex.RemoteStorageTarget_Policies) map[string]any {
	return map[string]any{"fast-start-max-size": policies.GetFastStartMaxSize()}
}

// settingValue returns the value of setting s in target, masking a secret unless showSecrets is set.
func settingValue(s cRst.Setting, target *flex.RemoteStorageTarget, showSecrets bool) string {
	v := s.Get(target)
	if s.Secret && !showSecrets && v != "" {
		return maskedSecret
	}
	return v
}

// listKeys returns the keys of every entry of list in target, joined, or "-" when it has none.
func listKeys(list cRst.KeyedList, target *flex.RemoteStorageTarget) string {
	keys := []string{}
	for _, entry := range list.Entries(target) {
		keys = append(keys, entry.Key)
	}
	return orDash(strings.Join(keys, ", "))
}

// listView returns every entry of list in target for JSON output. Each entry is an object with the
// list's key field and every other field that is set, such as
// {"name": "GLACIER", "retrieval-tier": "Bulk"}. A list with no entries is an empty array, not null.
func listView(list cRst.KeyedList, target *flex.RemoteStorageTarget) []map[string]string {
	view := []map[string]string{}
	for _, entry := range list.Entries(target) {
		item := map[string]string{list.Key().Key: entry.Key}
		for i, field := range list.Fields() {
			if entry.Values[i] != "" {
				item[field.Key] = entry.Values[i]
			}
		}
		view = append(view, item)
	}
	return view
}

// targetView returns shown for JSON output: name, type and policies, then every effective setting
// and keyed list its type declares. A target also has its id, its parent, and the sorted keys of
// what it inherits from that parent. A parent has its children instead. A secret is masked unless
// showSecrets is set. A target whose type no client registered, such as mock, has no settings or
// lists.
func targetView(shown shownTarget, showSecrets bool) map[string]any {
	target := shown.effective
	view := map[string]any{
		"name":     target.GetName(),
		"type":     cRst.TypeName(target),
		"policies": policiesView(target.GetPolicies()),
	}
	if shown.isParent {
		view["children"] = map[string]any{"targets": shown.childTargets}
	} else {
		view["id"] = target.GetId()
		view["parent"] = shown.parent
		view["inherited"] = shown.inherited
	}
	t, ok := cRst.TargetTypeOf(target)
	if !ok {
		return view
	}
	for _, s := range t.Settings() {
		view[s.Key] = settingValue(s, target, showSecrets)
	}
	for _, list := range t.Lists() {
		view[list.Plural()] = listView(list, target)
	}
	return view
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// printTargetDetail prints one target or parent as a sectioned block, with every setting and one
// table per keyed list. An inherited value is followed by the parent that sets it.
func printTargetDetail(shown shownTarget, showSecrets bool) {
	target := shown.effective
	fmt.Println(shown.title())
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  type:\t%s\n", cRst.TypeName(target))
	if shown.isParent {
		fmt.Fprintf(w, "  children:\t%s\n", shown.children())
	} else {
		fmt.Fprintf(w, "  parent:\t%s\n", orDash(shown.parent))
	}
	fmt.Fprintf(w, "  policies:\t%s%s\n", formatPolicies(target.GetPolicies()), shown.from("fast-start-max-size"))
	t, ok := cRst.TargetTypeOf(target)
	if !ok {
		w.Flush()
		return
	}
	for _, s := range t.Settings() {
		fmt.Fprintf(w, "  %s:\t%s%s\n", s.Key, orDash(settingValue(s, target, showSecrets)), shown.from(s.Key))
	}
	w.Flush()

	for _, list := range t.Lists() {
		fmt.Printf("  %s:\n", list.Plural())
		entries := list.Entries(target)
		if len(entries) == 0 {
			fmt.Println("    -")
			continue
		}
		header := []string{strings.ToUpper(list.Key().Key)}
		for _, field := range list.Fields() {
			header = append(header, strings.ToUpper(field.Key))
		}
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "    %s\n", strings.Join(header, "\t"))
		for _, entry := range entries {
			row := []string{entry.Key + shown.from(list.Flag()+"="+entry.Key)}
			for _, v := range entry.Values {
				row = append(row, orDash(v))
			}
			fmt.Fprintf(w, "    %s\n", strings.Join(row, "\t"))
		}
		w.Flush()
	}
}

// applyView returns how a change reached each sync node, for JSON output. A change that reached no
// node, such as a delete dry run, has an empty node list.
func applyView(apply *beeremote.ApplyResult) map[string]any {
	nodes := []map[string]string{}
	for _, n := range apply.GetNodes() {
		nodes = append(nodes, map[string]string{
			"node-id": n.GetNodeId(),
			"outcome": strings.ToLower(n.GetOutcome().String()),
			"message": n.GetMessage(),
		})
	}
	return map[string]any{"nodes": nodes}
}

// applyError returns an error when a connected node rejected the change, so the command exits
// non-zero in every output mode.
func applyError(apply *beeremote.ApplyResult) error {
	rejected := 0
	for _, n := range apply.GetNodes() {
		if n.GetOutcome() == beeremote.ApplyResult_REJECTED {
			rejected++
		}
	}
	if rejected > 0 {
		return fmt.Errorf("%d sync nodes rejected the change and receive no work until they accept it (hint: check their logs)", rejected)
	}
	return nil
}

// printApplyResult prints how a change reached each sync node. It prints a table of the nodes only
// when some did not apply the change. It returns applyError.
func printApplyResult(apply *beeremote.ApplyResult) error {
	nodes := apply.GetNodes()
	applied := 0
	for _, n := range nodes {
		if n.GetOutcome() == beeremote.ApplyResult_APPLIED {
			applied++
		}
	}
	fmt.Printf("Applied on %d of %d sync nodes.\n", applied, len(nodes))
	if applied < len(nodes) {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  NODE\tOUTCOME\tMESSAGE")
		for _, n := range nodes {
			fmt.Fprintf(w, "  %s\t%s\t%s\n", n.GetNodeId(), strings.ToLower(n.GetOutcome().String()), orDash(n.GetMessage()))
		}
		w.Flush()
	}
	return applyError(apply)
}
