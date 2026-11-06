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
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
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
// A target may inherit values from a backend (see common/rst/backend.go). Output shows its
// effective values, and marks each inherited one with the backend that sets it. A backend's output
// leaves out the bucket setting (see rst.BucketSetting), because a backend never has one.

const maskedSecret = "*****"

// shownTarget is a target or backend as the CLI shows it.
type shownTarget struct {
	// effective has every value filled in, including what is inherited.
	effective *flex.RemoteStorageTarget
	// backend is the backend the definition names, or "". Always "" for a backend.
	backend string
	// inherited holds the keys of the values taken from backend, sorted and named as rst.Inherited
	// names them. Always empty for a backend.
	inherited []string
	// isBackend is set when this is a backend. A backend has no ID, and shows the targets that name
	// it.
	isBackend bool
	// usedBy holds the IDs of the targets that name this backend. Only set for a backend.
	usedBy []uint32
}

// newShownTarget returns a target for output. effective comes from Remote for a target, or is
// resolved from backends for a backend. definition is the stored form, with only its own values.
// backends maps each backend's name to its definition.
func newShownTarget(effective, definition *flex.RemoteStorageTarget, backends map[string]*flex.RemoteStorageTarget) (shownTarget, error) {
	inherited, err := cRst.Inherited(definition, backends)
	if err != nil {
		return shownTarget{}, err
	}
	return shownTarget{effective: effective, backend: definition.GetBackend(), inherited: inherited}, nil
}

// newShownBackend returns the backend called name from targets for output. A backend names no
// backend, so it is shown as stored, with nothing inherited.
func newShownBackend(targets rst.Targets, name string) (shownTarget, error) {
	definition, ok := targets.Backends[name]
	if !ok {
		return shownTarget{}, fmt.Errorf("remote backend %q does not exist", name)
	}
	return shownTarget{
		effective: definition,
		inherited: []string{},
		isBackend: true,
		usedBy:    targets.TargetsUsing(name),
	}, nil
}

// title returns how a heading names the target, such as "Remote target 1 (archive)".
func (s shownTarget) title() string {
	if s.isBackend {
		return fmt.Sprintf("Remote backend %s", s.effective.GetName())
	}
	return fmt.Sprintf("Remote target %d (%s)", s.effective.GetId(), s.effective.GetName())
}

// from returns " (from <backend>)" when key is inherited, and "" otherwise.
func (s shownTarget) from(key string) string {
	if slices.Contains(s.inherited, key) {
		return " (from " + s.backend + ")"
	}
	return ""
}

// inheritedKeys returns the inherited keys joined, or "-" when there are none.
func (s shownTarget) inheritedKeys() string {
	return orDash(strings.Join(s.inherited, ", "))
}

// usedByText returns the IDs of the targets that name a backend as text, such as "1, 2", or "-".
func (s shownTarget) usedByText() string {
	return orDash(formatIDs(s.usedBy))
}

// settings returns the settings of t that shown can have: all of them for a target, and those its
// type allows on a backend for a backend.
func (s shownTarget) settings(t cRst.TargetType) []cRst.Setting {
	if !s.isBackend {
		return t.Settings()
	}
	return settingsWithoutBucket(t)
}

// settingsWithoutBucket returns the settings of t but the one that names a target's bucket. A
// backend never holds a bucket, and target modify cannot change one.
func settingsWithoutBucket(t cRst.TargetType) []cRst.Setting {
	settings := []cRst.Setting{}
	for _, s := range t.Settings() {
		if s.Kind != cRst.BucketSetting {
			settings = append(settings, s)
		}
	}
	return settings
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

// settingValue returns the value of setting s that target uses, masking a secret unless showSecrets
// is set. A setting target leaves unset shows its default, such as the S3 region (see
// Setting.Effective). target is an effective configuration, so a value its backend provides shows
// too.
func settingValue(s cRst.Setting, target *flex.RemoteStorageTarget, showSecrets bool) string {
	v := s.Effective(target)
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
// and keyed list its type declares. A target also has its id, its backend, and the sorted keys of
// what it inherits from that backend. A backend has the IDs of the targets that name it instead,
// and leaves out the bucket setting. A secret is masked unless showSecrets is set.
// A target whose type no client registered, such as mock, has no settings or lists.
func targetView(shown shownTarget, showSecrets bool) map[string]any {
	target := shown.effective
	view := map[string]any{
		"name":     target.GetName(),
		"type":     cRst.TypeName(target),
		"policies": policiesView(target.GetPolicies()),
	}
	if shown.isBackend {
		view["targets"] = shown.usedBy
	} else {
		view["id"] = target.GetId()
		view["backend"] = shown.backend
		view["inherited"] = shown.inherited
	}
	t, ok := cRst.TargetTypeOf(target)
	if !ok {
		return view
	}
	for _, s := range shown.settings(t) {
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

// printTargetDetail prints one target or backend as a sectioned block, with every setting and one
// table per keyed list. An inherited value is followed by the backend that sets it.
func printTargetDetail(shown shownTarget, showSecrets bool) {
	target := shown.effective
	fmt.Println(shown.title())
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  type:\t%s\n", cRst.TypeName(target))
	if shown.isBackend {
		fmt.Fprintf(w, "  targets:\t%s\n", shown.usedByText())
	} else {
		fmt.Fprintf(w, "  backend:\t%s\n", orDash(shown.backend))
	}
	fmt.Fprintf(w, "  policies:\t%s%s\n", formatPolicies(target.GetPolicies()), shown.from("fast-start-max-size"))
	t, ok := cRst.TargetTypeOf(target)
	if !ok {
		w.Flush()
		return
	}
	for _, s := range shown.settings(t) {
		source := shown.from(s.Key)
		if s.Get(target) == "" && s.Default != "" {
			source = " (default)"
		}
		fmt.Fprintf(w, "  %s:\t%s%s\n", s.Key, orDash(settingValue(s, target, showSecrets)), source)
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

// bucketResultView returns what a target change did to the target's bucket, for JSON output. It
// returns nil when the change did nothing to a bucket, such as a create of a type without one.
func bucketResultView(bucket *beeremote.BucketResult) map[string]any {
	if bucket.GetAction() == beeremote.BucketResult_NONE {
		return nil
	}
	return map[string]any{
		"name":   bucket.GetName(),
		"action": strings.ToLower(bucket.GetAction().String()),
	}
}

// printBucketResult prints one line that says what a target change did to the target's bucket. It
// prints nothing when the change did nothing to a bucket.
func printBucketResult(bucket *beeremote.BucketResult) {
	switch bucket.GetAction() {
	case beeremote.BucketResult_CREATED:
		fmt.Printf("Created bucket %s.\n", bucket.GetName())
	case beeremote.BucketResult_ADOPTED:
		fmt.Printf("Adopted existing bucket %s.\n", bucket.GetName())
	}
}

// adoptResult returns the JSON output of an adopt of every unused bucket: the targets created, or
// that a dry run would create, and the buckets skipped because targets use them. bucket is the
// setting that names a target's bucket.
func adoptResult(resp *beeremote.AdoptRSTBucketsResponse, bucket cRst.Setting, dryRun bool) map[string]any {
	adopted := []map[string]any{}
	for _, a := range resp.GetAdopted() {
		adopted = append(adopted, map[string]any{
			"id":     a.GetRst().GetId(),
			"name":   a.GetRst().GetName(),
			"bucket": bucket.Get(a.GetRst()),
		})
	}
	skipped := []map[string]any{}
	for _, s := range resp.GetSkipped() {
		skipped = append(skipped, map[string]any{
			"bucket":          s.GetBucket(),
			"live-targets":    nonNilIDs(s.GetLiveTargets()),
			"deleted-targets": nonNilIDs(s.GetDeletedTargets()),
		})
	}
	return map[string]any{
		"dry-run": dryRun,
		"adopted": adopted,
		"skipped": skipped,
		"apply":   applyView(resp.GetApply()),
	}
}

func nonNilIDs(ids []uint32) []uint32 {
	if ids == nil {
		return []uint32{}
	}
	return ids
}

// printAdoptResult prints the targets an adopt of every unused bucket created, or that a dry run
// would create, then the buckets it skipped and which targets use them. bucket is the setting that
// names a target's bucket.
func printAdoptResult(resp *beeremote.AdoptRSTBucketsResponse, bucket cRst.Setting, backend string, dryRun bool) {
	verb := "Adopted"
	if dryRun {
		verb = "Would adopt"
	}
	if len(resp.GetAdopted()) == 0 {
		fmt.Printf("No unused bucket to adopt in the store of remote backend %s.\n", backend)
	} else {
		fmt.Printf("%s %d buckets of remote backend %s as targets:\n", verb, len(resp.GetAdopted()), backend)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  ID\tNAME\tBUCKET")
		for _, a := range resp.GetAdopted() {
			fmt.Fprintf(w, "  %d\t%s\t%s\n", a.GetRst().GetId(), a.GetRst().GetName(), bucket.Get(a.GetRst()))
		}
		w.Flush()
	}
	if len(resp.GetSkipped()) == 0 {
		return
	}
	fmt.Printf("Skipped %d buckets that targets use:\n", len(resp.GetSkipped()))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  BUCKET\tUSED BY")
	for _, s := range resp.GetSkipped() {
		users := []string{}
		if ids := s.GetLiveTargets(); len(ids) > 0 {
			users = append(users, "target "+formatIDs(ids))
		}
		if ids := s.GetDeletedTargets(); len(ids) > 0 {
			users = append(users, "deleted target "+formatIDs(ids))
		}
		fmt.Fprintf(w, "  %s\t%s\n", s.GetBucket(), strings.Join(users, ", "))
	}
	w.Flush()
}

func formatIDs(ids []uint32) string {
	text := []string{}
	for _, id := range ids {
		text = append(text, strconv.FormatUint(uint64(id), 10))
	}
	return strings.Join(text, ", ")
}
