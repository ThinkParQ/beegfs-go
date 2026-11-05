package rst

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/thinkparq/beegfs-go/common/registry"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// TargetChanges is a set of changes to an RST config. A nil or absent field leaves the value it
// would set unchanged. ApplyTargetChanges applies the set to a config, for a new RST and for an
// existing one alike.
//
// Settings and lists are named the way the RST's type declares them (see rst.TargetType in
// common/rst), so this works for every type without code of its own.
type TargetChanges struct {
	Name *string
	// Parent names the parent to inherit unset values from. "" removes the parent, and the caller
	// decides what the RST keeps (see UpdateTarget).
	Parent   *string
	Policies PolicyChanges
	// Settings maps the key of a setting, such as "bucket", to its new value as text.
	Settings map[string]string
	// Lists maps the flag of a keyed list, such as "storage-class", to the changes to that list.
	Lists map[string]ListChanges
}

type PolicyChanges struct {
	FastStartMaxSize *int64
}

// ListChanges changes one keyed list. They apply in field order: Clear, then Remove, then Upsert.
type ListChanges struct {
	Clear  bool
	Remove []string
	Upsert []ListEntryChange
}

// ListEntryChange adds the entry identified by Key when it is new. Otherwise it changes only the
// fields in Values.
type ListEntryChange struct {
	Key    string
	Values map[string]string
}

// NewTarget returns an empty config of the type called typeName, ready for ApplyTargetChanges. id may
// be 0 to let Remote assign one.
func NewTarget(typeName string, id uint32) (*flex.RemoteStorageTarget, error) {
	t, ok := cRst.TargetTypeByName(typeName)
	if !ok {
		return nil, fmt.Errorf("unsupported remote target type %q", typeName)
	}
	target := t.NewConfig()
	target.SetId(id)
	return target, nil
}

// ApplyTargetChanges returns a copy of target with changes applied. It refuses a setting or list the
// target's type does not have, such as a bulk operation for an s3 target, and the removal of a list
// entry the target does not have. The type parses each value, so a malformed value is refused here.
// It does not validate the result as a whole. Remote does that when it stores the config.
func ApplyTargetChanges(target *flex.RemoteStorageTarget, changes TargetChanges) (*flex.RemoteStorageTarget, error) {
	target = proto.Clone(target).(*flex.RemoteStorageTarget)

	if changes.Name != nil {
		target.SetName(*changes.Name)
	}
	if changes.Parent != nil {
		target.SetParent(*changes.Parent)
	}
	if changes.Policies.FastStartMaxSize != nil {
		if !target.HasPolicies() {
			target.SetPolicies(&flex.RemoteStorageTarget_Policies{})
		}
		target.GetPolicies().SetFastStartMaxSize(*changes.Policies.FastStartMaxSize)
	}

	t, ok := cRst.TargetTypeOf(target)
	if !ok {
		if len(changes.Settings) == 0 && len(changes.Lists) == 0 {
			return target, nil
		}
		return nil, fmt.Errorf("remote target %d has type %s, whose settings the CLI cannot change", target.GetId(), cRst.TypeName(target))
	}

	for _, key := range slices.Sorted(maps.Keys(changes.Settings)) {
		i := slices.IndexFunc(t.Settings(), func(s cRst.Setting) bool { return s.Key == key })
		if i < 0 {
			return nil, notForType("--"+key, t, TargetTypesWith(key))
		}
		if err := t.Settings()[i].Set(target, changes.Settings[key]); err != nil {
			return nil, fmt.Errorf("invalid --%s: %w", key, err)
		}
	}

	for _, flag := range slices.Sorted(maps.Keys(changes.Lists)) {
		i := slices.IndexFunc(t.Lists(), func(l cRst.KeyedList) bool { return l.Flag() == flag })
		if i < 0 {
			return nil, notForType("--"+flag, t, TargetTypesWith(flag))
		}
		if err := applyListChanges(target, t.Lists()[i], changes.Lists[flag]); err != nil {
			return nil, err
		}
	}
	return target, nil
}

func applyListChanges(target *flex.RemoteStorageTarget, list cRst.KeyedList, changes ListChanges) error {
	if changes.Clear {
		list.Clear(target)
	}
	for _, key := range changes.Remove {
		if err := list.Remove(target, key); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, entry := range changes.Upsert {
		if seen[entry.Key] {
			return fmt.Errorf("%s %q is given more than once", list.Flag(), entry.Key)
		}
		seen[entry.Key] = true
		if err := list.Upsert(target, entry.Key, entry.Values); err != nil {
			return fmt.Errorf("invalid --%s %q: %w", list.Flag(), entry.Key, err)
		}
	}
	return nil
}

func notForType(flag string, t cRst.TargetType, typesWith []string) error {
	if len(typesWith) == 0 {
		return fmt.Errorf("%s does not apply to any remote target type", flag)
	}
	return fmt.Errorf("%s does not apply to %s targets (it applies to %s targets)", flag, t.Name(), strings.Join(typesWith, " and "))
}

// TargetTypesWith returns the names of the types that have a setting or keyed list called name, such
// as "bucket" or "bulk-operation", sorted.
func TargetTypesWith(name string) []string {
	names := []string{}
	for _, t := range cRst.TargetTypes() {
		hasSetting := slices.ContainsFunc(t.Settings(), func(s cRst.Setting) bool { return s.Key == name })
		hasList := slices.ContainsFunc(t.Lists(), func(l cRst.KeyedList) bool { return l.Flag() == name })
		if hasSetting || hasList {
			names = append(names, t.Name())
		}
	}
	return names
}

// requireRSTManagement returns an error when Remote cannot create, update or delete RSTs.
func requireRSTManagement(ctx context.Context) (beeremote.BeeRemoteClient, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}
	reg, err := config.BeeRemoteRegistry(ctx, false)
	if err != nil {
		return nil, err
	}
	if err := reg.RequireFeature(ctx, registry.FeatureRSTManagement); err != nil {
		return nil, fmt.Errorf("this Remote cannot manage remote targets (hint: upgrade beegfs-remote): %w", err)
	}
	return beeRemote, nil
}

// Targets holds every live RST and every parent Remote stores, in the forms the CLI needs. A
// definition is what Remote stores for an RST or parent: only the values it sets itself. An effective
// configuration has the values it inherits from its parents filled in (see common/rst/inherit.go).
type Targets struct {
	// Effective holds the effective configuration of every live RST, sorted by ID. The job builder
	// is left out, because no one configures it.
	Effective []*flex.RemoteStorageTarget
	// Definitions maps the ID of every live RST to its definition.
	Definitions map[uint32]*flex.RemoteStorageTarget
	// Parents maps the name of every parent to its definition.
	Parents map[string]*flex.RemoteStorageTarget
}

// GetTargets reads every live RST and every parent from Remote.
//
// It does not require the rst-management feature, because "remote target list" must keep working
// with an older Remote. Such a Remote ignores the request for definitions, and has no parents, so
// each RST's definition is its effective configuration.
func GetTargets(ctx context.Context) (Targets, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return Targets{}, err
	}
	resp, err := beeRemote.GetRSTConfig(ctx, beeremote.GetRSTConfigRequest_builder{IncludeDefinitions: true}.Build())
	if err != nil {
		return Targets{}, err
	}
	targets := Targets{
		Effective:   []*flex.RemoteStorageTarget{},
		Definitions: map[uint32]*flex.RemoteStorageTarget{},
		Parents:     map[string]*flex.RemoteStorageTarget{},
	}
	for _, target := range resp.GetRsts() {
		if target.GetId() != cRst.JobBuilderRstId {
			targets.Effective = append(targets.Effective, target)
		}
	}
	slices.SortFunc(targets.Effective, func(a, b *flex.RemoteStorageTarget) int { return int(a.GetId()) - int(b.GetId()) })
	for _, effective := range targets.Effective {
		targets.Definitions[effective.GetId()] = effective
	}
	for _, definition := range resp.GetDefinitions() {
		targets.Definitions[definition.GetId()] = definition
	}
	for _, parent := range resp.GetParents() {
		targets.Parents[parent.GetName()] = parent
	}
	return targets, nil
}

// EffectiveOf returns the effective configuration of the live RST id.
func (t Targets) EffectiveOf(id uint32) (*flex.RemoteStorageTarget, bool) {
	i := slices.IndexFunc(t.Effective, func(target *flex.RemoteStorageTarget) bool { return target.GetId() == id })
	if i < 0 {
		return nil, false
	}
	return t.Effective[i], true
}

// Children returns the IDs of the live RSTs that name the parent called name, sorted. A parent names
// no parent, so these are every RST the parent's values reach.
func (t Targets) Children(name string) []uint32 {
	ids := []uint32{}
	for id, definition := range t.Definitions {
		if definition.GetParent() == name {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// startingPoint returns the definition a modify applies its changes to. read is the definition as
// stored, and effective is the RST's effective configuration. The rules:
//   - Changes apply to read, so values the RST inherits stay inherited. That includes a change that
//     removes the parent: Remote then fills in the values the RST inherited, from the effective
//     configuration it holds when it applies the change (see UpdateRST in Remote).
//   - A list entry the change names, and the RST only inherits, is copied whole from effective into
//     the result first. Upsert then changes only the fields given. Without the copy, Upsert would
//     add an entry with only those fields. Entries are inherited whole, so that entry would replace
//     the parent's, and every other field the parent set would be lost.
//   - With the list's Clear, nothing is copied. The admin asked for the RST's own entries to go, so
//     an entry named after it is a new one, given in full.
//
// read is never modified.
func startingPoint(read *flex.RemoteStorageTarget, effective *flex.RemoteStorageTarget, changes TargetChanges) (*flex.RemoteStorageTarget, error) {
	t, ok := cRst.TargetTypeOf(read)
	if !ok || read.GetParent() == "" {
		return read, nil
	}
	start := proto.Clone(read).(*flex.RemoteStorageTarget)
	for _, list := range t.Lists() {
		listChanges, ok := changes.Lists[list.Flag()]
		if !ok || listChanges.Clear {
			continue
		}
		own := entryKeys(list, read)
		inherited := entryKeys(list, effective)
		for _, entry := range listChanges.Upsert {
			if slices.Contains(own, entry.Key) || !slices.Contains(inherited, entry.Key) {
				continue
			}
			if err := list.CopyEntry(start, effective, entry.Key); err != nil {
				return nil, fmt.Errorf("unable to copy the inherited %s %q before changing it: %w", list.Flag(), entry.Key, err)
			}
		}
	}
	return start, nil
}

func entryKeys(list cRst.KeyedList, config *flex.RemoteStorageTarget) []string {
	keys := []string{}
	for _, entry := range list.Entries(config) {
		keys = append(keys, entry.Key)
	}
	return keys
}

// hintInherited adds a hint to an error from ApplyTargetChanges when the change removes list
// entries from an RST or parent with a parent. The entry may be one it inherits, which only the
// parent that sets it can remove.
func hintInherited(err error, read *flex.RemoteStorageTarget, changes TargetChanges) error {
	if read.GetParent() == "" {
		return err
	}
	for _, list := range changes.Lists {
		if len(list.Remove) > 0 {
			return fmt.Errorf("%w (hint: an entry inherited from parent %q can only be removed from the parent that sets it)", err, read.GetParent())
		}
	}
	return err
}

// CreateTarget creates target and returns it as Remote stored it. With force, Remote accepts the ID
// of a deleted target, and that target's unfinished jobs continue on the new one.
func CreateTarget(ctx context.Context, target *flex.RemoteStorageTarget, force bool) (*beeremote.CreateRSTResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	return beeRemote.CreateRST(ctx, beeremote.CreateRSTRequest_builder{Rst: target, Force: force}.Build())
}

// UpdateTarget reads the definition of the RST id, applies changes, and stores the result. It
// returns the new definition. Remote refuses the update if someone else changed the RST after it
// was read. With force, Remote moves the RST to another endpoint, region, bucket or partition even
// while jobs use it.
//
// Changes apply to the definition, not to the effective configuration, so the RST keeps inheriting
// what it inherited. Setting a setting to "" makes it inherit again. Removing the parent is the
// exception: see startingPoint.
func UpdateTarget(ctx context.Context, id uint32, changes TargetChanges, force bool) (*flex.RemoteStorageTarget, *beeremote.UpdateRSTResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, nil, err
	}
	targets, err := GetTargets(ctx)
	if err != nil {
		return nil, nil, err
	}
	read, ok := targets.Definitions[id]
	effective, live := targets.EffectiveOf(id)
	if !ok || !live {
		return nil, nil, fmt.Errorf("remote target %d does not exist", id)
	}
	start, err := startingPoint(read, effective, changes)
	if err != nil {
		return nil, nil, err
	}
	updated, err := ApplyTargetChanges(start, changes)
	if err != nil {
		return nil, nil, hintInherited(err, read, changes)
	}
	resp, err := beeRemote.UpdateRST(ctx, beeremote.UpdateRSTRequest_builder{Rst: updated, Expected: read, Force: force}.Build())
	if err != nil {
		return nil, nil, err
	}
	return updated, resp, nil
}

// NewParent returns an empty parent of the type called typeName, ready for ApplyTargetChanges.
func NewParent(typeName string) (*flex.RemoteStorageTarget, error) {
	return NewTarget(typeName, 0)
}

// CreateParent creates parent and returns it as Remote stored it.
func CreateParent(ctx context.Context, parent *flex.RemoteStorageTarget) (*beeremote.CreateRSTParentResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	return beeRemote.CreateRSTParent(ctx, beeremote.CreateRSTParentRequest_builder{Parent: parent}.Build())
}

// UpdateParent reads the parent called name, applies changes, and stores the result. It returns
// the new definition. A new name renames the parent, and Remote changes every RST that names it.
// The change reaches every RST that names the parent. With force, Remote applies it even when it
// moves an RST that unfinished jobs use. A parent names no parent, so it inherits nothing, and the
// changes apply to it as stored.
func UpdateParent(ctx context.Context, name string, changes TargetChanges, force bool) (*flex.RemoteStorageTarget, *beeremote.UpdateRSTParentResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, nil, err
	}
	targets, err := GetTargets(ctx)
	if err != nil {
		return nil, nil, err
	}
	read, ok := targets.Parents[name]
	if !ok {
		return nil, nil, fmt.Errorf("remote target parent %q does not exist", name)
	}
	updated, err := ApplyTargetChanges(read, changes)
	if err != nil {
		return nil, nil, err
	}
	resp, err := beeRemote.UpdateRSTParent(ctx, beeremote.UpdateRSTParentRequest_builder{Name: name, Parent: updated, Expected: read, Force: force}.Build())
	if err != nil {
		return nil, nil, err
	}
	return updated, resp, nil
}

// DeleteParent deletes the parent called name. Remote refuses the delete while any RST names the
// parent, so a delete never changes an RST.
func DeleteParent(ctx context.Context, name string) error {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return err
	}
	_, err = beeRemote.DeleteRSTParent(ctx, beeremote.DeleteRSTParentRequest_builder{Name: name}.Build())
	return err
}

// DeleteTarget deletes the RST id. With dryRun it only reports how many unfinished jobs use it.
func DeleteTarget(ctx context.Context, id uint32, reason string, dryRun bool) (*beeremote.DeleteRSTResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	request := beeremote.DeleteRSTRequest_builder{Id: id, DryRun: dryRun}.Build()
	if reason != "" {
		request.SetReason(reason)
	}
	return beeRemote.DeleteRST(ctx, request)
}

// GetDeletedTargets returns the record of every deleted RST.
//
// An older Remote ignores the request for deleted targets and returns none, which would look like
// nothing was ever deleted. So this requires the target management feature, which came with deletes.
func GetDeletedTargets(ctx context.Context) ([]*beeremote.DeletedRST, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := beeRemote.GetRSTConfig(ctx, beeremote.GetRSTConfigRequest_builder{IncludeDeleted: true}.Build())
	if err != nil {
		return nil, err
	}
	return resp.GetDeleted(), nil
}
