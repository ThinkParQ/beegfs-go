package rst

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

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
	// Backend names the backend to inherit unset values from. "" removes the backend, and the
	// caller decides what the RST keeps (see UpdateTarget).
	Backend  *string
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
	if changes.Backend != nil {
		target.SetBackend(*changes.Backend)
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

// Targets holds every live RST and every backend Remote stores, in the forms the CLI needs. A
// definition is what Remote stores for an RST or backend: only the values it sets itself. An
// effective configuration has the values it inherits from its backend filled in (see
// common/rst/backend.go).
type Targets struct {
	// Effective holds the effective configuration of every live RST, sorted by ID. The job builder
	// is left out, because no one configures it.
	Effective []*flex.RemoteStorageTarget
	// Definitions maps the ID of every live RST to its definition.
	Definitions map[uint32]*flex.RemoteStorageTarget
	// Backends maps the name of every backend to its definition.
	Backends map[string]*flex.RemoteStorageTarget
}

// GetTargets reads every live RST and every backend from Remote.
//
// It does not require the rst-management feature, because "remote target list" must keep working
// with an older Remote. Such a Remote ignores the request for definitions, and has no backends, so
// each RST's definition is its effective configuration.
func GetTargets(ctx context.Context) (Targets, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return Targets{}, err
	}
	resp, err := beeRemote.GetRSTConfig(ctx, &beeremote.GetRSTConfigRequest{IncludeDefinitions: true})
	if err != nil {
		return Targets{}, err
	}
	targets := Targets{
		Effective:   []*flex.RemoteStorageTarget{},
		Definitions: map[uint32]*flex.RemoteStorageTarget{},
		Backends:    map[string]*flex.RemoteStorageTarget{},
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
	for _, backend := range resp.GetBackends() {
		targets.Backends[backend.GetName()] = backend
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

// TargetsUsing returns the IDs of the live RSTs that name the backend called name, sorted. A
// backend names no backend, so these are every RST the backend's values reach.
func (t Targets) TargetsUsing(name string) []uint32 {
	ids := []uint32{}
	for id, definition := range t.Definitions {
		if definition.GetBackend() == name {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// startingPoint returns the definition a modify applies its changes to. read is the definition as
// stored, and effective is the RST's effective configuration. The rules:
//   - Changes apply to read, so values the RST inherits stay inherited. That includes a change that
//     removes the backend: Remote then fills in the values the RST inherited, from the backend's
//     definition it holds when it applies the change (see UpdateRST in Remote).
//   - A list entry the change names, and the RST only inherits, is copied whole from effective into
//     the result first. Upsert then changes only the fields given. Without the copy, Upsert would
//     add an entry with only those fields. Entries are inherited whole, so that entry would replace
//     the backend's, and every other field the backend set would be lost.
//   - With the list's Clear, nothing is copied. The admin asked for the RST's own entries to go, so
//     an entry named after it is a new one, given in full.
//
// read is never modified.
func startingPoint(read *flex.RemoteStorageTarget, effective *flex.RemoteStorageTarget, changes TargetChanges) (*flex.RemoteStorageTarget, error) {
	t, ok := cRst.TargetTypeOf(read)
	if !ok || read.GetBackend() == "" {
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
// entries from an RST or backend with a backend. The entry may be one it inherits, which only the
// backend that sets it can remove.
func hintInherited(err error, read *flex.RemoteStorageTarget, changes TargetChanges) error {
	if read.GetBackend() == "" {
		return err
	}
	for _, list := range changes.Lists {
		if len(list.Remove) > 0 {
			return fmt.Errorf("%w (hint: an entry inherited from backend %q can only be removed from the backend that sets it)", err, read.GetBackend())
		}
	}
	return err
}

// CreateTarget creates target and returns it as Remote stored it. With force, Remote accepts the ID
// of a deleted target, and that target's unfinished jobs continue on the new one.
//
// A target that names a backend is checked against the backends first (checkTargetBackend), so a
// backend of another type is refused before the create is sent.
func CreateTarget(ctx context.Context, target *flex.RemoteStorageTarget, force bool) (*beeremote.CreateRSTResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	if target.GetBackend() != "" {
		targets, err := GetTargets(ctx)
		if err != nil {
			return nil, err
		}
		if err := checkTargetBackend(target, targets.Backends); err != nil {
			return nil, err
		}
	}
	return beeRemote.CreateRST(ctx, &beeremote.CreateRSTRequest{Rst: target, Force: force})
}

// UpdateTarget reads the definition of the RST id, applies changes, and stores the result. It
// returns the new definition. Remote refuses the update if someone else changed the RST after it
// was read. With force, Remote moves the RST to another endpoint, region, bucket or partition even
// while jobs use it.
//
// Changes apply to the definition, not to the effective configuration, so the RST keeps inheriting
// what it inherited. Setting a setting to "" makes it inherit again. Removing the backend is the
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
	if err := checkTargetBackend(updated, targets.Backends); err != nil {
		return nil, nil, err
	}
	resp, err := beeRemote.UpdateRST(ctx, &beeremote.UpdateRSTRequest{Rst: updated, Expected: read, Force: force})
	if err != nil {
		return nil, nil, err
	}
	return updated, resp, nil
}

// NewBackend returns an empty backend of the type called typeName, ready for ApplyTargetChanges.
func NewBackend(typeName string) (*flex.RemoteStorageTarget, error) {
	return NewTarget(typeName, 0)
}

// CreateBackend creates backend and returns it as Remote stored it. With verify, Remote first checks
// that backend reaches its store from Remote, and stores nothing when it does not.
func CreateBackend(ctx context.Context, backend *flex.RemoteStorageTarget, verify bool) (*beeremote.CreateRSTBackendResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	return beeRemote.CreateRSTBackend(ctx, &beeremote.CreateRSTBackendRequest{Backend: backend, Verify: verify})
}

// UpdateBackend reads the backend called name, applies changes, and stores the result. It returns
// the new definition. Remote refuses the update if someone else changed the backend after it was
// read. With force, Remote applies a change that moves where a target that names the backend keeps
// its objects, even while jobs use it. With verify, Remote first checks that the new definition
// reaches its store from Remote, and stores nothing when it does not.
func UpdateBackend(ctx context.Context, name string, changes TargetChanges, force bool, verify bool) (*flex.RemoteStorageTarget, *beeremote.UpdateRSTBackendResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, nil, err
	}
	targets, err := GetTargets(ctx)
	if err != nil {
		return nil, nil, err
	}
	read, ok := targets.Backends[name]
	if !ok {
		return nil, nil, fmt.Errorf("remote backend %q does not exist", name)
	}
	updated, err := ApplyTargetChanges(read, changes)
	if err != nil {
		return nil, nil, err
	}
	resp, err := beeRemote.UpdateRSTBackend(ctx, &beeremote.UpdateRSTBackendRequest{Name: name, Backend: updated, Expected: read, Force: force, Verify: verify})
	if err != nil {
		return nil, nil, err
	}
	return updated, resp, nil
}

// VerifyBackend asks Remote to check that the stored backend called name reaches its store, from
// Remote and from every sync node. timeout bounds each host. Zero means Remote's default. The check
// contacts the store but changes nothing.
//
// The request carries the timeout as whole milliseconds in a uint32. So any other timeout under
// 1ms, or one that does not fit, is refused here. A cast would turn a negative timeout into about 49
// days, and one under 1ms into zero, which Remote reads as its default. Remote checks the upper
// bound it allows itself.
func VerifyBackend(ctx context.Context, name string, timeout time.Duration) (*beeremote.VerifyRSTBackendResponse, error) {
	switch {
	case timeout == 0:
	case timeout < time.Millisecond:
		return nil, fmt.Errorf("the verify timeout must be 0 for Remote's default, or at least 1ms, not %s", timeout)
	case timeout.Milliseconds() > math.MaxUint32:
		return nil, fmt.Errorf("the verify timeout %s is too long", timeout)
	}
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	return beeRemote.VerifyRSTBackend(ctx, &beeremote.VerifyRSTBackendRequest{Name: name, TimeoutMs: uint32(timeout.Milliseconds())})
}

// checkTargetBackend returns an error when target names a backend that does not exist, or that has
// another type. Remote checks the same, and is the authority. The check here gives the same answer
// before anything is sent, using backends as read.
func checkTargetBackend(target *flex.RemoteStorageTarget, backends map[string]*flex.RemoteStorageTarget) error {
	name := target.GetBackend()
	if name == "" {
		return nil
	}
	backend, ok := backends[name]
	if !ok {
		return fmt.Errorf("remote backend %q does not exist", name)
	}
	return cRst.CheckBackendType(backend, target)
}

// DeleteBackend deletes the backend called name. Remote refuses the delete while any RST names the
// backend, so a delete never changes an RST.
func DeleteBackend(ctx context.Context, name string) error {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return err
	}
	_, err = beeRemote.DeleteRSTBackend(ctx, &beeremote.DeleteRSTBackendRequest{Name: name})
	return err
}

// DeleteTarget deletes the RST id. With dryRun it only reports how many unfinished jobs use it.
func DeleteTarget(ctx context.Context, id uint32, reason string, dryRun bool) (*beeremote.DeleteRSTResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	request := &beeremote.DeleteRSTRequest{Id: id, DryRun: dryRun}
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
	resp, err := beeRemote.GetRSTConfig(ctx, &beeremote.GetRSTConfigRequest{IncludeDeleted: true})
	if err != nil {
		return nil, err
	}
	return resp.GetDeleted(), nil
}
