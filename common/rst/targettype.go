package rst

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file defines how an RST type describes its own configuration. Each RST client type, such as
// S3 or xtreemstore, implements TargetType in its own file and registers it. The code that
// configures RSTs only goes through the registry:
//   - New builds a provider through the type's NewProvider.
//   - The beegfs-remote TOML decoder finds a type's section by its name.
//   - Remote asks the type whether a modify moves the RST's objects (IdentityChanged).
//   - The beegfs CLI builds the flags of "remote target create" and "modify", the list columns and
//     the detail view from the type's settings and lists. It builds the flags of "remote backend"
//     the same way, without the bucket setting.
//   - Remote resolves a target's backend (see backend.go) through the type's settings and lists.
//   - The CLI and Remote check every target and backend against the kinds of the type's settings
//     (CheckDefinitionSettings). Remote verifies that a backend reaches its store through
//     VerifyBackend.
//
// So adding an RST type means adding a provider and its TargetType, and nothing else.
//
// Two terms describe the configuration a type exposes:
//   - A setting is one scalar value, such as the bucket. It is read and written as text, and the
//     type parses and validates the text.
//   - A keyed list is a repeated message whose entries are identified by one field, called the key,
//     such as storage classes identified by name. The key lets a modify add, change or remove one
//     entry without restating the whole list.
//
// A backend is the named connection to one remote store, which targets of the same type inherit
// from (see backend.go). Each setting has a kind, which says what the setting is. The kind decides
// who owns the setting, meaning who sets it, and whether it is required. Every setting declares its
// kind. registerTargetType refuses one that does not, so a forgotten kind fails at startup rather
// than leaving the setting unchecked. CheckDefinitionSettings holds the owner rules:
//   - RequiredConnectionSetting: how a target reaches its store, such as the S3 endpoint, region
//     and keys. Every backend sets it, and so does every target without a backend. A target that
//     names a backend must not, because the backend already provides it, and two values for one
//     setting would conflict.
//   - OptionalConnectionSetting: like RequiredConnectionSetting, but a store may not need it, such
//     as an S3 partition. A backend or a target without a backend may set it. A target that names a
//     backend must not.
//   - BucketSetting: names the target's bucket in its store (see bucket.go). Every target sets its
//     own, and no backend may. It cannot change once the target exists, because create made or
//     checked that bucket. "create --backend --adopt" may leave it out, to adopt every unused
//     bucket of the backend's store as a target of its own.
//   - SharedSetting: any other setting, such as a policy default. A backend may set it as a default
//     for its targets, and a target's own value wins. It is never required.
//
// The rules differ by the role of the definition they check. There are four roles (see
// DefinitionRole):
//
//	| DefinitionRole         | Must set                         | Must not set           |
//	|------------------------|----------------------------------|------------------------|
//	| BackendDefinition      | every RequiredConnectionSetting  | the BucketSetting      |
//	| TargetWithBackend      | the BucketSetting                | any connection setting |
//	| TargetWithBackendAdopt | nothing                          | any connection setting |
//	| TargetWithoutBackend   | every RequiredConnectionSetting, | nothing                |
//	|                        | the BucketSetting                |                        |
//
// Keyed lists, such as storage classes, are always shared. A backend may hold entries, and a target
// adds or replaces entries by key (see backend.go). An empty list is always valid, so no list is
// required.
//
// Flag names are shared by every type. A modify registers one --bucket flag for all types that have
// a bucket, because it does not know the target's type until it reads the target. So two types that
// use the same setting key or list flag must mean the same thing by it. A type that extends
// another, as xtreemstore extends S3, gets this by reusing the other type's settings and lists.

// TargetType is the interface every RST client type implements. It describes the type's
// configuration to the CLI and Remote, and builds the type's provider.
type TargetType interface {
	// Name is how admins name the type: the TOML section, the CLI subcommand and the type shown in
	// lists, such as "s3".
	Name() string
	// NewConfig returns an empty config of this type, with the type oneof set. The oneof case it sets
	// is how the registry tells which type a config has.
	NewConfig() *flex.RemoteStorageTarget
	// NewProvider builds the provider for config, which has this type. It must not contact the RST,
	// because Remote calls it to validate a config before storing it.
	NewProvider(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) (Provider, error)
	// Settings returns the type's settings, in display order.
	Settings() []Setting
	// Lists returns the type's keyed lists, in display order.
	Lists() []KeyedList
	// Example returns a "beegfs remote target create <type>" command that creates a complete target
	// of this type, for the command's help. A type that extends another must return its own, or it
	// would show the other type's command.
	Example() string
	// IdentityChanged reports whether new keeps its objects somewhere else than old. Remote refuses
	// such a change while unfinished jobs use the RST, because they may refer to objects the change
	// moves. Both configs have this type.
	IdentityChanged(old, new *flex.RemoteStorageTarget) bool
	// VerifyBackend contacts the store of backend, a valid backend of this type, and returns an
	// error when the store cannot be reached or rejects the backend's credentials. It makes no
	// change in the store. note is a detail about a check that passed, or "". It is the only method
	// that contacts the store, so only an explicit verify calls it.
	VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget) (note string, err error)
	// Buckets returns how this type manages the bucket of a target in its store (see bucket.go),
	// or nil when it cannot. A type that returns one has exactly one setting of kind BucketSetting.
	Buckets() BucketManager
}

// SettingKind says what a setting is. The kind decides who owns the setting, meaning who sets it,
// and whether it is required. See the top of this file. Each value is one valid combination of
// owner and requirement, so no setting can declare a requirement its owner does not allow.
type SettingKind int

const (
	// SettingKindUnset is the zero value, so a setting that declares no kind has it. registerTargetType
	// refuses it.
	SettingKindUnset SettingKind = iota
	// RequiredConnectionSetting is how a target reaches its store, when no store can be reached without
	// it. Every backend sets it, and so does every target without a backend. A target that names a
	// backend must not.
	RequiredConnectionSetting
	// OptionalConnectionSetting is how a target reaches its store, when a store may not need it. A backend
	// or a target without a backend may set it. A target that names a backend must not.
	OptionalConnectionSetting
	// BucketSetting names the target's bucket in its store. Every target sets it, and no backend may. It
	// is always required, and a type has one exactly when it has a BucketManager.
	BucketSetting
	// SharedSetting is any other setting. A backend may set it as a default, and a target's own value
	// wins. It is never required.
	SharedSetting
)

// isConnection reports whether k is how a target reaches its store, required or not.
func (k SettingKind) isConnection() bool {
	return k == RequiredConnectionSetting || k == OptionalConnectionSetting
}

// DefinitionRole says which definition CheckDefinitionSettings checks: a backend's, or a target's
// in one of three cases. Each value is one row of the table at the top of this file. TargetRole
// picks the row for a target.
type DefinitionRole int

const (
	// definitionRoleUnset is the zero value. CheckDefinitionSettings panics on it, because only a caller that
	// forgot to pick a row passes it, and no request can cause that.
	definitionRoleUnset DefinitionRole = iota
	// BackendDefinition is a backend's definition.
	BackendDefinition
	// TargetWithoutBackend is the definition of a target that names no backend, with or without
	// adopt.
	TargetWithoutBackend
	// TargetWithBackend is the definition of a target that names a backend, in a request that does
	// not adopt existing buckets.
	TargetWithBackend
	// TargetWithBackendAdopt is the definition of a target that names a backend, in a create that
	// adopts existing buckets. An omitted bucket then adopts every unused bucket of the store.
	TargetWithBackendAdopt
)

// namesBackend reports whether d is the definition of a target that names a backend.
func (d DefinitionRole) namesBackend() bool {
	return d == TargetWithBackend || d == TargetWithBackendAdopt
}

// TargetRole returns the DefinitionRole of a target's own definition. hasBackend is whether the
// target names a backend. adopt is whether the request adopts existing buckets. Adopt without a
// backend exempts nothing, because adopting every unused bucket needs a backend's store to list
// them. So such a target is a TargetWithoutBackend.
func TargetRole(hasBackend, adopt bool) DefinitionRole {
	switch {
	case !hasBackend:
		return TargetWithoutBackend
	case adopt:
		return TargetWithBackendAdopt
	default:
		return TargetWithBackend
	}
}

// Setting is one scalar value of a type, such as the bucket. It is plain data and an accessor pair,
// rather than an interface, because a type has many settings and each one is a few lines.
type Setting struct {
	// Key names the setting as a flag, a list column and a label in the detail view, such as
	// "endpoint-url".
	Key  string
	Help string
	// Identity is set when a change to the setting moves where the RST keeps its objects.
	Identity bool
	// Secret is set for a value output must mask, such as a secret key. The CLI also offers a
	// --<key>-file flag for it, so the value stays out of the process list and shell history.
	Secret bool
	// Kind says what the setting is, and so who sets it and whether it is required. Every setting
	// must declare one.
	Kind SettingKind
	// Summary is set for a setting the default list view shows.
	Summary bool
	// Default is the value a target uses when neither the target nor its backend sets the setting,
	// or "" when the setting has none. Only a setting that is never required may have one.
	Default string
	// Get returns the setting's value in config, or "" when it is not set. It never returns
	// Default. Inheritance and the owner rules must tell a value a definition sets from one it
	// leaves unset, and they read that from Get.
	Get func(config *flex.RemoteStorageTarget) string
	// Set parses value and stores it in config.
	Set func(config *flex.RemoteStorageTarget, value string) error
}

// Effective returns the value of s that config uses: the value config sets, or s.Default when it
// sets none. config should be an effective configuration (see Resolve). For a target with a
// backend, the definition alone leaves out what the backend provides.
func (s Setting) Effective(config *flex.RemoteStorageTarget) string {
	if v := s.Get(config); v != "" {
		return v
	}
	return s.Default
}

// KeyedList is a repeated message of a type whose entries are identified by one field, the key.
type KeyedList interface {
	// Flag names one entry, such as "storage-class". The CLI derives --remove-<Flag> from it.
	Flag() string
	// Plural names the whole list, such as "storage-classes". The CLI uses it for the list column
	// and derives --clear-<Plural> from it.
	Plural() string
	Help() string
	// Key is the field that identifies an entry, such as "name".
	Key() KeyedListField
	// Fields are the other fields of an entry, in display order.
	Fields() []KeyedListField
	// Summary is set for a list whose keys the default list view shows.
	Summary() bool
	// Entries returns the entries of config, in their stored order.
	Entries(config *flex.RemoteStorageTarget) []KeyedListEntry
	// Upsert adds the entry identified by key when it is new. Otherwise it changes only the fields
	// in values, and every other field of the entry keeps its value. values holds field keys from
	// Fields, never the key itself.
	Upsert(config *flex.RemoteStorageTarget, key string, values map[string]string) error
	// Remove removes the entry identified by key. It returns an error when there is none.
	Remove(config *flex.RemoteStorageTarget, key string) error
	// Clear removes every entry.
	Clear(config *flex.RemoteStorageTarget)
	// CopyEntry copies the entry of src identified by key into dst, replacing the entry of dst with
	// that key or adding one. It copies the entry's message as it is, so a field that is not set
	// stays not set. Upsert cannot do this, because it takes text, and text cannot tell an unset
	// field from one set to its zero value. Overlay calls it to inherit an entry from a backend. It
	// returns an error when src has no such entry.
	CopyEntry(dst, src *flex.RemoteStorageTarget, key string) error
	// ValidateEntries returns an error for the first entry of config that is not complete on its
	// own, by the rules the provider applies to it. Entries are inherited whole (see backend.go),
	// so an entry a backend holds must already be one a provider accepts. A backend never builds a
	// provider, so Remote calls this for every backend instead (rst.ValidateBackend).
	ValidateEntries(config *flex.RemoteStorageTarget) error
}

// KeyedListField is one field of a keyed list entry.
type KeyedListField struct {
	// Key names the field in a list entry, as in "retrieval-tier=Bulk".
	Key string
	// Placeholder describes the value in help text, such as "<duration>".
	Placeholder string
}

// KeyedListEntry is one entry of a keyed list, as text.
type KeyedListEntry struct {
	Key string
	// Values holds one value per field of the list, in the order of KeyedList.Fields. A field that
	// is not set is "".
	Values []string
}

// ReservedTargetFlags are the flags "remote target create" and "modify", and "remote backend
// create" and "modify", define for every type. No setting or list may use one of them.
var ReservedTargetFlags = []string{"id", "name", "backend", "policies", "force", "message", "help", "verify", "timeout"}

var targetTypes = map[string]TargetType{}

// registerTargetType adds t to the registry. Each type's file calls it from an init function. It
// panics on a second type with the same name or case, because that is a programming error.
func registerTargetType(t TargetType) {
	for _, existing := range targetTypes {
		if existing.Name() == t.Name() || existing.NewConfig().WhichType() == t.NewConfig().WhichType() {
			panic(fmt.Sprintf("RST type %q is registered twice", t.Name()))
		}
	}
	if err := checkKinds(t); err != nil {
		panic(err.Error())
	}
	targetTypes[t.Name()] = t
}

// checkKinds returns an error when t declares the kinds of its settings in a way the owner rules
// cannot work with:
//   - A setting with no kind (SettingKindUnset), or with a value that is not a SettingKind.
//   - A required setting with a Default. A definition must set a required setting, so its default
//     would never be used, and the declaration would mislead.
//   - More than one BucketSetting. Adopt-all and the bucket step work on one bucket per target.
//   - A BucketSetting without a BucketManager, or a BucketManager without a BucketSetting.
func checkKinds(t TargetType) error {
	var buckets []string
	for _, s := range t.Settings() {
		switch s.Kind {
		case RequiredConnectionSetting, OptionalConnectionSetting, SharedSetting:
		case BucketSetting:
			buckets = append(buckets, s.Key)
		case SettingKindUnset:
			return fmt.Errorf("RST type %q: setting %q declares no kind", t.Name(), s.Key)
		default:
			return fmt.Errorf("RST type %q: setting %q has unknown kind %d", t.Name(), s.Key, s.Kind)
		}
		if s.Default != "" && (s.Kind == RequiredConnectionSetting || s.Kind == BucketSetting) {
			return fmt.Errorf("RST type %q: setting %q is required, so it cannot have a default", t.Name(), s.Key)
		}
	}
	if len(buckets) > 1 {
		return fmt.Errorf("RST type %q has more than one Bucket setting: %s", t.Name(), strings.Join(buckets, ", "))
	}
	if len(buckets) == 1 && t.Buckets() == nil {
		return fmt.Errorf("RST type %q: setting %q names the bucket, but the type cannot manage buckets", t.Name(), buckets[0])
	}
	if len(buckets) == 0 && t.Buckets() != nil {
		return fmt.Errorf("RST type %q can manage buckets, but has no Bucket setting to name the bucket", t.Name())
	}
	return nil
}

// BucketSettingOf returns the setting of t that names a target's bucket: the one of kind
// BucketSetting. It returns false for a type that cannot manage buckets.
func BucketSettingOf(t TargetType) (Setting, bool) {
	for _, s := range t.Settings() {
		if s.Kind == BucketSetting {
			return s, true
		}
	}
	return Setting{}, false
}

// CheckDefinitionSettings applies the owner rules to one definition of type t, a backend's or a
// target's own. The CLI calls it on its flags, and Remote on a definition it is about to store, so
// both apply the same rules. It returns the settings the definition must set but does not
// (missing), and those it sets but must not (conflicting). Its inputs:
//   - role: which role the definition has, which picks the row of the table at the top of this
//     file. A caller with a target uses TargetRole. CheckDefinitionSettings panics on a value that
//     is not one of the four DefinitionRole values, because that is a programming error.
//   - set: reports whether the definition sets a setting.
//
// A SharedSetting is never missing and never conflicting. The TargetWithBackendAdopt row only
// checks a request. Remote checks each target that request makes again, as a TargetWithBackend
// with its bucket filled in, before it stores any of them.
func CheckDefinitionSettings(t TargetType, role DefinitionRole, set func(Setting) bool) (missing, conflicting []Setting) {
	if role < BackendDefinition || role > TargetWithBackendAdopt {
		panic(fmt.Sprintf("CheckDefinitionSettings: unknown role %d", role))
	}
	missing, conflicting = []Setting{}, []Setting{}
	for _, s := range t.Settings() {
		given := set(s)
		switch {
		case s.Kind.isConnection():
			if role.namesBackend() {
				if given {
					conflicting = append(conflicting, s)
				}
			} else if s.Kind == RequiredConnectionSetting && !given {
				missing = append(missing, s)
			}
		case s.Kind == BucketSetting:
			if role == BackendDefinition {
				if given {
					conflicting = append(conflicting, s)
				}
			} else if role != TargetWithBackendAdopt && !given {
				missing = append(missing, s)
			}
		}
	}
	return missing, conflicting
}

// DefinitionSettingsError returns the error for the result of CheckDefinitionSettings on a
// definition whose role is describes, or nil when both lists are empty. The message names every
// offending setting at once.
func DefinitionSettingsError(t TargetType, role DefinitionRole, missing, conflicting []Setting) error {
	subject, because := "it", "because its backend provides them"
	if role == BackendDefinition {
		subject, because = fmt.Sprintf("a %s backend", t.Name()), "because each target sets its own"
	}
	problems := []string{}
	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("%s must set %s", subject, settingKeys(missing)))
	}
	if len(conflicting) > 0 {
		problems = append(problems, fmt.Sprintf("%s cannot set %s, %s", subject, settingKeys(conflicting), because))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(problems, "; "))
}

// CheckTargetDefinition applies CheckDefinitionSettings to config, a target's own definition, and
// returns the error DefinitionSettingsError formats. adopt is whether the request adopts existing
// buckets. A type no client registered, such as mock, declares no kinds, so nothing is checked for
// it.
func CheckTargetDefinition(config *flex.RemoteStorageTarget, adopt bool) error {
	t, ok := TargetTypeOf(config)
	if !ok {
		return nil
	}
	role := TargetRole(config.GetBackend() != "", adopt)
	missing, conflicting := CheckDefinitionSettings(t, role, func(s Setting) bool { return s.Get(config) != "" })
	return DefinitionSettingsError(t, role, missing, conflicting)
}

// BucketChanged reports whether after names another bucket than before. Both are definitions of
// type t. A type without a Bucket setting never changes its bucket.
func BucketChanged(t TargetType, before, after *flex.RemoteStorageTarget) bool {
	s, ok := BucketSettingOf(t)
	return ok && s.Get(before) != s.Get(after)
}

// settingKeys joins the keys of settings for a message, such as "endpoint-url, region".
func settingKeys(settings []Setting) string {
	keys := make([]string, 0, len(settings))
	for _, s := range settings {
		keys = append(keys, s.Key)
	}
	return strings.Join(keys, ", ")
}

// TargetTypes returns every registered type, sorted by name.
func TargetTypes() []TargetType {
	types := make([]TargetType, 0, len(targetTypes))
	for _, t := range targetTypes {
		types = append(types, t)
	}
	slices.SortFunc(types, func(a, b TargetType) int { return strings.Compare(a.Name(), b.Name()) })
	return types
}

// TargetTypeByName returns the registered type called name.
func TargetTypeByName(name string) (TargetType, bool) {
	t, ok := targetTypes[name]
	return t, ok
}

// TargetTypeOf returns the registered type of config. It returns false for a config whose type is
// not set, or is a type no client registered, such as mock.
func TargetTypeOf(config *flex.RemoteStorageTarget) (TargetType, bool) {
	for _, t := range targetTypes {
		if t.NewConfig().WhichType() == config.WhichType() {
			return t, true
		}
	}
	return nil, false
}

// TypeName returns the name of config's type. For a type no client registered, such as mock, it
// returns the name of the oneof field the config sets. It returns "unknown" when no type is set.
func TypeName(config *flex.RemoteStorageTarget) string {
	if t, ok := TargetTypeOf(config); ok {
		return t.Name()
	}
	m := config.ProtoReflect()
	field := m.WhichOneof(m.Descriptor().Oneofs().ByName("type"))
	if field == nil {
		return "unknown"
	}
	return string(field.Name())
}

// IdentityChanged reports whether new keeps its objects somewhere else than old. A change of type
// always does. For a type no client registered, it reports false, because nothing describes what
// such a type's identity is.
func IdentityChanged(old, new *flex.RemoteStorageTarget) bool {
	if old.WhichType() != new.WhichType() {
		return true
	}
	t, ok := TargetTypeOf(old)
	if !ok {
		return false
	}
	return t.IdentityChanged(old, new)
}

// identityFromSettings reports whether any Identity setting differs between old and new. Types use
// it as their IdentityChanged unless their identity holds more than settings. It compares
// effective values, so an unset setting and one set to its Default name the same place.
func identityFromSettings(settings []Setting, old, new *flex.RemoteStorageTarget) bool {
	for _, s := range settings {
		if s.Identity && s.Effective(old) != s.Effective(new) {
			return true
		}
	}
	return false
}

// KeyedListFormat returns the format of one entry of list for help text, such as
// "name=<name>[,retrieval-tier=<tier>]". It is built from the list's fields, so help text always
// matches what Upsert accepts.
func KeyedListFormat(list KeyedList) string {
	var b strings.Builder
	key := list.Key()
	fmt.Fprintf(&b, "%s=%s", key.Key, key.Placeholder)
	for _, f := range list.Fields() {
		fmt.Fprintf(&b, "[,%s=%s]", f.Key, f.Placeholder)
	}
	return b.String()
}

// tomlDecodeTarget returns what the beegfs-remote TOML decoder needs to decode a section of type t:
// the oneof wrapper to store in the config's Type field, and the address of the wrapper's message
// field to decode into.
//
// It reads both from the config t.NewConfig returns. The generated Go code stores a oneof as a
// wrapper struct with a single field that holds the type's message, such as
// RemoteStorageTarget_S3_{S3: ...}. The decoder needs that field's address, because decoding into a
// copy would leave the wrapper empty.
func tomlDecodeTarget(t TargetType) (wrapper any, field any) {
	wrapper = t.NewConfig().Type
	return wrapper, reflect.ValueOf(wrapper).Elem().Field(0).Addr().Interface()
}

// TOMLDecodeTarget returns the decode target for the TOML section called name, or false when no
// registered type has that name. See tomlDecodeTarget.
func TOMLDecodeTarget(name string) (wrapper any, field any, ok bool) {
	t, ok := TargetTypeByName(name)
	if !ok {
		return nil, nil, false
	}
	wrapper, field = tomlDecodeTarget(t)
	return wrapper, field, true
}

// checkListFields refuses a field list does not have. Upsert implementations call it first.
func checkListFields(list KeyedList, values map[string]string) error {
	for key := range values {
		if !slices.ContainsFunc(list.Fields(), func(f KeyedListField) bool { return f.Key == key }) {
			return fmt.Errorf("--%s has no field %q", list.Flag(), key)
		}
	}
	return nil
}

// RegisterTargetTypeForTesting adds t to the registry until the returned function is called. Only
// tests call it, to show that a new type needs no code outside its own declaration.
func RegisterTargetTypeForTesting(t TargetType) (unregister func()) {
	registerTargetType(t)
	return func() { delete(targetTypes, t.Name()) }
}
