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
//     the detail view from the type's settings and lists.
//
//   - Remote resolves a target's parents (see inherit.go) through the type's settings and lists.
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
// Flag names are shared by every type. A modify registers one --bucket flag for all types that have
// a bucket, because it does not know the target's type until it reads the target. So two types that
// use the same setting key or list flag must mean the same thing by it. A type that extends another,
// as xtreemstore extends S3, gets this by reusing the other type's settings and lists.

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
	// RequiredOnCreate is set for a setting a new RST must have.
	RequiredOnCreate bool
	// Summary is set for a setting the default list view shows.
	Summary bool
	// Get returns the setting's value in config, or "" when it is not set.
	Get func(config *flex.RemoteStorageTarget) string
	// Set parses value and stores it in config.
	Set func(config *flex.RemoteStorageTarget, value string) error
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
	Key() ListField
	// Fields are the other fields of an entry, in display order.
	Fields() []ListField
	// Summary is set for a list whose keys the default list view shows.
	Summary() bool
	// Entries returns the entries of config, in their stored order.
	Entries(config *flex.RemoteStorageTarget) []ListEntry
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
	// field from one set to its zero value. Overlay calls it to inherit an entry from a parent. It
	// returns an error when src has no such entry.
	CopyEntry(dst, src *flex.RemoteStorageTarget, key string) error
	// ValidateEntries returns an error for the first entry of config that is not complete on its own,
	// by the rules the provider applies to it. Entries are inherited whole (see inherit.go), so an
	// entry a parent holds must already be one a provider accepts. A parent never builds a provider,
	// so Remote calls this for every parent instead (rst.ValidateParent).
	ValidateEntries(config *flex.RemoteStorageTarget) error
}

// ListField is one field of a keyed list entry.
type ListField struct {
	// Key names the field in a list entry, as in "retrieval-tier=Bulk".
	Key string
	// Placeholder describes the value in help text, such as "<duration>".
	Placeholder string
}

// ListEntry is one entry of a keyed list, as text.
type ListEntry struct {
	Key string
	// Values holds one value per field of the list, in the order of KeyedList.Fields. A field that
	// is not set is "".
	Values []string
}

// ReservedTargetFlags are the flags "remote target create" and "modify" define for every type. No
// setting or list may use one of them.
var ReservedTargetFlags = []string{"id", "name", "parent", "policies", "force", "message", "help"}

var targetTypes = map[string]TargetType{}

// registerTargetType adds t to the registry. Each type's file calls it from an init function. It
// panics on a second type with the same name or case, because that is a programming error.
func registerTargetType(t TargetType) {
	for _, existing := range targetTypes {
		if existing.Name() == t.Name() || existing.NewConfig().WhichType() == t.NewConfig().WhichType() {
			panic(fmt.Sprintf("RST type %q is registered twice", t.Name()))
		}
	}
	targetTypes[t.Name()] = t
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
// it as their IdentityChanged unless their identity holds more than settings.
func identityFromSettings(settings []Setting, old, new *flex.RemoteStorageTarget) bool {
	for _, s := range settings {
		if s.Identity && s.Get(old) != s.Get(new) {
			return true
		}
	}
	return false
}

// ListFormat returns the format of one entry of list for help text, such as
// "name=<name>[,retrieval-tier=<tier>]". It is built from the list's fields, so help text always
// matches what Upsert accepts.
func ListFormat(list KeyedList) string {
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
		if !slices.ContainsFunc(list.Fields(), func(f ListField) bool { return f.Key == key }) {
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
