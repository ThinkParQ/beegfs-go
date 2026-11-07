package rst

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// sampleListKeys holds a valid key for each keyed list, because a key is parsed by its list. A new
// list needs an entry here.
var sampleListKeys = map[string]string{
	"storage-class":  "GLACIER",
	"bulk-operation": "efficient-retrieve",
}

func TestTargetTypesAreRegistered(t *testing.T) {
	names := []string{}
	for _, tt := range TargetTypes() {
		names = append(names, tt.Name())
	}
	assert.Equal(t, []string{"s3", "xtreemstore"}, names)

	for _, tt := range TargetTypes() {
		config := tt.NewConfig()
		assert.NotEqual(t, flex.RemoteStorageTarget_Type_not_set_case, config.WhichType(), "%s: NewConfig must set the type", tt.Name())
		found, ok := TargetTypeOf(config)
		require.True(t, ok, tt.Name())
		assert.Equal(t, tt.Name(), found.Name())
		assert.Equal(t, tt.Name(), TypeName(config))
		// An extending type that forgot its own example would show the command of the type it
		// embeds.
		assert.Contains(t, tt.Example(), "beegfs remote target create "+tt.Name()+" ", "%s: the example must create this type", tt.Name())
	}
}

// Modify registers one flag per name for every type, so a name shared by two types must mean the same
// thing in both.
func TestTargetTypesShareFlagMeanings(t *testing.T) {
	settings := map[string]Setting{}
	lists := map[string]KeyedList{}
	for _, tt := range TargetTypes() {
		for _, s := range tt.Settings() {
			if seen, ok := settings[s.Key]; ok {
				assert.Equal(t, [5]any{seen.Help, seen.Identity, seen.Secret, seen.Summary, seen.Backend}, [5]any{s.Help, s.Identity, s.Secret, s.Summary, s.Backend}, "setting %q differs in %s", s.Key, tt.Name())
			}
			settings[s.Key] = s
		}
		for _, l := range tt.Lists() {
			if seen, ok := lists[l.Flag()]; ok {
				assert.Equal(t, [5]any{seen.Plural(), seen.Key(), seen.Fields(), seen.Help(), seen.BackendForbidden()}, [5]any{l.Plural(), l.Key(), l.Fields(), l.Help(), l.BackendForbidden()}, "list %q differs in %s", l.Flag(), tt.Name())
			}
			lists[l.Flag()] = l
		}
	}
}

// A setting every target needs, but no backend may give, must be set by each target itself. That is
// the point of forbidding it, as for the bucket. A new such setting is a choice to review, so this
// test names each one.
func TestBackendRolesOfRegisteredTypes(t *testing.T) {
	targetOnly := map[string]bool{}
	for _, tt := range TargetTypes() {
		required := []string{}
		for _, s := range tt.Settings() {
			if s.Backend == BackendForbidden && s.RequiredOnCreate {
				targetOnly[s.Key] = true
			}
			if s.Backend == BackendRequired {
				required = append(required, s.Key)
			}
		}
		assert.NotEmpty(t, required, "%s: a backend must require what it needs to reach its store", tt.Name())
	}
	assert.Equal(t, map[string]bool{"bucket": true}, targetOnly)
}

// Every flag a type's settings and lists produce must be distinct, and must not clash with a flag
// the CLI defines for every type.
func TestTargetTypeFlagsDoNotClash(t *testing.T) {
	for _, tt := range TargetTypes() {
		flags := map[string]string{}
		add := func(flag, from string) {
			assert.NotContains(t, ReservedTargetFlags, flag, "%s: %s uses a reserved flag", tt.Name(), from)
			if other, ok := flags[flag]; ok {
				t.Errorf("%s: --%s comes from both %s and %s", tt.Name(), flag, other, from)
			}
			flags[flag] = from
		}
		for _, s := range tt.Settings() {
			add(s.Key, "setting "+s.Key)
			if s.Secret {
				add(s.Key+"-file", "setting "+s.Key)
			}
		}
		for _, l := range tt.Lists() {
			add(l.Flag(), "list "+l.Flag())
			add("remove-"+l.Flag(), "list "+l.Flag())
			add("clear-"+l.Plural(), "list "+l.Flag())
		}
	}
}

// Each setting and list entry must round-trip, and must write into the type's own message. An
// extending type whose embedded S3 settings wrote into a top-level S3 part would switch the config
// to the S3 type, which the check of WhichType catches.
func TestTargetTypesRoundTrip(t *testing.T) {
	for _, tt := range TargetTypes() {
		config := tt.NewConfig()
		want := config.WhichType()
		for _, s := range tt.Settings() {
			value := "value-of-" + s.Key
			require.NoError(t, s.Set(config, value), "%s: %s", tt.Name(), s.Key)
			assert.Equal(t, value, s.Get(config), "%s: %s", tt.Name(), s.Key)
			assert.Equal(t, want, config.WhichType(), "%s: setting %s changed the type", tt.Name(), s.Key)
		}
		for _, l := range tt.Lists() {
			key, ok := sampleListKeys[l.Flag()]
			require.True(t, ok, "add a sample key for list %q to sampleListKeys", l.Flag())
			values := map[string]string{}
			for _, f := range l.Fields() {
				values[f.Key] = "1"
			}
			require.NoError(t, l.Upsert(config, key, values), "%s: %s", tt.Name(), l.Flag())
			assert.Equal(t, want, config.WhichType(), "%s: list %s changed the type", tt.Name(), l.Flag())
			entries := l.Entries(config)
			require.Len(t, entries, 1, "%s: %s", tt.Name(), l.Flag())
			assert.Len(t, entries[0].Values, len(l.Fields()), "%s: %s needs one value per field", tt.Name(), l.Flag())
			assert.NotContains(t, entries[0].Values, "", "%s: %s lost a field", tt.Name(), l.Flag())

			require.NoError(t, l.Upsert(config, key, nil), "upserting an existing entry with no values changes nothing")
			assert.Len(t, l.Entries(config), 1)
			require.NoError(t, l.Remove(config, entries[0].Key), "%s: %s removes by the key it shows", tt.Name(), l.Flag())
			assert.Error(t, l.Remove(config, key), "%s: removing a missing entry is an error", tt.Name())
			require.NoError(t, l.Upsert(config, key, nil))
			l.Clear(config)
			assert.Empty(t, l.Entries(config))
		}
	}
}

func TestIdentityChanged(t *testing.T) {
	for _, tt := range TargetTypes() {
		for _, s := range tt.Settings() {
			old := tt.NewConfig()
			new := proto.Clone(old).(*flex.RemoteStorageTarget)
			require.NoError(t, s.Set(new, "changed"))
			assert.Equal(t, s.Identity, IdentityChanged(old, new), "%s: changing %s", tt.Name(), s.Key)
		}
	}
	s3, _ := TargetTypeByName("s3")
	xtreemstore, _ := TargetTypeByName("xtreemstore")
	assert.True(t, IdentityChanged(s3.NewConfig(), xtreemstore.NewConfig()), "a change of type moves the objects")

	mock := &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}
	assert.False(t, IdentityChanged(mock, mock))
	assert.Equal(t, "mock", TypeName(mock), "an unregistered type is named after its oneof field")
	assert.Equal(t, "unknown", TypeName(&flex.RemoteStorageTarget{}))
}

func TestListFormat(t *testing.T) {
	xtreemstore, _ := TargetTypeByName("xtreemstore")
	formats := []string{}
	for _, l := range xtreemstore.Lists() {
		formats = append(formats, ListFormat(l))
	}
	assert.Equal(t, []string{
		"name=<name>[,retrieval-tier=<tier>][,retention-days=<days>][,check-time=<duration>][,recheck-time=<duration>][,auto-restore=<true|false>]",
		"operation=<efficient-retrieve>[,retry-delay=<duration>][,poll-delay=<duration>]",
	}, formats)
}

// The TOML decoder stores the wrapper in a config's Type field and decodes into the field address.
// Writing through that address must reach the config, or the decoded section would be lost.
func TestTOMLDecodeTarget(t *testing.T) {
	for _, tt := range TargetTypes() {
		wrapper, field, ok := TOMLDecodeTarget(tt.Name())
		require.True(t, ok)
		config := &flex.RemoteStorageTarget{}
		reflect.ValueOf(config).Elem().FieldByName("Type").Set(reflect.ValueOf(wrapper))
		assert.Equal(t, tt.NewConfig().WhichType(), config.WhichType(), tt.Name())

		target := reflect.ValueOf(field).Elem()
		decoded := reflect.New(target.Type().Elem())
		target.Set(decoded)
		m := config.ProtoReflect()
		got := m.Get(m.WhichOneof(m.Descriptor().Oneofs().ByName("type"))).Message().Interface()
		assert.Same(t, decoded.Interface(), got, "%s: the decoded message must land in the config", tt.Name())
	}
	_, _, ok := TOMLDecodeTarget("azure")
	assert.False(t, ok, "azure has no registered type")
}
