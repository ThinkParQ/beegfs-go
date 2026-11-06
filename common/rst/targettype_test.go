package rst

import (
	"reflect"
	"slices"
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
				assert.Equal(t, [5]any{seen.Help, seen.Identity, seen.Secret, seen.Summary, seen.Kind}, [5]any{s.Help, s.Identity, s.Secret, s.Summary, s.Kind}, "setting %q differs in %s", s.Key, tt.Name())
			}
			settings[s.Key] = s
		}
		for _, l := range tt.Lists() {
			if seen, ok := lists[l.Flag()]; ok {
				assert.Equal(t, [4]any{seen.Plural(), seen.Key(), seen.Fields(), seen.Help()}, [4]any{l.Plural(), l.Key(), l.Fields(), l.Help()}, "list %q differs in %s", l.Flag(), tt.Name())
			}
			lists[l.Flag()] = l
		}
	}
}

// A backend must be able to reach its store, so every type has a required connection setting. The
// bucket is the only setting each target sets itself and no backend may. A new Bucket setting is a
// choice to review, so this test names each one.
func TestKindsOfRegisteredTypes(t *testing.T) {
	bucketOwned := map[string]bool{}
	for _, tt := range TargetTypes() {
		required := []string{}
		for _, s := range tt.Settings() {
			if s.Kind == BucketSetting {
				bucketOwned[s.Key] = true
			}
			if s.Kind == RequiredConnectionSetting {
				required = append(required, s.Key)
			}
		}
		assert.NotEmpty(t, required, "%s: a backend must require what it needs to reach its store", tt.Name())
	}
	assert.Equal(t, map[string]bool{"bucket": true}, bucketOwned)
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
		formats = append(formats, KeyedListFormat(l))
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

// settingsOnlyType is a TargetType with only settings and a bucket manager, for the rules that read
// nothing else. Any other method panics, because the embedded interface is nil.
type settingsOnlyType struct {
	TargetType
	settings []Setting
	buckets  BucketManager
}

func (t settingsOnlyType) Name() string           { return "test" }
func (t settingsOnlyType) Settings() []Setting    { return t.settings }
func (t settingsOnlyType) Buckets() BucketManager { return t.buckets }

func TestCheckKinds(t *testing.T) {
	bucket := Setting{Key: "bucket", Kind: BucketSetting}
	other := Setting{Key: "other", Kind: BucketSetting}
	manager := s3Buckets{}
	for _, c := range []struct {
		name    string
		t       settingsOnlyType
		wantErr string
	}{
		{"bucket type", settingsOnlyType{settings: []Setting{bucket}, buckets: manager}, ""},
		{"plain type", settingsOnlyType{settings: []Setting{{Key: "x", Kind: RequiredConnectionSetting}, {Key: "y", Kind: OptionalConnectionSetting}, {Key: "z", Kind: SharedSetting}}}, ""},
		{"two bucket settings", settingsOnlyType{settings: []Setting{bucket, other}, buckets: manager}, "more than one"},
		{"a setting without a kind", settingsOnlyType{settings: []Setting{{Key: "x"}}}, `setting "x" declares no kind`},
		{"a kind outside the enum", settingsOnlyType{settings: []Setting{{Key: "x", Kind: SharedSetting + 1}}}, `setting "x" has unknown kind`},
		{"no manager", settingsOnlyType{settings: []Setting{bucket}}, "cannot manage buckets"},
		{"manager without setting", settingsOnlyType{buckets: manager}, "has no Bucket setting"},
	} {
		err := checkKinds(c.t)
		if c.wantErr == "" {
			assert.NoError(t, err, c.name)
		} else {
			assert.ErrorContains(t, err, c.wantErr, c.name)
		}
	}
	for _, tt := range TargetTypes() {
		assert.NoError(t, checkKinds(tt), tt.Name())
	}
}

// TestCheckDefinitionSettings covers each rule of CheckDefinitionSettings on the S3 type, whose
// connection is endpoint-url, region, access-key and secret-key (required) and partition-id
// (optional).
func TestCheckDefinitionSettings(t *testing.T) {
	s3, ok := TargetTypeByName("s3")
	require.True(t, ok)
	keys := func(settings []Setting) []string {
		out := []string{}
		for _, s := range settings {
			out = append(out, s.Key)
		}
		return out
	}
	given := func(keys ...string) func(Setting) bool {
		return func(s Setting) bool { return slices.Contains(keys, s.Key) }
	}
	connection := []string{"endpoint-url", "region", "access-key", "secret-key"}
	// withBucket lists the required settings of a standalone target, in declaration order.
	withBucket := []string{"endpoint-url", "region", "bucket", "access-key", "secret-key"}
	everything := append(slices.Clone(connection), "partition-id", "bucket")

	// Target rows go through TargetRole, so the test also covers how a target's backend and
	// adopt pick its row.
	for _, c := range []struct {
		name                         string
		role                         DefinitionRole
		given                        []string
		wantMissing, wantConflicting []string
	}{
		{"a backend needs its connection", BackendDefinition, nil, connection, []string{}},
		{"a complete backend", BackendDefinition, connection, []string{}, []string{}},
		{"a backend never holds a bucket", BackendDefinition, everything, []string{}, []string{"bucket"}},
		{"a standalone target needs its connection and bucket", TargetRole(false, false), nil, withBucket, []string{}},
		{"adopt without a backend exempts nothing", TargetRole(false, true), nil, withBucket, []string{}},
		{"a complete standalone target", TargetRole(false, false), everything, []string{}, []string{}},
		{"a target with a backend needs only its bucket", TargetRole(true, false), nil, []string{"bucket"}, []string{}},
		{"a target with a backend and adopt may omit the bucket", TargetRole(true, true), nil, []string{}, []string{}},
		{"a target with a backend cannot set its connection", TargetRole(true, false), everything, []string{}, []string{"endpoint-url", "partition-id", "region", "access-key", "secret-key"}},
		{"not even with adopt", TargetRole(true, true), []string{"region"}, []string{}, []string{"region"}},
	} {
		missing, conflicting := CheckDefinitionSettings(s3, c.role, given(c.given...))
		assert.Equal(t, c.wantMissing, keys(missing), "%s: missing", c.name)
		assert.Equal(t, c.wantConflicting, keys(conflicting), "%s: conflicting", c.name)
	}
	assert.Panics(t, func() { CheckDefinitionSettings(s3, definitionRoleUnset, given()) }, "a caller must pick a definition")
	assert.Panics(t, func() { CheckDefinitionSettings(s3, TargetWithBackendAdopt+1, given()) }, "a definition outside the enum")

	assert.NoError(t, DefinitionSettingsError(s3, TargetWithoutBackend, nil, nil))
	assert.EqualError(t, DefinitionSettingsError(s3, TargetWithBackend, []Setting{{Key: "bucket"}}, []Setting{{Key: "region"}, {Key: "access-key"}}),
		"it must set bucket; it cannot set region, access-key, because its backend provides them")
	assert.EqualError(t, DefinitionSettingsError(s3, BackendDefinition, nil, []Setting{{Key: "bucket"}}),
		"a s3 backend cannot set bucket, because each target sets its own")
}

func TestBucketChanged(t *testing.T) {
	s3, ok := TargetTypeByName("s3")
	require.True(t, ok)
	setting, ok := BucketSettingOf(s3)
	require.True(t, ok)
	assert.Equal(t, "bucket", setting.Key)
	a := &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "a", Region: "r1"}}}
	b := &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "a", Region: "r2"}}}
	assert.False(t, BucketChanged(s3, a, b), "another setting changed, not the bucket")
	b.GetS3().SetBucket("b")
	assert.True(t, BucketChanged(s3, a, b))
	assert.False(t, BucketChanged(settingsOnlyType{}, a, b), "a type without a bucket never changes one")
}

func TestGenerateTargetName(t *testing.T) {
	a, b := GenerateTargetName(42), GenerateTargetName(42)
	assert.Regexp(t, `^rst-42-[0-9A-HJKMNP-TV-Z]{26}$`, a)
	assert.NotEqual(t, a, b, "two targets that reuse an ID get different names")
	assert.NoError(t, CheckName("remote storage target", GenerateTargetName(^uint32(0))))
	assert.LessOrEqual(t, len(GenerateTargetName(^uint32(0))), 41)
}
