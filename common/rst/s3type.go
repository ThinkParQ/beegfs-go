package rst

import (
	"context"
	"fmt"
	"strconv"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

// s3DefaultRegion is the region of an S3 target that neither it nor its backend sets. AWS uses
// us-east-1 for a bucket created without a location, because us-east-1 existed before buckets had
// one. Most S3-compatible stores also assume us-east-1 when no region is configured.
const s3DefaultRegion = "us-east-1"

// This file implements TargetType for S3 targets (see targettype.go). Every type that embeds an S3
// part, such as xtreemstore, extends s3TargetType rather than declaring the S3 settings again, so
// the S3 flags mean the same thing for every such type.

// s3PartFunc returns the S3 part of config. With create, it adds an empty S3 part to the config's
// type message when there is none, so a setting can be written. Without create, it may return nil,
// and the generated getters treat nil as empty.
type s3PartFunc func(config *flex.RemoteStorageTarget, create bool) *flex.RemoteStorageTarget_S3

// s3TargetType describes S3 targets. A type that embeds an S3 part extends it by embedding
// s3TargetType with its own part function. Go has no virtual methods, so the embedded methods reach
// the extending type's S3 part through this field, not through a method the extending type
// overrides. defaultRegion is the region of a target that sets none (see s3Region). Each type
// gives its own, because what a store assumes without a region depends on the store.
type s3TargetType struct {
	part          s3PartFunc
	defaultRegion string
}

var _ TargetType = s3TargetType{}

func init() {
	registerTargetType(s3TargetType{part: plainS3Part, defaultRegion: s3DefaultRegion})
}

func plainS3Part(config *flex.RemoteStorageTarget, create bool) *flex.RemoteStorageTarget_S3 {
	if create && !config.HasS3() {
		config.SetS3(&flex.RemoteStorageTarget_S3{})
	}
	return config.GetS3()
}

// s3Part returns the S3 part of config, or nil when it has none. A type that extends s3TargetType
// inherits it with its own part function, which is how s3PartOf finds the S3 part of any such type.
func (t s3TargetType) s3Part(config *flex.RemoteStorageTarget) *flex.RemoteStorageTarget_S3 {
	return t.part(config, false)
}

func (s3TargetType) Name() string { return "s3" }

func (s3TargetType) NewConfig() *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
}

func (t s3TargetType) NewProvider(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) (Provider, error) {
	return newS3(ctx, config, t.defaultRegion, mountPoint)
}

func (t s3TargetType) Settings() []Setting {
	setting := func(s Setting, get func(*flex.RemoteStorageTarget_S3) string, set func(*flex.RemoteStorageTarget_S3, string)) Setting {
		s.Get = func(config *flex.RemoteStorageTarget) string { return get(t.part(config, false)) }
		s.Set = func(config *flex.RemoteStorageTarget, value string) error {
			set(t.part(config, true), value)
			return nil
		}
		return s
	}
	return []Setting{
		setting(Setting{Key: "endpoint-url", Help: "The URL of the S3 endpoint.", Identity: true, Kind: RequiredConnectionSetting, Summary: true},
			(*flex.RemoteStorageTarget_S3).GetEndpointUrl, (*flex.RemoteStorageTarget_S3).SetEndpointUrl),
		setting(Setting{Key: "partition-id", Help: "The partition of the S3 endpoint, if it uses one.", Identity: true, Kind: OptionalConnectionSetting},
			(*flex.RemoteStorageTarget_S3).GetPartitionId, (*flex.RemoteStorageTarget_S3).SetPartitionId),
		setting(Setting{Key: "region", Help: fmt.Sprintf("The region of the bucket. Without one, the target uses %s.", t.defaultRegion), Identity: true, Kind: OptionalConnectionSetting, Default: t.defaultRegion},
			(*flex.RemoteStorageTarget_S3).GetRegion, (*flex.RemoteStorageTarget_S3).SetRegion),
		setting(Setting{Key: "bucket", Help: "The real name of the bucket in the S3 store that objects are kept in.", Identity: true, Kind: BucketSetting, Summary: true},
			(*flex.RemoteStorageTarget_S3).GetBucket, (*flex.RemoteStorageTarget_S3).SetBucket),
		setting(Setting{Key: "access-key", Help: "The access key.", Kind: RequiredConnectionSetting},
			(*flex.RemoteStorageTarget_S3).GetAccessKey, (*flex.RemoteStorageTarget_S3).SetAccessKey),
		setting(Setting{Key: "secret-key", Help: "The secret key.", Secret: true, Kind: RequiredConnectionSetting},
			(*flex.RemoteStorageTarget_S3).GetSecretKey, (*flex.RemoteStorageTarget_S3).SetSecretKey),
	}
}

func (s3TargetType) Example() string {
	return `  beegfs remote target create s3 --name=archive \
    --endpoint-url=https://s3.example.com --region=us-east-1 --bucket=beegfs-archive \
    --access-key=AKIAEXAMPLE --secret-key-file=/etc/beegfs/archive.secret \
    --policies fast-start-max-size=1GiB \
    --storage-class name=GLACIER,retrieval-tier=Bulk,retention-days=7,check-time=1h,recheck-time=15m`
}

func (t s3TargetType) Lists() []KeyedList {
	return []KeyedList{s3StorageClasses{s3Part: t.part}}
}

// IdentityChanged reports a change of endpoint, partition, region or bucket. Credentials and storage
// classes are not part of the identity, because changing them does not move existing objects.
func (t s3TargetType) IdentityChanged(old, new *flex.RemoteStorageTarget) bool {
	return identityFromSettings(t.Settings(), old, new)
}

// VerifyBackend lists at most one bucket. ListBuckets is the one S3 request that needs no bucket.
// See verifyS3Backend for how the answer is read.
func (t s3TargetType) VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget) (string, error) {
	return verifyS3Backend(ctx, t.part(backend, false), t.defaultRegion)
}

// Buckets returns the S3 bucket manager, on the type's own S3 part. So an extending type such as
// xtreemstore manages its buckets through the same S3 requests.
func (t s3TargetType) Buckets() BucketManager { return newS3Buckets(t.part, t.defaultRegion) }

// s3StorageClasses is the storage-class list of an S3 part, keyed by the class name. The other
// fields are the class's archival settings. A class with no archival settings shows them as unset.
type s3StorageClasses struct {
	s3Part s3PartFunc
}

var _ KeyedList = s3StorageClasses{}

func (s3StorageClasses) Flag() string   { return "storage-class" }
func (s3StorageClasses) Plural() string { return "storage-classes" }
func (s3StorageClasses) Help() string {
	return "Add a storage class, or change the given settings of an existing one. Repeat the flag for more classes."
}
func (s3StorageClasses) Key() KeyedListField {
	return KeyedListField{Key: "name", Placeholder: "<name>"}
}
func (s3StorageClasses) Fields() []KeyedListField {
	return []KeyedListField{
		{Key: "retrieval-tier", Placeholder: "<tier>"},
		{Key: "retention-days", Placeholder: "<days>"},
		{Key: "check-time", Placeholder: "<duration>"},
		{Key: "recheck-time", Placeholder: "<duration>"},
		{Key: "auto-restore", Placeholder: "<true|false>"},
	}
}
func (s3StorageClasses) Summary() bool { return true }

func (l s3StorageClasses) Entries(config *flex.RemoteStorageTarget) []KeyedListEntry {
	entries := []KeyedListEntry{}
	for _, c := range l.s3Part(config, false).GetStorageClass() {
		entry := KeyedListEntry{Key: c.GetName(), Values: make([]string, len(l.Fields()))}
		if c.HasArchival() {
			a := c.GetArchival()
			entry.Values = []string{a.GetRetrievalTier(), strconv.Itoa(int(a.GetRetentionDays())), a.GetCheckTime(), a.GetRecheckTime(), strconv.FormatBool(a.GetAutoRestore())}
		}
		entries = append(entries, entry)
	}
	return entries
}

func (l s3StorageClasses) Upsert(config *flex.RemoteStorageTarget, key string, values map[string]string) error {
	if key == "" {
		return fmt.Errorf("a storage class needs a name")
	}
	if err := checkListFields(l, values); err != nil {
		return err
	}
	s3 := l.s3Part(config, true)
	classes, class := upsertListEntry(s3.GetStorageClass(), storageClassNamed(key), &flex.RemoteStorageTarget_S3_StorageClass{Name: key})
	s3.SetStorageClass(classes)
	if len(values) == 0 {
		return nil
	}
	if !class.HasArchival() {
		class.SetArchival(&flex.RemoteStorageTarget_S3_StorageClass_Archival{})
	}
	a := class.GetArchival()
	if v, ok := values["retrieval-tier"]; ok {
		a.SetRetrievalTier(v)
	}
	if v, ok := values["retention-days"]; ok {
		days, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return fmt.Errorf("storage class %q: retention-days must be a whole number: %w", key, err)
		}
		a.SetRetentionDays(int32(days))
	}
	if v, ok := values["check-time"]; ok {
		a.SetCheckTime(v)
	}
	if v, ok := values["recheck-time"]; ok {
		a.SetRecheckTime(v)
	}
	if v, ok := values["auto-restore"]; ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("storage class %q: auto-restore must be true or false", key)
		}
		a.SetAutoRestore(b)
	}
	return nil
}

func (l s3StorageClasses) Remove(config *flex.RemoteStorageTarget, key string) error {
	s3 := l.s3Part(config, false)
	classes, ok := removeListEntry(s3.GetStorageClass(), storageClassNamed(key))
	if !ok {
		return fmt.Errorf("the target has no storage class %q to remove", key)
	}
	s3.SetStorageClass(classes)
	return nil
}

func (l s3StorageClasses) Clear(config *flex.RemoteStorageTarget) {
	if s3 := l.s3Part(config, false); s3 != nil {
		s3.SetStorageClass(nil)
	}
}

func (l s3StorageClasses) CopyEntry(dst, src *flex.RemoteStorageTarget, key string) error {
	s3 := l.s3Part(dst, true)
	classes, err := copyListEntry(s3.GetStorageClass(), l.s3Part(src, false).GetStorageClass(), storageClassNamed(key), fmt.Sprintf("storage class %q", key))
	if err != nil {
		return err
	}
	s3.SetStorageClass(classes)
	return nil
}

// storageClassNamed returns a match for the storage class called name.
func storageClassNamed(name string) func(*flex.RemoteStorageTarget_S3_StorageClass) bool {
	return func(c *flex.RemoteStorageTarget_S3_StorageClass) bool { return c.GetName() == name }
}

func (l s3StorageClasses) ValidateEntries(config *flex.RemoteStorageTarget) error {
	for _, class := range l.s3Part(config, false).GetStorageClass() {
		if _, err := parseS3StorageClass(class); err != nil {
			return err
		}
	}
	return nil
}
