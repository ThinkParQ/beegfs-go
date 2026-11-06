package rst

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

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
// overrides.
type s3TargetType struct {
	part s3PartFunc
}

var _ TargetType = s3TargetType{}

func init() {
	registerTargetType(s3TargetType{part: plainS3Part})
}

func plainS3Part(config *flex.RemoteStorageTarget, create bool) *flex.RemoteStorageTarget_S3 {
	if create && !config.HasS3() {
		config.SetS3(&flex.RemoteStorageTarget_S3{})
	}
	return config.GetS3()
}

func (s3TargetType) Name() string { return "s3" }

func (s3TargetType) NewConfig() *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
}

func (s3TargetType) NewProvider(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) (Provider, error) {
	return newS3(ctx, config, mountPoint)
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
		setting(Setting{Key: "endpoint-url", Help: "The URL of the S3 endpoint.", Identity: true, RequiredOnCreate: true, Backend: BackendRequired, Summary: true},
			(*flex.RemoteStorageTarget_S3).GetEndpointUrl, (*flex.RemoteStorageTarget_S3).SetEndpointUrl),
		setting(Setting{Key: "partition-id", Help: "The partition of the S3 endpoint, if it uses one.", Identity: true},
			(*flex.RemoteStorageTarget_S3).GetPartitionId, (*flex.RemoteStorageTarget_S3).SetPartitionId),
		setting(Setting{Key: "region", Help: "The region of the bucket.", Identity: true, Backend: BackendRequired},
			(*flex.RemoteStorageTarget_S3).GetRegion, (*flex.RemoteStorageTarget_S3).SetRegion),
		setting(Setting{Key: "bucket", Help: "The bucket objects are stored in.", Identity: true, RequiredOnCreate: true, Backend: BackendForbidden, Summary: true},
			(*flex.RemoteStorageTarget_S3).GetBucket, (*flex.RemoteStorageTarget_S3).SetBucket),
		setting(Setting{Key: "access-key", Help: "The access key.", Backend: BackendRequired},
			(*flex.RemoteStorageTarget_S3).GetAccessKey, (*flex.RemoteStorageTarget_S3).SetAccessKey),
		setting(Setting{Key: "secret-key", Help: "The secret key.", Secret: true, Backend: BackendRequired},
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
	return verifyS3Backend(ctx, t.part(backend, false))
}

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
func (s3StorageClasses) Key() ListField { return ListField{Key: "name", Placeholder: "<name>"} }
func (s3StorageClasses) Fields() []ListField {
	return []ListField{
		{Key: "retrieval-tier", Placeholder: "<tier>"},
		{Key: "retention-days", Placeholder: "<days>"},
		{Key: "check-time", Placeholder: "<duration>"},
		{Key: "recheck-time", Placeholder: "<duration>"},
		{Key: "auto-restore", Placeholder: "<true|false>"},
	}
}
func (s3StorageClasses) Summary() bool { return true }

// BackendForbidden is false: a storage class describes how the store archives objects, not one
// bucket, so a backend may hold the classes its targets share.
func (s3StorageClasses) BackendForbidden() bool { return false }

func (l s3StorageClasses) Entries(config *flex.RemoteStorageTarget) []ListEntry {
	entries := []ListEntry{}
	for _, c := range l.s3Part(config, false).GetStorageClass() {
		entry := ListEntry{Key: c.GetName(), Values: make([]string, len(l.Fields()))}
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
	classes := s3.GetStorageClass()
	i := slices.IndexFunc(classes, func(c *flex.RemoteStorageTarget_S3_StorageClass) bool { return c.GetName() == key })
	if i < 0 {
		classes = append(classes, &flex.RemoteStorageTarget_S3_StorageClass{Name: key})
		i = len(classes) - 1
		s3.SetStorageClass(classes)
	}
	if len(values) == 0 {
		return nil
	}
	class := classes[i]
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
	classes := s3.GetStorageClass()
	i := slices.IndexFunc(classes, func(c *flex.RemoteStorageTarget_S3_StorageClass) bool { return c.GetName() == key })
	if i < 0 {
		return fmt.Errorf("the target has no storage class %q to remove", key)
	}
	s3.SetStorageClass(slices.Delete(classes, i, i+1))
	return nil
}

func (l s3StorageClasses) Clear(config *flex.RemoteStorageTarget) {
	if s3 := l.s3Part(config, false); s3 != nil {
		s3.SetStorageClass(nil)
	}
}

func (l s3StorageClasses) CopyEntry(dst, src *flex.RemoteStorageTarget, key string) error {
	matches := func(c *flex.RemoteStorageTarget_S3_StorageClass) bool { return c.GetName() == key }
	s3 := l.s3Part(dst, true)
	classes, err := copyListEntry(s3.GetStorageClass(), l.s3Part(src, false).GetStorageClass(), matches, fmt.Sprintf("storage class %q", key))
	if err != nil {
		return err
	}
	s3.SetStorageClass(classes)
	return nil
}

func (l s3StorageClasses) ValidateEntries(config *flex.RemoteStorageTarget) error {
	for _, class := range l.s3Part(config, false).GetStorageClass() {
		if _, err := parseS3StorageClass(class); err != nil {
			return err
		}
	}
	return nil
}
