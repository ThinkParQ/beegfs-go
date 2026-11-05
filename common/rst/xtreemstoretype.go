package rst

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file implements TargetType for xtreemstore targets (see targettype.go). An xtreemstore
// target is an S3 target with bulk operations, so its type extends s3TargetType: it keeps every S3
// setting and the storage-class list, and adds the bulk-operation list.

type xtreemstoreTargetType struct {
	s3TargetType
}

var _ TargetType = xtreemstoreTargetType{}

func init() {
	registerTargetType(xtreemstoreTargetType{s3TargetType{part: xtreemstoreS3Part}})
}

// xtreemstoreS3Part returns the S3 part embedded in the xtreemstore message of config.
func xtreemstoreS3Part(config *flex.RemoteStorageTarget, create bool) *flex.RemoteStorageTarget_S3 {
	xtreemstore := xtreemstorePart(config, create)
	if create && !xtreemstore.HasS3() {
		xtreemstore.SetS3(&flex.RemoteStorageTarget_S3{})
	}
	return xtreemstore.GetS3()
}

// xtreemstorePart returns the xtreemstore message of config. With create, it adds an empty one when
// config has none. Without create, it may return nil.
func xtreemstorePart(config *flex.RemoteStorageTarget, create bool) *flex.RemoteStorageTarget_XtreemStore {
	if create && !config.HasXtreemstore() {
		config.SetXtreemstore(&flex.RemoteStorageTarget_XtreemStore{})
	}
	return config.GetXtreemstore()
}

func (xtreemstoreTargetType) Name() string { return "xtreemstore" }

func (xtreemstoreTargetType) NewConfig() *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{
		Xtreemstore: flex.RemoteStorageTarget_XtreemStore_builder{S3: &flex.RemoteStorageTarget_S3{}}.Build(),
	}.Build()
}

func (xtreemstoreTargetType) NewProvider(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) (Provider, error) {
	return newXtreemstore(ctx, config, mountPoint, stateRoot)
}

func (xtreemstoreTargetType) Example() string {
	return `  beegfs remote target create xtreemstore --name=tape --id=10 \
    --endpoint-url=https://xts.example.com:9000 --bucket=tape-pool \
    --access-key=tapeuser --secret-key-file=/etc/beegfs/tape.secret \
    --storage-class name=TAPE,retrieval-tier=Standard,retention-days=1,check-time=30m,recheck-time=10m \
    --bulk-operation operation=efficient-retrieve,retry-delay=1h,poll-delay=5m`
}

func (t xtreemstoreTargetType) Lists() []KeyedList {
	return append(t.s3TargetType.Lists(), xtreemstoreBulkOperations{})
}

// xtreemstoreBulkOperations is the bulk-operation list of an xtreemstore target, keyed by the
// operation. An operation is written in the CLI's kebab-case form, such as efficient-retrieve, and
// its enum name, such as EFFICIENT_RETRIEVE, is accepted as well.
type xtreemstoreBulkOperations struct{}

var _ KeyedList = xtreemstoreBulkOperations{}

func (xtreemstoreBulkOperations) Flag() string   { return "bulk-operation" }
func (xtreemstoreBulkOperations) Plural() string { return "bulk-operations" }
func (xtreemstoreBulkOperations) Help() string {
	return "Add a bulk operation, or change the given settings of an existing one. Repeat the flag for more operations."
}
func (xtreemstoreBulkOperations) Key() ListField {
	return ListField{Key: "operation", Placeholder: "<" + strings.Join(bulkOperationFlagNames(), "|") + ">"}
}
func (xtreemstoreBulkOperations) Fields() []ListField {
	return []ListField{
		{Key: "retry-delay", Placeholder: "<duration>"},
		{Key: "poll-delay", Placeholder: "<duration>"},
	}
}
func (xtreemstoreBulkOperations) Summary() bool { return true }

func (xtreemstoreBulkOperations) Entries(config *flex.RemoteStorageTarget) []ListEntry {
	entries := []ListEntry{}
	for _, op := range config.GetXtreemstore().GetBulkOperations() {
		entries = append(entries, ListEntry{
			Key:    bulkOperationFlagName(op.GetOperation()),
			Values: []string{op.GetRetryDelay(), op.GetPollDelay()},
		})
	}
	return entries
}

func (l xtreemstoreBulkOperations) Upsert(config *flex.RemoteStorageTarget, key string, values map[string]string) error {
	operation, err := parseBulkOperationFlag(key)
	if err != nil {
		return err
	}
	if err := checkListFields(l, values); err != nil {
		return err
	}
	xtreemstore := xtreemstorePart(config, true)
	operations := xtreemstore.GetBulkOperations()
	i := slices.IndexFunc(operations, func(o *flex.RemoteStorageTarget_XtreemStore_BulkOperation) bool { return o.GetOperation() == operation })
	if i < 0 {
		operations = append(operations, flex.RemoteStorageTarget_XtreemStore_BulkOperation_builder{Operation: operation}.Build())
		i = len(operations) - 1
		xtreemstore.SetBulkOperations(operations)
	}
	if v, ok := values["retry-delay"]; ok {
		operations[i].SetRetryDelay(v)
	}
	if v, ok := values["poll-delay"]; ok {
		operations[i].SetPollDelay(v)
	}
	return nil
}

func (xtreemstoreBulkOperations) Remove(config *flex.RemoteStorageTarget, key string) error {
	operation, err := parseBulkOperationFlag(key)
	if err != nil {
		return err
	}
	xtreemstore := xtreemstorePart(config, false)
	operations := xtreemstore.GetBulkOperations()
	i := slices.IndexFunc(operations, func(o *flex.RemoteStorageTarget_XtreemStore_BulkOperation) bool { return o.GetOperation() == operation })
	if i < 0 {
		return fmt.Errorf("the target has no bulk operation %s to remove", bulkOperationFlagName(operation))
	}
	xtreemstore.SetBulkOperations(slices.Delete(operations, i, i+1))
	return nil
}

func (xtreemstoreBulkOperations) Clear(config *flex.RemoteStorageTarget) {
	if xtreemstore := xtreemstorePart(config, false); xtreemstore != nil {
		xtreemstore.SetBulkOperations(nil)
	}
}

func (xtreemstoreBulkOperations) CopyEntry(dst, src *flex.RemoteStorageTarget, key string) error {
	operation, err := parseBulkOperationFlag(key)
	if err != nil {
		return err
	}
	matches := func(o *flex.RemoteStorageTarget_XtreemStore_BulkOperation) bool { return o.GetOperation() == operation }
	xtreemstore := xtreemstorePart(dst, true)
	operations, err := copyListEntry(xtreemstore.GetBulkOperations(), xtreemstorePart(src, false).GetBulkOperations(), matches, "bulk operation "+bulkOperationFlagName(operation))
	if err != nil {
		return err
	}
	xtreemstore.SetBulkOperations(operations)
	return nil
}

func (xtreemstoreBulkOperations) ValidateEntries(config *flex.RemoteStorageTarget) error {
	for _, operation := range xtreemstorePart(config, false).GetBulkOperations() {
		if _, err := parseBulkOperationConfig(operation); err != nil {
			return err
		}
	}
	return nil
}

// parseBulkOperationFlag accepts an operation in the CLI's kebab-case form, such as
// efficient-retrieve, or as its enum name, such as EFFICIENT_RETRIEVE.
func parseBulkOperationFlag(name string) (flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation, error) {
	enumName := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(name), "-", "_"))
	value, ok := flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation_value[enumName]
	if !ok || value == int32(flex.RemoteStorageTarget_XtreemStore_BulkOperation_UNKNOWN) {
		return 0, fmt.Errorf("unknown bulk operation %q (known: %s)", name, strings.Join(bulkOperationFlagNames(), ", "))
	}
	return flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation(value), nil
}

func bulkOperationFlagNames() []string {
	names := []string{}
	for value := range flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation_name {
		if value != int32(flex.RemoteStorageTarget_XtreemStore_BulkOperation_UNKNOWN) {
			names = append(names, bulkOperationFlagName(flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation(value)))
		}
	}
	slices.Sort(names)
	return names
}

// bulkOperationFlagName returns an operation in the CLI's kebab-case form.
func bulkOperationFlagName(op flex.RemoteStorageTarget_XtreemStore_BulkOperation_Operation) string {
	return strings.ToLower(strings.ReplaceAll(op.String(), "_", "-"))
}
