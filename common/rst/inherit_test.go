package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

func s3Class(name string, archival *flex.RemoteStorageTarget_S3_StorageClass_Archival) *flex.RemoteStorageTarget_S3_StorageClass {
	return flex.RemoteStorageTarget_S3_StorageClass_builder{Name: name, Archival: archival}.Build()
}

func xtsConfig(name, parent string, s3 *flex.RemoteStorageTarget_S3, ops ...*flex.RemoteStorageTarget_XtreemStore_BulkOperation) *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{
		Name:        name,
		Parent:      parent,
		Xtreemstore: flex.RemoteStorageTarget_XtreemStore_builder{S3: s3, BulkOperations: ops}.Build(),
	}.Build()
}

func TestOverlayFillsOnlyUnsetValues(t *testing.T) {
	glacier := flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetrievalTier: "Bulk", RetentionDays: 7, AutoRestore: true}.Build()
	base := flex.RemoteStorageTarget_builder{
		Name:     "defaults",
		Policies: flex.RemoteStorageTarget_Policies_builder{FastStartMaxSize: 1024}.Build(),
		S3: flex.RemoteStorageTarget_S3_builder{
			EndpointUrl: "https://s3.example.com", Region: "eu", Bucket: "shared", AccessKey: "AK", SecretKey: "SK",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", glacier), s3Class("STANDARD", nil)},
		}.Build(),
	}.Build()
	over := flex.RemoteStorageTarget_builder{
		Id: 4, Name: "archive", Parent: "defaults",
		S3: flex.RemoteStorageTarget_S3_builder{
			Bucket: "archive",
			// The child's GLACIER has no archival settings. It replaces the parent's whole entry,
			// so the parent's archival settings must not leak into it.
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", nil), s3Class("DEEP", nil)},
		}.Build(),
	}.Build()
	before := proto.Clone(over)

	got, err := Overlay(base, over)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, over), "Overlay must not modify its arguments")

	assert.Equal(t, uint32(4), got.GetId())
	assert.Equal(t, "archive", got.GetName(), "the name is never inherited")
	assert.Equal(t, "defaults", got.GetParent(), "the result keeps its own parent")
	assert.Equal(t, int64(1024), got.GetPolicies().GetFastStartMaxSize())
	assert.Equal(t, "archive", got.GetS3().GetBucket(), "a value the child sets wins")
	assert.Equal(t, "https://s3.example.com", got.GetS3().GetEndpointUrl())
	assert.Equal(t, "eu", got.GetS3().GetRegion())
	assert.Equal(t, "SK", got.GetS3().GetSecretKey())

	classes := map[string]*flex.RemoteStorageTarget_S3_StorageClass{}
	for _, c := range got.GetS3().GetStorageClass() {
		classes[c.GetName()] = c
	}
	require.Len(t, classes, 3, "the child's entries plus the parent's STANDARD")
	assert.False(t, classes["GLACIER"].HasArchival(), "a child entry replaces the parent's whole entry")
	assert.Contains(t, classes, "DEEP")
	assert.Contains(t, classes, "STANDARD")
}

func TestOverlayCopiesListEntriesExactly(t *testing.T) {
	// Unset optional fields must stay unset. Upsert could not do this, because it takes text.
	op := flex.RemoteStorageTarget_XtreemStore_BulkOperation_builder{
		Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE,
		PollDelay: proto.String("5m"),
	}.Build()
	zeroArchival := flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetentionDays: 0, AutoRestore: false}.Build()
	base := xtsConfig("tape-defaults", "", flex.RemoteStorageTarget_S3_builder{
		StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("TAPE", zeroArchival)},
	}.Build(), op)
	over := xtsConfig("tape", "tape-defaults", nil)

	got, err := Overlay(base, over)
	require.NoError(t, err)
	require.Len(t, got.GetXtreemstore().GetBulkOperations(), 1)
	gotOp := got.GetXtreemstore().GetBulkOperations()[0]
	assert.True(t, proto.Equal(op, gotOp))
	assert.False(t, gotOp.HasRetryDelay(), "an unset optional field stays unset")
	require.Len(t, got.GetXtreemstore().GetS3().GetStorageClass(), 1)
	assert.True(t, got.GetXtreemstore().GetS3().GetStorageClass()[0].HasArchival(), "an archival message with only zero fields is kept")
	assert.NotSame(t, op, gotOp, "the entry is copied, not shared with the parent")
}

func TestOverlayRefusesAnotherType(t *testing.T) {
	_, err := Overlay(flex.RemoteStorageTarget_builder{S3: &flex.RemoteStorageTarget_S3{}}.Build(), xtsConfig("tape", "p", nil))
	require.ErrorContains(t, err, "a xtreemstore configuration cannot inherit from a s3 parent")

	_, err = Overlay(flex.RemoteStorageTarget_builder{Mock: proto.String("m")}.Build(), flex.RemoteStorageTarget_builder{Mock: proto.String("m")}.Build())
	require.ErrorContains(t, err, "mock configurations cannot have parents")
}

func TestResolve(t *testing.T) {
	parents := map[string]*flex.RemoteStorageTarget{
		"site": flex.RemoteStorageTarget_builder{Name: "site", S3: flex.RemoteStorageTarget_S3_builder{
			EndpointUrl:  "https://site",
			Region:       "site-region",
			AccessKey:    "site-key",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", nil)},
		}.Build()}.Build(),
	}
	target := flex.RemoteStorageTarget_builder{Id: 1, Name: "t", Parent: "site", S3: flex.RemoteStorageTarget_S3_builder{Bucket: "b", Region: "own-region"}.Build()}.Build()

	effective, err := Resolve(target, parents)
	require.NoError(t, err)
	assert.Equal(t, "https://site", effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "own-region", effective.GetS3().GetRegion(), "a value the target sets wins")
	assert.Equal(t, "b", effective.GetS3().GetBucket())
	assert.Empty(t, effective.GetParent(), "an effective configuration names no parent")
	require.NoError(t, CheckRequired(effective))

	inherited, err := Inherited(target, parents)
	require.NoError(t, err)
	assert.Equal(t, []string{"access-key", "endpoint-url", "storage-class=GLACIER"}, inherited)

	noBucket := flex.RemoteStorageTarget_builder{Id: 2, Name: "u", Parent: "site", S3: &flex.RemoteStorageTarget_S3{}}.Build()
	effective, err = Resolve(noBucket, parents)
	require.NoError(t, err)
	require.ErrorContains(t, CheckRequired(effective), "no bucket is set")

	unparented := flex.RemoteStorageTarget_builder{Id: 3, Name: "v", S3: flex.RemoteStorageTarget_S3_builder{Bucket: "c"}.Build()}.Build()
	effective, err = Resolve(unparented, parents)
	require.NoError(t, err)
	assert.True(t, proto.Equal(unparented, effective), "a target without a parent resolves to itself")
	inherited, err = Inherited(unparented, parents)
	require.NoError(t, err)
	assert.Empty(t, inherited)
}

func TestResolveErrors(t *testing.T) {
	parents := map[string]*flex.RemoteStorageTarget{
		"nested": flex.RemoteStorageTarget_builder{Name: "nested", Parent: "site", S3: &flex.RemoteStorageTarget_S3{}}.Build(),
		"xts":    xtsConfig("xts", "", &flex.RemoteStorageTarget_S3{}),
	}
	s3Child := func(parent string) *flex.RemoteStorageTarget {
		return flex.RemoteStorageTarget_builder{Name: "t", Parent: parent, S3: &flex.RemoteStorageTarget_S3{}}.Build()
	}

	_, err := Resolve(s3Child("missing"), parents)
	require.ErrorContains(t, err, `parent "missing" does not exist`)
	_, err = Resolve(s3Child("nested"), parents)
	require.ErrorContains(t, err, "a parent cannot have a parent")
	_, err = Inherited(s3Child("nested"), parents)
	require.ErrorContains(t, err, "a parent cannot have a parent")
	_, err = Resolve(s3Child("xts"), parents)
	require.ErrorContains(t, err, "cannot inherit from a xtreemstore parent")
}

func TestValidateParentRequiresCompleteEntries(t *testing.T) {
	incomplete := flex.RemoteStorageTarget_builder{Name: "site", S3: flex.RemoteStorageTarget_S3_builder{
		StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER",
			flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetrievalTier: "Bulk", RetentionDays: 7}.Build())},
	}.Build()}.Build()
	require.ErrorContains(t, ValidateParent(incomplete), "storage class, GLACIER, must specify check-time",
		"a parent is partial, but each entry it holds is inherited whole, so it must be complete")

	complete := proto.Clone(incomplete).(*flex.RemoteStorageTarget)
	archival := complete.GetS3().GetStorageClass()[0].GetArchival()
	archival.SetCheckTime("1h")
	archival.SetRecheckTime("15m")
	require.NoError(t, ValidateParent(complete))

	require.NoError(t, ValidateParent(flex.RemoteStorageTarget_builder{Name: "partial", S3: &flex.RemoteStorageTarget_S3{}}.Build()),
		"a parent without a bucket or endpoint is fine, because those are settings, not entries")

	fastPoll := xtsConfig("tape", "", nil, flex.RemoteStorageTarget_XtreemStore_BulkOperation_builder{
		Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE,
		PollDelay: proto.String("100ms"),
	}.Build())
	require.ErrorContains(t, ValidateParent(fastPoll), "poll-delay >= '1s'")

	require.ErrorContains(t, ValidateParent(flex.RemoteStorageTarget_builder{Name: "m", Mock: proto.String("m")}.Build()), "cannot be parents")
	require.ErrorContains(t, ValidateParent(flex.RemoteStorageTarget_builder{Name: "team", Parent: "site", S3: &flex.RemoteStorageTarget_S3{}}.Build()), "a parent cannot name a parent")
}

func TestValidateTargetRefusesUnregisteredTypes(t *testing.T) {
	ctx := context.Background()
	mock := flex.RemoteStorageTarget_builder{Id: 1, Name: "m", Mock: proto.String("m")}.Build()
	azure := flex.RemoteStorageTarget_builder{Id: 1, Name: "a", Azure: &flex.RemoteStorageTarget_Azure{}}.Build()
	noType := flex.RemoteStorageTarget_builder{Id: 1, Name: "n"}.Build()

	require.ErrorContains(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot), "the mock target type cannot be configured")
	require.ErrorContains(t, ValidateTarget(ctx, azure, filesystem.NewMockFS(), DefaultStateRoot), "the azure target type cannot be configured")
	require.ErrorContains(t, ValidateTarget(ctx, noType, filesystem.NewMockFS(), DefaultStateRoot), "cannot be configured")

	restore := AllowMockTargetsForTesting()
	require.NoError(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot), "tests may opt in to Mock")
	restore()
	require.Error(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot))
}
