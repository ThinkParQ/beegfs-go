package rst

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

func ptr[T any](v T) *T { return &v }

func storageClasses(changes ListChanges) TargetChanges {
	return TargetChanges{Lists: map[string]ListChanges{"storage-class": changes}}
}

func archiveTarget() *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{
		Id:   1,
		Name: "archive",
		S3: flex.RemoteStorageTarget_S3_builder{
			EndpointUrl: "https://s3.example.com",
			Bucket:      "beegfs-archive",
			AccessKey:   "old",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{
				flex.RemoteStorageTarget_S3_StorageClass_builder{Name: "GLACIER", Archival: flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m"}.Build()}.Build(),
				flex.RemoteStorageTarget_S3_StorageClass_builder{Name: "DEEP_ARCHIVE", Archival: flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetrievalTier: "Standard", RetentionDays: 3}.Build()}.Build(),
			},
		}.Build(),
	}.Build()
}

func TestApplyTargetChangesKeepsWhatIsNotGiven(t *testing.T) {
	before := archiveTarget()
	after, err := ApplyTargetChanges(before, TargetChanges{
		Name:     ptr("renamed"),
		Settings: map[string]string{"access-key": "new"},
	})
	require.NoError(t, err)
	assert.True(t, proto.Equal(archiveTarget(), before), "the input must not be modified")

	want := archiveTarget()
	want.SetName("renamed")
	want.GetS3().SetAccessKey("new")
	assert.True(t, proto.Equal(want, after))
}

func TestApplyTargetChangesStorageClasses(t *testing.T) {
	t.Run("a new class is added and existing ones are kept", func(t *testing.T) {
		after, err := ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "GLACIER_IR", Values: map[string]string{"retrieval-tier": "Expedited", "retention-days": "1"}}}}))
		require.NoError(t, err)
		classes := after.GetS3().GetStorageClass()
		require.Len(t, classes, 3)
		assert.Equal(t, "GLACIER_IR", classes[2].GetName())
		assert.Equal(t, "Expedited", classes[2].GetArchival().GetRetrievalTier())
	})

	t.Run("an existing class changes only the given settings", func(t *testing.T) {
		after, err := ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "GLACIER", Values: map[string]string{"recheck-time": "30m"}}}}))
		require.NoError(t, err)
		glacier := after.GetS3().GetStorageClass()[0].GetArchival()
		assert.Equal(t, "30m", glacier.GetRecheckTime())
		assert.Equal(t, "Bulk", glacier.GetRetrievalTier())
		assert.Equal(t, int32(7), glacier.GetRetentionDays())
		assert.Equal(t, "1h", glacier.GetCheckTime())
	})

	t.Run("classes are removed by name", func(t *testing.T) {
		after, err := ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Remove: []string{"DEEP_ARCHIVE"}}))
		require.NoError(t, err)
		require.Len(t, after.GetS3().GetStorageClass(), 1)
		assert.Equal(t, "GLACIER", after.GetS3().GetStorageClass()[0].GetName())

		_, err = ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Remove: []string{"MISSING"}}))
		assert.Error(t, err)
	})

	t.Run("clear runs before additions, which replaces the list", func(t *testing.T) {
		after, err := ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Clear: true, Upsert: []ListEntryChange{{Key: "ONLY", Values: map[string]string{"retention-days": "2"}}}}))
		require.NoError(t, err)
		require.Len(t, after.GetS3().GetStorageClass(), 1)
		assert.Equal(t, "ONLY", after.GetS3().GetStorageClass()[0].GetName())
	})

	t.Run("a class given twice is refused", func(t *testing.T) {
		_, err := ApplyTargetChanges(archiveTarget(), storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "X"}, {Key: "X"}}}))
		assert.Error(t, err)
	})
}

func TestApplyTargetChangesBulkOperations(t *testing.T) {
	bulk := func(changes ListChanges) TargetChanges {
		return TargetChanges{Lists: map[string]ListChanges{"bulk-operation": changes}}
	}

	_, err := ApplyTargetChanges(archiveTarget(), bulk(ListChanges{Upsert: []ListEntryChange{{Key: "efficient-retrieve"}}}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "applies to xtreemstore targets", "the error names the types that have the flag")

	tape, err := NewTarget("xtreemstore", 10)
	require.NoError(t, err)
	changes := bulk(ListChanges{Upsert: []ListEntryChange{{Key: "efficient-retrieve", Values: map[string]string{"retry-delay": "1h", "poll-delay": "5m"}}}})
	changes.Name = ptr("tape")
	changes.Settings = map[string]string{"bucket": "tape-pool"}
	tape, err = ApplyTargetChanges(tape, changes)
	require.NoError(t, err)
	assert.Equal(t, "tape-pool", tape.GetXtreemstore().GetS3().GetBucket(), "S3 settings reach the S3 part xtreemstore embeds")
	assert.False(t, tape.HasS3(), "the target stays an xtreemstore target")

	tape, err = ApplyTargetChanges(tape, bulk(ListChanges{Upsert: []ListEntryChange{{Key: "EFFICIENT_RETRIEVE", Values: map[string]string{"poll-delay": "2m"}}}}))
	require.NoError(t, err)
	ops := tape.GetXtreemstore().GetBulkOperations()
	require.Len(t, ops, 1)
	assert.Equal(t, "2m", ops[0].GetPollDelay())
	assert.Equal(t, "1h", ops[0].GetRetryDelay(), "the retry delay is kept")

	_, err = ApplyTargetChanges(tape, bulk(ListChanges{Upsert: []ListEntryChange{{Key: "teleport"}}}))
	assert.Error(t, err, "an unknown operation is refused")

	tape, err = ApplyTargetChanges(tape, bulk(ListChanges{Remove: []string{"efficient-retrieve"}}))
	require.NoError(t, err)
	assert.Empty(t, tape.GetXtreemstore().GetBulkOperations())
}

func TestApplyTargetChangesRefusesBadValues(t *testing.T) {
	for _, changes := range []TargetChanges{
		storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "X", Values: map[string]string{"retention-days": "seven"}}}}),
		storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "X", Values: map[string]string{"auto-restore": "maybe"}}}}),
		storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "X", Values: map[string]string{"colour": "blue"}}}}),
		{Settings: map[string]string{"no-such-setting": "x"}},
	} {
		_, err := ApplyTargetChanges(archiveTarget(), changes)
		assert.Error(t, err, "%+v", changes)
	}
}

func TestApplyTargetChangesPolicies(t *testing.T) {
	after, err := ApplyTargetChanges(archiveTarget(), TargetChanges{Policies: PolicyChanges{FastStartMaxSize: ptr(int64(1 << 30))}})
	require.NoError(t, err)
	assert.Equal(t, int64(1<<30), after.GetPolicies().GetFastStartMaxSize())
}

func TestStartingPointAppliesToTheDefinition(t *testing.T) {
	read := flex.RemoteStorageTarget_builder{Id: 1, Name: "t", Parent: "site", S3: flex.RemoteStorageTarget_S3_builder{Bucket: "b"}.Build()}.Build()
	effective := flex.RemoteStorageTarget_builder{Id: 1, Name: "t", S3: flex.RemoteStorageTarget_S3_builder{Bucket: "b", EndpointUrl: "https://site"}.Build()}.Build()
	none := ""
	other := "other"

	for name, changes := range map[string]TargetChanges{
		"a setting":         {Settings: map[string]string{"bucket": "c"}},
		"a new parent":      {Parent: &other},
		"removing a parent": {Parent: &none},
	} {
		start, err := startingPoint(read, effective, changes)
		require.NoError(t, err)
		assert.True(t, proto.Equal(read, start), "%s applies to the definition", name)
	}

	// Removing the parent sends the definition without one. Remote fills in the inherited values,
	// from the configuration it holds when it applies the change.
	detached, err := ApplyTargetChanges(read, TargetChanges{Parent: &none})
	require.NoError(t, err)
	assert.Empty(t, detached.GetParent())
	assert.Empty(t, detached.GetS3().GetEndpointUrl(), "the CLI does not copy values it read earlier")
}

// TestModifyInheritedListEntryKeepsItsOtherFields checks a target that inherits storage class
// GLACIER and changes one of its fields. Entries are inherited whole, so the target's own entry must
// start as a copy of the inherited one. Otherwise the other fields the parent set would be lost.
func TestModifyInheritedListEntryKeepsItsOtherFields(t *testing.T) {
	glacier := flex.RemoteStorageTarget_S3_StorageClass_builder{Name: "GLACIER", Archival: flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{
		RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m", AutoRestore: true,
	}.Build()}.Build()
	read := flex.RemoteStorageTarget_builder{Id: 1, Name: "t", Parent: "site", S3: flex.RemoteStorageTarget_S3_builder{Bucket: "b"}.Build()}.Build()
	effective := flex.RemoteStorageTarget_builder{Id: 1, Name: "t", S3: flex.RemoteStorageTarget_S3_builder{
		Bucket: "b", EndpointUrl: "https://site", StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{glacier},
	}.Build()}.Build()
	before := proto.Clone(read)
	changes := storageClasses(ListChanges{Upsert: []ListEntryChange{{Key: "GLACIER", Values: map[string]string{"recheck-time": "30m"}}}})

	start, err := startingPoint(read, effective, changes)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, read), "the definition as read is not modified")
	updated, err := ApplyTargetChanges(start, changes)
	require.NoError(t, err)

	require.Len(t, updated.GetS3().GetStorageClass(), 1)
	got := updated.GetS3().GetStorageClass()[0].GetArchival()
	assert.Equal(t, "30m", got.GetRecheckTime(), "the given field changes")
	assert.Equal(t, "Bulk", got.GetRetrievalTier(), "every other field keeps the inherited value")
	assert.Equal(t, int32(7), got.GetRetentionDays())
	assert.Equal(t, "1h", got.GetCheckTime())
	assert.True(t, got.GetAutoRestore())
	assert.Empty(t, updated.GetS3().GetEndpointUrl(), "inherited settings stay inherited")

	cleared := storageClasses(ListChanges{Clear: true, Upsert: []ListEntryChange{{Key: "GLACIER", Values: map[string]string{"check-time": "2h", "recheck-time": "1h"}}}})
	start, err = startingPoint(read, effective, cleared)
	require.NoError(t, err)
	assert.Empty(t, start.GetS3().GetStorageClass(), "with clear, an entry named after it is new and given in full")
}

func TestHintInherited(t *testing.T) {
	base := errors.New("the target has no storage class \"GLACIER\" to remove")
	removal := TargetChanges{Lists: map[string]ListChanges{"storage-class": {Remove: []string{"GLACIER"}}}}
	withParent := flex.RemoteStorageTarget_builder{Parent: "site"}.Build()

	err := hintInherited(base, withParent, removal)
	require.ErrorIs(t, err, base)
	assert.Contains(t, err.Error(), `inherited from parent "site"`)
	assert.Equal(t, base, hintInherited(base, &flex.RemoteStorageTarget{}, removal), "no hint without a parent")
	assert.Equal(t, base, hintInherited(base, withParent, TargetChanges{}), "no hint without a removal")
}

func TestTargetsChildren(t *testing.T) {
	targets := Targets{
		Definitions: map[uint32]*flex.RemoteStorageTarget{
			2: flex.RemoteStorageTarget_builder{Id: 2, Parent: "site"}.Build(),
			1: flex.RemoteStorageTarget_builder{Id: 1, Parent: "site"}.Build(),
			3: flex.RemoteStorageTarget_builder{Id: 3, Parent: "team"}.Build(),
		},
		Parents: map[string]*flex.RemoteStorageTarget{
			"team":   flex.RemoteStorageTarget_builder{Name: "team"}.Build(),
			"site":   flex.RemoteStorageTarget_builder{Name: "site"}.Build(),
			"unused": flex.RemoteStorageTarget_builder{Name: "unused"}.Build(),
		},
	}
	assert.Equal(t, []uint32{1, 2}, targets.Children("site"))
	assert.Equal(t, []uint32{3}, targets.Children("team"))
	assert.Empty(t, targets.Children("unused"))
}
