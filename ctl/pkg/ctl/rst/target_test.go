package rst

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

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
	return &flex.RemoteStorageTarget{
		Id:   1,
		Name: "archive",
		Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
			EndpointUrl: "https://s3.example.com",
			Bucket:      "beegfs-archive",
			AccessKey:   "old",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{
				&flex.RemoteStorageTarget_S3_StorageClass{Name: "GLACIER", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m"}},
				&flex.RemoteStorageTarget_S3_StorageClass{Name: "DEEP_ARCHIVE", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Standard", RetentionDays: 3}},
			},
		}},
	}
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
	read := &flex.RemoteStorageTarget{Id: 1, Name: "t", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "b"}}}
	effective := &flex.RemoteStorageTarget{Id: 1, Name: "t", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "b", EndpointUrl: "https://site"}}}
	none := ""
	other := "other"

	for name, changes := range map[string]TargetChanges{
		"a setting":          {Settings: map[string]string{"bucket": "c"}},
		"a new backend":      {Backend: &other},
		"removing a backend": {Backend: &none},
	} {
		start, err := startingPoint(read, effective, changes)
		require.NoError(t, err)
		assert.True(t, proto.Equal(read, start), "%s applies to the definition", name)
	}

	// Removing the backend sends the definition without one. Remote fills in the inherited values,
	// from the configuration it holds when it applies the change.
	detached, err := ApplyTargetChanges(read, TargetChanges{Backend: &none})
	require.NoError(t, err)
	assert.Empty(t, detached.GetBackend())
	assert.Empty(t, detached.GetS3().GetEndpointUrl(), "the CLI does not copy values it read earlier")
}

// TestModifyInheritedListEntryKeepsItsOtherFields checks a target that inherits storage class
// GLACIER and changes one of its fields. Entries are inherited whole, so the target's own entry
// must start as a copy of the inherited one. Otherwise the other fields the backend set would be
// lost.
func TestModifyInheritedListEntryKeepsItsOtherFields(t *testing.T) {
	glacier := &flex.RemoteStorageTarget_S3_StorageClass{Name: "GLACIER", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{
		RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m", AutoRestore: true,
	}}
	read := &flex.RemoteStorageTarget{Id: 1, Name: "t", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "b"}}}
	effective := &flex.RemoteStorageTarget{Id: 1, Name: "t", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
		Bucket: "b", EndpointUrl: "https://site", StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{glacier},
	}}}
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
	withBackend := &flex.RemoteStorageTarget{Backend: "site"}

	err := hintInherited(base, withBackend, removal)
	require.ErrorIs(t, err, base)
	assert.Contains(t, err.Error(), `inherited from backend "site"`)
	assert.Equal(t, base, hintInherited(base, &flex.RemoteStorageTarget{}, removal), "no hint without a backend")
	assert.Equal(t, base, hintInherited(base, withBackend, TargetChanges{}), "no hint without a removal")
}

func TestTargetsUsing(t *testing.T) {
	targets := Targets{
		Definitions: map[uint32]*flex.RemoteStorageTarget{
			2: &flex.RemoteStorageTarget{Id: 2, Backend: "site"},
			1: &flex.RemoteStorageTarget{Id: 1, Backend: "site"},
			3: &flex.RemoteStorageTarget{Id: 3, Backend: "team"},
		},
		Backends: map[string]*flex.RemoteStorageTarget{
			"team":   &flex.RemoteStorageTarget{Name: "team"},
			"site":   &flex.RemoteStorageTarget{Name: "site"},
			"unused": &flex.RemoteStorageTarget{Name: "unused"},
		},
	}
	assert.Equal(t, []uint32{1, 2}, targets.TargetsUsing("site"))
	assert.Equal(t, []uint32{3}, targets.TargetsUsing("team"))
	assert.Empty(t, targets.TargetsUsing("unused"))
}

func TestCheckTargetBackend(t *testing.T) {
	backends := map[string]*flex.RemoteStorageTarget{
		"site": &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}},
	}
	s3Target := func(backend string) *flex.RemoteStorageTarget {
		return &flex.RemoteStorageTarget{Id: 1, Backend: backend, Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
	}
	require.NoError(t, checkTargetBackend(s3Target(""), backends), "a target without a backend needs none")
	require.NoError(t, checkTargetBackend(s3Target("site"), backends))
	require.ErrorContains(t, checkTargetBackend(s3Target("missing"), backends), `remote backend "missing" does not exist`)
	xts := &flex.RemoteStorageTarget{Id: 1, Backend: "site", Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}}
	require.ErrorContains(t, checkTargetBackend(xts, backends), `backend "site" is of type s3, but the target is of type xtreemstore`)
}

// TestVerifyBackendRefusesTimeoutsTheRequestCannotCarry checks the timeouts refused before Remote is
// contacted. The request carries whole milliseconds in a uint32, so a cast would turn them into
// other values.
func TestVerifyBackendRefusesTimeoutsTheRequestCannotCarry(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Second, time.Microsecond, (math.MaxUint32 + 1) * time.Millisecond} {
		_, err := VerifyBackend(context.Background(), "site", timeout)
		assert.ErrorContains(t, err, "the verify timeout", timeout.String())
	}
}
