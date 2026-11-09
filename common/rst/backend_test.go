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
	return &flex.RemoteStorageTarget_S3_StorageClass{Name: name, Archival: archival}
}

func xtsConfig(name, backend string, s3 *flex.RemoteStorageTarget_S3, ops ...*flex.RemoteStorageTarget_XtreemStore_BulkOperation) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{
		Name:    name,
		Backend: backend,
		Type:    &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{S3: s3, BulkOperations: ops}},
	}
}

func TestOverlayFillsOnlyUnsetValues(t *testing.T) {
	glacier := &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk", RetentionDays: 7, AutoRestore: true}
	base := &flex.RemoteStorageTarget{
		Name:     "defaults",
		Policies: &flex.RemoteStorageTarget_Policies{FastStartMaxSize: 1024},
		Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
			EndpointUrl: "https://s3.example.com", Region: "eu", Bucket: "shared", AccessKey: "AK", SecretKey: "SK",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", glacier), s3Class("STANDARD", nil)},
		}},
	}
	over := &flex.RemoteStorageTarget{
		Id: 4, Name: "archive", Backend: "defaults",
		Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
			Bucket: "archive",
			// The child's GLACIER has no archival settings. It replaces the backend's whole entry,
			// so the backend's archival settings must not leak into it.
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", nil), s3Class("DEEP", nil)},
		}},
	}
	before := proto.Clone(over)

	got, err := Overlay(base, over)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, over), "Overlay must not modify its arguments")

	assert.Equal(t, uint32(4), got.GetId())
	assert.Equal(t, "archive", got.GetName(), "the name is never inherited")
	assert.Equal(t, "defaults", got.GetBackend(), "the result keeps its own backend")
	assert.Equal(t, int64(1024), got.GetPolicies().GetFastStartMaxSize())
	assert.Equal(t, "archive", got.GetS3().GetBucket(), "a value the child sets wins")
	assert.Equal(t, "https://s3.example.com", got.GetS3().GetEndpointUrl())
	assert.Equal(t, "eu", got.GetS3().GetRegion())
	assert.Equal(t, "SK", got.GetS3().GetSecretKey())

	classes := map[string]*flex.RemoteStorageTarget_S3_StorageClass{}
	for _, c := range got.GetS3().GetStorageClass() {
		classes[c.GetName()] = c
	}
	require.Len(t, classes, 3, "the child's entries plus the backend's STANDARD")
	assert.False(t, classes["GLACIER"].HasArchival(), "a child entry replaces the backend's whole entry")
	assert.Contains(t, classes, "DEEP")
	assert.Contains(t, classes, "STANDARD")
}

func TestOverlayCopiesListEntriesExactly(t *testing.T) {
	// Unset optional fields must stay unset. Upsert could not do this, because it takes text.
	op := &flex.RemoteStorageTarget_XtreemStore_BulkOperation{
		Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE,
		PollDelay: proto.String("5m"),
	}
	zeroArchival := &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetentionDays: 0, AutoRestore: false}
	base := xtsConfig("tape-defaults", "", &flex.RemoteStorageTarget_S3{
		StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("TAPE", zeroArchival)},
	}, op)
	over := xtsConfig("tape", "tape-defaults", nil)

	got, err := Overlay(base, over)
	require.NoError(t, err)
	require.Len(t, got.GetXtreemstore().GetBulkOperations(), 1)
	gotOp := got.GetXtreemstore().GetBulkOperations()[0]
	assert.True(t, proto.Equal(op, gotOp))
	assert.False(t, gotOp.HasRetryDelay(), "an unset optional field stays unset")
	require.Len(t, got.GetXtreemstore().GetS3().GetStorageClass(), 1)
	assert.True(t, got.GetXtreemstore().GetS3().GetStorageClass()[0].HasArchival(), "an archival message with only zero fields is kept")
	assert.NotSame(t, op, gotOp, "the entry is copied, not shared with the backend")
}

func TestOverlayRefusesAnotherType(t *testing.T) {
	_, err := Overlay(&flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}, xtsConfig("tape", "p", nil))
	require.ErrorContains(t, err, "is of type s3, but the target is of type xtreemstore")

	_, err = Overlay(&flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Mock{Mock: "m"}}, &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Mock{Mock: "m"}})
	require.ErrorContains(t, err, "mock configurations cannot have backends")
}

func TestResolve(t *testing.T) {
	backends := map[string]*flex.RemoteStorageTarget{
		"site": &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
			EndpointUrl:  "https://site",
			Region:       "site-region",
			AccessKey:    "site-key",
			StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER", nil)},
		}}},
	}
	target := &flex.RemoteStorageTarget{Id: 1, Name: "t", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "b", Region: "own-region"}}}

	effective, err := Resolve(target, backends)
	require.NoError(t, err)
	assert.Equal(t, "https://site", effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "own-region", effective.GetS3().GetRegion(), "a value the target sets wins")
	assert.Equal(t, "b", effective.GetS3().GetBucket())
	assert.Empty(t, effective.GetBackend(), "an effective configuration names no backend")
	require.NoError(t, CheckRequired(effective))

	inherited, err := Inherited(target, backends)
	require.NoError(t, err)
	assert.Equal(t, []string{"access-key", "endpoint-url", "storage-class=GLACIER"}, inherited)

	noBucket := &flex.RemoteStorageTarget{Id: 2, Name: "u", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
	effective, err = Resolve(noBucket, backends)
	require.NoError(t, err)
	require.ErrorContains(t, CheckRequired(effective), "no bucket is set")

	unbackended := &flex.RemoteStorageTarget{Id: 3, Name: "v", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "c"}}}
	effective, err = Resolve(unbackended, backends)
	require.NoError(t, err)
	assert.True(t, proto.Equal(unbackended, effective), "a target without a backend resolves to itself")
	inherited, err = Inherited(unbackended, backends)
	require.NoError(t, err)
	assert.Empty(t, inherited)
}

func TestResolveErrors(t *testing.T) {
	backends := map[string]*flex.RemoteStorageTarget{
		"nested": &flex.RemoteStorageTarget{Name: "nested", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}},
		"xts":    xtsConfig("xts", "", &flex.RemoteStorageTarget_S3{}),
	}
	s3Child := func(backend string) *flex.RemoteStorageTarget {
		return &flex.RemoteStorageTarget{Name: "t", Backend: backend, Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
	}

	_, err := Resolve(s3Child("missing"), backends)
	require.ErrorContains(t, err, `backend "missing" does not exist`)
	_, err = Resolve(s3Child("nested"), backends)
	require.ErrorContains(t, err, "a backend cannot have a backend")
	_, err = Inherited(s3Child("nested"), backends)
	require.ErrorContains(t, err, "a backend cannot have a backend")
	_, err = Resolve(s3Child("xts"), backends)
	require.ErrorContains(t, err, `backend "xts" is of type xtreemstore, but the target is of type s3`)
}

// s3Connection returns the S3 part of a backend with every setting S3 requires of a backend.
func s3Connection() *flex.RemoteStorageTarget_S3 {
	return &flex.RemoteStorageTarget_S3{EndpointUrl: "https://s3.example.com", Region: "us-east-1", AccessKey: "key", SecretKey: "secret"}
}

func TestValidateBackendRequiresCompleteEntries(t *testing.T) {
	s3 := s3Connection()
	s3.SetStorageClass([]*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER",
		&flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk", RetentionDays: 7})})
	incomplete := &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: s3}}
	require.ErrorContains(t, ValidateBackend(incomplete), "storage class, GLACIER, must specify check-time",
		"each entry a backend holds is inherited whole, so it must be complete")

	complete := proto.Clone(incomplete).(*flex.RemoteStorageTarget)
	archival := complete.GetS3().GetStorageClass()[0].GetArchival()
	archival.SetCheckTime("1h")
	archival.SetRecheckTime("15m")
	require.NoError(t, ValidateBackend(complete))

	fastPoll := xtsConfig("tape", "", s3Connection(), &flex.RemoteStorageTarget_XtreemStore_BulkOperation{
		Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE,
		PollDelay: proto.String("100ms"),
	})
	require.ErrorContains(t, ValidateBackend(fastPoll), "poll-delay >= '1s'")

	require.ErrorContains(t, ValidateBackend(&flex.RemoteStorageTarget{Name: "m", Type: &flex.RemoteStorageTarget_Mock{Mock: "m"}}), "cannot be backends")
	require.ErrorContains(t, ValidateBackend(&flex.RemoteStorageTarget{Name: "team", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}), "a backend cannot name a backend")
}

func TestValidateTargetRefusesUnregisteredTypes(t *testing.T) {
	ctx := context.Background()
	mock := &flex.RemoteStorageTarget{Id: 1, Name: "m", Type: &flex.RemoteStorageTarget_Mock{Mock: "m"}}
	azure := &flex.RemoteStorageTarget{Id: 1, Name: "a", Type: &flex.RemoteStorageTarget_Azure_{Azure: &flex.RemoteStorageTarget_Azure{}}}
	noType := &flex.RemoteStorageTarget{Id: 1, Name: "n"}

	require.ErrorContains(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot), "the mock target type cannot be configured")
	require.ErrorContains(t, ValidateTarget(ctx, azure, filesystem.NewMockFS(), DefaultStateRoot), "the azure target type cannot be configured")
	require.ErrorContains(t, ValidateTarget(ctx, noType, filesystem.NewMockFS(), DefaultStateRoot), "cannot be configured")

	restore := AllowMockTargetsForTesting()
	require.NoError(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot), "tests may opt in to Mock")
	restore()
	require.Error(t, ValidateTarget(ctx, mock, filesystem.NewMockFS(), DefaultStateRoot))
}

func TestValidateBackendRoles(t *testing.T) {
	for _, newBackend := range map[string]func(*flex.RemoteStorageTarget_S3) *flex.RemoteStorageTarget{
		"s3": func(s3 *flex.RemoteStorageTarget_S3) *flex.RemoteStorageTarget {
			return &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: s3}}
		},
		"xtreemstore": func(s3 *flex.RemoteStorageTarget_S3) *flex.RemoteStorageTarget {
			return xtsConfig("site", "", s3)
		},
	} {
		require.NoError(t, ValidateBackend(newBackend(s3Connection())))

		withDefaults := s3Connection()
		withDefaults.SetPartitionId("p")
		withDefaults.SetStorageClass([]*flex.RemoteStorageTarget_S3_StorageClass{s3Class("GLACIER",
			&flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m"})})
		backend := newBackend(withDefaults)
		backend.SetPolicies(&flex.RemoteStorageTarget_Policies{FastStartMaxSize: 1})
		require.NoError(t, ValidateBackend(backend), "every setting a backend neither must nor must not set is a default")

		for _, clear := range []func(*flex.RemoteStorageTarget_S3){
			func(s *flex.RemoteStorageTarget_S3) { s.SetEndpointUrl("") },
			func(s *flex.RemoteStorageTarget_S3) { s.SetRegion("") },
			func(s *flex.RemoteStorageTarget_S3) { s.SetAccessKey("") },
			func(s *flex.RemoteStorageTarget_S3) { s.SetSecretKey("") },
		} {
			s3 := s3Connection()
			clear(s3)
			require.ErrorContains(t, ValidateBackend(newBackend(s3)), "backend must set", "a backend needs every setting to reach its store")
		}
		err := ValidateBackend(newBackend(&flex.RemoteStorageTarget_S3{}))
		require.ErrorContains(t, err, "must set endpoint-url, region, access-key, secret-key", "the error names every missing setting at once")

		withBucket := s3Connection()
		withBucket.SetBucket("b")
		require.ErrorContains(t, ValidateBackend(newBackend(withBucket)), "cannot set bucket, because each target sets its own")

		odd := s3Connection()
		odd.SetEndpointUrl("s3.example.com")
		require.NoError(t, ValidateBackend(newBackend(odd)), "the endpoint's form is not checked, only verify proves it works")
	}
}

func TestTargetMayOverrideWhatItsBackendMustSet(t *testing.T) {
	backends := map[string]*flex.RemoteStorageTarget{"site": &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: s3Connection()}}}
	own := &flex.RemoteStorageTarget{Id: 1, Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
		EndpointUrl: "https://other.example.com", AccessKey: "own-key", SecretKey: "own-secret", Bucket: "b",
	}}}
	effective, err := Resolve(own, backends)
	require.NoError(t, err)
	assert.Equal(t, "https://other.example.com", effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "own-key", effective.GetS3().GetAccessKey())
	assert.Equal(t, "own-secret", effective.GetS3().GetSecretKey())
	inherited, err := Inherited(own, backends)
	require.NoError(t, err)
	assert.Equal(t, []string{"region"}, inherited, "only what the target leaves unset comes from the backend")
}
