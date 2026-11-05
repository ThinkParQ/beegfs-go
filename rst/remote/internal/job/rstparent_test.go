package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func s3Parent(name, parent string, s3 *flex.RemoteStorageTarget_S3) *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{Name: name, Parent: parent, S3: s3}.Build()
}

func s3Child(id uint32, parent, bucket string) *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{Id: id, Name: bucket, Parent: parent, S3: flex.RemoteStorageTarget_S3_builder{Bucket: bucket}.Build()}.Build()
}

// newParentTestManager returns a manager with these parents and targets. A parent names no parent.
//
//	site (endpoint https://site, access key)
//	└── target 3 (bucket b3)
//	team (endpoint https://team, access key, region team-r)
//	├── target 1 (bucket b1)
//	└── target 2 (bucket b2)
func newParentTestManager(t *testing.T) *Manager {
	t.Helper()
	ctx := context.Background()
	m := newRSTConfigTestManager(t)
	_, err := m.CreateRSTParent(ctx, s3Parent("site", "", flex.RemoteStorageTarget_S3_builder{EndpointUrl: "https://site", AccessKey: "key-1"}.Build()))
	require.NoError(t, err)
	_, err = m.CreateRSTParent(ctx, s3Parent("team", "", flex.RemoteStorageTarget_S3_builder{EndpointUrl: "https://team", AccessKey: "key-1", Region: "team-r"}.Build()))
	require.NoError(t, err)
	for _, child := range []*flex.RemoteStorageTarget{s3Child(1, "team", "b1"), s3Child(2, "team", "b2"), s3Child(3, "site", "b3")} {
		_, _, _, _, err := m.CreateRST(ctx, child, false)
		require.NoError(t, err)
	}
	return m
}

func liveByID(t *testing.T, m *Manager) map[uint32]*flex.RemoteStorageTarget {
	t.Helper()
	live, _ := m.GetRSTConfig(false)
	byID := map[uint32]*flex.RemoteStorageTarget{}
	for _, config := range live {
		byID[config.GetId()] = config
	}
	return byID
}

func definitionsByID(t *testing.T, m *Manager) (map[uint32]*flex.RemoteStorageTarget, map[string]*flex.RemoteStorageTarget) {
	t.Helper()
	parents, definitions, err := m.GetRSTDefinitions()
	require.NoError(t, err)
	byID := map[uint32]*flex.RemoteStorageTarget{}
	for _, config := range definitions {
		byID[config.GetId()] = config
	}
	byName := map[string]*flex.RemoteStorageTarget{}
	for _, config := range parents {
		byName[config.GetName()] = config
	}
	return byID, byName
}

// addRunningJob stores an unfinished job of RST id, so changes that move the RST need force.
func addRunningJob(t *testing.T, m *Manager, id uint32) {
	t.Helper()
	_, _, commit, err := m.pathStore.CreateAndLockEntry("/test/running", kvstore.WithValue(map[string]*Job{
		"job-1": {Job: beeremote.Job_builder{
			Id:      "job-1",
			Request: beeremote.JobRequest_builder{Path: "/test/running", RemoteStorageTarget: id}.Build(),
			Status:  beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
		}.Build()},
	}))
	require.NoError(t, err)
	require.NoError(t, commit())
}

func TestCreateRSTWithParent(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)

	t.Run("the target is stored without what it inherits", func(t *testing.T) {
		definitions, _ := definitionsByID(t, m)
		assert.Empty(t, definitions[1].GetS3().GetEndpointUrl())
		assert.Equal(t, "team", definitions[1].GetParent())

		effective := liveByID(t, m)[1]
		assert.Equal(t, "https://team", effective.GetS3().GetEndpointUrl())
		assert.Equal(t, "team-r", effective.GetS3().GetRegion())
		assert.Equal(t, "b1", effective.GetS3().GetBucket())
		assert.Empty(t, effective.GetParent(), "providers and sync nodes get configurations without a parent")
	})

	t.Run("create returns the effective configuration", func(t *testing.T) {
		stored, effective, _, _, err := m.CreateRST(ctx, s3Child(4, "team", "b4"), false)
		require.NoError(t, err)
		assert.Empty(t, stored.GetS3().GetEndpointUrl())
		assert.Equal(t, "https://team", effective.GetS3().GetEndpointUrl())
	})

	t.Run("a required setting must come from the target or a parent", func(t *testing.T) {
		noBucket := flex.RemoteStorageTarget_builder{Id: 10, Name: "x", Parent: "team", S3: &flex.RemoteStorageTarget_S3{}}.Build()
		_, _, _, _, err := m.CreateRST(ctx, noBucket, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "no bucket is set")

		_, _, _, _, err = m.CreateRST(ctx, s3Child(10, "", "b"), false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "no endpoint-url is set")
	})

	t.Run("the parent must exist and have the same type", func(t *testing.T) {
		_, _, _, _, err := m.CreateRST(ctx, s3Child(10, "missing", "b"), false)
		requireCode(t, err, codes.InvalidArgument)

		xts := flex.RemoteStorageTarget_builder{Id: 10, Name: "x", Parent: "team", Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}.Build()
		_, _, _, _, err = m.CreateRST(ctx, xts, false)
		requireCode(t, err, codes.InvalidArgument)
	})
}

func TestCreateRSTParent(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)

	_, err := m.CreateRSTParent(ctx, s3Parent("site", "", &flex.RemoteStorageTarget_S3{}))
	requireCode(t, err, codes.AlreadyExists)

	_, err = m.CreateRSTParent(ctx, s3Parent("", "", &flex.RemoteStorageTarget_S3{}))
	requireCode(t, err, codes.InvalidArgument)

	withID := s3Parent("p", "", &flex.RemoteStorageTarget_S3{})
	withID.SetId(5)
	_, err = m.CreateRSTParent(ctx, withID)
	requireCode(t, err, codes.InvalidArgument)

	_, err = m.CreateRSTParent(ctx, mockTarget(0, "mock-parent"))
	requireCode(t, err, codes.InvalidArgument)

	// A parent names no parent, whether the one it names exists or not.
	for _, named := range []string{"site", "missing", "p"} {
		_, err = m.CreateRSTParent(ctx, s3Parent("p", named, &flex.RemoteStorageTarget_S3{}))
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "a parent cannot name a parent")
	}

	// A parent is partial, but targets inherit each list entry whole, so an entry must be complete.
	incomplete := flex.RemoteStorageTarget_S3_builder{StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{
		flex.RemoteStorageTarget_S3_StorageClass_builder{Name: "GLACIER", Archival: flex.RemoteStorageTarget_S3_StorageClass_Archival_builder{RetrievalTier: "Bulk"}.Build()}.Build(),
	}}.Build()
	_, err = m.CreateRSTParent(ctx, s3Parent("glacier", "", incomplete))
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must specify check-time")

	_, parents := definitionsByID(t, m)
	site := parents["site"]
	changed := proto.Clone(site).(*flex.RemoteStorageTarget)
	changed.GetS3().SetStorageClass(incomplete.GetStorageClass())
	_, _, err = m.UpdateRSTParent(ctx, "site", changed, site, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must specify check-time", "modify checks entries too")
}

func TestUpdateRSTParent(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)
	read := func(name string) *flex.RemoteStorageTarget {
		_, parents := definitionsByID(t, m)
		return parents[name]
	}

	t.Run("a change reaches every target that names the parent, and no other", func(t *testing.T) {
		team := read("team")
		changed := proto.Clone(team).(*flex.RemoteStorageTarget)
		changed.GetS3().SetAccessKey("key-2")
		_, affected, err := m.UpdateRSTParent(ctx, "team", changed, team, false)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 2}, affected)
		live := liveByID(t, m)
		assert.Equal(t, "key-2", live[1].GetS3().GetAccessKey())
		assert.Equal(t, "key-2", live[2].GetS3().GetAccessKey())
		assert.Equal(t, "key-1", live[3].GetS3().GetAccessKey(), "target 3 names another parent")
	})

	t.Run("a stale read is refused", func(t *testing.T) {
		stale := s3Parent("team", "", flex.RemoteStorageTarget_S3_builder{EndpointUrl: "https://team", AccessKey: "key-1", Region: "team-r"}.Build())
		_, _, err := m.UpdateRSTParent(ctx, "team", stale, stale, false)
		requireCode(t, err, codes.Aborted)
	})

	t.Run("a rename changes every reference and no target", func(t *testing.T) {
		team := read("team")
		renamed := proto.Clone(team).(*flex.RemoteStorageTarget)
		renamed.SetName("group")
		_, affected, err := m.UpdateRSTParent(ctx, "team", renamed, team, false)
		require.NoError(t, err)
		assert.Empty(t, affected, "effective configurations name no parent, so a rename changes none")
		definitions, parents := definitionsByID(t, m)
		assert.Equal(t, "group", definitions[1].GetParent())
		assert.Equal(t, "group", definitions[2].GetParent())
		assert.NotContains(t, parents, "team")

		_, _, err = m.UpdateRSTParent(ctx, "group", read("site"), read("group"), false)
		requireCode(t, err, codes.AlreadyExists)
	})

	t.Run("a parent cannot name a parent", func(t *testing.T) {
		site := read("site")
		nested := proto.Clone(site).(*flex.RemoteStorageTarget)
		nested.SetParent("group")
		_, _, err := m.UpdateRSTParent(ctx, "site", nested, site, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "a parent cannot name a parent")
	})

	t.Run("the type cannot change", func(t *testing.T) {
		group := read("group")
		xts := flex.RemoteStorageTarget_builder{Name: "group", Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}.Build()
		_, _, err := m.UpdateRSTParent(ctx, "group", xts, group, false)
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("moving a target with unfinished jobs needs force", func(t *testing.T) {
		addRunningJob(t, m, 1)
		group := read("group")
		moved := proto.Clone(group).(*flex.RemoteStorageTarget)
		moved.GetS3().SetEndpointUrl("https://elsewhere")
		_, _, err := m.UpdateRSTParent(ctx, "group", moved, group, false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, "1 (1 unfinished jobs)")
		assert.Equal(t, "https://team", liveByID(t, m)[1].GetS3().GetEndpointUrl(), "a refused update changes nothing")

		_, affected, err := m.UpdateRSTParent(ctx, "group", moved, group, true)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 2}, affected)
		assert.Equal(t, "https://elsewhere", liveByID(t, m)[1].GetS3().GetEndpointUrl())
	})
}

func TestUpdateRSTChangesParent(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)
	definitions, _ := definitionsByID(t, m)

	moved := proto.Clone(definitions[3]).(*flex.RemoteStorageTarget)
	moved.SetParent("team")
	effective, _, err := m.UpdateRST(ctx, moved, definitions[3], false)
	require.NoError(t, err)
	assert.Equal(t, "team-r", effective.GetS3().GetRegion(), "the target now inherits from the new parent")

	missing := proto.Clone(moved).(*flex.RemoteStorageTarget)
	missing.SetParent("missing")
	_, _, err = m.UpdateRST(ctx, missing, moved, false)
	requireCode(t, err, codes.InvalidArgument)
}

func TestDeleteRSTParent(t *testing.T) {
	ctx := context.Background()

	t.Run("a parent that targets name is refused, and the error lists them", func(t *testing.T) {
		m := newParentTestManager(t)
		err := m.DeleteRSTParent(ctx, "team")
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, `parent "team" is used by remote storage targets 1, 2`)
		assert.ErrorContains(t, err, "--parent=")
		definitions, parents := definitionsByID(t, m)
		assert.Contains(t, parents, "team", "a refused delete changes nothing")
		assert.Equal(t, "team", definitions[1].GetParent())
	})

	t.Run("a parent no target names is deleted, and no target changes", func(t *testing.T) {
		m := newParentTestManager(t)
		definitions, _ := definitionsByID(t, m)
		detached := proto.Clone(definitions[3]).(*flex.RemoteStorageTarget)
		detached.SetParent("")
		_, _, err := m.UpdateRST(ctx, detached, definitions[3], false)
		require.NoError(t, err)
		before := liveByID(t, m)

		require.NoError(t, m.DeleteRSTParent(ctx, "site"))
		_, parents := definitionsByID(t, m)
		assert.NotContains(t, parents, "site")
		after := liveByID(t, m)
		for id := range before {
			assert.True(t, proto.Equal(before[id], after[id]), "target %d", id)
		}
	})

	t.Run("an unknown parent", func(t *testing.T) {
		m := newParentTestManager(t)
		requireCode(t, m.DeleteRSTParent(ctx, "missing"), codes.NotFound)
	})
}

func TestDeleteRSTWithParentKeepsEffectiveConfig(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)
	_, _, err := m.DeleteRST(ctx, 3, "", false)
	require.NoError(t, err)

	deleted := m.workerManager.DeletedRSTs()[3].Config
	assert.Equal(t, "https://site", deleted.GetS3().GetEndpointUrl(), "the retired provider is built from the effective configuration")
	assert.Empty(t, deleted.GetParent())

	// A later change to the parent must not reach the retired provider.
	_, parents := definitionsByID(t, m)
	site := parents["site"]
	changed := proto.Clone(site).(*flex.RemoteStorageTarget)
	changed.GetS3().SetEndpointUrl("https://elsewhere")
	_, affected, err := m.UpdateRSTParent(ctx, "site", changed, site, false)
	require.NoError(t, err)
	assert.Empty(t, affected, "target 3 was the only target that named site")
	assert.Equal(t, "https://site", m.workerManager.DeletedRSTs()[3].Config.GetS3().GetEndpointUrl())
}

// TestDetachKeepsTheParentsCurrentValues checks a target that leaves its parent after the parent
// changed since the caller read it. Remote fills in the inherited values from what it holds when it
// applies the change, so the target keeps the parent's current values, not the ones read earlier.
func TestDetachKeepsTheParentsCurrentValues(t *testing.T) {
	ctx := context.Background()
	m := newParentTestManager(t)
	definitions, parents := definitionsByID(t, m)
	read := definitions[1]

	team := parents["team"]
	rotated := proto.Clone(team).(*flex.RemoteStorageTarget)
	rotated.GetS3().SetAccessKey("key-2")
	_, _, err := m.UpdateRSTParent(ctx, "team", rotated, team, false)
	require.NoError(t, err)

	detached := proto.Clone(read).(*flex.RemoteStorageTarget)
	detached.SetParent("")
	effective, _, err := m.UpdateRST(ctx, detached, read, false)
	require.NoError(t, err)
	assert.Equal(t, "key-2", effective.GetS3().GetAccessKey(), "the parent's current value, not the one read earlier")

	definitions, _ = definitionsByID(t, m)
	stored := definitions[1]
	assert.Empty(t, stored.GetParent())
	assert.Equal(t, "https://team", stored.GetS3().GetEndpointUrl(), "inherited values are now the target's own")
	assert.Equal(t, "team-r", stored.GetS3().GetRegion())
	assert.Equal(t, "key-2", stored.GetS3().GetAccessKey())
}
