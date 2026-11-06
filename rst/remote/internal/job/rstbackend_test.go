package job

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func s3Backend(name, backend string, s3 *flex.RemoteStorageTarget_S3) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Name: name, Backend: backend, Type: &flex.RemoteStorageTarget_S3_{S3: s3}}
}

// xtsBackend returns a valid xtreemstore backend called name.
func xtsBackend(name string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Name: name, Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{
		S3: s3Connection("https://"+name, "k", ""),
	}}}
}

// s3Connection returns an S3 part with every setting S3 requires of a backend. An empty region
// becomes us-east-1, because a backend must set one.
func s3Connection(endpoint, accessKey, region string) *flex.RemoteStorageTarget_S3 {
	if region == "" {
		region = "us-east-1"
	}
	return &flex.RemoteStorageTarget_S3{EndpointUrl: endpoint, AccessKey: accessKey, SecretKey: "secret", Region: region}
}

func s3Child(id uint32, backend, bucket string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Id: id, Name: bucket, Backend: backend, Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: bucket}}}
}

// newBackendTestManager returns a manager with these backends and targets. A backend names no backend.
//
//	site (endpoint https://site, access key)
//	└── target 3 (bucket b3)
//	team (endpoint https://team, access key, region team-r)
//	├── target 1 (bucket b1)
//	└── target 2 (bucket b2)
func newBackendTestManager(t *testing.T) *Manager {
	t.Helper()
	ctx := context.Background()
	m := newRSTConfigTestManager(t)
	_, _, err := m.CreateRSTBackend(ctx, s3Backend("site", "", s3Connection("https://site", "key-1", "")), false)
	require.NoError(t, err)
	_, _, err = m.CreateRSTBackend(ctx, s3Backend("team", "", s3Connection("https://team", "key-1", "team-r")), false)
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
	backends, definitions, err := m.GetRSTDefinitions()
	require.NoError(t, err)
	byID := map[uint32]*flex.RemoteStorageTarget{}
	for _, config := range definitions {
		byID[config.GetId()] = config
	}
	byName := map[string]*flex.RemoteStorageTarget{}
	for _, config := range backends {
		byName[config.GetName()] = config
	}
	return byID, byName
}

// addRunningJob stores an unfinished job of RST id, so changes that move the RST need force.
func addRunningJob(t *testing.T, m *Manager, id uint32) {
	t.Helper()
	_, _, commit, err := m.pathStore.CreateAndLockEntry("/test/running", kvstore.WithValue(map[string]*Job{
		"job-1": {Job: &beeremote.Job{
			Id:      "job-1",
			Request: &beeremote.JobRequest{Path: "/test/running", RemoteStorageTarget: id},
			Status:  &beeremote.Job_Status{State: beeremote.Job_RUNNING},
		}},
	}))
	require.NoError(t, err)
	require.NoError(t, commit())
}

func TestCreateRSTWithBackend(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)

	t.Run("the target is stored without what it inherits", func(t *testing.T) {
		definitions, _ := definitionsByID(t, m)
		assert.Empty(t, definitions[1].GetS3().GetEndpointUrl())
		assert.Equal(t, "team", definitions[1].GetBackend())

		effective := liveByID(t, m)[1]
		assert.Equal(t, "https://team", effective.GetS3().GetEndpointUrl())
		assert.Equal(t, "team-r", effective.GetS3().GetRegion())
		assert.Equal(t, "b1", effective.GetS3().GetBucket())
		assert.Empty(t, effective.GetBackend(), "providers and sync nodes get configurations without a backend")
	})

	t.Run("create returns the effective configuration", func(t *testing.T) {
		stored, effective, _, _, err := m.CreateRST(ctx, s3Child(4, "team", "b4"), false)
		require.NoError(t, err)
		assert.Empty(t, stored.GetS3().GetEndpointUrl())
		assert.Equal(t, "https://team", effective.GetS3().GetEndpointUrl())
	})

	t.Run("a required setting must come from the target or a backend", func(t *testing.T) {
		noBucket := &flex.RemoteStorageTarget{Id: 10, Name: "x", Backend: "team", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}
		_, _, _, _, err := m.CreateRST(ctx, noBucket, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "no bucket is set")

		_, _, _, _, err = m.CreateRST(ctx, s3Child(10, "", "b"), false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "no endpoint-url is set")
	})

	t.Run("the backend must exist and have the same type", func(t *testing.T) {
		_, _, _, _, err := m.CreateRST(ctx, s3Child(10, "missing", "b"), false)
		requireCode(t, err, codes.InvalidArgument)

		xts := &flex.RemoteStorageTarget{Id: 10, Name: "x", Backend: "team", Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}}
		_, _, _, _, err = m.CreateRST(ctx, xts, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, `backend "team" is of type s3, but the target is of type xtreemstore`)
	})
}

func TestCreateRSTBackend(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)

	_, _, err := m.CreateRSTBackend(ctx, s3Backend("site", "", s3Connection("https://other", "k", "")), false)
	requireCode(t, err, codes.AlreadyExists)

	_, _, err = m.CreateRSTBackend(ctx, s3Backend("", "", &flex.RemoteStorageTarget_S3{}), false)
	requireCode(t, err, codes.InvalidArgument)

	withID := s3Backend("p", "", &flex.RemoteStorageTarget_S3{})
	withID.SetId(5)
	_, _, err = m.CreateRSTBackend(ctx, withID, false)
	requireCode(t, err, codes.InvalidArgument)

	_, _, err = m.CreateRSTBackend(ctx, mockTarget(0, "mock-backend"), false)
	requireCode(t, err, codes.InvalidArgument)

	// A backend names no backend, whether the one it names exists or not.
	for _, named := range []string{"site", "missing", "p"} {
		_, _, err = m.CreateRSTBackend(ctx, s3Backend("p", named, s3Connection("https://p", "k", "")), false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "a backend cannot name a backend")
	}

	// Targets inherit each list entry whole, so an entry a backend holds must be complete.
	incomplete := s3Connection("https://glacier", "k", "")
	incomplete.SetStorageClass([]*flex.RemoteStorageTarget_S3_StorageClass{
		&flex.RemoteStorageTarget_S3_StorageClass{Name: "GLACIER", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk"}},
	})
	_, _, err = m.CreateRSTBackend(ctx, s3Backend("glacier", "", incomplete), false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must specify check-time")

	_, backends := definitionsByID(t, m)
	site := backends["site"]
	changed := proto.Clone(site).(*flex.RemoteStorageTarget)
	changed.GetS3().SetStorageClass(incomplete.GetStorageClass())
	_, _, _, err = m.UpdateRSTBackend(ctx, "site", changed, site, false, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must specify check-time", "modify checks entries too")
}

// A backend must set what its type requires to reach the store, and nothing its type forbids. Both
// rules hold on create and on update, so an update cannot clear a required setting.
func TestRSTBackendRoles(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)

	noEndpoint := s3Connection("", "k", "")
	_, _, err := m.CreateRSTBackend(ctx, s3Backend("new", "", noEndpoint), false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must set endpoint-url")

	withBucket := s3Connection("https://new", "k", "")
	withBucket.SetBucket("b")
	_, _, err = m.CreateRSTBackend(ctx, s3Backend("new", "", withBucket), false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "cannot set bucket")

	_, backends := definitionsByID(t, m)
	site := backends["site"]
	cleared := proto.Clone(site).(*flex.RemoteStorageTarget)
	cleared.GetS3().SetAccessKey("")
	_, _, _, err = m.UpdateRSTBackend(ctx, "site", cleared, site, false, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "must set access-key")
	_, backends = definitionsByID(t, m)
	assert.Equal(t, "key-1", backends["site"].GetS3().GetAccessKey(), "a refused update changes nothing")
}

// A target may set any setting its backend sets, including one the backend must set.
func TestRSTOverridesWhatItsBackendMustSet(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	own := &flex.RemoteStorageTarget_S3{EndpointUrl: "https://own", AccessKey: "own-key", SecretKey: "own-secret", Bucket: "b9"}
	_, effective, _, _, err := m.CreateRST(ctx, &flex.RemoteStorageTarget{Id: 9, Backend: "team", Type: &flex.RemoteStorageTarget_S3_{S3: own}}, false)
	require.NoError(t, err)
	assert.Equal(t, "https://own", effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "own-key", effective.GetS3().GetAccessKey())
	assert.Equal(t, "team-r", effective.GetS3().GetRegion(), "what the target leaves unset still comes from the backend")
}

func TestUpdateRSTBackend(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	read := func(name string) *flex.RemoteStorageTarget {
		_, backends := definitionsByID(t, m)
		return backends[name]
	}

	t.Run("a change reaches every target that names the backend, and no other", func(t *testing.T) {
		team := read("team")
		changed := proto.Clone(team).(*flex.RemoteStorageTarget)
		changed.GetS3().SetAccessKey("key-2")
		_, affected, _, err := m.UpdateRSTBackend(ctx, "team", changed, team, false, false)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 2}, affected)
		live := liveByID(t, m)
		assert.Equal(t, "key-2", live[1].GetS3().GetAccessKey())
		assert.Equal(t, "key-2", live[2].GetS3().GetAccessKey())
		assert.Equal(t, "key-1", live[3].GetS3().GetAccessKey(), "target 3 names another backend")
	})

	t.Run("a stale read is refused", func(t *testing.T) {
		stale := s3Backend("team", "", s3Connection("https://team", "key-1", "team-r"))
		_, _, _, err := m.UpdateRSTBackend(ctx, "team", stale, stale, false, false)
		requireCode(t, err, codes.Aborted)
	})

	t.Run("a rename changes every reference and no target", func(t *testing.T) {
		team := read("team")
		renamed := proto.Clone(team).(*flex.RemoteStorageTarget)
		renamed.SetName("group")
		_, affected, _, err := m.UpdateRSTBackend(ctx, "team", renamed, team, false, false)
		require.NoError(t, err)
		assert.Empty(t, affected, "effective configurations name no backend, so a rename changes none")
		definitions, backends := definitionsByID(t, m)
		assert.Equal(t, "group", definitions[1].GetBackend())
		assert.Equal(t, "group", definitions[2].GetBackend())
		assert.NotContains(t, backends, "team")

		_, _, _, err = m.UpdateRSTBackend(ctx, "group", read("site"), read("group"), false, false)
		requireCode(t, err, codes.AlreadyExists)
	})

	t.Run("a backend cannot name a backend", func(t *testing.T) {
		site := read("site")
		nested := proto.Clone(site).(*flex.RemoteStorageTarget)
		nested.SetBackend("group")
		_, _, _, err := m.UpdateRSTBackend(ctx, "site", nested, site, false, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "a backend cannot name a backend")
	})

	t.Run("the type cannot change", func(t *testing.T) {
		group := read("group")
		xts := &flex.RemoteStorageTarget{Name: "group", Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}}
		_, _, _, err := m.UpdateRSTBackend(ctx, "group", xts, group, false, false)
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("moving a target with unfinished jobs needs force", func(t *testing.T) {
		addRunningJob(t, m, 1)
		group := read("group")
		moved := proto.Clone(group).(*flex.RemoteStorageTarget)
		moved.GetS3().SetEndpointUrl("https://elsewhere")
		_, _, _, err := m.UpdateRSTBackend(ctx, "group", moved, group, false, false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, "1 (1 unfinished jobs)")
		assert.Equal(t, "https://team", liveByID(t, m)[1].GetS3().GetEndpointUrl(), "a refused update changes nothing")

		_, affected, _, err := m.UpdateRSTBackend(ctx, "group", moved, group, true, false)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 2}, affected)
		assert.Equal(t, "https://elsewhere", liveByID(t, m)[1].GetS3().GetEndpointUrl())
	})
}

func TestUpdateRSTChangesBackend(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	definitions, _ := definitionsByID(t, m)

	moved := proto.Clone(definitions[3]).(*flex.RemoteStorageTarget)
	moved.SetBackend("team")
	effective, _, err := m.UpdateRST(ctx, moved, definitions[3], false)
	require.NoError(t, err)
	assert.Equal(t, "team-r", effective.GetS3().GetRegion(), "the target now inherits from the new backend")

	missing := proto.Clone(moved).(*flex.RemoteStorageTarget)
	missing.SetBackend("missing")
	_, _, err = m.UpdateRST(ctx, missing, moved, false)
	requireCode(t, err, codes.InvalidArgument)

	xts := xtsBackend("tape")
	_, _, err = m.CreateRSTBackend(ctx, xts, false)
	require.NoError(t, err)
	otherType := proto.Clone(moved).(*flex.RemoteStorageTarget)
	otherType.SetBackend("tape")
	_, _, err = m.UpdateRST(ctx, otherType, moved, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, `backend "tape" is of type xtreemstore, but the target is of type s3`)
}

func TestDeleteRSTBackend(t *testing.T) {
	ctx := context.Background()

	t.Run("a backend that targets name is refused, and the error lists them", func(t *testing.T) {
		m := newBackendTestManager(t)
		err := m.DeleteRSTBackend(ctx, "team")
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, `backend "team" is used by remote storage targets 1, 2`)
		assert.ErrorContains(t, err, "--backend=")
		definitions, backends := definitionsByID(t, m)
		assert.Contains(t, backends, "team", "a refused delete changes nothing")
		assert.Equal(t, "team", definitions[1].GetBackend())
	})

	t.Run("a backend no target names is deleted, and no target changes", func(t *testing.T) {
		m := newBackendTestManager(t)
		definitions, _ := definitionsByID(t, m)
		detached := proto.Clone(definitions[3]).(*flex.RemoteStorageTarget)
		detached.SetBackend("")
		_, _, err := m.UpdateRST(ctx, detached, definitions[3], false)
		require.NoError(t, err)
		before := liveByID(t, m)

		require.NoError(t, m.DeleteRSTBackend(ctx, "site"))
		_, backends := definitionsByID(t, m)
		assert.NotContains(t, backends, "site")
		after := liveByID(t, m)
		for id := range before {
			assert.True(t, proto.Equal(before[id], after[id]), "target %d", id)
		}
	})

	t.Run("an unknown backend", func(t *testing.T) {
		m := newBackendTestManager(t)
		requireCode(t, m.DeleteRSTBackend(ctx, "missing"), codes.NotFound)
	})
}

func TestDeleteRSTWithBackendKeepsEffectiveConfig(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	_, _, err := m.DeleteRST(ctx, 3, "", false)
	require.NoError(t, err)

	deleted := m.workerManager.DeletedRSTs()[3].Config
	assert.Equal(t, "https://site", deleted.GetS3().GetEndpointUrl(), "the retired provider is built from the effective configuration")
	assert.Empty(t, deleted.GetBackend())

	// A later change to the backend must not reach the retired provider.
	_, backends := definitionsByID(t, m)
	site := backends["site"]
	changed := proto.Clone(site).(*flex.RemoteStorageTarget)
	changed.GetS3().SetEndpointUrl("https://elsewhere")
	_, affected, _, err := m.UpdateRSTBackend(ctx, "site", changed, site, false, false)
	require.NoError(t, err)
	assert.Empty(t, affected, "target 3 was the only target that named site")
	assert.Equal(t, "https://site", m.workerManager.DeletedRSTs()[3].Config.GetS3().GetEndpointUrl())
}

// TestDetachKeepsTheBackendsCurrentValues checks a target that leaves its backend after the backend
// changed since the caller read it. Remote fills in the inherited values from what it holds when it
// applies the change, so the target keeps the backend's current values, not the ones read earlier.
func TestDetachKeepsTheBackendsCurrentValues(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	definitions, backends := definitionsByID(t, m)
	read := definitions[1]

	team := backends["team"]
	rotated := proto.Clone(team).(*flex.RemoteStorageTarget)
	rotated.GetS3().SetAccessKey("key-2")
	_, _, _, err := m.UpdateRSTBackend(ctx, "team", rotated, team, false, false)
	require.NoError(t, err)

	detached := proto.Clone(read).(*flex.RemoteStorageTarget)
	detached.SetBackend("")
	effective, _, err := m.UpdateRST(ctx, detached, read, false)
	require.NoError(t, err)
	assert.Equal(t, "key-2", effective.GetS3().GetAccessKey(), "the backend's current value, not the one read earlier")

	definitions, _ = definitionsByID(t, m)
	stored := definitions[1]
	assert.Empty(t, stored.GetBackend())
	assert.Equal(t, "https://team", stored.GetS3().GetEndpointUrl(), "inherited values are now the target's own")
	assert.Equal(t, "team-r", stored.GetS3().GetRegion())
	assert.Equal(t, "key-2", stored.GetS3().GetAccessKey())
}

// A modify that removes the backend and clears values of the target's own keeps only what the
// backend gives. The cleared values must not come back from the old effective configuration.
func TestDetachKeepsValuesClearedInTheSameChange(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	own := &flex.RemoteStorageTarget{Id: 4, Name: "b4", Backend: "team", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
		Bucket: "b4",
		Region: "own-r",
		StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{&flex.RemoteStorageTarget_S3_StorageClass{Name: "OWN", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{
			RetrievalTier: "Bulk", RetentionDays: 7, CheckTime: "1h", RecheckTime: "15m",
		}}},
	}}}
	_, _, _, _, err := m.CreateRST(ctx, own, false)
	require.NoError(t, err)
	definitions, _ := definitionsByID(t, m)
	read := definitions[4]

	detached := proto.Clone(read).(*flex.RemoteStorageTarget)
	detached.SetBackend("")
	detached.GetS3().SetRegion("")
	detached.GetS3().SetStorageClass(nil)
	_, _, err = m.UpdateRST(ctx, detached, read, false)
	require.NoError(t, err)

	definitions, _ = definitionsByID(t, m)
	stored := definitions[4]
	assert.Empty(t, stored.GetBackend())
	assert.Equal(t, "team-r", stored.GetS3().GetRegion(), "the cleared region falls back to the backend's")
	assert.Empty(t, stored.GetS3().GetStorageClass(), "the cleared storage class stays cleared")
	assert.Equal(t, "https://team", stored.GetS3().GetEndpointUrl(), "inherited values are now the target's own")
}

// TestVerifyRSTBackend checks which backend a verify uses, and that it holds no lock a change
// needs. Remote's own check runs against a local HTTP server that answers like an S3 store, and the
// test node passes every verify.
func TestVerifyRSTBackend(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	release := make(chan struct{})
	reached := make(chan struct{}, 1)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case reached <- struct{}{}:
		default:
		}
		<-release
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`)
	}))
	defer store.Close()

	_, backends := definitionsByID(t, m)
	site := backends["site"]
	local := proto.Clone(site).(*flex.RemoteStorageTarget)
	local.GetS3().SetEndpointUrl(store.URL)
	local.GetS3().SetRegion("us-east-1")
	_, _, _, err := m.UpdateRSTBackend(ctx, "site", local, site, false, false)
	require.NoError(t, err)

	t.Run("a stored backend is verified on Remote and every node, without holding up a change", func(t *testing.T) {
		done := make(chan []*beeremote.VerifyRSTBackendResponse_HostResult)
		go func() {
			hosts, err := m.VerifyRSTBackend(ctx, "site", 0)
			assert.NoError(t, err)
			done <- hosts
		}()
		<-reached
		_, backends := definitionsByID(t, m)
		team := backends["team"]
		changed := proto.Clone(team).(*flex.RemoteStorageTarget)
		changed.GetS3().SetAccessKey("key-3")
		_, _, _, err := m.UpdateRSTBackend(ctx, "team", changed, team, false, false)
		require.NoError(t, err, "a change goes ahead while a verify waits for the store")
		close(release)

		hosts := <-done
		require.Len(t, hosts, 2)
		assert.Equal(t, "remote", hosts[0].GetHost())
		assert.Equal(t, beeremote.VerifyRSTBackendResponse_OK, hosts[0].GetOutcome(), hosts[0].GetMessage())
		assert.Equal(t, "0", hosts[1].GetHost())
	})

	t.Run("refusals", func(t *testing.T) {
		_, err := m.VerifyRSTBackend(ctx, "missing", 0)
		requireCode(t, err, codes.NotFound)
		_, err = m.VerifyRSTBackend(ctx, "site", time.Hour)
		requireCode(t, err, codes.InvalidArgument)
	})
}

// With verify, a create or update checks the new definition from Remote first, and stores nothing
// when the store cannot be reached. Remote's check runs against a local HTTP server that answers
// like an S3 store, and against a port where nothing listens.
func TestRSTBackendChangeWithVerify(t *testing.T) {
	ctx := context.Background()
	m := newBackendTestManager(t)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`)
	}))
	defer store.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := "http://" + listener.Addr().String()
	listener.Close()

	_, _, err = m.CreateRSTBackend(ctx, s3Backend("down", "", s3Connection(unreachable, "k", "")), true)
	requireCode(t, err, codes.FailedPrecondition)
	assert.ErrorContains(t, err, "does not reach its store from Remote, so nothing was stored")
	_, backends := definitionsByID(t, m)
	assert.NotContains(t, backends, "down")

	created, note, err := m.CreateRSTBackend(ctx, s3Backend("up", "", s3Connection(store.URL, "k", "")), true)
	require.NoError(t, err)
	assert.Equal(t, "up", created.GetName())
	assert.Empty(t, note)

	_, backends = definitionsByID(t, m)
	up := backends["up"]
	moved := proto.Clone(up).(*flex.RemoteStorageTarget)
	moved.GetS3().SetEndpointUrl(unreachable)
	_, _, _, err = m.UpdateRSTBackend(ctx, "up", moved, up, false, true)
	requireCode(t, err, codes.FailedPrecondition)
	_, backends = definitionsByID(t, m)
	assert.Equal(t, store.URL, backends["up"].GetS3().GetEndpointUrl(), "a refused update changes nothing")

	_, _, _, err = m.UpdateRSTBackend(ctx, "up", moved, up, false, false)
	require.NoError(t, err, "without verify, Remote stores a backend it has not contacted")
}
