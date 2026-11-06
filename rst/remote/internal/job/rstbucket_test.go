package job

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
)

// fakeBuckets is a BucketManager over buckets in memory, keyed by endpoint and bucket name. It
// decides which targets use a bucket the way the S3 type does, so the rules Remote applies are
// tested against the real notion of use. It records every call that contacts the store.
type fakeBuckets struct {
	mu sync.Mutex
	// objects holds the number of objects in each bucket, keyed by bucketKey.
	objects map[string]int64
	// calls holds one entry per call that contacts the store, such as "create https://team/b".
	calls   []string
	updates []rst.BucketChanges
	// Each of these, when set, makes its call fail with it.
	failList, failCheck, failUpdate, failDelete error
	// beforeUpdate, when set, runs at the start of UpdateBucket.
	beforeUpdate func()
}

func newFakeBuckets() *fakeBuckets { return &fakeBuckets{objects: map[string]int64{}} }

// installFakeBuckets makes m manage the buckets of every type that can manage buckets through a new
// fakeBuckets, and returns it. Other types keep having no bucket manager.
func installFakeBuckets(m *Manager) *fakeBuckets {
	buckets := newFakeBuckets()
	m.bucketsOf = func(config *flex.RemoteStorageTarget) (rst.BucketManager, bool) {
		if _, ok := rst.BucketsOf(config); !ok {
			return nil, false
		}
		return buckets, true
	}
	return buckets
}

// fakeBucketsOf returns the fakeBuckets that newRSTConfigTestManager installed in m.
func fakeBucketsOf(t *testing.T, m *Manager) *fakeBuckets {
	t.Helper()
	b, ok := m.bucketsOf(s3Target(1, "", "b"))
	require.True(t, ok)
	return b.(*fakeBuckets)
}

// s3PartOf returns the S3 part of an s3 or xtreemstore config.
func s3PartOf(config *flex.RemoteStorageTarget) *flex.RemoteStorageTarget_S3 {
	if s3 := config.GetS3(); s3 != nil {
		return s3
	}
	return config.GetXtreemstore().GetS3()
}

func bucketKey(store *flex.RemoteStorageTarget) string {
	return s3PartOf(store).GetEndpointUrl() + "/" + s3PartOf(store).GetBucket()
}

func (f *fakeBuckets) record(call string, store *flex.RemoteStorageTarget) {
	f.calls = append(f.calls, call+" "+bucketKey(store))
}

func (f *fakeBuckets) UsesBucket(store, target *flex.RemoteStorageTarget) bool {
	real, _ := rst.BucketsOf(store)
	return real.UsesBucket(store, target)
}

func (f *fakeBuckets) ListBuckets(_ context.Context, store *flex.RemoteStorageTarget) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "list "+s3PartOf(store).GetEndpointUrl())
	if f.failList != nil {
		return nil, f.failList
	}
	prefix := s3PartOf(store).GetEndpointUrl() + "/"
	names := []string{}
	for key := range f.objects {
		if name, ok := strings.CutPrefix(key, prefix); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func (f *fakeBuckets) CheckBucket(_ context.Context, store *flex.RemoteStorageTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("check", store)
	if f.failCheck != nil {
		return f.failCheck
	}
	if _, ok := f.objects[bucketKey(store)]; !ok {
		return rst.ErrBucketNotFound
	}
	return nil
}

func (f *fakeBuckets) CreateBucket(_ context.Context, store *flex.RemoteStorageTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("create", store)
	if _, ok := f.objects[bucketKey(store)]; ok {
		return rst.ErrBucketExists
	}
	f.objects[bucketKey(store)] = 0
	return nil
}

func (f *fakeBuckets) UpdateBucket(_ context.Context, store *flex.RemoteStorageTarget, changes rst.BucketChanges) error {
	if f.beforeUpdate != nil {
		f.beforeUpdate()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("update", store)
	if f.failUpdate != nil {
		return f.failUpdate
	}
	f.updates = append(f.updates, changes)
	return nil
}

func (f *fakeBuckets) IsBucketEmpty(_ context.Context, store *flex.RemoteStorageTarget) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("is-empty", store)
	return f.objects[bucketKey(store)] == 0, nil
}

func (f *fakeBuckets) EmptyBucket(_ context.Context, store *flex.RemoteStorageTarget) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("empty", store)
	n := f.objects[bucketKey(store)]
	f.objects[bucketKey(store)] = 0
	return n, 0, nil
}

// DeleteBucket fails on a context that is done, as a real client does.
func (f *fakeBuckets) DeleteBucket(ctx context.Context, store *flex.RemoteStorageTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("delete", store)
	if f.failDelete != nil {
		return f.failDelete
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.objects[bucketKey(store)] > 0 {
		return rst.ErrBucketNotEmpty
	}
	delete(f.objects, bucketKey(store))
	return nil
}

// teamTarget returns a target of backend team, with only its bucket and name set.
func teamTarget(id uint32, name, bucket string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Id: id, Name: name, Backend: "team", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: bucket}}}
}

func TestCreateRSTCreatesTheBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("the bucket is created, and the name is generated", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		result, err := m.CreateRST(ctx, teamTarget(0, "", "new"), CreateRSTOptions{})
		require.NoError(t, err)
		assert.Equal(t, beeremote.BucketResult_CREATED, result.Bucket.GetAction())
		assert.Equal(t, "new", result.Bucket.GetName())
		assert.Contains(t, buckets.objects, "https://team/new", "the bucket is in the backend's store")
		assert.Regexp(t, `^rst-4-[0-9A-Z]{26}$`, result.Stored.GetName())
		assert.Equal(t, result.Stored.GetName(), liveByID(t, m)[4].GetName())
		assert.Empty(t, buckets.updates, "a create without bucket settings changes nothing after it")
	})

	t.Run("an explicit name wins, and bucket settings are applied", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		changes := rst.BucketChanges{Versioning: beeremote.BucketChanges_ENABLED, SetTags: map[string]string{"a": "1"}}
		result, err := m.CreateRST(ctx, teamTarget(0, "archive", "new"), CreateRSTOptions{Bucket: changes})
		require.NoError(t, err)
		assert.Equal(t, "archive", result.Stored.GetName())
		assert.Equal(t, []rst.BucketChanges{changes}, buckets.updates)
	})

	t.Run("a target without a backend gets its bucket in its own store", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		own := &flex.RemoteStorageTarget{Name: "own", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
			EndpointUrl: "https://own", Region: "r", AccessKey: "k", SecretKey: "s", Bucket: "mine",
		}}}
		_, err := m.CreateRST(ctx, own, CreateRSTOptions{})
		require.NoError(t, err)
		assert.Contains(t, buckets.objects, "https://own/mine")
	})

	t.Run("an existing bucket is refused, with a hint to adopt", func(t *testing.T) {
		m := newBackendTestManager(t)
		_, err := m.CreateRST(ctx, teamTarget(0, "", "b1"), CreateRSTOptions{})
		requireCode(t, err, codes.AlreadyExists)
		assert.ErrorContains(t, err, "hint: use adopt")
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m), "a refused create stores nothing")
	})
}

func TestCreateRSTAdoptsTheBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("an existing bucket is adopted, and the store is not changed", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		buckets.objects["https://team/spare"] = 3
		buckets.calls = nil
		result, err := m.CreateRST(ctx, teamTarget(0, "", "spare"), CreateRSTOptions{Adopt: true})
		require.NoError(t, err)
		assert.Equal(t, beeremote.BucketResult_ADOPTED, result.Bucket.GetAction())
		assert.Equal(t, []string{"check https://team/spare"}, buckets.calls)
		assert.Equal(t, int64(3), buckets.objects["https://team/spare"])
	})

	t.Run("a missing bucket is refused, with a hint to drop adopt", func(t *testing.T) {
		m := newBackendTestManager(t)
		_, err := m.CreateRST(ctx, teamTarget(0, "", "missing"), CreateRSTOptions{Adopt: true})
		requireCode(t, err, codes.NotFound)
		assert.ErrorContains(t, err, "without adopt")
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
	})

	t.Run("a bucket the store will not let us use is refused with the store's message", func(t *testing.T) {
		m := newBackendTestManager(t)
		fakeBucketsOf(t, m).failCheck = errors.New("Forbidden")
		_, err := m.CreateRST(ctx, teamTarget(0, "", "b1"), CreateRSTOptions{Adopt: true})
		requireCode(t, err, codes.Unavailable)
		assert.ErrorContains(t, err, "Forbidden")
		assert.NotContains(t, err.Error(), "hint")
	})

	t.Run("requests that do not fit are refused before the store is contacted", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		buckets.calls = nil
		for _, c := range []struct {
			config *flex.RemoteStorageTarget
			opts   CreateRSTOptions
			want   string
		}{
			{teamTarget(0, "", "b"), CreateRSTOptions{Adopt: true, Bucket: rst.BucketChanges{SetTags: map[string]string{"a": "1"}}}, "takes no bucket settings"},
			{teamTarget(0, "", "b"), CreateRSTOptions{Bucket: rst.BucketChanges{RemoveTags: []string{"a"}}}, "cannot remove any"},
			{teamTarget(0, "", "b"), CreateRSTOptions{Bucket: rst.BucketChanges{ClearTags: true}}, "cannot remove any"},
			{teamTarget(0, "", ""), CreateRSTOptions{Adopt: true}, "AdoptRSTBuckets"},
			{teamTarget(0, "", ""), CreateRSTOptions{}, "it must set bucket"},
			{mockTarget(0, "mock"), CreateRSTOptions{Adopt: true}, "adopt and bucket settings do not apply"},
		} {
			_, err := m.CreateRST(ctx, c.config, c.opts)
			requireCode(t, err, codes.InvalidArgument)
			assert.ErrorContains(t, err, c.want)
		}
		assert.Empty(t, buckets.calls)
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
	})

	t.Run("a type that cannot manage buckets never contacts the store", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		buckets.calls = nil
		result, err := m.CreateRST(ctx, mockTarget(0, "mock"), CreateRSTOptions{})
		require.NoError(t, err)
		assert.Equal(t, beeremote.BucketResult_NONE, result.Bucket.GetAction())
		assert.Empty(t, buckets.calls)
	})
}

func TestCreateRSTUndoesTheBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("a bucket whose settings fail is deleted again", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		buckets.failUpdate = errors.New("AccessDenied")
		_, err := m.CreateRST(ctx, teamTarget(0, "", "new"), CreateRSTOptions{Bucket: rst.BucketChanges{SetTags: map[string]string{"a": "1"}}})
		requireCode(t, err, codes.Unavailable)
		assert.NotContains(t, buckets.objects, "https://team/new")
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
	})

	t.Run("an undo that fails names the bucket left behind", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		buckets.failUpdate = errors.New("AccessDenied")
		buckets.failDelete = errors.New("SlowDown")
		_, err := m.CreateRST(ctx, teamTarget(0, "", "new"), CreateRSTOptions{Bucket: rst.BucketChanges{SetTags: map[string]string{"a": "1"}}})
		requireCode(t, err, codes.Unavailable)
		assert.ErrorContains(t, err, `bucket "new" was created for the target and could not be deleted again, so it is left in the store: SlowDown`)
	})

	t.Run("a request cancelled during the bucket step still undoes the bucket", func(t *testing.T) {
		m := newBackendTestManager(t)
		buckets := fakeBucketsOf(t, m)
		cancelled, cancel := context.WithCancel(ctx)
		buckets.beforeUpdate = cancel
		buckets.failUpdate = context.Canceled
		_, err := m.CreateRST(cancelled, teamTarget(0, "", "new"), CreateRSTOptions{Bucket: rst.BucketChanges{SetTags: map[string]string{"a": "1"}}})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "left in the store")
		assert.NotContains(t, buckets.objects, "https://team/new")
	})
}

// newAdoptTestManager returns the manager of newBackendTestManager, with these buckets in the store
// of team: b1 and b2 (live targets 1 and 2), old (deleted target 4), and x, y and z (unused).
func newAdoptTestManager(t *testing.T) (*Manager, *fakeBuckets) {
	t.Helper()
	ctx := context.Background()
	m := newBackendTestManager(t)
	buckets := fakeBucketsOf(t, m)
	_, err := m.CreateRST(ctx, teamTarget(4, "old", "old"), CreateRSTOptions{})
	require.NoError(t, err)
	_, _, err = m.DeleteRST(ctx, 4, "", false)
	require.NoError(t, err)
	for _, name := range []string{"x", "y", "z"} {
		buckets.objects["https://team/"+name] = 1
	}
	buckets.calls = nil
	return m, buckets
}

func adoptTemplate() *flex.RemoteStorageTarget {
	return teamTarget(0, "", "")
}

func TestAdoptRSTBuckets(t *testing.T) {
	ctx := context.Background()

	t.Run("a dry run shows what would be adopted and stores nothing", func(t *testing.T) {
		m, buckets := newAdoptTestManager(t)
		response, err := m.AdoptRSTBuckets(ctx, adoptTemplate(), true)
		require.NoError(t, err)
		adopted := map[uint32]string{}
		for _, a := range response.GetAdopted() {
			adopted[a.GetRst().GetId()] = a.GetRst().GetS3().GetBucket()
			assert.Equal(t, "https://team", a.GetEffective().GetS3().GetEndpointUrl())
		}
		assert.Equal(t, map[uint32]string{5: "x", 6: "y", 7: "z"}, adopted, "IDs follow the highest ever used, deleted target 4 included")
		skipped := map[string][2][]uint32{}
		for _, s := range response.GetSkipped() {
			skipped[s.GetBucket()] = [2][]uint32{s.GetLiveTargets(), s.GetDeletedTargets()}
		}
		assert.Equal(t, map[string][2][]uint32{"b1": {{1}, nil}, "b2": {{2}, nil}, "old": {nil, {4}}}, skipped)
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
		assert.Nil(t, response.GetApply())
		assert.Equal(t, []string{"list https://team"}, buckets.calls, "nothing in the store changes")
	})

	t.Run("every unused bucket becomes a target", func(t *testing.T) {
		m, buckets := newAdoptTestManager(t)
		response, err := m.AdoptRSTBuckets(ctx, adoptTemplate(), false)
		require.NoError(t, err)
		require.Len(t, response.GetAdopted(), 3)
		assert.NotNil(t, response.GetApply())
		assert.Equal(t, []uint32{1, 2, 3, 5, 6, 7}, liveIDs(m))
		definitions, _ := definitionsByID(t, m)
		for id, bucket := range map[uint32]string{5: "x", 6: "y", 7: "z"} {
			assert.Equal(t, "team", definitions[id].GetBackend())
			assert.Equal(t, bucket, definitions[id].GetS3().GetBucket())
			assert.Empty(t, definitions[id].GetS3().GetEndpointUrl(), "an adopted target inherits from its backend like any other")
			assert.Regexp(t, fmt.Sprintf(`^rst-%d-[0-9A-Z]{26}$`, id), definitions[id].GetName())
		}
		assert.Equal(t, []string{"list https://team"}, buckets.calls, "nothing in the store changes")

		_, err = m.AdoptRSTBuckets(ctx, adoptTemplate(), false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, `"b1" is used by target 1`)
		assert.ErrorContains(t, err, `"old" is used by deleted target 4`)

		response, err = m.AdoptRSTBuckets(ctx, adoptTemplate(), true)
		require.NoError(t, err, "a dry run with nothing to adopt is not an error")
		assert.Empty(t, response.GetAdopted())
	})

	t.Run("the template's other settings apply to every target", func(t *testing.T) {
		m, _ := newAdoptTestManager(t)
		template := adoptTemplate()
		template.SetPolicies(&flex.RemoteStorageTarget_Policies{FastStartMaxSize: 4096})
		_, err := m.AdoptRSTBuckets(ctx, template, false)
		require.NoError(t, err)
		live := liveByID(t, m)
		for _, id := range []uint32{5, 6, 7} {
			assert.Equal(t, int64(4096), live[id].GetPolicies().GetFastStartMaxSize())
		}
	})

	t.Run("a template that sets a connection value is refused", func(t *testing.T) {
		m, buckets := newAdoptTestManager(t)
		template := adoptTemplate()
		template.GetS3().SetRegion("own-r")
		_, err := m.AdoptRSTBuckets(ctx, template, true)
		requireCode(t, err, codes.InvalidArgument)
		assert.ErrorContains(t, err, "it cannot set region, because its backend provides them")
		assert.Empty(t, buckets.calls)
	})

	t.Run("a template that names one target is refused", func(t *testing.T) {
		m, buckets := newAdoptTestManager(t)
		for _, c := range []struct {
			template *flex.RemoteStorageTarget
			want     string
		}{
			{s3Target(0, "", ""), "needs a backend"},
			{teamTarget(9, "", ""), "cannot name one"},
			{teamTarget(0, "n", ""), "cannot name one"},
			{teamTarget(0, "", "x"), "create a target with adopt"},
			{&flex.RemoteStorageTarget{Backend: "team", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}, "no buckets Remote can adopt"},
		} {
			_, err := m.AdoptRSTBuckets(ctx, c.template, false)
			requireCode(t, err, codes.InvalidArgument)
			assert.ErrorContains(t, err, c.want)
		}
		_, err := m.AdoptRSTBuckets(ctx, &flex.RemoteStorageTarget{Backend: "missing", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}, false)
		requireCode(t, err, codes.InvalidArgument)
		assert.Empty(t, buckets.calls)
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
	})

	t.Run("a store that cannot be listed fails the request", func(t *testing.T) {
		m, buckets := newAdoptTestManager(t)
		buckets.failList = errors.New("AccessDenied")
		_, err := m.AdoptRSTBuckets(ctx, adoptTemplate(), false)
		requireCode(t, err, codes.Unavailable)
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))
	})
}
