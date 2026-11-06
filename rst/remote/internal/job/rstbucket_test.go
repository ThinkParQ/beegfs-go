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

// fakeBuckets is a BucketManager over buckets in memory, keyed by backend endpoint and bucket name.
// It decides which targets use a bucket the way the S3 type does, so the rules Remote applies are
// tested against the real notion of use.
type fakeBuckets struct {
	mu sync.Mutex
	// objects holds the number of objects in each bucket.
	objects map[string]int64
	// emptying, when set, makes EmptyBucket signal it started and wait to be released.
	emptying chan chan struct{}
	updates  []rst.BucketChanges
	failList error
	// failDetails makes ListBuckets with details report that it could not read some details.
	failDetails bool
	// failGet makes GetBucket fail, as a store does when a policy denies reading a bucket's tags.
	failGet error
}

func newFakeBuckets() *fakeBuckets { return &fakeBuckets{objects: map[string]int64{}} }

func bucketID(backend *flex.RemoteStorageTarget, name string) string {
	return backend.GetS3().GetEndpointUrl() + "/" + name
}

func (f *fakeBuckets) BucketSetting() string { return "bucket" }

func (f *fakeBuckets) UsesBucket(backend, target *flex.RemoteStorageTarget, name string) bool {
	real, _ := rst.BucketsOf(backend)
	return real.UsesBucket(backend, target, name)
}

func (f *fakeBuckets) ListBuckets(_ context.Context, backend *flex.RemoteStorageTarget, details bool) ([]*beeremote.RSTBucket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failList != nil {
		return nil, f.failList
	}
	prefix := backend.GetS3().GetEndpointUrl() + "/"
	buckets := []*beeremote.RSTBucket{}
	for id := range f.objects {
		if name, ok := strings.CutPrefix(id, prefix); ok {
			buckets = append(buckets, &beeremote.RSTBucket{Name: name, Details: details})
		}
	}
	slices.SortFunc(buckets, func(a, b *beeremote.RSTBucket) int { return strings.Compare(a.GetName(), b.GetName()) })
	if details && f.failDetails {
		buckets[0].Details = false
		return buckets, fmt.Errorf("%w: access denied", rst.ErrBucketDetailsUnread)
	}
	return buckets, nil
}

func (f *fakeBuckets) GetBucket(_ context.Context, backend *flex.RemoteStorageTarget, name string) (*beeremote.RSTBucket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet != nil {
		return nil, f.failGet
	}
	if _, ok := f.objects[bucketID(backend, name)]; !ok {
		return nil, rst.ErrBucketNotFound
	}
	return &beeremote.RSTBucket{Name: name, Details: true, Versioning: beeremote.RSTBucket_UNVERSIONED}, nil
}

func (f *fakeBuckets) CreateBucket(_ context.Context, backend *flex.RemoteStorageTarget, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[bucketID(backend, name)]; ok {
		return rst.ErrBucketExists
	}
	f.objects[bucketID(backend, name)] = 0
	return nil
}

func (f *fakeBuckets) UpdateBucket(_ context.Context, backend *flex.RemoteStorageTarget, name string, changes rst.BucketChanges) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[bucketID(backend, name)]; !ok {
		return rst.ErrBucketNotFound
	}
	f.updates = append(f.updates, changes)
	return nil
}

func (f *fakeBuckets) IsBucketEmpty(_ context.Context, backend *flex.RemoteStorageTarget, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.objects[bucketID(backend, name)]
	if !ok {
		return false, rst.ErrBucketNotFound
	}
	return n == 0, nil
}

func (f *fakeBuckets) EmptyBucket(_ context.Context, backend *flex.RemoteStorageTarget, name string) (int64, int64, error) {
	if f.emptying != nil {
		release := make(chan struct{})
		f.emptying <- release
		<-release
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.objects[bucketID(backend, name)]
	f.objects[bucketID(backend, name)] = 0
	return n, 1, nil
}

func (f *fakeBuckets) DeleteBucket(_ context.Context, backend *flex.RemoteStorageTarget, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.objects[bucketID(backend, name)]
	if !ok {
		return rst.ErrBucketNotFound
	}
	if n > 0 {
		return rst.ErrBucketNotEmpty
	}
	delete(f.objects, bucketID(backend, name))
	return nil
}

// newBucketTestManager returns the manager of newBackendTestManager, with fake buckets in the store
// of each backend. Targets 1 and 2 use b1 and b2 of team. Target 3 uses b3 of site.
func newBucketTestManager(t *testing.T) (*Manager, *fakeBuckets) {
	t.Helper()
	m := newBackendTestManager(t)
	buckets := newFakeBuckets()
	m.bucketsOf = func(*flex.RemoteStorageTarget) (rst.BucketManager, bool) { return buckets, true }
	buckets.objects["https://team/b1"] = 5
	buckets.objects["https://team/b2"] = 0
	buckets.objects["https://team/unused"] = 0
	buckets.objects["https://team/full"] = 7
	buckets.objects["https://site/b3"] = 1
	return m, buckets
}

func TestListRSTBuckets(t *testing.T) {
	ctx := context.Background()
	m, buckets := newBucketTestManager(t)

	backends, err := m.ListRSTBuckets(ctx, "", false)
	require.NoError(t, err)
	require.Len(t, backends, 2)
	assert.Equal(t, "site", backends[0].GetBackend())
	assert.Equal(t, "team", backends[1].GetBackend())
	byName := map[string][]uint32{}
	for _, bucket := range backends[1].GetBuckets() {
		byName[bucket.GetName()] = bucket.GetTargets()
	}
	assert.Equal(t, map[string][]uint32{"b1": {1}, "b2": {2}, "full": {}, "unused": {}}, byName)

	backends, err = m.ListRSTBuckets(ctx, "site", true)
	require.NoError(t, err)
	require.Len(t, backends, 1)
	assert.Equal(t, []uint32{3}, backends[0].GetBuckets()[0].GetTargets())

	_, err = m.ListRSTBuckets(ctx, "missing", false)
	requireCode(t, err, codes.NotFound)

	buckets.failList = errors.New("access denied")
	backends, err = m.ListRSTBuckets(ctx, "", false)
	require.NoError(t, err, "one backend that fails does not fail the list")
	assert.Equal(t, "access denied", backends[0].GetError())
}

// TestListRSTBucketsWithUnreadDetails checks that details a store did not give back do not fail the
// backend's list. The buckets are listed, and those without details show it.
func TestListRSTBucketsWithUnreadDetails(t *testing.T) {
	m, buckets := newBucketTestManager(t)
	buckets.failDetails = true
	backends, err := m.ListRSTBuckets(context.Background(), "team", true)
	require.NoError(t, err)
	require.Len(t, backends, 1)
	assert.Empty(t, backends[0].GetError())
	require.Len(t, backends[0].GetBuckets(), 4)
	assert.False(t, backends[0].GetBuckets()[0].GetDetails())
	assert.Equal(t, []uint32{1}, backends[0].GetBuckets()[0].GetTargets())
}

// TestCreateRSTBucketWhoseReadBackFails checks that a create or update that succeeded is not
// reported as failed because the bucket could not be read back. A retry of the create would only
// answer AlreadyExists.
func TestCreateRSTBucketWhoseReadBackFails(t *testing.T) {
	ctx := context.Background()
	m, buckets := newBucketTestManager(t)
	buckets.failGet = errors.New("AccessDenied: GetBucketTagging")

	bucket, err := m.CreateRSTBucket(ctx, "team", "new", rst.BucketChanges{SetTags: map[string]string{"a": "1"}})
	require.NoError(t, err)
	assert.Equal(t, "new", bucket.GetName())
	assert.False(t, bucket.GetDetails())
	assert.Contains(t, buckets.objects, "https://team/new")

	bucket, err = m.UpdateRSTBucket(ctx, "team", "b1", rst.BucketChanges{Versioning: beeremote.RSTBucket_ENABLED})
	require.NoError(t, err)
	assert.False(t, bucket.GetDetails())
	assert.Equal(t, []uint32{1}, bucket.GetTargets(), "the targets come from Remote, not the store, so they are still known")
}

func TestCreateAndUpdateRSTBucket(t *testing.T) {
	ctx := context.Background()
	m, buckets := newBucketTestManager(t)

	bucket, err := m.CreateRSTBucket(ctx, "team", "new", rst.BucketChanges{})
	require.NoError(t, err)
	assert.Equal(t, "new", bucket.GetName())
	assert.Empty(t, buckets.updates, "a create without settings changes nothing after it")

	_, err = m.CreateRSTBucket(ctx, "team", "tagged", rst.BucketChanges{SetTags: map[string]string{"a": "1"}})
	require.NoError(t, err)
	require.Len(t, buckets.updates, 1)

	_, err = m.CreateRSTBucket(ctx, "team", "new", rst.BucketChanges{})
	requireCode(t, err, codes.AlreadyExists)
	_, err = m.CreateRSTBucket(ctx, "missing", "x", rst.BucketChanges{})
	requireCode(t, err, codes.NotFound)
	_, err = m.CreateRSTBucket(ctx, "team", "x", rst.BucketChanges{Versioning: beeremote.RSTBucket_UNVERSIONED})
	requireCode(t, err, codes.InvalidArgument)

	bucket, err = m.UpdateRSTBucket(ctx, "team", "b1", rst.BucketChanges{Versioning: beeremote.RSTBucket_ENABLED})
	require.NoError(t, err)
	assert.Equal(t, []uint32{1}, bucket.GetTargets())
	_, err = m.UpdateRSTBucket(ctx, "team", "b1", rst.BucketChanges{})
	requireCode(t, err, codes.InvalidArgument)
	_, err = m.UpdateRSTBucket(ctx, "team", "b1", rst.BucketChanges{Versioning: beeremote.RSTBucket_UNVERSIONED})
	requireCode(t, err, codes.InvalidArgument)
	_, err = m.UpdateRSTBucket(ctx, "team", "missing", rst.BucketChanges{Versioning: beeremote.RSTBucket_ENABLED})
	requireCode(t, err, codes.NotFound)
}

func TestDeleteRSTBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("a bucket a live target uses is refused, even with force", func(t *testing.T) {
		m, buckets := newBucketTestManager(t)
		for _, force := range []bool{false, true} {
			_, _, err := m.DeleteRSTBucket(ctx, "team", "b2", force)
			requireCode(t, err, codes.FailedPrecondition)
			assert.ErrorContains(t, err, "used by remote storage targets 2")
		}
		assert.Contains(t, buckets.objects, "https://team/b2")
	})

	t.Run("a target of another type, with the endpoint written differently, is a user", func(t *testing.T) {
		m, buckets := newBucketTestManager(t)
		xts := &flex.RemoteStorageTarget{Id: 20, Name: "tape", Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{
			S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://TEAM:443/", Region: "team-r", Bucket: "full", AccessKey: "k", SecretKey: "s"},
		}}}
		_, _, _, _, err := m.CreateRST(ctx, xts, false)
		require.NoError(t, err)
		_, _, err = m.DeleteRSTBucket(ctx, "team", "full", true)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, "used by remote storage targets 20")
		assert.Equal(t, int64(7), buckets.objects["https://team/full"], "a refused delete removes nothing")
	})

	t.Run("a populated bucket needs force, which empties it first", func(t *testing.T) {
		m, buckets := newBucketTestManager(t)
		_, _, err := m.DeleteRSTBucket(ctx, "team", "full", false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, "use force")
		assert.Equal(t, int64(7), buckets.objects["https://team/full"], "a refused delete removes nothing")

		objects, uploads, err := m.DeleteRSTBucket(ctx, "team", "full", true)
		require.NoError(t, err)
		assert.Equal(t, int64(7), objects)
		assert.Equal(t, int64(1), uploads)
		assert.NotContains(t, buckets.objects, "https://team/full")
	})

	t.Run("an empty bucket is deleted without force", func(t *testing.T) {
		m, buckets := newBucketTestManager(t)
		objects, _, err := m.DeleteRSTBucket(ctx, "team", "unused", false)
		require.NoError(t, err)
		assert.Zero(t, objects)
		assert.NotContains(t, buckets.objects, "https://team/unused")
	})

	t.Run("a missing bucket or backend", func(t *testing.T) {
		m, _ := newBucketTestManager(t)
		_, _, err := m.DeleteRSTBucket(ctx, "team", "missing", true)
		requireCode(t, err, codes.NotFound)
		_, _, err = m.DeleteRSTBucket(ctx, "missing", "b", true)
		requireCode(t, err, codes.NotFound)
	})

	t.Run("a type that cannot manage buckets", func(t *testing.T) {
		m, _ := newBucketTestManager(t)
		m.bucketsOf = func(*flex.RemoteStorageTarget) (rst.BucketManager, bool) { return nil, false }
		_, _, err := m.DeleteRSTBucket(ctx, "team", "unused", false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.ErrorContains(t, err, "cannot manage buckets")
	})
}

// While a forced delete empties a bucket, no target may be pointed at it, and a second delete of
// the same bucket is refused. Other target changes go ahead, because emptying holds no lock.
func TestDeleteRSTBucketBlocksTargetsMeanwhile(t *testing.T) {
	ctx := context.Background()
	m, buckets := newBucketTestManager(t)
	buckets.emptying = make(chan chan struct{})

	done := make(chan error)
	go func() {
		_, _, err := m.DeleteRSTBucket(ctx, "team", "full", true)
		done <- err
	}()
	release := <-buckets.emptying

	_, _, _, _, err := m.CreateRST(ctx, s3Child(9, "team", "full"), false)
	requireCode(t, err, codes.FailedPrecondition)
	assert.ErrorContains(t, err, "which is being deleted")

	_, _, err = m.DeleteRSTBucket(ctx, "team", "full", true)
	requireCode(t, err, codes.Aborted)

	_, _, _, _, err = m.CreateRST(ctx, s3Child(10, "team", "unused"), false)
	require.NoError(t, err, "a target of another bucket is created while the delete runs")

	close(release)
	require.NoError(t, <-done)
	_, _, _, _, err = m.CreateRST(ctx, s3Child(9, "team", "full"), false)
	require.NoError(t, err, "once the delete ends, the name is free again")
}
