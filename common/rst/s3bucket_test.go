package rst

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// fakeS3Store is an S3 store in memory, with just enough behavior for bucket management. Its pages
// are small, so every loop in s3bucket.go runs more than once.
type fakeS3Store struct {
	buckets map[string]*fakeBucket
	// noVersions makes ListObjectVersions answer NotImplemented, as some stores do.
	noVersions bool
	// refuse holds keys DeleteObjects reports as failed, as a store does for a locked object.
	refuse map[string]bool
	// created records the LocationConstraint of each CreateBucket, "" for none.
	created map[string]string
	// legacyCreate makes CreateBucket of an existing bucket succeed, as AWS does in us-east-1.
	legacyCreate bool
	// taggingErr makes GetBucketTagging of a bucket answer with this code, such as AccessDenied
	// from a policy, or NotImplemented from a store without bucket tagging.
	taggingErr map[string]string
}

type fakeBucket struct {
	region     string
	created    time.Time
	versioning types.BucketVersioningStatus
	tags       map[string]string
	objects    []types.ObjectIdentifier
	markers    []types.ObjectIdentifier
	uploads    []types.MultipartUpload
}

const fakePage = 400

func newFakeS3Store() *fakeS3Store {
	return &fakeS3Store{buckets: map[string]*fakeBucket{}, refuse: map[string]bool{}, created: map[string]string{}, taggingErr: map[string]string{}}
}

func apiError(code string) error { return &smithy.GenericAPIError{Code: code, Message: code} }

func (f *fakeS3Store) bucket(name *string) (*fakeBucket, error) {
	b, ok := f.buckets[aws.ToString(name)]
	if !ok {
		return nil, apiError("NoSuchBucket")
	}
	return b, nil
}

// page returns the slice of n items that starts at marker, the next marker, and whether more remain.
func page(marker *string, n int, max int32) (int, int, bool) {
	start, _ := strconv.Atoi(aws.ToString(marker))
	end := min(start+int(max), n)
	return start, end, end < n
}

func (f *fakeS3Store) ListBuckets(_ context.Context, in *s3.ListBucketsInput, _ ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	out := &s3.ListBucketsOutput{}
	names := []string{}
	for name := range f.buckets {
		names = append(names, name)
	}
	slices.Sort(names)
	slices.Reverse(names) // The store's order must not matter.
	start, end, more := page(in.ContinuationToken, len(names), 2)
	for _, name := range names[start:end] {
		b := f.buckets[name]
		out.Buckets = append(out.Buckets, types.Bucket{Name: aws.String(name), CreationDate: aws.Time(b.created), BucketRegion: aws.String(b.region)})
	}
	if more {
		out.ContinuationToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func (f *fakeS3Store) HeadBucket(_ context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if _, ok := f.buckets[aws.ToString(in.Bucket)]; !ok {
		return nil, apiError("NotFound")
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3Store) CreateBucket(_ context.Context, in *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	name := aws.ToString(in.Bucket)
	if _, ok := f.buckets[name]; ok {
		if f.legacyCreate {
			return &s3.CreateBucketOutput{}, nil
		}
		return nil, apiError("BucketAlreadyOwnedByYou")
	}
	region := ""
	if in.CreateBucketConfiguration != nil {
		region = string(in.CreateBucketConfiguration.LocationConstraint)
	}
	f.created[name] = region
	f.buckets[name] = &fakeBucket{region: region, tags: map[string]string{}}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeS3Store) DeleteBucket(_ context.Context, in *s3.DeleteBucketInput, _ ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	if len(b.objects) > 0 || len(b.markers) > 0 {
		return nil, apiError("BucketNotEmpty")
	}
	delete(f.buckets, aws.ToString(in.Bucket))
	return &s3.DeleteBucketOutput{}, nil
}

func (f *fakeS3Store) GetBucketVersioning(_ context.Context, in *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	return &s3.GetBucketVersioningOutput{Status: b.versioning}, nil
}

func (f *fakeS3Store) PutBucketVersioning(_ context.Context, in *s3.PutBucketVersioningInput, _ ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	b.versioning = in.VersioningConfiguration.Status
	return &s3.PutBucketVersioningOutput{}, nil
}

func (f *fakeS3Store) GetBucketTagging(_ context.Context, in *s3.GetBucketTaggingInput, _ ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	if code := f.taggingErr[aws.ToString(in.Bucket)]; code != "" {
		return nil, apiError(code)
	}
	if len(b.tags) == 0 {
		return nil, apiError("NoSuchTagSet")
	}
	out := &s3.GetBucketTaggingOutput{}
	for k, v := range b.tags {
		out.TagSet = append(out.TagSet, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return out, nil
}

func (f *fakeS3Store) PutBucketTagging(_ context.Context, in *s3.PutBucketTaggingInput, _ ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	if len(in.Tagging.TagSet) == 0 {
		return nil, apiError("MalformedXML")
	}
	b.tags = map[string]string{}
	for _, tag := range in.Tagging.TagSet {
		b.tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return &s3.PutBucketTaggingOutput{}, nil
}

func (f *fakeS3Store) DeleteBucketTagging(_ context.Context, in *s3.DeleteBucketTaggingInput, _ ...func(*s3.Options)) (*s3.DeleteBucketTaggingOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	b.tags = map[string]string{}
	return &s3.DeleteBucketTaggingOutput{}, nil
}

func (f *fakeS3Store) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	if f.noVersions {
		return nil, apiError("NotImplemented")
	}
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	all := slices.Concat(b.objects, b.markers)
	start, end, more := page(in.KeyMarker, len(all), min(aws.ToInt32(in.MaxKeys), fakePage))
	out := &s3.ListObjectVersionsOutput{IsTruncated: aws.Bool(more)}
	for i, id := range all[start:end] {
		if start+i < len(b.objects) {
			out.Versions = append(out.Versions, types.ObjectVersion{Key: id.Key, VersionId: id.VersionId})
		} else {
			out.DeleteMarkers = append(out.DeleteMarkers, types.DeleteMarkerEntry{Key: id.Key, VersionId: id.VersionId})
		}
	}
	if more {
		// A real store continues after the last key it returned. The fake continues at an index,
		// which works while DeleteObjects runs between pages only because refused objects stay.
		out.NextKeyMarker = aws.String(strconv.Itoa(f.remainingBefore(b, end)))
	}
	return out, nil
}

// remainingBefore returns how many of the first end listed entries are still in b. Deleting a
// page shifts the rest down, so the next page starts there.
func (f *fakeS3Store) remainingBefore(b *fakeBucket, end int) int {
	kept := 0
	for _, id := range slices.Concat(b.objects, b.markers)[:min(end, len(b.objects)+len(b.markers))] {
		if f.refuse[aws.ToString(id.Key)] {
			kept++
		}
	}
	return kept
}

func (f *fakeS3Store) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	start, end, more := page(in.ContinuationToken, len(b.objects), min(aws.ToInt32(in.MaxKeys), fakePage))
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(more)}
	for _, id := range b.objects[start:end] {
		out.Contents = append(out.Contents, types.Object{Key: id.Key})
	}
	if more {
		out.NextContinuationToken = aws.String(strconv.Itoa(f.remainingBefore(b, end)))
	}
	return out, nil
}

func (f *fakeS3Store) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	if len(in.Delete.Objects) > s3DeleteBatch {
		return nil, apiError("MalformedXML")
	}
	out := &s3.DeleteObjectsOutput{}
	for _, id := range in.Delete.Objects {
		if f.refuse[aws.ToString(id.Key)] {
			out.Errors = append(out.Errors, types.Error{Key: id.Key, Code: aws.String("AccessDenied"), Message: aws.String("locked")})
			continue
		}
		// A delete without a version ID deletes the object by key, as on an unversioned store.
		match := func(o types.ObjectIdentifier) bool {
			return aws.ToString(o.Key) == aws.ToString(id.Key) && (id.VersionId == nil || aws.ToString(o.VersionId) == aws.ToString(id.VersionId))
		}
		b.objects = slices.DeleteFunc(b.objects, match)
		b.markers = slices.DeleteFunc(b.markers, match)
	}
	return out, nil
}

func (f *fakeS3Store) ListMultipartUploads(_ context.Context, in *s3.ListMultipartUploadsInput, _ ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	b, err := f.bucket(in.Bucket)
	if err != nil {
		return nil, err
	}
	max := int32(2)
	if in.MaxUploads != nil {
		max = min(*in.MaxUploads, max)
	}
	start, end, more := page(in.KeyMarker, len(b.uploads), max)
	return &s3.ListMultipartUploadsOutput{Uploads: slices.Clone(b.uploads[start:end]), IsTruncated: aws.Bool(more), NextKeyMarker: aws.String(strconv.Itoa(end))}, nil
}

func (f *fakeS3Store) AbortMultipartUpload(_ context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	// Uploads are removed at the end of the listing (see emptyAndCheck), so paging stays stable.
	return &s3.AbortMultipartUploadOutput{}, nil
}

// fakeBuckets returns an S3 bucket manager on store, and a backend in region.
func fakeBuckets(store *fakeS3Store, region string) (s3Buckets, *flex.RemoteStorageTarget) {
	b := s3Buckets{part: plainS3Part, api: func(context.Context, *flex.RemoteStorageTarget_S3) (s3BucketAPI, error) { return store, nil }}
	backend := &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
		EndpointUrl: "https://s3.example.com", Region: region, AccessKey: "k", SecretKey: "s",
	}}}
	return b, backend
}

func TestS3BucketsCreate(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, euBackend := fakeBuckets(store, "eu-1")
	_, usBackend := fakeBuckets(store, "us-east-1")

	require.NoError(t, b.CreateBucket(ctx, euBackend, "eu"))
	require.NoError(t, b.CreateBucket(ctx, usBackend, "us"))
	assert.Equal(t, map[string]string{"eu": "eu-1", "us": ""}, store.created, "us-east-1 is the default, and is never sent as a location")
	require.ErrorIs(t, b.CreateBucket(ctx, euBackend, "eu"), ErrBucketExists)
	store.legacyCreate = true
	require.ErrorIs(t, b.CreateBucket(ctx, usBackend, "us"), ErrBucketExists,
		"a store that answers 200 for an existing bucket is still refused")

	bucket, err := b.GetBucket(ctx, euBackend, "eu")
	require.NoError(t, err)
	assert.Equal(t, beeremote.RSTBucket_UNVERSIONED, bucket.GetVersioning())
	assert.True(t, bucket.GetDetails())
	assert.Empty(t, bucket.GetTags())
	_, err = b.GetBucket(ctx, euBackend, "missing")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestS3BucketsList(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, backend := fakeBuckets(store, "eu-1")
	for i, name := range []string{"c", "a", "b", "elsewhere", "unknown"} {
		region := "eu-1"
		switch name {
		case "elsewhere":
			region = "us-west-2"
		case "unknown":
			region = ""
		}
		store.buckets[name] = &fakeBucket{region: region, created: time.Unix(int64(i), 0), tags: map[string]string{}}
	}
	store.buckets["a"].versioning = types.BucketVersioningStatusSuspended
	store.buckets["a"].tags = map[string]string{"owner": "hpc"}

	buckets, err := b.ListBuckets(ctx, backend, false)
	require.NoError(t, err)
	names := []string{}
	for _, bucket := range buckets {
		names = append(names, bucket.GetName())
		assert.False(t, bucket.GetDetails())
	}
	assert.Equal(t, []string{"a", "b", "c", "unknown"}, names, "a bucket of another region is left out, and pages are joined")
	assert.True(t, time.Unix(1, 0).Equal(buckets[0].GetCreated().AsTime()))

	buckets, err = b.ListBuckets(ctx, backend, true)
	require.NoError(t, err)
	assert.Equal(t, beeremote.RSTBucket_SUSPENDED, buckets[0].GetVersioning())
	assert.Equal(t, map[string]string{"owner": "hpc"}, buckets[0].GetTags())
	assert.Equal(t, beeremote.RSTBucket_UNVERSIONED, buckets[1].GetVersioning())
}

// TestS3BucketsDetailsThatCannotBeRead checks that a bucket whose details cannot be read does not
// fail the read of the others, and that a store without bucket tagging has buckets without tags.
func TestS3BucketsDetailsThatCannotBeRead(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, backend := fakeBuckets(store, "eu-1")
	for _, name := range []string{"a", "denied", "untaggable"} {
		require.NoError(t, b.CreateBucket(ctx, backend, name))
	}
	store.taggingErr["denied"] = "AccessDenied"
	store.taggingErr["untaggable"] = "NotImplemented"

	bucket, err := b.GetBucket(ctx, backend, "untaggable")
	require.NoError(t, err)
	assert.True(t, bucket.GetDetails())
	assert.Empty(t, bucket.GetTags(), "a store without bucket tagging holds no tags")

	_, err = b.GetBucket(ctx, backend, "denied")
	require.Error(t, err)

	buckets, err := b.ListBuckets(ctx, backend, true)
	require.ErrorIs(t, err, ErrBucketDetailsUnread)
	require.ErrorContains(t, err, `1 of 3 buckets, such as "denied"`)
	details := map[string]bool{}
	for _, bucket := range buckets {
		details[bucket.GetName()] = bucket.GetDetails()
	}
	assert.Equal(t, map[string]bool{"a": true, "denied": false, "untaggable": true}, details, "the list is complete")
	assert.Equal(t, beeremote.RSTBucket_UNKNOWN, buckets[1].GetVersioning(), "a bucket whose details failed has none of them set")
}

func TestS3BucketsUpdate(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, backend := fakeBuckets(store, "eu-1")
	require.NoError(t, b.CreateBucket(ctx, backend, "b"))

	require.NoError(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{Versioning: beeremote.RSTBucket_ENABLED, SetTags: map[string]string{"a": "1", "b": "2"}}))
	assert.Equal(t, types.BucketVersioningStatusEnabled, store.buckets["b"].versioning)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, store.buckets["b"].tags)

	require.NoError(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{RemoveTags: []string{"a", "missing"}, SetTags: map[string]string{"c": "3"}}))
	assert.Equal(t, map[string]string{"b": "2", "c": "3"}, store.buckets["b"].tags, "a tag change keeps the tags it does not name")

	require.NoError(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{ClearTags: true, SetTags: map[string]string{"d": "4"}}))
	assert.Equal(t, map[string]string{"d": "4"}, store.buckets["b"].tags)

	require.NoError(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{RemoveTags: []string{"d"}}), "an empty tag set is removed, not written")
	assert.Empty(t, store.buckets["b"].tags)

	require.NoError(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{Versioning: beeremote.RSTBucket_SUSPENDED}))
	assert.Equal(t, types.BucketVersioningStatusSuspended, store.buckets["b"].versioning)

	require.Error(t, b.UpdateBucket(ctx, backend, "b", BucketChanges{Versioning: beeremote.RSTBucket_UNVERSIONED}))
	require.ErrorIs(t, b.UpdateBucket(ctx, backend, "missing", BucketChanges{Versioning: beeremote.RSTBucket_ENABLED}), ErrBucketNotFound)
}

// fill gives bucket n object versions, a delete marker per tenth object, and uploads uploads.
func fill(bucket *fakeBucket, n int, uploads int) {
	for i := range n {
		key := fmt.Sprintf("k%05d", i)
		bucket.objects = append(bucket.objects, types.ObjectIdentifier{Key: aws.String(key), VersionId: aws.String("v1")})
		if i%10 == 0 {
			bucket.markers = append(bucket.markers, types.ObjectIdentifier{Key: aws.String(key), VersionId: aws.String("m1")})
		}
	}
	for i := range uploads {
		bucket.uploads = append(bucket.uploads, types.MultipartUpload{Key: aws.String(fmt.Sprintf("u%d", i)), UploadId: aws.String(strconv.Itoa(i))})
	}
}

func TestS3BucketsEmptyAndDelete(t *testing.T) {
	for _, noVersions := range []bool{false, true} {
		t.Run(fmt.Sprintf("noVersions=%t", noVersions), func(t *testing.T) {
			ctx := context.Background()
			store := newFakeS3Store()
			store.noVersions = noVersions
			b, backend := fakeBuckets(store, "eu-1")
			require.NoError(t, b.CreateBucket(ctx, backend, "b"))
			bucket := store.buckets["b"]
			fill(bucket, 2500, 3)
			if noVersions {
				bucket.markers = nil
			}

			empty, err := b.IsBucketEmpty(ctx, backend, "b")
			require.NoError(t, err)
			assert.False(t, empty)
			require.ErrorIs(t, b.DeleteBucket(ctx, backend, "b"), ErrBucketNotEmpty)

			objects, uploads, err := b.EmptyBucket(ctx, backend, "b")
			require.NoError(t, err)
			wantObjects := int64(2500 + 250)
			if noVersions {
				wantObjects = 2500
			}
			assert.Equal(t, wantObjects, objects)
			assert.Equal(t, int64(3), uploads)
			assert.Empty(t, bucket.objects)
			assert.Empty(t, bucket.markers)

			bucket.uploads = nil // The fake does not remove aborted uploads itself.
			empty, err = b.IsBucketEmpty(ctx, backend, "b")
			require.NoError(t, err)
			assert.True(t, empty)
			require.NoError(t, b.DeleteBucket(ctx, backend, "b"))
			_, err = b.GetBucket(ctx, backend, "b")
			require.ErrorIs(t, err, ErrBucketNotFound)
		})
	}
}

func TestS3BucketsUnfinishedUploadsPopulateABucket(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, backend := fakeBuckets(store, "eu-1")
	require.NoError(t, b.CreateBucket(ctx, backend, "b"))
	fill(store.buckets["b"], 0, 1)
	empty, err := b.IsBucketEmpty(ctx, backend, "b")
	require.NoError(t, err)
	assert.False(t, empty, "an unfinished upload holds data the store charges for, so the bucket is not empty")
}

func TestS3BucketsEmptyReportsRefusedObjects(t *testing.T) {
	ctx := context.Background()
	store := newFakeS3Store()
	b, backend := fakeBuckets(store, "eu-1")
	require.NoError(t, b.CreateBucket(ctx, backend, "b"))
	fill(store.buckets["b"], 10, 0)
	store.refuse["k00003"] = true

	objects, _, err := b.EmptyBucket(ctx, backend, "b")
	require.ErrorContains(t, err, `unable to delete 1 objects of bucket "b", such as "k00003": AccessDenied`)
	assert.Equal(t, int64(10), objects, "every other object and marker of the page was deleted")
}

func TestS3BucketsUsesBucket(t *testing.T) {
	b, backend := fakeBuckets(newFakeS3Store(), "eu-1")
	target := func(endpoint, bucket string) *flex.RemoteStorageTarget {
		return &flex.RemoteStorageTarget{Id: 1, Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: endpoint, Bucket: bucket}}}
	}
	assert.True(t, b.UsesBucket(backend, target("https://s3.example.com", "b"), "b"))
	assert.False(t, b.UsesBucket(backend, target("https://s3.example.com", "c"), "b"))
	assert.False(t, b.UsesBucket(backend, target("https://other.example.com", "b"), "b"), "a target that overrides the endpoint uses another store")

	// Endpoints that are written differently but reach the backend's store.
	for _, endpoint := range []string{
		"https://s3.example.com/",
		"https://S3.Example.com",
		"https://s3.example.com:443",
		"https://b.s3.example.com",
		"https://B.S3.example.com",
	} {
		assert.True(t, b.UsesBucket(backend, target(endpoint, "b"), "b"), endpoint)
	}
	// Endpoints that reach another store.
	for _, endpoint := range []string{
		"http://s3.example.com",
		"https://s3.example.com:9000",
		"https://s3.example.com/prefix",
		"https://c.s3.example.com",
	} {
		assert.False(t, b.UsesBucket(backend, target(endpoint, "b"), "b"), endpoint)
	}
	assert.True(t, b.UsesBucket(backend, target("://bad", "b"), "b"), "a target whose store cannot be known is taken to use the bucket")
	assert.False(t, b.UsesBucket(backend, &flex.RemoteStorageTarget{Id: 2, Type: &flex.RemoteStorageTarget_Mock{Mock: "b"}}, "b"),
		"a type without an S3 part keeps no objects in an S3 bucket")

	xts, ok := TargetTypeByName("xtreemstore")
	require.True(t, ok)
	xtsBackend := &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://xts"}}}}
	xtsTarget := &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://xts", Bucket: "tape"}}}}
	assert.True(t, xts.Buckets().UsesBucket(xtsBackend, xtsTarget, "tape"), "xtreemstore manages buckets through its own S3 part")
	s3TapeTarget := target("https://xts", "tape")
	assert.True(t, xts.Buckets().UsesBucket(xtsBackend, s3TapeTarget, "tape"), "an S3 target can use the store of an xtreemstore backend")
	xtsTarget.GetXtreemstore().GetS3().SetEndpointUrl("https://s3.example.com")
	xtsTarget.GetXtreemstore().GetS3().SetBucket("b")
	assert.True(t, b.UsesBucket(backend, xtsTarget, "b"), "an xtreemstore target can use the store of an S3 backend")
	assert.Equal(t, "bucket", xts.Buckets().BucketSetting())
}
