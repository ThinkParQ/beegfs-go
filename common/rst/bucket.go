package rst

import (
	"context"
	"errors"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file defines how an RST type manages the bucket a target keeps its objects in. A type that
// can manage buckets returns a BucketManager from TargetType.Buckets. Remote runs every bucket
// request through it, so the CLI host never needs a route to the store or its credentials.
//
// "Bucket" is the S3 name for what other stores call a container, a share, an export or a volume.
//
// Terms used below:
//   - store: the effective configuration of a target, its own settings merged with its backend's
//     (see backend.go). It holds what reaches the store, such as the endpoint, region and keys. It
//     also names the target's bucket, in the setting BucketSettingOf returns. A method that works
//     on one bucket works on that one.
//   - A bucket's name is its real name in the store. It is unrelated to the target's name.
//
// Today Remote uses CheckBucket and CreateBucket when it creates a target, UpdateBucket to set up a
// bucket it creates, DeleteBucket to undo that create, and ListBuckets and UsesBucket to adopt a
// store's unused buckets. IsBucketEmpty and EmptyBucket have no caller in Remote yet. They are
// the steps a bucket delete needs, and are kept with their tests for it. A delete is split into
// steps because Remote applies its own rules between them, such as refusing a bucket that a live
// target uses.

var (
	// ErrBucketNotFound is returned for a bucket the store does not have.
	ErrBucketNotFound = errors.New("the bucket does not exist")
	// ErrBucketExists is returned when a create names a bucket the store already has.
	ErrBucketExists = errors.New("the bucket already exists")
	// ErrBucketNotEmpty is returned when the store refuses to delete a bucket that holds objects.
	ErrBucketNotEmpty = errors.New("the bucket is not empty")
)

// BucketManager manages the buckets in a target's store. Every method takes store, the effective
// configuration of a valid target of the manager's type. Every method but UsesBucket contacts the
// store.
type BucketManager interface {
	// UsesBucket reports whether target, the effective configuration of a target of any type,
	// keeps its objects in store's bucket. A target of another type may still use the store, so the
	// caller does not filter by type. A target that names the same bucket in another store does
	// not use it. When the answer cannot be known, UsesBucket answers true, so the bucket is taken
	// as used.
	UsesBucket(store, target *flex.RemoteStorageTarget) bool
	// ListBuckets returns the names of the buckets in store, sorted. It ignores store's bucket.
	ListBuckets(ctx context.Context, store *flex.RemoteStorageTarget) ([]string, error)
	// CheckBucket returns nil when store's bucket exists and the store lets store's credentials use
	// it. It returns an error wrapping ErrBucketNotFound when the store has no such bucket. It
	// changes nothing.
	CheckBucket(ctx context.Context, store *flex.RemoteStorageTarget) error
	// CreateBucket creates store's bucket, in store's region. It returns an error wrapping
	// ErrBucketExists when the store already has a bucket with that name.
	CreateBucket(ctx context.Context, store *flex.RemoteStorageTarget) error
	// UpdateBucket applies changes to store's bucket.
	UpdateBucket(ctx context.Context, store *flex.RemoteStorageTarget, changes BucketChanges) error
	// IsBucketEmpty reports whether store's bucket holds no object, no old version of an object and
	// no unfinished multipart upload.
	IsBucketEmpty(ctx context.Context, store *flex.RemoteStorageTarget) (bool, error)
	// EmptyBucket deletes every object and every old version of an object in store's bucket, and
	// aborts every unfinished multipart upload. It returns how many of each it removed, also when
	// it fails part way.
	EmptyBucket(ctx context.Context, store *flex.RemoteStorageTarget) (objects int64, uploads int64, err error)
	// DeleteBucket deletes store's bucket. It returns an error wrapping ErrBucketNotEmpty when the
	// store refuses because the bucket still holds objects.
	DeleteBucket(ctx context.Context, store *flex.RemoteStorageTarget) error
}

// BucketChanges is a change to a bucket's settings. A zero field changes nothing. Create uses it to
// set up a bucket it makes, and refuses the removals, which only make sense on an existing bucket.
type BucketChanges struct {
	// Versioning is ENABLED or SUSPENDED to change it. UNKNOWN leaves it.
	Versioning beeremote.BucketChanges_Versioning
	// SetTags adds tags, or changes their values.
	SetTags map[string]string
	// RemoveTags removes the tags with these keys. A key that is not set is ignored.
	RemoveTags []string
	// ClearTags removes every tag before SetTags is applied.
	ClearTags bool
}

// BucketChangesFromProto converts the wire form of a change. A nil message changes nothing.
func BucketChangesFromProto(m *beeremote.BucketChanges) BucketChanges {
	return BucketChanges{
		Versioning: m.GetVersioning(),
		SetTags:    m.GetSetTags(),
		RemoveTags: m.GetRemoveTags(),
		ClearTags:  m.GetClearTags(),
	}
}

// Proto returns the wire form of c.
func (c BucketChanges) Proto() *beeremote.BucketChanges {
	return &beeremote.BucketChanges{
		Versioning: c.Versioning,
		SetTags:    c.SetTags,
		RemoveTags: c.RemoveTags,
		ClearTags:  c.ClearTags,
	}
}

// IsEmpty reports whether c changes nothing.
func (c BucketChanges) IsEmpty() bool {
	return c.Versioning == beeremote.BucketChanges_UNKNOWN && len(c.SetTags) == 0 && !c.RemovesTags()
}

// RemovesTags reports whether c removes any tag, by key or by clearing them all.
func (c BucketChanges) RemovesTags() bool {
	return len(c.RemoveTags) > 0 || c.ClearTags
}

// BucketsOf returns the BucketManager of config's type. It returns false for a type that cannot
// manage buckets, or that no client registered.
func BucketsOf(config *flex.RemoteStorageTarget) (BucketManager, bool) {
	t, ok := TargetTypeOf(config)
	if !ok {
		return nil, false
	}
	b := t.Buckets()
	return b, b != nil
}
