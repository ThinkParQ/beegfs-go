package rst

import (
	"context"
	"errors"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file defines how an RST type manages the buckets in the store of a backend (see backend.go).
// A type that can manage buckets returns a BucketManager from TargetType.Buckets. Remote runs every
// bucket request through it, with the backend's settings, so the CLI host never needs a route to
// the store or the backend's credentials.
//
// "Bucket" is the S3 name for what other stores call a container, a share, an export or a volume.
// The CLI offers "container" as an alias for the same reason.
//
// Deleting a bucket is split into steps (IsBucketEmpty, EmptyBucket, DeleteBucket), because Remote
// applies its own rules between them: it refuses a bucket a live target uses, refuses a populated
// bucket unless forced, and holds its change lock only for the final delete.

var (
	// ErrBucketNotFound is returned for a bucket the store does not have.
	ErrBucketNotFound = errors.New("the bucket does not exist")
	// ErrBucketExists is returned when a create names a bucket the store already has.
	ErrBucketExists = errors.New("the bucket already exists")
	// ErrBucketNotEmpty is returned when the store refuses to delete a bucket that holds objects.
	ErrBucketNotEmpty = errors.New("the bucket is not empty")
	// ErrBucketDetailsUnread is returned by a list with details when the versioning or tags of
	// some buckets could not be read. The list itself is complete, and those buckets have details
	// false.
	ErrBucketDetailsUnread = errors.New("the details of some buckets could not be read")
)

// BucketManager manages the buckets in the store a backend connects to. Every method takes the
// backend's definition, which has every setting the type requires of a backend (see
// ValidateBackend). Every method but BucketSetting and UsesBucket contacts the store.
type BucketManager interface {
	// BucketSetting is the key of the target setting that names a target's bucket, such as
	// "bucket". The CLI uses it to create a target for a new bucket.
	BucketSetting() string
	// UsesBucket reports whether target, the effective configuration of a target of any type,
	// keeps its objects in the bucket called name, in the store backend connects to. A target of
	// another type may still use the store, so the caller does not filter by type. A target that
	// overrides the backend's endpoint with another store does not use the bucket, even when the
	// names match. When the answer cannot be known, UsesBucket answers true, so a delete is refused.
	UsesBucket(backend, target *flex.RemoteStorageTarget, name string) bool
	// ListBuckets returns the buckets of the store, sorted by name. With details, each bucket's
	// versioning and tags are read too, which costs requests per bucket. A bucket whose details
	// cannot be read is still listed, with details false. The buckets are then returned together
	// with an error that wraps ErrBucketDetailsUnread. The targets field is left for the caller,
	// which knows the targets.
	ListBuckets(ctx context.Context, backend *flex.RemoteStorageTarget, details bool) ([]*beeremote.RSTBucket, error)
	// GetBucket returns the bucket called name with its details. It returns ErrBucketNotFound when
	// the store has no such bucket.
	GetBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (*beeremote.RSTBucket, error)
	// CreateBucket creates the bucket called name, in the backend's region. It returns
	// ErrBucketExists when the store already has a bucket with that name.
	CreateBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) error
	// UpdateBucket applies changes to the bucket called name.
	UpdateBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string, changes BucketChanges) error
	// IsBucketEmpty reports whether the bucket called name holds no object, no old version of an
	// object and no unfinished multipart upload.
	IsBucketEmpty(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (bool, error)
	// EmptyBucket deletes every object and every old version of an object in the bucket called
	// name, and aborts every unfinished multipart upload. It returns how many of each it removed,
	// also when it fails part way.
	EmptyBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (objects int64, uploads int64, err error)
	// DeleteBucket deletes the bucket called name. It returns ErrBucketNotEmpty when the store
	// refuses because the bucket still holds objects.
	DeleteBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) error
}

// BucketChanges is a change to a bucket's settings. A zero field changes nothing.
type BucketChanges struct {
	// Versioning is ENABLED or SUSPENDED to change it. UNKNOWN leaves it.
	Versioning beeremote.RSTBucket_Versioning
	// SetTags adds tags, or changes their values.
	SetTags map[string]string
	// RemoveTags removes the tags with these keys. A key that is not set is ignored.
	RemoveTags []string
	// ClearTags removes every tag before SetTags is applied.
	ClearTags bool
}

// IsEmpty reports whether c changes nothing.
func (c BucketChanges) IsEmpty() bool {
	return c.Versioning == beeremote.RSTBucket_UNKNOWN && len(c.SetTags) == 0 && len(c.RemoveTags) == 0 && !c.ClearTags
}

// BucketsOf returns the BucketManager of backend's type. It returns false for a type that cannot
// manage buckets, or that no client registered.
func BucketsOf(backend *flex.RemoteStorageTarget) (BucketManager, bool) {
	t, ok := TargetTypeOf(backend)
	if !ok {
		return nil, false
	}
	b := t.Buckets()
	return b, b != nil
}
