package rst

import (
	"context"
	"fmt"

	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file manages the buckets in the store of a backend through Remote. Remote runs every request
// against the store with the backend's settings (see common/rst/bucket.go).

// ListBuckets lists the buckets of the backend called backend, or of every backend that can manage
// buckets when backend is "". With details, each bucket's versioning and tags are read too.
func ListBuckets(ctx context.Context, backend string, details bool) ([]*beeremote.ListRSTBucketsResponse_BackendBuckets, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := beeRemote.ListRSTBuckets(ctx, &beeremote.ListRSTBucketsRequest{Backend: backend, Details: details})
	if err != nil {
		return nil, err
	}
	return resp.GetBackends(), nil
}

// CreateBucket creates the bucket called name in the store of the backend called backend, with the
// settings in changes. Remote creates it in the backend's region.
func CreateBucket(ctx context.Context, backend string, name string, changes cRst.BucketChanges) (*beeremote.RSTBucket, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := beeRemote.CreateRSTBucket(ctx, &beeremote.CreateRSTBucketRequest{
		Backend:    backend,
		Name:       name,
		Versioning: changes.Versioning,
		Tags:       changes.SetTags,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetBucket(), nil
}

// UpdateBucket applies changes to the bucket called name in the store of the backend called
// backend, and returns the bucket after the change.
func UpdateBucket(ctx context.Context, backend string, name string, changes cRst.BucketChanges) (*beeremote.RSTBucket, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := beeRemote.UpdateRSTBucket(ctx, &beeremote.UpdateRSTBucketRequest{
		Backend:    backend,
		Name:       name,
		Versioning: changes.Versioning,
		SetTags:    changes.SetTags,
		RemoveTags: changes.RemoveTags,
		ClearTags:  changes.ClearTags,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetBucket(), nil
}

// DeleteBucket deletes the bucket called name from the store of the backend called backend. Remote
// refuses a bucket a live target uses. It refuses a bucket that holds objects unless force is set,
// and then deletes them first.
func DeleteBucket(ctx context.Context, backend string, name string, force bool) (*beeremote.DeleteRSTBucketResponse, error) {
	beeRemote, err := requireRSTManagement(ctx)
	if err != nil {
		return nil, err
	}
	return beeRemote.DeleteRSTBucket(ctx, &beeremote.DeleteRSTBucketRequest{Backend: backend, Name: name, Force: force})
}

// NewBucketTarget returns a new target that keeps its objects in the bucket called bucket, in the
// store of the backend called backend. It has the backend's type, names the backend, and sets only
// the bucket, so it inherits every other setting. id 0 lets Remote assign one.
func NewBucketTarget(ctx context.Context, backend string, bucket string, id uint32, name string) (*flex.RemoteStorageTarget, error) {
	targets, err := GetTargets(ctx)
	if err != nil {
		return nil, err
	}
	definition, ok := targets.Backends[backend]
	if !ok {
		return nil, fmt.Errorf("remote backend %q does not exist", backend)
	}
	buckets, ok := cRst.BucketsOf(definition)
	if !ok {
		return nil, fmt.Errorf("remote backend %q is of type %s, which cannot manage buckets", backend, cRst.TypeName(definition))
	}
	target, err := NewTarget(cRst.TypeName(definition), id)
	if err != nil {
		return nil, err
	}
	changes := TargetChanges{
		Backend:  &backend,
		Settings: map[string]string{buckets.BucketSetting(): bucket},
	}
	if name != "" {
		changes.Name = &name
	}
	return ApplyTargetChanges(target, changes)
}
