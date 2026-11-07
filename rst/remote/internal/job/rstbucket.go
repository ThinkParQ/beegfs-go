package job

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file manages the buckets in the store of a backend: list, create, update and delete. Each
// request runs against the store through the backend type's rst.BucketManager, with the backend's
// settings. Nothing here changes rstDB, so nothing is applied to worker nodes.
//
// Deleting a bucket has rules of its own, because a deleted bucket takes every object of a target
// with it:
//   - A bucket that a live target uses is refused, with or without force. The admin first deletes
//     the target, or points it at another bucket. A target uses a bucket when its effective
//     configuration names it in the backend's store (rst.BucketManager.UsesBucket).
//   - A bucket that holds objects, old versions or unfinished multipart uploads is refused without
//     force. With force, Remote empties it first.
//   - Emptying can take long, so it runs without rstConfigMu. Instead, the bucket is recorded in
//     bucketDeletes for the whole delete. validateEffective refuses any target change that would
//     make a target use it. The final delete runs under rstConfigMu, after the target check runs
//     again.
//
// A deleted target is not checked. Its delete record keeps its configuration, and its stubs may
// still name its ID, but nothing runs new work on it. Deleting its bucket strands those stubs.

const (
	// bucketRequestTimeout bounds a list, create, update or the final delete of a bucket. A list
	// with details sends requests per bucket, so it gets the same generous bound.
	bucketRequestTimeout = 5 * time.Minute
)

// bucketKey names one bucket in the store of one backend.
type bucketKey struct {
	backend string
	bucket  string
}

// bucketManagerOf returns the bucket manager of backend, through m.bucketsOf. It returns
// FailedPrecondition for a type that cannot manage buckets.
func (m *Manager) bucketManagerOf(backend *flex.RemoteStorageTarget) (rst.BucketManager, error) {
	bucketsOf := m.bucketsOf
	if bucketsOf == nil {
		bucketsOf = rst.BucketsOf
	}
	buckets, ok := bucketsOf(backend)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "remote storage backend %q is of type %s, which cannot manage buckets", backend.GetName(), rst.TypeName(backend))
	}
	return buckets, nil
}

// loadBackend returns the stored set and the backend called name in it.
func (m *Manager) loadBackend(name string) (rstdb.Set, *flex.RemoteStorageTarget, error) {
	if m.rstDB == nil {
		return rstdb.Set{}, nil, errRSTManagementUnavailable
	}
	set, err := m.rstDB.Load()
	if err != nil {
		return rstdb.Set{}, nil, status.Error(codes.Internal, err.Error())
	}
	backend, ok := set.Backends[name]
	if !ok {
		return rstdb.Set{}, nil, status.Errorf(codes.NotFound, "remote storage backend %q does not exist", name)
	}
	return set, backend, nil
}

// targetsUsingBucket returns the IDs of the live targets in set that use the bucket called name in
// backend's store, sorted. Targets of every type are checked, because a target of another type can
// use the same store, such as an xtreemstore target in the store of an S3 backend.
func targetsUsingBucket(set rstdb.Set, buckets rst.BucketManager, backend *flex.RemoteStorageTarget, name string) ([]uint32, error) {
	live, err := set.EffectiveLive()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ids := []uint32{}
	for _, target := range live {
		if buckets.UsesBucket(backend, target, name) {
			ids = append(ids, target.GetId())
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// bucketStatus turns an error of a bucket manager into a gRPC status: ErrBucketNotFound is
// NotFound, ErrBucketExists is AlreadyExists, ErrBucketNotEmpty is FailedPrecondition, and any other
// error is Unavailable, because it comes from the store.
func bucketStatus(err error) error {
	switch {
	case errors.Is(err, rst.ErrBucketNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, rst.ErrBucketExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, rst.ErrBucketNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

// ListRSTBuckets lists the buckets of the backend called name, or of every backend that can manage
// buckets when name is "". Each bucket names the live targets that use it. A backend whose store
// fails is reported in its entry and does not fail the others.
func (m *Manager) ListRSTBuckets(ctx context.Context, name string, details bool) ([]*beeremote.ListRSTBucketsResponse_BackendBuckets, error) {
	if m.rstDB == nil {
		return nil, errRSTManagementUnavailable
	}
	set, err := m.rstDB.Load()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	names := []string{name}
	if name == "" {
		names = slices.Sorted(maps.Keys(set.Backends))
	} else if _, ok := set.Backends[name]; !ok {
		return nil, status.Errorf(codes.NotFound, "remote storage backend %q does not exist", name)
	}

	ctx, cancel := context.WithTimeout(ctx, bucketRequestTimeout)
	defer cancel()
	result := []*beeremote.ListRSTBucketsResponse_BackendBuckets{}
	for _, backendName := range names {
		backend := set.Backends[backendName]
		buckets, err := m.bucketManagerOf(backend)
		if err != nil {
			if name == "" {
				continue
			}
			return nil, err
		}
		entry := &beeremote.ListRSTBucketsResponse_BackendBuckets{Backend: backendName}
		listed, err := buckets.ListBuckets(ctx, backend, details)
		switch {
		case errors.Is(err, rst.ErrBucketDetailsUnread):
			// The list is complete. The buckets whose details are missing show them as not read,
			// and the reason goes to the log rather than failing the whole backend.
			m.log.Warn("unable to read the details of some buckets", zap.String("backend", backendName), zap.Error(err))
		case err != nil:
			entry.Error = err.Error()
		}
		for _, bucket := range listed {
			if bucket.Targets, err = targetsUsingBucket(set, buckets, backend, bucket.GetName()); err != nil {
				return nil, err
			}
		}
		entry.Buckets = listed
		result = append(result, entry)
	}
	return result, nil
}

// CreateRSTBucket creates the bucket called name in the store of the backend called backendName,
// then applies changes, such as tags. It returns the new bucket. When changes fail after the
// bucket was created, the error says so, and the bucket stays.
func (m *Manager) CreateRSTBucket(ctx context.Context, backendName string, name string, changes rst.BucketChanges) (*beeremote.RSTBucket, error) {
	set, backend, err := m.loadBackend(backendName)
	if err != nil {
		return nil, err
	}
	buckets, err := m.bucketManagerOf(backend)
	if err != nil {
		return nil, err
	}
	if changes.Versioning == beeremote.RSTBucket_UNVERSIONED {
		return nil, status.Error(codes.InvalidArgument, "versioning can only be enabled or suspended")
	}
	ctx, cancel := context.WithTimeout(ctx, bucketRequestTimeout)
	defer cancel()
	if err := buckets.CreateBucket(ctx, backend, name); err != nil {
		return nil, bucketStatus(err)
	}
	m.log.Info("created bucket", zap.String("backend", backendName), zap.String("bucket", name))
	if !changes.IsEmpty() {
		if err := buckets.UpdateBucket(ctx, backend, name, changes); err != nil {
			return nil, status.Errorf(codes.Unavailable, "bucket %q was created, but its settings could not be applied (hint: retry with 'beegfs remote bucket modify'): %s", name, err)
		}
	}
	return m.readBucket(ctx, set, buckets, backend, name)
}

// UpdateRSTBucket applies changes to the bucket called name in the store of the backend called
// backendName, and returns the bucket after the change.
func (m *Manager) UpdateRSTBucket(ctx context.Context, backendName string, name string, changes rst.BucketChanges) (*beeremote.RSTBucket, error) {
	set, backend, err := m.loadBackend(backendName)
	if err != nil {
		return nil, err
	}
	buckets, err := m.bucketManagerOf(backend)
	if err != nil {
		return nil, err
	}
	if changes.IsEmpty() {
		return nil, status.Error(codes.InvalidArgument, "the request changes nothing")
	}
	if changes.Versioning == beeremote.RSTBucket_UNVERSIONED {
		return nil, status.Error(codes.InvalidArgument, "versioning cannot be turned off once enabled, only suspended")
	}
	ctx, cancel := context.WithTimeout(ctx, bucketRequestTimeout)
	defer cancel()
	if err := buckets.UpdateBucket(ctx, backend, name, changes); err != nil {
		return nil, bucketStatus(err)
	}
	m.log.Info("updated bucket", zap.String("backend", backendName), zap.String("bucket", name))
	return m.readBucket(ctx, set, buckets, backend, name)
}

// readBucket returns the bucket called name with its details and the targets that use it. It runs
// after a create or update succeeded, so a failed read does not fail the request. A retry of a
// create would only answer AlreadyExists. The bucket is then returned with details false, and the
// reason goes to the log. A store can fail the read, for example, when a policy denies
// GetBucketVersioning or GetBucketTagging.
func (m *Manager) readBucket(ctx context.Context, set rstdb.Set, buckets rst.BucketManager, backend *flex.RemoteStorageTarget, name string) (*beeremote.RSTBucket, error) {
	bucket, err := buckets.GetBucket(ctx, backend, name)
	if err != nil {
		m.log.Warn("unable to read the bucket back after a change", zap.String("backend", backend.GetName()), zap.String("bucket", name), zap.Error(err))
		bucket = &beeremote.RSTBucket{Name: name}
	}
	if bucket.Targets, err = targetsUsingBucket(set, buckets, backend, name); err != nil {
		return nil, err
	}
	return bucket, nil
}

// DeleteRSTBucket deletes the bucket called name from the store of the backend called backendName,
// by the rules at the top of this file. It returns how many objects it deleted and how many
// uploads it aborted to empty the bucket.
func (m *Manager) DeleteRSTBucket(ctx context.Context, backendName string, name string, force bool) (int64, int64, error) {
	_, backend, err := m.loadBackend(backendName)
	if err != nil {
		return 0, 0, err
	}
	buckets, err := m.bucketManagerOf(backend)
	if err != nil {
		return 0, 0, err
	}

	key := bucketKey{backend: backendName, bucket: name}
	if err := m.startBucketDelete(key, buckets, backend); err != nil {
		return 0, 0, err
	}
	defer m.endBucketDelete(key)

	empty, err := buckets.IsBucketEmpty(ctx, backend, name)
	if err != nil {
		return 0, 0, bucketStatus(err)
	}
	var objects, uploads int64
	if !empty {
		if !force {
			return 0, 0, status.Errorf(codes.FailedPrecondition, "bucket %q holds objects, old object versions or unfinished uploads (hint: use force to delete them with the bucket)", name)
		}
		m.log.Warn("emptying bucket to delete it", zap.String("backend", backendName), zap.String("bucket", name))
		objects, uploads, err = buckets.EmptyBucket(ctx, backend, name)
		if err != nil {
			return objects, uploads, status.Errorf(codes.Unavailable, "bucket %q was only partly emptied (%d objects deleted, %d uploads aborted), and was not deleted: %s", name, objects, uploads, err)
		}
	}

	// The final delete runs under rstConfigMu, after the targets are checked again. No target can
	// have started using the bucket meanwhile (see validateEffective), but the check is cheap and
	// keeps the rule in one place.
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	if err := m.checkBucketUnused(buckets, backend, name); err != nil {
		return objects, uploads, err
	}
	deleteCtx, cancel := context.WithTimeout(ctx, bucketRequestTimeout)
	defer cancel()
	if err := buckets.DeleteBucket(deleteCtx, backend, name); err != nil {
		return objects, uploads, bucketStatus(err)
	}
	m.log.Info("deleted bucket", zap.String("backend", backendName), zap.String("bucket", name), zap.Int64("deletedObjects", objects), zap.Int64("abortedUploads", uploads))
	return objects, uploads, nil
}

// startBucketDelete checks that no live target uses the bucket key names, and records it in
// bucketDeletes, under rstConfigMu. A second delete of the same bucket is refused while the first
// runs.
func (m *Manager) startBucketDelete(key bucketKey, buckets rst.BucketManager, backend *flex.RemoteStorageTarget) error {
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	if _, ok := m.bucketDeletes[key]; ok {
		return status.Errorf(codes.Aborted, "bucket %q of remote storage backend %q is already being deleted", key.bucket, key.backend)
	}
	if err := m.checkBucketUnused(buckets, backend, key.bucket); err != nil {
		return err
	}
	if m.bucketDeletes == nil {
		m.bucketDeletes = map[bucketKey]*flex.RemoteStorageTarget{}
	}
	m.bucketDeletes[key] = backend
	return nil
}

func (m *Manager) endBucketDelete(key bucketKey) {
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	delete(m.bucketDeletes, key)
}

// checkBucketUnused returns FailedPrecondition when a live target uses the bucket called name in
// backend's store. The caller holds rstConfigMu.
func (m *Manager) checkBucketUnused(buckets rst.BucketManager, backend *flex.RemoteStorageTarget, name string) error {
	set, err := m.rstDB.Load()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	users, err := targetsUsingBucket(set, buckets, backend, name)
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return status.Errorf(codes.FailedPrecondition, "bucket %q is used by remote storage targets %s (hint: delete them, or point them at another bucket, first)", name, joinIDs(users))
	}
	return nil
}

// checkNotBeingDeleted returns FailedPrecondition when effective, the effective configuration of a
// target about to be stored, uses a bucket a DeleteRSTBucket is emptying or deleting. The caller
// holds rstConfigMu. validateEffective calls it for every target change.
func (m *Manager) checkNotBeingDeleted(effective *flex.RemoteStorageTarget) error {
	for key, backend := range m.bucketDeletes {
		buckets, err := m.bucketManagerOf(backend)
		if err != nil {
			continue
		}
		if buckets.UsesBucket(backend, effective, key.bucket) {
			return status.Errorf(codes.FailedPrecondition, "remote storage target %d (%s) would use bucket %q of remote storage backend %q, which is being deleted", effective.GetId(), effective.GetName(), key.bucket, key.backend)
		}
	}
	return nil
}
