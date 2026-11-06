package job

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file holds the bucket step of a target change: the part that runs in the target's store,
// through its type's rst.BucketManager. Only types that can manage buckets have one, such as s3 and
// xtreemstore. A target of any other type never contacts its store here.
//
// A target change that touches the bucket runs four steps, all under rstConfigMu (see
// rstconfig.go):
//  1. Check the request against the stored set, and validate the new target.
//  2. Run the bucket step in the store. It returns a BucketResult that says what it did, and an
//     undo func when it changed the store.
//  3. Store the change in rstDB.
//  4. When storing fails, run the undo func.
//
// Today only create has a bucket step (createBucketStep). It makes the target's bucket, or with
// adopt checks that the bucket exists. AdoptRSTBuckets lists the unused buckets of a store
// (unusedBuckets), and changes nothing in it, so it has no undo. A bucket modify or delete would add
// its own step at the same point of UpdateRST or DeleteRST.
//
// The bucket a step works on is the one the target's effective configuration names, in the store
// that configuration reaches. That is the backend's store, unless the target overrides the
// endpoint. So a target without a backend gets a bucket too.
//
// Store I/O runs under rstConfigMu, but never inside the rstDB.Update callback, which holds the
// MapStore entry lock. Holding rstConfigMu keeps two creates of one bucket from racing. Without it,
// both could see "created" in a store that answers 200 for an existing bucket. One undo could then
// delete the bucket the other target was just stored with.

const (
	// bucketRequestTimeout bounds the store requests of one bucket step, and of its undo.
	bucketRequestTimeout = 5 * time.Minute
)

// bucketManagerOf returns the bucket manager of config's type, through m.bucketsOf. It returns
// false for a type that cannot manage buckets.
func (m *Manager) bucketManagerOf(config *flex.RemoteStorageTarget) (rst.BucketManager, bool) {
	bucketsOf := m.bucketsOf
	if bucketsOf == nil {
		bucketsOf = rst.BucketsOf
	}
	return bucketsOf(config)
}

// bucketSettingOf returns the setting that names config's bucket. It returns false for a type that
// cannot manage buckets.
func bucketSettingOf(config *flex.RemoteStorageTarget) (rst.Setting, bool) {
	t, ok := rst.TargetTypeOf(config)
	if !ok {
		return rst.Setting{}, false
	}
	return rst.BucketSettingOf(t)
}

// bucketStatus turns an error of a bucket manager into a gRPC status: ErrBucketNotFound is
// NotFound, ErrBucketExists is AlreadyExists, ErrBucketNotEmpty is FailedPrecondition, and any other
// error is Unavailable, because it comes from the store. hint, when not "", is added to the
// message.
func bucketStatus(err error, hint string) error {
	code := codes.Unavailable
	switch {
	case errors.Is(err, rst.ErrBucketNotFound):
		code = codes.NotFound
	case errors.Is(err, rst.ErrBucketExists):
		code = codes.AlreadyExists
	case errors.Is(err, rst.ErrBucketNotEmpty):
		code = codes.FailedPrecondition
	}
	if hint != "" && code != codes.Unavailable {
		return status.Errorf(code, "%s (hint: %s)", err, hint)
	}
	return status.Error(code, err.Error())
}

// bucketUndo reverses what a bucket step changed in the store.
type bucketUndo func(ctx context.Context) error

// createBucketStep runs the bucket step of CreateRST, after the target passed every check and
// before it is stored. effective is the new target's effective configuration. It returns what it
// did, and an undo func when it created a bucket. The rules:
//   - A type that cannot manage buckets: nothing happens, and the result is nil.
//   - Adopt: the bucket must exist, and the store must let the target's credentials use it.
//     Nothing in the store changes, and the action is ADOPTED.
//   - Otherwise the bucket is created, and changes are applied to it. A bucket that exists is
//     refused, because it may hold another target's objects. The action is CREATED. When the
//     changes fail, the bucket is deleted again before the error is returned.
func (m *Manager) createBucketStep(ctx context.Context, effective *flex.RemoteStorageTarget, adopt bool, changes rst.BucketChanges) (*beeremote.BucketResult, bucketUndo, error) {
	buckets, ok := m.bucketManagerOf(effective)
	if !ok {
		return nil, nil, nil
	}
	setting, ok := bucketSettingOf(effective)
	if !ok {
		return nil, nil, status.Errorf(codes.Internal, "remote storage target type %s can manage buckets, but has no bucket setting", rst.TypeName(effective))
	}
	name := setting.Get(effective)
	stepCtx, cancel := context.WithTimeout(ctx, bucketRequestTimeout)
	defer cancel()

	if adopt {
		if err := buckets.CheckBucket(stepCtx, effective); err != nil {
			return nil, nil, bucketStatus(err, "create the target without adopt to create the bucket")
		}
		return &beeremote.BucketResult{Name: name, Action: beeremote.BucketResult_ADOPTED}, nil, nil
	}

	if err := buckets.CreateBucket(stepCtx, effective); err != nil {
		return nil, nil, bucketStatus(err, "use adopt to keep the target's objects in the existing bucket")
	}
	undo := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketRequestTimeout)
		defer cancel()
		return buckets.DeleteBucket(ctx, effective)
	}
	result := &beeremote.BucketResult{Name: name, Action: beeremote.BucketResult_CREATED}
	if !changes.IsEmpty() {
		if err := buckets.UpdateBucket(stepCtx, effective, changes); err != nil {
			return nil, nil, undoBucketStep(ctx, result, undo, bucketStatus(err, ""))
		}
	}
	return result, undo, nil
}

// undoBucketStep runs undo after cause failed a target change, and returns cause. When undo fails
// too, the returned error keeps cause's code and adds which bucket was left behind and why, so the
// admin can remove it. A nil undo means there is nothing to reverse.
func undoBucketStep(ctx context.Context, result *beeremote.BucketResult, undo bucketUndo, cause error) error {
	if undo == nil {
		return cause
	}
	err := undo(ctx)
	if err == nil {
		return cause
	}
	st, _ := status.FromError(cause)
	return status.Errorf(st.Code(), "%s; bucket %q was created for the target and could not be deleted again, so it is left in the store: %s", st.Message(), result.GetName(), err)
}

// unusedBuckets lists the buckets in the store that store reaches, and splits them into those no
// target uses and those some target uses. store is an effective configuration, such as the
// resolved template of an adopt. Its own bucket is ignored. A bucket counts as used when
// UsesBucket matches it against either of these:
//   - a live target's effective configuration
//   - a deleted target's stored configuration, because the deleted target's stubs may still name
//     its old ID. The admin revives such a target with an explicit ID, force and adopt.
//
// Targets of every type are checked, because a target of another type can use the same store.
func unusedBuckets(ctx context.Context, set rstdb.Set, buckets rst.BucketManager, setting rst.Setting, store *flex.RemoteStorageTarget) ([]string, []*beeremote.AdoptRSTBucketsResponse_Skipped, error) {
	live, err := set.EffectiveLive()
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	names, err := buckets.ListBuckets(ctx, store)
	if err != nil {
		return nil, nil, bucketStatus(err, "")
	}
	unused := []string{}
	skipped := []*beeremote.AdoptRSTBucketsResponse_Skipped{}
	for _, name := range names {
		probe := proto.Clone(store).(*flex.RemoteStorageTarget)
		if err := setting.Set(probe, name); err != nil {
			return nil, nil, status.Errorf(codes.Internal, "unable to name bucket %q in a target: %s", name, err)
		}
		used := &beeremote.AdoptRSTBucketsResponse_Skipped{Bucket: name}
		for _, target := range live {
			if buckets.UsesBucket(probe, target) {
				used.LiveTargets = append(used.LiveTargets, target.GetId())
			}
		}
		for _, id := range slices.Sorted(maps.Keys(set.Deleted)) {
			if buckets.UsesBucket(probe, set.Deleted[id].Config) {
				used.DeletedTargets = append(used.DeletedTargets, id)
			}
		}
		if len(used.LiveTargets) == 0 && len(used.DeletedTargets) == 0 {
			unused = append(unused, name)
		} else {
			skipped = append(skipped, used)
		}
	}
	return unused, skipped, nil
}

// describeSkipped names each skipped bucket and the targets that use it, for an error message.
func describeSkipped(skipped []*beeremote.AdoptRSTBucketsResponse_Skipped) string {
	if len(skipped) == 0 {
		return "the store has no bucket"
	}
	parts := []string{}
	for _, s := range skipped {
		users := []string{}
		if len(s.GetLiveTargets()) > 0 {
			users = append(users, "target "+joinIDs(s.GetLiveTargets()))
		}
		if len(s.GetDeletedTargets()) > 0 {
			users = append(users, "deleted target "+joinIDs(s.GetDeletedTargets()))
		}
		parts = append(parts, fmt.Sprintf("%q is used by %s", s.GetBucket(), strings.Join(users, " and ")))
	}
	return strings.Join(parts, "; ")
}
