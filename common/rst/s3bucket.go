package rst

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file implements BucketManager for S3 and every type that embeds an S3 part, such as
// xtreemstore (see bucket.go).
//
// Facts about S3 that it relies on:
//   - ListBuckets returns the buckets of every region of the account. A client for one region
//     cannot use a bucket of another, so buckets whose reported region differs from the backend's
//     are left out. A store that reports no region shows every bucket.
//   - CreateBucket takes the region as LocationConstraint, except us-east-1. AWS rejects
//     us-east-1 as a LocationConstraint, because it is the default.
//   - In us-east-1, CreateBucket of a bucket the caller already owns succeeds (200 OK) instead of
//     answering BucketAlreadyOwnedByYou. So CreateBucket checks with HeadBucket first, or a create
//     would quietly apply its settings to an existing bucket.
//   - Versioning cannot be turned off once enabled, only suspended.
//   - PutBucketTagging replaces the whole tag set, so a change reads the set first. An empty set
//     cannot be written; DeleteBucketTagging removes it instead.
//   - DeleteBucket refuses a bucket that holds objects, old versions or delete markers
//     (BucketNotEmpty). Unfinished multipart uploads do not always stop it, so IsBucketEmpty checks
//     for them too.
//   - ListObjectVersions lists the objects of an unversioned bucket too, each with version ID
//     "null". A store that does not implement it answers NotImplemented. EmptyBucket then falls back
//     to ListObjectsV2.
//   - DeleteObjects deletes at most 1000 objects per request, and reports a failed object in its
//     response rather than as an error.

// s3DeleteBatch is the most objects one DeleteObjects request may name.
const s3DeleteBatch = 1000

// s3BucketAPI is the part of the S3 client that bucket management uses. *s3.Client implements it.
// Tests implement it in memory.
type s3BucketAPI interface {
	ListBuckets(context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(context.Context, *s3.DeleteBucketInput, ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	PutBucketVersioning(context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
	GetBucketTagging(context.Context, *s3.GetBucketTaggingInput, ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error)
	PutBucketTagging(context.Context, *s3.PutBucketTaggingInput, ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error)
	DeleteBucketTagging(context.Context, *s3.DeleteBucketTaggingInput, ...func(*s3.Options)) (*s3.DeleteBucketTaggingOutput, error)
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// s3Buckets manages the buckets of an S3 store. part finds the S3 part of a config, as it does for
// s3TargetType. api builds the client for an S3 part. It is newS3AWSClient, and tests replace it.
type s3Buckets struct {
	part s3PartFunc
	api  func(ctx context.Context, s3Config *flex.RemoteStorageTarget_S3) (s3BucketAPI, error)
}

var _ BucketManager = s3Buckets{}

func newS3Buckets(part s3PartFunc) s3Buckets {
	return s3Buckets{part: part, api: func(ctx context.Context, s3Config *flex.RemoteStorageTarget_S3) (s3BucketAPI, error) {
		return newS3AWSClient(ctx, s3Config)
	}}
}

func (b s3Buckets) client(ctx context.Context, backend *flex.RemoteStorageTarget) (s3BucketAPI, *flex.RemoteStorageTarget_S3, error) {
	s3Config := b.part(backend, false)
	api, err := b.api(ctx, s3Config)
	return api, s3Config, err
}

func (s3Buckets) BucketSetting() string { return "bucket" }

// UsesBucket compares the store as well as the bucket name, because two stores can each have a
// bucket of the same name. target may be of any type that holds an S3 part, not only the backend's
// type. An xtreemstore target can keep its objects in the store of an S3 backend, for example.
// The rules:
//   - A target of a type without an S3 part never uses the bucket.
//   - A target whose bucket has another name never uses it.
//   - Otherwise the target uses the bucket when s3StoreOf gives the same store for the target and
//     the backend.
//   - When either endpoint cannot be parsed, the target is taken to use the bucket. A delete is then
//     refused rather than allowed to empty a bucket a target may still use.
func (b s3Buckets) UsesBucket(backend, target *flex.RemoteStorageTarget, name string) bool {
	targetS3, ok := s3PartOf(target)
	if !ok || targetS3.GetBucket() != name {
		return false
	}
	targetStore, targetErr := s3StoreOf(targetS3.GetEndpointUrl(), name)
	backendStore, backendErr := s3StoreOf(b.part(backend, false).GetEndpointUrl(), "")
	if targetErr != nil || backendErr != nil {
		return true
	}
	return targetStore == backendStore
}

// s3PartOf returns the S3 part of config, for config of any type that holds one: s3, and every type
// that extends s3TargetType. It returns false for any other type.
func s3PartOf(config *flex.RemoteStorageTarget) (*flex.RemoteStorageTarget_S3, bool) {
	t, ok := TargetTypeOf(config)
	if !ok {
		return nil, false
	}
	embedder, ok := t.(interface {
		s3Part(*flex.RemoteStorageTarget) *flex.RemoteStorageTarget_S3
	})
	if !ok {
		return nil, false
	}
	part := embedder.s3Part(config)
	return part, part != nil
}

// s3StoreOf returns a key for the store an S3 endpoint URL reaches. Two endpoints that reach the
// same store get the same key, even when they are written differently. bucket is the bucket the
// endpoint is used with, or "" for a backend, which has none. The key ignores what does not change
// the store:
//   - The case of the host.
//   - A default port, given or not: 80 for http and 443 for https.
//   - A trailing slash on the path.
//   - The bucket as the first label of the host. newS3AWSClient then uses virtual-hosted style, so
//     the rest of the host is the store (see s3VirtualHosted).
//
// The scheme stays part of the key only through its port, so http://h:443 and https://h are the
// same store. An endpoint without a host is an error.
func s3StoreOf(endpoint string, bucket string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("endpoint %q has no host", endpoint)
	}
	if s3VirtualHosted(host, bucket) {
		host = strings.TrimPrefix(host, bucket+".")
	}
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	return net.JoinHostPort(strings.ToLower(host), port) + strings.TrimRight(u.Path, "/"), nil
}

func (b s3Buckets) ListBuckets(ctx context.Context, backend *flex.RemoteStorageTarget, details bool) ([]*beeremote.RSTBucket, error) {
	api, s3Config, err := b.client(ctx, backend)
	if err != nil {
		return nil, err
	}
	buckets := []*beeremote.RSTBucket{}
	input := &s3.ListBucketsInput{}
	for {
		out, err := api.ListBuckets(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("unable to list buckets: %w", err)
		}
		for _, bucket := range out.Buckets {
			if region := aws.ToString(bucket.BucketRegion); region != "" && region != s3Config.GetRegion() {
				continue
			}
			listed := &beeremote.RSTBucket{Name: aws.ToString(bucket.Name)}
			if bucket.CreationDate != nil {
				listed.Created = timestamppb.New(*bucket.CreationDate)
			}
			buckets = append(buckets, listed)
		}
		if aws.ToString(out.ContinuationToken) == "" {
			break
		}
		input.ContinuationToken = out.ContinuationToken
	}
	slices.SortFunc(buckets, func(x, y *beeremote.RSTBucket) int { return strings.Compare(x.GetName(), y.GetName()) })
	if !details {
		return buckets, nil
	}
	var unread []string
	var firstErr error
	for _, bucket := range buckets {
		if err := readBucketDetails(ctx, api, bucket); err != nil {
			unread = append(unread, bucket.GetName())
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if len(unread) > 0 {
		return buckets, fmt.Errorf("%w: %d of %d buckets, such as %q: %w", ErrBucketDetailsUnread, len(unread), len(buckets), unread[0], firstErr)
	}
	return buckets, nil
}

func (b s3Buckets) GetBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (*beeremote.RSTBucket, error) {
	api, _, err := b.client(ctx, backend)
	if err != nil {
		return nil, err
	}
	if _, err := api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); err != nil {
		return nil, bucketError(err, "unable to read bucket %q", name)
	}
	bucket := &beeremote.RSTBucket{Name: name}
	if err := readBucketDetails(ctx, api, bucket); err != nil {
		return nil, err
	}
	return bucket, nil
}

// readBucketDetails fills in the versioning and tags of bucket. When either read fails, it changes
// nothing in bucket, so details stays false.
func readBucketDetails(ctx context.Context, api s3BucketAPI, bucket *beeremote.RSTBucket) error {
	versioning, err := api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket.GetName())})
	if err != nil {
		return bucketError(err, "unable to read the versioning of bucket %q", bucket.GetName())
	}
	tags, err := readBucketTags(ctx, api, bucket.GetName())
	if err != nil {
		return err
	}
	switch versioning.Status {
	case types.BucketVersioningStatusEnabled:
		bucket.Versioning = beeremote.RSTBucket_ENABLED
	case types.BucketVersioningStatusSuspended:
		bucket.Versioning = beeremote.RSTBucket_SUSPENDED
	default:
		bucket.Versioning = beeremote.RSTBucket_UNVERSIONED
	}
	bucket.Tags = tags
	bucket.Details = true
	return nil
}

// readBucketTags returns the tags of the bucket called name. Two answers mean an empty set:
//   - NoSuchTagSet, from a bucket without tags.
//   - NotImplemented, from a store without bucket tagging. Such a store cannot hold tags.
func readBucketTags(ctx context.Context, api s3BucketAPI, name string) (map[string]string, error) {
	out, err := api.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(name)})
	if code := errorCode(err); code == "NoSuchTagSet" || code == "NotImplemented" {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, bucketError(err, "unable to read the tags of bucket %q", name)
	}
	tags := make(map[string]string, len(out.TagSet))
	for _, tag := range out.TagSet {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags, nil
}

func (b s3Buckets) CreateBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) error {
	api, s3Config, err := b.client(ctx, backend)
	if err != nil {
		return err
	}
	// HeadBucket succeeds only when the bucket exists and the caller may use it. A bucket someone
	// else owns answers 403, and is left to CreateBucket to refuse.
	if _, err := api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); err == nil {
		return fmt.Errorf("unable to create bucket %q: %w", name, ErrBucketExists)
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if region := s3Config.GetRegion(); region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(region)}
	}
	if _, err := api.CreateBucket(ctx, input); err != nil {
		return bucketError(err, "unable to create bucket %q", name)
	}
	return nil
}

func (b s3Buckets) UpdateBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string, changes BucketChanges) error {
	api, _, err := b.client(ctx, backend)
	if err != nil {
		return err
	}
	switch changes.Versioning {
	case beeremote.RSTBucket_UNKNOWN:
	case beeremote.RSTBucket_ENABLED, beeremote.RSTBucket_SUSPENDED:
		status := types.BucketVersioningStatusEnabled
		if changes.Versioning == beeremote.RSTBucket_SUSPENDED {
			status = types.BucketVersioningStatusSuspended
		}
		if _, err := api.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(name),
			VersioningConfiguration: &types.VersioningConfiguration{Status: status},
		}); err != nil {
			return bucketError(err, "unable to change the versioning of bucket %q", name)
		}
	default:
		return fmt.Errorf("versioning can only be enabled or suspended, not set to %s", changes.Versioning)
	}

	if !changes.ClearTags && len(changes.SetTags) == 0 && len(changes.RemoveTags) == 0 {
		return nil
	}
	tags := map[string]string{}
	if !changes.ClearTags {
		if tags, err = readBucketTags(ctx, api, name); err != nil {
			return err
		}
	}
	for _, key := range changes.RemoveTags {
		delete(tags, key)
	}
	maps.Copy(tags, changes.SetTags)
	if len(tags) == 0 {
		if _, err := api.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(name)}); err != nil {
			return bucketError(err, "unable to remove the tags of bucket %q", name)
		}
		return nil
	}
	tagSet := []types.Tag{}
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		tagSet = append(tagSet, types.Tag{Key: aws.String(key), Value: aws.String(tags[key])})
	}
	if _, err := api.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(name), Tagging: &types.Tagging{TagSet: tagSet}}); err != nil {
		return bucketError(err, "unable to change the tags of bucket %q", name)
	}
	return nil
}

func (b s3Buckets) IsBucketEmpty(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (bool, error) {
	api, _, err := b.client(ctx, backend)
	if err != nil {
		return false, err
	}
	versions, err := api.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(name), MaxKeys: aws.Int32(1)})
	switch {
	case errorCode(err) == "NotImplemented":
		objects, err := api.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(name), MaxKeys: aws.Int32(1)})
		if err != nil {
			return false, bucketError(err, "unable to list the objects of bucket %q", name)
		}
		if len(objects.Contents) > 0 {
			return false, nil
		}
	case err != nil:
		return false, bucketError(err, "unable to list the objects of bucket %q", name)
	case len(versions.Versions) > 0 || len(versions.DeleteMarkers) > 0:
		return false, nil
	}
	uploads, err := api.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(name), MaxUploads: aws.Int32(1)})
	if err != nil {
		return false, bucketError(err, "unable to list the multipart uploads of bucket %q", name)
	}
	return len(uploads.Uploads) == 0, nil
}

func (b s3Buckets) EmptyBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) (int64, int64, error) {
	api, _, err := b.client(ctx, backend)
	if err != nil {
		return 0, 0, err
	}
	objects, err := deleteObjectVersions(ctx, api, name)
	if errorCode(err) == "NotImplemented" {
		objects, err = deleteObjects(ctx, api, name)
	}
	if err != nil {
		return objects, 0, err
	}
	uploads, err := abortUploads(ctx, api, name)
	return objects, uploads, err
}

// deleteObjectVersions deletes every version and delete marker in the bucket called name, one page
// of ListObjectVersions at a time. It returns how many it deleted.
func deleteObjectVersions(ctx context.Context, api s3BucketAPI, name string) (int64, error) {
	var deleted int64
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(name), MaxKeys: aws.Int32(s3DeleteBatch)}
	for {
		out, err := api.ListObjectVersions(ctx, input)
		if err != nil {
			return deleted, bucketError(err, "unable to list the objects of bucket %q", name)
		}
		batch := []types.ObjectIdentifier{}
		for _, v := range out.Versions {
			batch = append(batch, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			batch = append(batch, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		n, err := deleteBatch(ctx, api, name, batch)
		deleted += n
		if err != nil || !aws.ToBool(out.IsTruncated) {
			return deleted, err
		}
		input.KeyMarker, input.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

// deleteObjects deletes every object in the bucket called name, for a store without
// ListObjectVersions. Such a store keeps no versions to delete.
func deleteObjects(ctx context.Context, api s3BucketAPI, name string) (int64, error) {
	var deleted int64
	input := &s3.ListObjectsV2Input{Bucket: aws.String(name), MaxKeys: aws.Int32(s3DeleteBatch)}
	for {
		out, err := api.ListObjectsV2(ctx, input)
		if err != nil {
			return deleted, bucketError(err, "unable to list the objects of bucket %q", name)
		}
		batch := []types.ObjectIdentifier{}
		for _, o := range out.Contents {
			batch = append(batch, types.ObjectIdentifier{Key: o.Key})
		}
		n, err := deleteBatch(ctx, api, name, batch)
		deleted += n
		if err != nil || !aws.ToBool(out.IsTruncated) {
			return deleted, err
		}
		input.ContinuationToken = out.NextContinuationToken
	}
}

// deleteBatch deletes objects, at most s3DeleteBatch of them, with one DeleteObjects request. It
// returns how many were deleted, and an error naming the first object the store refused.
func deleteBatch(ctx context.Context, api s3BucketAPI, name string, objects []types.ObjectIdentifier) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	out, err := api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(name),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return 0, bucketError(err, "unable to delete objects of bucket %q", name)
	}
	deleted := int64(len(objects) - len(out.Errors))
	if len(out.Errors) > 0 {
		e := out.Errors[0]
		return deleted, fmt.Errorf("unable to delete %d objects of bucket %q, such as %q: %s: %s", len(out.Errors), name, aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
	}
	return deleted, nil
}

// abortUploads aborts every unfinished multipart upload in the bucket called name, and returns how
// many it aborted.
func abortUploads(ctx context.Context, api s3BucketAPI, name string) (int64, error) {
	var aborted int64
	input := &s3.ListMultipartUploadsInput{Bucket: aws.String(name)}
	for {
		out, err := api.ListMultipartUploads(ctx, input)
		if err != nil {
			return aborted, bucketError(err, "unable to list the multipart uploads of bucket %q", name)
		}
		for _, u := range out.Uploads {
			if _, err := api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(name), Key: u.Key, UploadId: u.UploadId}); err != nil && errorCode(err) != "NoSuchUpload" {
				return aborted, bucketError(err, "unable to abort an upload of %q in bucket %q", aws.ToString(u.Key), name)
			}
			aborted++
		}
		if !aws.ToBool(out.IsTruncated) {
			return aborted, nil
		}
		input.KeyMarker, input.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
}

func (b s3Buckets) DeleteBucket(ctx context.Context, backend *flex.RemoteStorageTarget, name string) error {
	api, _, err := b.client(ctx, backend)
	if err != nil {
		return err
	}
	if _, err := api.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
		return bucketError(err, "unable to delete bucket %q", name)
	}
	return nil
}

// errorCode returns the S3 error code of err, or "" when err has none.
func errorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// bucketError wraps err with a message, and with the bucket error that matches its S3 code, so
// Remote can answer with the right status:
//   - NoSuchBucket, or NotFound from HeadBucket: ErrBucketNotFound.
//   - BucketAlreadyExists or BucketAlreadyOwnedByYou: ErrBucketExists.
//   - BucketNotEmpty: ErrBucketNotEmpty.
func bucketError(err error, format string, args ...any) error {
	var kind error
	switch errorCode(err) {
	case "NoSuchBucket", "NotFound":
		kind = ErrBucketNotFound
	case "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
		kind = ErrBucketExists
	case "BucketNotEmpty":
		kind = ErrBucketNotEmpty
	}
	message := fmt.Sprintf(format, args...)
	if kind != nil {
		return fmt.Errorf("%s: %w: %w", message, kind, err)
	}
	return fmt.Errorf("%s: %w", message, err)
}
