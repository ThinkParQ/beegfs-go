package rst

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newTestS3Client builds an S3Client for the bucket "test-bucket". See newTestS3ClientWithConfig.
func newTestS3Client(t *testing.T, mountPoint filesystem.Provider, opts ...s3ProviderOption) *S3Client {
	t.Helper()
	return newTestS3ClientWithConfig(t, mountPoint, &flex.RemoteStorageTarget_S3{Bucket: "test-bucket"}, opts...)
}

// newTestS3ClientWithConfig builds an S3Client through newS3WithOptions, so every field is set the
// way production sets it. Tests build clients only through this helper, never as a struct literal.
func newTestS3ClientWithConfig(t *testing.T, mountPoint filesystem.Provider, s3Config *flex.RemoteStorageTarget_S3, opts ...s3ProviderOption) *S3Client {
	t.Helper()
	client, err := newS3WithOptions(
		context.Background(),
		&flex.RemoteStorageTarget{Policies: &flex.RemoteStorageTarget_Policies{}},
		s3Config,
		mountPoint,
		opts...,
	)
	require.NoError(t, err)
	return client
}

// withTestApiClient makes the client send every S3 call to api.
func withTestApiClient(api s3ApiClient) s3ProviderOption {
	return withS3ApiClient(func(s3ApiClient) s3ApiClient { return api })
}

func TestGenerateWorkRequests(t *testing.T) {
	var err error
	mp := filesystem.NewMockFS()
	mp.CreateWriteClose(baseTestJob.Request.GetPath(), make([]byte, 1023), 0644, false)
	// Ensure fast start max size is less than the size used for the mock file (1023). This way the
	// client doesn't try to create a multi-part upload, which can't be done without a real bucket.
	testS3Client := newTestS3Client(t, mp)
	testS3Client.config.Policies.FastStartMaxSize = 1024

	jobWithNoExternalID := proto.Clone(baseTestJob).(*beeremote.Job)
	jobWithNoExternalID.ExternalId = ""

	// Verify supported ops generate the correct request types:
	lockedInfo := &flex.JobLockedInfo{
		ReadWriteLocked: true,
		Size:            0,
		Mtime:           &timestamppb.Timestamp{},
		RemoteSize:      0,
		RemoteMtime:     &timestamppb.Timestamp{},
	}
	jobSyncUpload := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncUpload.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation:  flex.SyncJob_UPLOAD,
			LockedInfo: lockedInfo,
		},
	}
	jobSyncDownload := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncDownload.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation:  flex.SyncJob_DOWNLOAD,
			LockedInfo: lockedInfo,
		},
	}
	testJobs := []*beeremote.Job{jobSyncUpload, jobSyncDownload}
	// TODO: https://github.com/thinkparq/gobee/issues/28
	// Also test flex.SyncJob_DOWNLOAD once we have an s3MockProvider.
	for i, op := range []flex.SyncJob_Operation{flex.SyncJob_UPLOAD} {
		requests, err := testS3Client.GenerateWorkRequests(context.Background(), nil, testJobs[i], 1)
		assert.NoError(t, err)
		require.Len(t, requests, 1)
		assert.Equal(t, "", requests[0].ExternalId)
		assert.Equal(t, op, requests[0].GetSync().Operation)
	}

	// If an invalid job type is specified for this RST return the correct error:
	jobMock := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobMock.Request.Type = &beeremote.JobRequest_Mock{Mock: &flex.MockJob{}}
	_, err = testS3Client.GenerateWorkRequests(context.Background(), nil, jobMock, 1)
	assert.ErrorIs(t, err, ErrReqAndRSTTypeMismatch)

	// If the the job type is correct but the operation is not specified/supported, return the correct error:
	jobSyncInvalid := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncInvalid.Request.Type = &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{LockedInfo: lockedInfo}}
	_, err = testS3Client.GenerateWorkRequests(context.Background(), nil, jobSyncInvalid, 1)
	assert.ErrorIs(t, err, ErrUnsupportedOpForRST)

	// If the job already as an external ID it should not be allowed to generate new requests.
	jobWithExternalID := proto.Clone(baseTestJob).(*beeremote.Job)
	jobWithExternalID.Request.Type = &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}}
	_, err = testS3Client.GenerateWorkRequests(context.Background(), nil, jobWithExternalID, 1)
	assert.ErrorIs(t, err, ErrJobAlreadyHasExternalID)
}

// A CLI older than beegfs-remote sends the in-mount path as the key for a single-file push. The
// key check rejects it, and the error must point at the version mismatch instead of the key.
func TestGenerateWorkRequestsHintsAtOlderCLI(t *testing.T) {
	client := newTestS3Client(t, filesystem.NewMockFS())

	job := proto.Clone(baseTestJob).(*beeremote.Job)
	job.ExternalId = ""
	job.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation:  flex.SyncJob_UPLOAD,
			RemotePath: job.Request.Path,
		},
	}

	_, err := client.GenerateWorkRequests(context.Background(), nil, job, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrJobFailedPrecondition)
	assert.ErrorContains(t, err, "check that this version of the beegfs CLI is compatible")
}

// TestGenerateWorkRequestsDefaultRemotePath checks the key GenerateWorkRequests picks when the
// request names none. A download reuses the key of the last synced job, because that is where the
// object was last written or read. An upload always uses the in-mount path, so a push can never
// overwrite an object that an earlier pull named with --remote-path.
//
// The key is set on the request before the client takes the content lock. The mock file system
// has no file, so the call fails after that point and the test reads the key the call left behind.
func TestGenerateWorkRequestsDefaultRemotePath(t *testing.T) {
	syncedJob := func(remotePath string) *beeremote.Job {
		return &beeremote.Job{
			Request: &beeremote.JobRequest{
				Path: "/foo/bar",
				Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
					Operation:  flex.SyncJob_DOWNLOAD,
					RemotePath: remotePath,
				}},
			},
			Status: &beeremote.Job_Status{State: beeremote.Job_COMPLETED},
		}
	}

	tests := []struct {
		name          string
		operation     flex.SyncJob_Operation
		remotePath    string
		lastSyncedJob *beeremote.Job
		want          string
	}{
		{
			name:          "upload without a key ignores the last synced key",
			operation:     flex.SyncJob_UPLOAD,
			lastSyncedJob: syncedJob("helloworld"),
			want:          "foo/bar",
		},
		{
			name:          "upload keeps the caller's key",
			operation:     flex.SyncJob_UPLOAD,
			remotePath:    "custom",
			lastSyncedJob: syncedJob("helloworld"),
			want:          "custom",
		},
		{
			name:          "download without a key reuses the last synced key",
			operation:     flex.SyncJob_DOWNLOAD,
			lastSyncedJob: syncedJob("helloworld"),
			want:          "helloworld",
		},
		{
			name:      "download without a key or history uses the in-mount path",
			operation: flex.SyncJob_DOWNLOAD,
			want:      "foo/bar",
		},
		{
			name:          "download keeps the caller's key",
			operation:     flex.SyncJob_DOWNLOAD,
			remotePath:    "custom",
			lastSyncedJob: syncedJob("helloworld"),
			want:          "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestS3Client(t, filesystem.NewMockFS())
			job := proto.Clone(baseTestJob).(*beeremote.Job)
			job.ExternalId = ""
			job.Request.Type = &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
				Operation:  tt.operation,
				RemotePath: tt.remotePath,
			}}

			_, err := client.GenerateWorkRequests(context.Background(), tt.lastSyncedJob, job, 1)
			require.Error(t, err, "the mock file system has no file, so the call must fail after choosing the key")
			assert.Equal(t, tt.want, job.GetRequest().GetSync().GetRemotePath())
		})
	}
}

// More complex testing around completing requests is not possible without mocking.
// For now just verify errors are returned when job type or op is not supported.
func TestCompleteRequests(t *testing.T) {
	testS3Client := newTestS3Client(t, filesystem.NewMockFS())
	workResponses := make([]*flex.Work, 0)
	// If an invalid job type is specified for this RST return the correct error:
	jobMock := proto.Clone(baseTestJob).(*beeremote.Job)
	jobMock.Request.Type = &beeremote.JobRequest_Mock{Mock: &flex.MockJob{}}
	err := testS3Client.CompleteWorkRequests(context.Background(), jobMock, workResponses, true)
	assert.ErrorIs(t, err, ErrReqAndRSTTypeMismatch)

	// If the the job type is correct but the operation is not specified/supported, return the correct error:
	jobSyncInvalid := proto.Clone(baseTestJob).(*beeremote.Job)
	jobSyncInvalid.Request.Type = &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}}
	err = testS3Client.CompleteWorkRequests(context.Background(), jobSyncInvalid, workResponses, true)
	assert.ErrorIs(t, err, ErrUnsupportedOpForRST)
}

// countingHeadObjectClient records HeadObject calls so a test can assert readiness was answered
// without a remote round trip. Embedding s3ApiClient leaves every other method unimplemented, so an
// unexpected call panics rather than passing quietly.
type countingHeadObjectClient struct {
	s3ApiClient
	headObjectCalls int
}

func (c *countingHeadObjectClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.headObjectCalls++
	// ContentLength and LastModified are dereferenced unconditionally by getObjectMetadata.
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(0), LastModified: aws.Time(time.Time{})}, nil
}

// TestIsWorkRequestReadySkipsRemoteCallsWhileShuttingDown covers the one case that would otherwise
// hold a shutdown open: an archived download costs a HeadObject, and a RestoreObject on top of it,
// only to come back not ready and be rescheduled anyway.
func TestIsWorkRequestReadySkipsRemoteCallsWhileShuttingDown(t *testing.T) {
	api := &countingHeadObjectClient{}
	client := newTestS3Client(t, nil, withTestApiClient(api))

	request := &flex.WorkRequest{Type: &flex.WorkRequest_Sync{Sync: &flex.SyncJob{
		Operation:  flex.SyncJob_DOWNLOAD,
		RemotePath: "archived-object",
		LockedInfo: &flex.JobLockedInfo{IsArchived: true},
	}}}

	shutdownCtx, shutdown := context.WithCancel(context.Background())
	shutdown()

	ready, delay, err := client.IsWorkRequestReady(shutdownCtx, context.Background(), request)
	require.NoError(t, err, "a shutdown is not a failure of the request")
	assert.False(t, ready, "the request must be reported not ready so the caller reschedules it")
	assert.Zero(t, delay, "how long to wait before rechecking is left to the caller")
	assert.Zero(t, api.headObjectCalls, "readiness must not cost a remote round trip while shutting down")

	// The same request must still reach S3 when nothing is shutting down, so the assertion above is
	// about the shutdown rather than the request being skipped for some unrelated reason.
	_, _, _ = client.IsWorkRequestReady(context.Background(), context.Background(), request)
	assert.Equal(t, 1, api.headObjectCalls)
}

// TestIsWorkStarted covers the tri-state the download abort path dispatches on. A part that has
// never run reports false; only a result predating the Started field reports unknown.
func TestIsWorkStarted(t *testing.T) {
	part := func(started *bool) *flex.Work_Part { return &flex.Work_Part{Started: started} }
	work := func(parts ...*flex.Work_Part) []*flex.Work {
		return []*flex.Work{{Parts: parts}}
	}

	tests := []struct {
		name    string
		results []*flex.Work
		want    *bool
	}{
		{name: "no results", results: nil, want: new(false)},
		{name: "no parts", results: []*flex.Work{{}}, want: new(false)},
		{
			// This is the case newWorkFromRequest produces for a freshly built request. It must be
			// distinguishable from a legacy result or the abort path stubs a file it never touched.
			name:    "parts built but never run",
			results: work(part(new(false)), part(new(false))),
			want:    new(false),
		},
		{name: "a part has run", results: work(part(new(false)), part(new(true))), want: new(true)},
		{name: "every part has run", results: work(part(new(true))), want: new(true)},
		{
			// Only produced by a sync that predates the Started field.
			name:    "a part predates the Started field",
			results: work(part(new(false)), part(nil)),
			want:    nil,
		},
		{name: "a nil part", results: work(nil), want: nil},
		{
			// Unknown wins over not-started, but a part known to have run is decisive.
			name:    "a started part alongside a legacy part",
			results: work(part(new(true)), part(nil)),
			want:    new(true),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isWorkStarted(tt.results)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, *tt.want, *got)
		})
	}
}

// abortFS records the filesystem calls the download abort path makes. CreateWriteClose always
// fails so the stub branch stops before CreateOffloadedDataFile reaches beegfs, which lets the test
// observe that the branch was taken without needing a live management service.
type abortFS struct {
	filesystem.Provider
	stubWrites []string
	removed    []string
	resized    map[string]int64
	chtimes    map[string]time.Time
	// sizes holds the size Lstat reports for each path. A path missing from it does not exist.
	sizes map[string]int64
	// mtimes holds the mtime Lstat reports for each path in sizes.
	mtimes map[string]time.Time
	lstats int
}

var errStubWritten = errors.New("stub write reached")

func newAbortFS() *abortFS {
	return &abortFS{resized: map[string]int64{}, chtimes: map[string]time.Time{}, sizes: map[string]int64{}, mtimes: map[string]time.Time{}}
}

func (f *abortFS) Lstat(path string) (os.FileInfo, error) {
	f.lstats++
	size, ok := f.sizes[path]
	if !ok {
		return nil, &fs.PathError{Op: "lstat", Path: path, Err: fs.ErrNotExist}
	}
	return sizedFileInfo{name: filepath.Base(path), size: size, mtime: f.mtimes[path]}, nil
}

// sizedFileInfo is a regular file that reports only a name, a size and an mtime.
type sizedFileInfo struct {
	name  string
	size  int64
	mtime time.Time
}

func (i sizedFileInfo) Name() string       { return i.name }
func (i sizedFileInfo) Size() int64        { return i.size }
func (i sizedFileInfo) Mode() fs.FileMode  { return 0644 }
func (i sizedFileInfo) ModTime() time.Time { return i.mtime }
func (i sizedFileInfo) IsDir() bool        { return false }
func (i sizedFileInfo) Sys() any           { return nil }

func (f *abortFS) CreateWriteClose(path string, buf []byte, mode uint32, overwrite bool) error {
	f.stubWrites = append(f.stubWrites, string(buf))
	return errStubWritten
}

func (f *abortFS) Remove(path string) error {
	f.removed = append(f.removed, path)
	return nil
}

func (f *abortFS) CreateOrResizeFile(path string, size int64, overwrite bool) error {
	f.resized[path] = size
	return nil
}

func (f *abortFS) Chtimes(path string, atime time.Time, mtime time.Time) error {
	f.chtimes[path] = mtime
	return nil
}

func newDownloadAbortJob(lockedInfo *flex.JobLockedInfo) *beeremote.Job {
	return &beeremote.Job{
		Request: &beeremote.JobRequest{
			Path:                "/mnt/dest/file",
			RemoteStorageTarget: 2,
			Type: &beeremote.JobRequest_Sync{
				Sync: &flex.SyncJob{
					Operation:  flex.SyncJob_DOWNLOAD,
					RemotePath: "/bucket/key",
					LockedInfo: lockedInfo,
				},
			},
		},
		Status: &beeremote.Job_Status{},
	}
}

// contentLock is a content lock check for tests. It reports locked for the paths it holds, and
// returns err for every path when err is set.
type contentLock struct {
	locked map[string]bool
	err    error
}

func (c contentLock) check(ctx context.Context, path string) (bool, error) {
	return c.locked[path], c.err
}

// TestCompleteSyncWorkRequestsDownloadAbort covers how an aborted download resolves the local file.
// The abort decides from the work results first, then from the locked info the job was planned
// with. It must be safe to run again on a file it already resolved.
func TestCompleteSyncWorkRequestsDownloadAbort(t *testing.T) {
	const path = "/mnt/dest/file"
	mtime := timestamppb.New(time.Unix(1700000000, 0))
	notStarted := []*flex.Work{{Parts: []*flex.Work_Part{{Started: new(false)}}}}
	locked := contentLock{locked: map[string]bool{path: true}}
	unlocked := contentLock{}

	t.Run("a download that never began removes the locked file created for it", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		// The path did not exist before the job. This is the state prepareDownloadNoFile leaves:
		// the new file's attributes, with Exists still false.
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: false, ReadWriteLocked: true, Size: 500, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, []string{path}, fs.removed)
		assert.Empty(t, fs.stubWrites, "an untouched file must not be replaced with a stub")
	})

	t.Run("a reservation removes its locked placeholder", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		// A RESERVED job's request was built before the placeholder existed, so it records no
		// attributes for the file. A RESERVED job has no work results.
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: false, RemoteSize: 500})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, nil, true))

		assert.Equal(t, []string{path}, fs.removed)
	})

	t.Run("an unlocked file at a path that had no file is left alone", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(unlocked.check))
		// Someone else put this file there, or there is no file at all. Neither is the job's.
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: false, ReadWriteLocked: true, RemoteSize: 500, Mtime: mtime})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Empty(t, fs.removed)
		assert.Empty(t, fs.resized, "an abort must not create the file it never had")
		assert.Empty(t, fs.chtimes)
		assert.Empty(t, fs.stubWrites)
	})

	t.Run("a lock check that fails removes nothing", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(contentLock{err: errors.New("meta unreachable")}.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: false, RemoteSize: 500})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true)

		require.ErrorContains(t, err, "meta unreachable")
		assert.Empty(t, fs.removed)
	})

	t.Run("a download that never began restores an enlarged file", func(t *testing.T) {
		fs := newAbortFS()
		fs.sizes[path] = 500
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		// The file was preallocated out to the remote object's size but no bytes were written, so
		// truncating back to its original size restores the original contents.
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, map[string]int64{path: 100}, fs.resized)
		assert.Equal(t, mtime.AsTime(), fs.chtimes[path])
		assert.Empty(t, fs.stubWrites)
		assert.Empty(t, fs.removed)
	})

	t.Run("a download that never began leaves a file that was not enlarged at its size", func(t *testing.T) {
		fs := newAbortFS()
		fs.sizes[path] = 800
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 800, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Empty(t, fs.resized, "a file that was never enlarged must not be resized")
		assert.Equal(t, mtime.AsTime(), fs.chtimes[path])
	})

	t.Run("an abort that runs again restores a file it already shrank", func(t *testing.T) {
		fs := newAbortFS()
		// An earlier abort shrank the file back to its original size.
		fs.sizes[path] = 100
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, mtime.AsTime(), fs.chtimes[path])
	})

	t.Run("an unlocked existing file that an earlier abort restored is left alone", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(unlocked.check))
		// The earlier abort set the stop mtime, and remote then released the lock.
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})
		job.SetStopMtime(mtime)

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Zero(t, fs.lstats, "a restored file must not be checked again")
		assert.Empty(t, fs.resized)
		assert.Empty(t, fs.chtimes)
		assert.Empty(t, fs.removed)
	})

	t.Run("an unlocked existing file in its original state is left alone", func(t *testing.T) {
		fs := newAbortFS()
		fs.sizes[path] = 100
		fs.mtimes[path] = mtime.AsTime()
		r := newTestS3Client(t, fs, withContentLockCheck(unlocked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, mtime.AsTime(), job.GetStopMtime().AsTime())
		assert.Empty(t, fs.resized)
		assert.Empty(t, fs.chtimes)
	})

	t.Run("an unlocked existing file that was removed is left alone", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(unlocked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.True(t, job.HasStopMtime(), "a later abort must see this one finished")
		assert.Empty(t, fs.resized, "an abort must not create the file")
		assert.Empty(t, fs.chtimes)
	})

	t.Run("an unlocked existing file that changed is left alone", func(t *testing.T) {
		fs := newAbortFS()
		fs.sizes[path] = 500
		fs.mtimes[path] = mtime.AsTime()
		r := newTestS3Client(t, fs, withContentLockCheck(unlocked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Empty(t, fs.resized, "a file without the lock is not the job's to change")
		assert.Empty(t, fs.chtimes)
	})

	t.Run("an existing file whose size changed is not restored", func(t *testing.T) {
		fs := newAbortFS()
		// Neither the size the plan left (500) nor the original size (100).
		fs.sizes[path] = 700
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true)

		require.ErrorContains(t, err, "its size is 700 but the job left it at 500")
		assert.Empty(t, fs.resized, "data written to the file must not be truncated")
		assert.Empty(t, fs.chtimes)
	})

	t.Run("an existing file that is gone is not restored", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true)

		require.ErrorIs(t, err, os.ErrNotExist)
		assert.Empty(t, fs.resized, "an abort must not create the file")
		assert.Empty(t, fs.chtimes)
	})

	for _, tc := range []struct {
		name    string
		results []*flex.Work
	}{
		{"a download that began", []*flex.Work{{Parts: []*flex.Work_Part{{Started: new(false)}, {Started: new(true), Completed: true}}}}},
		// Parts predating the Started field. Whether bytes were written is unknowable.
		{"a download of unknown progress", []*flex.Work{{Parts: []*flex.Work_Part{{Started: nil}}}}},
	} {
		t.Run(tc.name+" but did not finish is replaced with a stub", func(t *testing.T) {
			fs := newAbortFS()
			r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
			job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime})

			err := r.completeSyncWorkRequests_Download(context.Background(), job, tc.results, true)

			// abortFS fails every stub write, so the error shows the stub branch was taken.
			require.ErrorIs(t, err, errStubWritten)
			assert.ErrorContains(t, err, "failed to replace incomplete download with stub file")
			require.Len(t, fs.stubWrites, 1)
			assert.Equal(t, "rst://2:/bucket/key\n", fs.stubWrites[0])
			assert.Empty(t, fs.resized, "partially written contents must not be passed off as the original")
			assert.Empty(t, fs.removed)
		})
	}

	t.Run("a download that finished is left as it is", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs, withContentLockCheck(locked.check))
		// A finished download whose job was relabelled FAILED to keep the lock, then cancelled.
		finished := []*flex.Work{
			{Parts: []*flex.Work_Part{{Started: new(true), Completed: true}}},
			{Parts: []*flex.Work_Part{{Started: new(true), Completed: true}}},
		}
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: false, ReadWriteLocked: true, Size: 500, RemoteSize: 500, Mtime: mtime})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, finished, true))

		assert.Empty(t, fs.stubWrites, "a finished download must not be stubbed again")
		assert.Empty(t, fs.removed)
		assert.Empty(t, fs.resized)
		assert.Empty(t, fs.chtimes)
	})

	t.Run("a request that was never planned changes nothing", func(t *testing.T) {
		fs := newAbortFS()
		r := newTestS3Client(t, fs)

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), newDownloadAbortJob(nil), nil, true))

		assert.Empty(t, fs.removed)
		assert.Empty(t, fs.chtimes)
	})
}

// abortUploadClient answers AbortMultipartUpload with err and records the upload IDs it was asked
// to abort.
type abortUploadClient struct {
	s3ApiClient
	err     error
	aborted []string
}

func (c *abortUploadClient) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	c.aborted = append(c.aborted, aws.ToString(params.UploadId))
	return &s3.AbortMultipartUploadOutput{}, c.err
}

// TestCompleteSyncWorkRequestsUploadAbort covers the upload abort. It never touches the local file.
// It releases the multipart upload and must be safe to repeat.
func TestCompleteSyncWorkRequestsUploadAbort(t *testing.T) {
	newUploadJob := func(jobExternalId string, plannedExternalId string) *beeremote.Job {
		return &beeremote.Job{
			ExternalId: jobExternalId,
			Request: &beeremote.JobRequest{
				Path: "/mnt/src/file",
				Type: &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{
					Operation:  flex.SyncJob_UPLOAD,
					RemotePath: "key",
					LockedInfo: &flex.JobLockedInfo{ExternalId: plannedExternalId},
				}},
			},
			Status: &beeremote.Job_Status{},
		}
	}

	t.Run("a scheduled job aborts its own upload", func(t *testing.T) {
		api := &abortUploadClient{}
		r := newTestS3Client(t, filesystem.NewMockFS(), withTestApiClient(api))

		require.NoError(t, r.completeSyncWorkRequests_Upload(context.Background(), newUploadJob("upload-1", "upload-1"), nil, true))

		assert.Equal(t, []string{"upload-1"}, api.aborted)
	})

	t.Run("a refused claim aborts the upload its plan created", func(t *testing.T) {
		api := &abortUploadClient{}
		r := newTestS3Client(t, filesystem.NewMockFS(), withTestApiClient(api))

		require.NoError(t, r.completeSyncWorkRequests_Upload(context.Background(), newUploadJob("", "upload-2"), nil, true))

		assert.Equal(t, []string{"upload-2"}, api.aborted)
	})

	t.Run("an upload that is already gone counts as aborted", func(t *testing.T) {
		api := &abortUploadClient{err: &smithy.GenericAPIError{Code: "NoSuchUpload"}}
		r := newTestS3Client(t, filesystem.NewMockFS(), withTestApiClient(api))

		require.NoError(t, r.completeSyncWorkRequests_Upload(context.Background(), newUploadJob("upload-3", "upload-3"), nil, true))
	})

	t.Run("other provider errors are returned", func(t *testing.T) {
		api := &abortUploadClient{err: &smithy.GenericAPIError{Code: "AccessDenied"}}
		r := newTestS3Client(t, filesystem.NewMockFS(), withTestApiClient(api))

		require.Error(t, r.completeSyncWorkRequests_Upload(context.Background(), newUploadJob("upload-4", "upload-4"), nil, true))
	})

	t.Run("an upload without a multipart upload has nothing to abort", func(t *testing.T) {
		api := &abortUploadClient{}
		r := newTestS3Client(t, filesystem.NewMockFS(), withTestApiClient(api))

		require.NoError(t, r.completeSyncWorkRequests_Upload(context.Background(), newUploadJob("", ""), nil, true))

		assert.Empty(t, api.aborted)
	})
}

// cancellingRestoreClient answers HeadObject with an archived object and, when RestoreObject is
// called, cancels the shutdown context before failing the way the AWS SDK does for a request that
// was aborted in flight. This reproduces the race the entry guard cannot cover: the shutdown lands
// after readiness has already decided to talk to S3.
type cancellingRestoreClient struct {
	s3ApiClient
	shutdown           context.CancelFunc
	headObjectErr      error
	restoreObjectCalls int
}

func (c *cancellingRestoreClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if c.headObjectErr != nil {
		c.shutdown()
		return nil, c.headObjectErr
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(0),
		LastModified:  aws.Time(time.Time{}),
		StorageClass:  types.StorageClassGlacier,
	}, nil
}

func (c *cancellingRestoreClient) RestoreObject(ctx context.Context, params *s3.RestoreObjectInput, optFns ...func(*s3.Options)) (*s3.RestoreObjectOutput, error) {
	c.restoreObjectCalls++
	c.shutdown()
	return nil, cancelledOperationError("RestoreObject")
}

// cancelledOperationError is what the SDK returns when a request is aborted before any response,
// which is what made these failures look like S3 errors rather than a shutdown.
func cancelledOperationError(operation string) error {
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: operation,
		Err:           context.Canceled,
	}
}

func newArchivedDownloadRequest() *flex.WorkRequest {
	return &flex.WorkRequest{Type: &flex.WorkRequest_Sync{Sync: &flex.SyncJob{
		Operation:  flex.SyncJob_DOWNLOAD,
		RemotePath: "archived-object",
		LockedInfo: &flex.JobLockedInfo{IsArchived: true},
	}}}
}

func newArchivedTestClient(t *testing.T, api s3ApiClient) *S3Client {
	t.Helper()
	s3Config := &flex.RemoteStorageTarget_S3{
		Bucket: "test-bucket",
		StorageClass: []*flex.RemoteStorageTarget_S3_StorageClass{{
			Name: string(types.StorageClassGlacier),
			Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{
				AutoRestore:   true,
				RetentionDays: 7,
				CheckTime:     "5h",
				RecheckTime:   "30m",
			},
		}},
	}
	return newTestS3ClientWithConfig(t, nil, s3Config, withTestApiClient(api))
}

// TestIsWorkRequestReadyReschedulesWhenCancelledInFlight covers the failure that took eight download
// jobs to a terminal FAILED state: a shutdown cancelled RestoreObject mid-request, and because the
// SDK reports that as an operation error rather than an APIError, readiness returned it as a hard
// failure instead of leaving the request resumable.
func TestIsWorkRequestReadyReschedulesWhenCancelledInFlight(t *testing.T) {
	t.Run("restore object", func(t *testing.T) {
		shutdownCtx, shutdown := context.WithCancel(context.Background())
		api := &cancellingRestoreClient{shutdown: shutdown}
		client := newArchivedTestClient(t, api)

		ready, delay, err := client.IsWorkRequestReady(shutdownCtx, context.Background(), newArchivedDownloadRequest())
		require.NoError(t, err, "a shutdown that lands mid-request is not a failure of the request")
		assert.False(t, ready, "the request must be reported not ready so the caller reschedules it")
		assert.Zero(t, delay, "how long to wait before rechecking is left to the caller")
		assert.Equal(t, 1, api.restoreObjectCalls, "the guard must cover the call rather than skip it")
	})

	t.Run("head object", func(t *testing.T) {
		shutdownCtx, shutdown := context.WithCancel(context.Background())
		api := &cancellingRestoreClient{shutdown: shutdown, headObjectErr: cancelledOperationError("HeadObject")}
		client := newArchivedTestClient(t, api)

		ready, _, err := client.IsWorkRequestReady(shutdownCtx, context.Background(), newArchivedDownloadRequest())
		require.NoError(t, err, "a shutdown that lands mid-request is not a failure of the request")
		assert.False(t, ready, "the request must be reported not ready so the caller reschedules it")
	})
}

// TestIsWorkRequestReadyFailsWhenOnlyTheWorkIsCancelled is the other half of the discriminator. An
// explicit `job cancel` cancels the work context but not the shutdown context, and that must stay
// terminal rather than being rescheduled forever.
func TestIsWorkRequestReadyFailsWhenOnlyTheWorkIsCancelled(t *testing.T) {
	api := &cancellingRestoreClient{shutdown: func() {}}
	client := newArchivedTestClient(t, api)

	_, ready, err := client.IsWorkRequestReady(context.Background(), context.Background(), newArchivedDownloadRequest())
	require.Error(t, err, "a cancellation that is not a shutdown must remain a failure")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, ready)
}
