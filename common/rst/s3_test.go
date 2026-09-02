package rst

import (
	"context"
	"errors"
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

var testS3Client = &S3Client{
	config: &flex.RemoteStorageTarget{
		Policies: &flex.RemoteStorageTarget_Policies{},
	},
	s3Config: &flex.RemoteStorageTarget_S3{
		Bucket: "test-bucket",
	},
}

func TestGenerateWorkRequests(t *testing.T) {
	var err error
	mp := filesystem.NewMockFS()
	mp.CreateWriteClose(baseTestJob.Request.GetPath(), make([]byte, 1023), 0644, false)
	// Ensure fast start max size is less than the size used for the mock file (1023). This way the
	// client doesn't try to create a multi-part upload, which can't be done without a real bucket.
	testS3Client.config.Policies.FastStartMaxSize = 1024
	testS3Client.mountPoint = mp

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

// More complex testing around completing requests is not possible without mocking.
// For now just verify errors are returned when job type or op is not supported.
func TestCompleteRequests(t *testing.T) {
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
	client := &S3Client{
		config:    &flex.RemoteStorageTarget{Policies: &flex.RemoteStorageTarget_Policies{}},
		s3Config:  &flex.RemoteStorageTarget_S3{Bucket: "test-bucket"},
		apiClient: api,
	}

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
}

var errStubWritten = errors.New("stub write reached")

func newAbortFS() *abortFS {
	return &abortFS{resized: map[string]int64{}, chtimes: map[string]time.Time{}}
}

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

// TestCompleteSyncWorkRequestsDownloadAbort covers how an aborted download resolves the local file.
// The recovery branches below the started check were unreachable while parts were built without
// Started, because a nil Started reads as "work began" and stubbed the file instead.
func TestCompleteSyncWorkRequestsDownloadAbort(t *testing.T) {
	mtime := timestamppb.New(time.Unix(1700000000, 0))
	notStarted := []*flex.Work{{Parts: []*flex.Work_Part{{Started: new(false)}}}}

	t.Run("a download that never began removes a file it created", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		// The path did not exist before the job, so nothing local is worth keeping.
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: false})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, []string{"/mnt/dest/file"}, fs.removed)
		assert.Empty(t, fs.stubWrites, "an untouched file must not be replaced with a stub")
	})

	t.Run("a download that never began restores an enlarged file", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		// The file was preallocated out to the remote object's size but no bytes were written, so
		// truncating back to its original size restores the original contents.
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Equal(t, map[string]int64{"/mnt/dest/file": 100}, fs.resized)
		assert.Equal(t, mtime.AsTime(), fs.chtimes["/mnt/dest/file"])
		assert.Empty(t, fs.stubWrites)
		assert.Empty(t, fs.removed)
	})

	t.Run("a download that never began leaves a file that was not enlarged at its size", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 500, RemoteSize: 500, Mtime: mtime,
		})

		require.NoError(t, r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true))

		assert.Empty(t, fs.resized, "a file that was never enlarged must not be resized")
		assert.Equal(t, mtime.AsTime(), fs.chtimes["/mnt/dest/file"])
	})

	t.Run("a download that began is replaced with a stub", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		started := []*flex.Work{{Parts: []*flex.Work_Part{{Started: new(false)}, {Started: new(true)}}}}
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, started, true)

		require.ErrorIs(t, err, errStubWritten)
		assert.ErrorContains(t, err, "failed to replace incomplete download with stub file")
		require.Len(t, fs.stubWrites, 1)
		assert.Equal(t, "rst://2:/bucket/key\n", fs.stubWrites[0])
		assert.Empty(t, fs.resized, "partially written contents must not be passed off as the original")
	})

	t.Run("a download of unknown progress is treated as begun", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		// Parts predating the Started field. Whether bytes were written is unknowable, so the file
		// is stubbed rather than restored, which would present partial contents as the original.
		legacy := []*flex.Work{{Parts: []*flex.Work_Part{{Started: nil}}}}
		job := newDownloadAbortJob(&flex.JobLockedInfo{Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, legacy, true)

		require.ErrorIs(t, err, errStubWritten)
		require.Len(t, fs.stubWrites, 1)
		assert.Empty(t, fs.resized)
	})

	t.Run("an offloaded file that was never downloaded is restored to its original stub", func(t *testing.T) {
		fs := newAbortFS()
		r := &S3Client{mountPoint: fs}
		// Overwrite lets the stub point somewhere other than the download source, so the original
		// url has to be restored rather than the job's.
		job := newDownloadAbortJob(&flex.JobLockedInfo{
			Exists: true, Size: 100, RemoteSize: 500, Mtime: mtime,
			StubUrlRstId: 7, StubUrlPath: "/other-bucket/original-key",
		})

		err := r.completeSyncWorkRequests_Download(context.Background(), job, notStarted, true)

		require.ErrorIs(t, err, errStubWritten)
		assert.ErrorContains(t, err, "failed to restore original stub file")
		require.Len(t, fs.stubWrites, 1)
		assert.Equal(t, "rst://7:/other-bucket/original-key\n", fs.stubWrites[0])
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

func newArchivedTestClient(api s3ApiClient) *S3Client {
	return &S3Client{
		config:    &flex.RemoteStorageTarget{Policies: &flex.RemoteStorageTarget_Policies{}},
		s3Config:  &flex.RemoteStorageTarget_S3{Bucket: "test-bucket"},
		apiClient: api,
		storageClasses: map[types.StorageClass]S3StorageClass{
			types.StorageClassGlacier: {
				archival:      true,
				autoRestore:   true,
				retentionDays: 7,
				checkTime:     5 * time.Hour,
				recheckTime:   30 * time.Minute,
			},
		},
	}
}

// TestIsWorkRequestReadyReschedulesWhenCancelledInFlight covers the failure that took eight download
// jobs to a terminal FAILED state: a shutdown cancelled RestoreObject mid-request, and because the
// SDK reports that as an operation error rather than an APIError, readiness returned it as a hard
// failure instead of leaving the request resumable.
func TestIsWorkRequestReadyReschedulesWhenCancelledInFlight(t *testing.T) {
	t.Run("restore object", func(t *testing.T) {
		shutdownCtx, shutdown := context.WithCancel(context.Background())
		api := &cancellingRestoreClient{shutdown: shutdown}
		client := newArchivedTestClient(api)

		ready, delay, err := client.IsWorkRequestReady(shutdownCtx, context.Background(), newArchivedDownloadRequest())
		require.NoError(t, err, "a shutdown that lands mid-request is not a failure of the request")
		assert.False(t, ready, "the request must be reported not ready so the caller reschedules it")
		assert.Zero(t, delay, "how long to wait before rechecking is left to the caller")
		assert.Equal(t, 1, api.restoreObjectCalls, "the guard must cover the call rather than skip it")
	})

	t.Run("head object", func(t *testing.T) {
		shutdownCtx, shutdown := context.WithCancel(context.Background())
		api := &cancellingRestoreClient{shutdown: shutdown, headObjectErr: cancelledOperationError("HeadObject")}
		client := newArchivedTestClient(api)

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
	client := newArchivedTestClient(api)

	_, ready, err := client.IsWorkRequestReady(context.Background(), context.Background(), newArchivedDownloadRequest())
	require.Error(t, err, "a cancellation that is not a shutdown must remain a failure")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, ready)
}
