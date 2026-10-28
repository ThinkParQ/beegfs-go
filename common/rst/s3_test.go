package rst

import (
	"context"
	"testing"

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
			client := &S3Client{
				config:     &flex.RemoteStorageTarget{Policies: &flex.RemoteStorageTarget_Policies{}},
				mountPoint: filesystem.NewMockFS(),
			}
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
