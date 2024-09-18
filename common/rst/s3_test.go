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
)

// A client that can be used for methods that don't actually require interacting with a real S3
// bucket. Note more complex unit tests would require reworking S3Client.client into an interface so
// it could be mocked. These could be based off the previous sync_tests that existed before the RST
// and Job packages were refactored:
// https://github.com/ThinkParQ/bee-remote/blob/6cdea09384f4dc4446fec61ab968565a7c649a65/internal/job/sync_test.go
var testS3Client = &S3Client{
	config: &flex.RemoteStorageTarget{
		Policies: &flex.RemoteStorageTarget_Policies{},
	},
}

func TestGenerateRequests(t *testing.T) {
	var err error
	mp := filesystem.NewMockFS()
	mp.CreateWriteClose(baseTestJob.Request.GetPath(), make([]byte, 1023))
	// Ensure fast start max size is less than the size used for the mock file (1023). This way the
	// client doesn't try to create a multi-part upload, which can't be done without a real bucket.
	testS3Client.config.Policies.FastStartMaxSize = 1024
	testS3Client.mountPoint = mp

	jobWithNoExternalID := proto.Clone(baseTestJob).(*beeremote.Job)
	jobWithNoExternalID.ExternalId = ""

	// Verify supported ops generate the correct request types:
	jobSyncUpload := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncUpload.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation: flex.SyncJob_UPLOAD,
		},
	}
	jobSyncDownload := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncDownload.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation: flex.SyncJob_DOWNLOAD,
		},
	}
	testJobs := []*beeremote.Job{jobSyncUpload, jobSyncDownload}
	// TODO: https://github.com/thinkparq/gobee/issues/28
	// Also test flex.SyncJob_DOWNLOAD once we have an s3MockProvider.
	for i, op := range []flex.SyncJob_Operation{flex.SyncJob_UPLOAD} {
		requests, retry, err := testS3Client.GenerateWorkRequests(context.Background(), testJobs[i], 1)
		assert.NoError(t, err)
		assert.True(t, retry)
		require.Len(t, requests, 1)
		assert.Equal(t, "", requests[0].ExternalId)
		assert.Equal(t, op, requests[0].GetSync().Operation)
	}

	// If an invalid job type is specified for this RST return the correct error:
	jobMock := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobMock.Request.Type = &beeremote.JobRequest_Mock{Mock: &flex.MockJob{}}
	_, retry, err := testS3Client.GenerateWorkRequests(context.Background(), jobMock, 1)
	assert.ErrorIs(t, err, ErrReqAndRSTTypeMismatch)
	assert.False(t, retry)

	// If the the job type is correct but the operation is not specified/supported, return the correct error:
	jobSyncInvalid := proto.Clone(jobWithNoExternalID).(*beeremote.Job)
	jobSyncInvalid.Request.Type = &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{}}
	_, retry, err = testS3Client.GenerateWorkRequests(context.Background(), jobSyncInvalid, 1)
	assert.ErrorIs(t, err, ErrUnsupportedOpForRST)
	assert.False(t, retry)

	// If the job already as an external ID it should not be allowed to generate new requests.
	jobWithExternalID := proto.Clone(baseTestJob).(*beeremote.Job)
	_, retry, err = testS3Client.GenerateWorkRequests(context.Background(), jobWithExternalID, 1)
	assert.ErrorIs(t, err, ErrJobAlreadyHasExternalID)
	assert.False(t, retry)
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
