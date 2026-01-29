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

func TestNewS3PlacementHint(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		wantName   string
		wantType   s3PlacementHintType
		wantErr    bool
	}{
		{
			name:       "StringType",
			definition: "user:str",
			wantName:   "user",
			wantType:   s3PlacementHintString,
		},
		{
			name:       "IntegerTypeTrimmedAndLowercased",
			definition: "  COUNT:INT  ",
			wantName:   "count",
			wantType:   s3PlacementHintInteger,
		},
		{
			name:       "FloatType",
			definition: "ratio:float",
			wantName:   "ratio",
			wantType:   s3PlacementHintFloat,
		},
		{
			name:       "InvalidDefinition",
			definition: "missingType",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint, err := news3PlacementHint(tt.definition)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, hint.name)
			assert.Equal(t, tt.wantType, hint.hintType)
		})
	}
}

func TestS3PlacementHintString(t *testing.T) {
	tests := []struct {
		name     string
		hintType s3PlacementHintType
		value    string
		expected string
	}{
		{
			name:     "StringPassthrough",
			hintType: s3PlacementHintString,
			value:    "MiXeDCasE",
			expected: "MiXeDCasE",
		},
		{
			name:     "IntegerPositive",
			hintType: s3PlacementHintInteger,
			value:    "10",
			expected: "800000000000000a",
		},
		{
			name:     "IntegerZero",
			hintType: s3PlacementHintInteger,
			value:    "0",
			expected: "8000000000000000",
		},
		{
			name:     "IntegerNegative",
			hintType: s3PlacementHintInteger,
			value:    "-10",
			expected: "7ffffffffffffff6",
		},
		{
			name:     "IntegerParseError",
			hintType: s3PlacementHintInteger,
			value:    "not-an-int",
			expected: "not-an-int",
		},
		{
			name:     "FloatPositive",
			hintType: s3PlacementHintFloat,
			value:    "1.5",
			expected: "bff8000000000000",
		},
		{
			name:     "FloatZero",
			hintType: s3PlacementHintFloat,
			value:    "0",
			expected: "8000000000000000",
		},
		{
			name:     "FloatNegative",
			hintType: s3PlacementHintFloat,
			value:    "-1.5",
			expected: "4007ffffffffffff",
		},
		{
			name:     "FloatParseError",
			hintType: s3PlacementHintFloat,
			value:    "abc",
			expected: "abc",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hint := s3PlacementHint{hintType: test.hintType}
			assert.Equal(t, test.expected, hint.SortableKey(test.value))
		})
	}
}
