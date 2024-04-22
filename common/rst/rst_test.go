package rst

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// Use to easily create jobs using proto.Clone():
var baseTestJob = &beeremote.Job{
	Id:         "0",
	ExternalId: "1234",
	Request: &beeremote.JobRequest{
		Path:                "/foo/bar",
		RemoteStorageTarget: "1",
	},
	Status: &beeremote.Job_Status{
		State:   beeremote.Job_SCHEDULED,
		Message: "hello world",
	},
}

// Use to easily create segments using getNewTestSegments():
var baseTestSegments = []*flex.WorkRequest_Segment{
	{
		OffsetStart: 0,
		OffsetStop:  1024,
		PartsStart:  1,
		PartsStop:   10,
	},
	{
		OffsetStart: 1025,
		OffsetStop:  2048,
		PartsStart:  11,
		PartsStop:   20,
	},
}

// Test helper function used to get a deep copy of the fromSegments slice.
func getNewTestSegments(fromSegments []*flex.WorkRequest_Segment) []*flex.WorkRequest_Segment {
	toSegments := []*flex.WorkRequest_Segment{}
	for _, s := range fromSegments {
		segment := proto.Clone(s).(*flex.WorkRequest_Segment)
		toSegments = append(toSegments, segment)
	}
	return toSegments
}

func TestRecreateWorkRequests(t *testing.T) {

	jobSync := proto.Clone(baseTestJob).(*beeremote.Job)
	jobSync.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation: flex.SyncJob_UPLOAD,
		},
	}
	jobMock := proto.Clone(baseTestJob).(*beeremote.Job)
	jobMock.Request.Type = &beeremote.JobRequest_Mock{
		Mock: &flex.MockJob{
			NumTestSegments: 2,
		},
	}

	syncRequests := RecreateWorkRequests(jobSync, getNewTestSegments(baseTestSegments))
	mockRequests := RecreateWorkRequests(jobMock, getNewTestSegments(baseTestSegments))
	for i, reqs := range [][]*flex.WorkRequest{syncRequests, mockRequests} {
		require.Len(t, reqs, len(baseTestSegments))
		for j, req := range reqs {
			assert.Equal(t, baseTestJob.Id, req.JobId)
			assert.Equal(t, strconv.Itoa(j), req.RequestId)
			assert.Equal(t, baseTestJob.ExternalId, req.ExternalId)
			assert.Equal(t, baseTestJob.Request.Path, req.Path)
			assert.True(t, proto.Equal(baseTestSegments[j], req.Segment))
			assert.Equal(t, baseTestJob.Request.RemoteStorageTarget, req.RemoteStorageTarget)

			switch i {
			case 0:
				assert.Equal(t, flex.SyncJob_UPLOAD, req.GetSync().Operation)
			case 1:
				assert.Equal(t, int32(2), req.GetMock().NumTestSegments)
			default:
				t.FailNow()
				assert.FailNow(t, "unknown request type", "does the test need to be updated?")
			}
		}
	}

	jobInvalid := proto.Clone(baseTestJob).(*beeremote.Job)
	invalidRequests := RecreateWorkRequests(jobInvalid, getNewTestSegments(baseTestSegments))
	assert.Nil(t, invalidRequests[0].Type)
}

func TestGenerateSegments(t *testing.T) {
	type expectation struct {
		offsetStart int64
		offsetStop  int64
		partsStart  int32
		partsStop   int32
	}

	type test struct {
		name            string
		fileSize        int64
		segmentCount    int
		partsPerSegment int
		expectations    map[string]expectation
	}

	// Test setup:
	tests := []test{
		{
			name:            "test when the file is empty",
			fileSize:        0,
			segmentCount:    1,
			partsPerSegment: 1,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  0,
					partsStart:  1,
					partsStop:   1,
				},
			},
		}, {
			name:            "test when the file size lets it be split into even segments",
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    2,
			partsPerSegment: 2,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  524287,
					partsStart:  1,
					partsStop:   2,
				},
				"1": {
					offsetStart: 524288,
					offsetStop:  1048575,
					partsStart:  3,
					partsStop:   4,
				},
			},
		}, {
			name:            "test when the file size does not let it be split into even segments",
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    6,
			partsPerSegment: 4,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  174761,
					partsStart:  1,
					partsStop:   4,
				},
				"1": {
					offsetStart: 174762,
					offsetStop:  349523,
					partsStart:  5,
					partsStop:   8,
				},
				"2": {
					offsetStart: 349524,
					offsetStop:  524285,
					partsStart:  9,
					partsStop:   12,
				},
				"3": {
					offsetStart: 524286,
					offsetStop:  699047,
					partsStart:  13,
					partsStop:   16,
				},
				"4": {
					offsetStart: 699048,
					offsetStop:  873809,
					partsStart:  17,
					partsStop:   20,
				},
				"5": {
					offsetStart: 873810,
					offsetStop:  1048575,
					partsStart:  21,
					partsStop:   24,
				},
			},
		},
	}

	for _, test := range tests {
		segments := generateSegments(test.fileSize, int64(test.segmentCount), int32(test.partsPerSegment))
		for j, s := range segments {
			e, ok := test.expectations[strconv.Itoa(j)]
			require.True(t, ok, test.name)
			assert.Equal(t, e.offsetStart, s.OffsetStart, test.name)
			assert.Equal(t, e.offsetStop, s.OffsetStop, test.name)
			assert.Equal(t, e.partsStart, s.PartsStart, test.name)
			assert.Equal(t, e.partsStop, s.PartsStop, test.name)
		}
	}
}
