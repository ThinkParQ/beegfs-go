package workmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestNewWorkResponseFromRequest(t *testing.T) {

	workRequest := workRequest{
		flex.WorkRequest_builder{
			JobId:     "123",
			RequestId: "456",
			Segment: flex.WorkRequest_Segment_builder{
				OffsetStart: 0,
				OffsetStop:  12,
				PartsStart:  0,
				PartsStop:   10,
			}.Build(),
		}.Build(),
	}

	workResult := newWorkFromRequest(&workRequest)

	assert.Equal(t, "123", workResult.JobId)
	assert.Equal(t, "456", workResult.RequestId)
	assert.Equal(t, flex.Work_SCHEDULED, workResult.Status.GetState())
	assert.Equal(t, "worker node accepted work request", workResult.Status.GetMessage())
	assert.Len(t, workResult.Parts, 11)
}

func TestGenerateParts(t *testing.T) {

	type expectation struct {
		partNum     int32
		offsetStart int64
		offsetStop  int64
	}

	type test struct {
		name     string
		segment  *flex.WorkRequest_Segment
		expected []expectation // map parts to a slice of tuples (offsetStart, offsetStop)
	}

	tests := []test{
		{
			name:     "test when the offset stop is -1 signaling an empty file",
			segment:  flex.WorkRequest_Segment_builder{OffsetStart: 0, OffsetStop: -1, PartsStart: 1, PartsStop: 2}.Build(),
			expected: []expectation{{1, 0, -1}, {-1, -1, -1}},
		},
		{
			name:     "test when offsets evenly divide into parts",
			segment:  flex.WorkRequest_Segment_builder{OffsetStart: 0, OffsetStop: 99, PartsStart: 1, PartsStop: 10}.Build(),
			expected: []expectation{{1, 0, 9}, {2, 10, 19}, {3, 20, 29}, {4, 30, 39}, {5, 40, 49}, {6, 50, 59}, {7, 60, 69}, {8, 70, 79}, {9, 80, 89}, {10, 90, 99}, {-1, -1, -1}},
			// expected: [2]int64{{0, 9}, {10, 19}, {20, 29}, {30, 39}, {40, 49}, {50, 59}, {60, 69}, {70, 79}, {80, 89}, {90, 99}, {-1, -1}},
		},
		{
			name:     "test when offsets don't evenly divide into parts",
			segment:  flex.WorkRequest_Segment_builder{OffsetStart: 0, OffsetStop: 100, PartsStart: 1, PartsStop: 10}.Build(),
			expected: []expectation{{1, 0, 9}, {2, 10, 19}, {3, 20, 29}, {4, 30, 39}, {5, 40, 49}, {6, 50, 59}, {7, 60, 69}, {8, 70, 79}, {9, 80, 89}, {10, 90, 100}, {-1, -1, -1}, {-1, -1, -1}},
		},
		{
			name:     "test when offsets do not start at zero and don't divide evenly into parts",
			segment:  flex.WorkRequest_Segment_builder{OffsetStart: 10, OffsetStop: 110, PartsStart: 2, PartsStop: 7}.Build(),
			expected: []expectation{{2, 10, 25}, {3, 26, 41}, {4, 42, 57}, {5, 58, 73}, {6, 74, 89}, {7, 90, 110}, {-1, -1, -1}},
		},
	}

	for _, test := range tests {
		partsGenerator := generatePartsFromSegment(test.segment)
		for _, expected := range test.expected {
			partNumber, offsetStart, offsetStop := partsGenerator()
			assert.Equal(t, expected.partNum, partNumber, "PartNumber does not match for segment %v of test: %s", test.segment, test.name)
			assert.Equal(t, expected.offsetStart, offsetStart, "OffsetStart does not match for segment %v of test: %s", test.segment, test.name)
			assert.Equal(t, expected.offsetStop, offsetStop, "OffsetStop does not match for segment %v of test: %s", test.segment, test.name)
		}
	}

}

func TestGetSubmissionIDPriorityRanges(t *testing.T) {
	expectedRangeValues := [6]string{"0000000000000", "4000000000000", "a000000000000", "e000000000000", "i000000000000", "m000000000000"}
	expectedPriorityCount := len(expectedRangeValues) - 1
	count, ranges := getSubmissionIDPriorityRanges()
	assert.Equal(t, count, expectedPriorityCount, "Unexpected priority count")
	assert.EqualValues(t, ranges, expectedRangeValues, "Unexpected priority submission id range")
}

type submissionExpectation struct {
	name          string
	priority      int32
	baseKey       string
	wantID        string
	wantPriority  int32
	wantBaseKey   string
	wantIncrement string
}

func TestSubmissionIDFunctions(t *testing.T) {

	tests := []submissionExpectation{
		{
			name:          "test lowest priority",
			priority:      0,
			baseKey:       "0000000000000",
			wantID:        "0000000000000",
			wantPriority:  0,
			wantBaseKey:   "0000000000000",
			wantIncrement: "0000000000001",
		},
		{
			name:          "test highest priority",
			priority:      4,
			baseKey:       "0000000000000",
			wantID:        "i000000000000",
			wantPriority:  4,
			wantBaseKey:   "0000000000000",
			wantIncrement: "i000000000001",
		},
		{
			name:          "test when submission id is the largest uint64 value and is incremented",
			priority:      3,
			baseKey:       "3w5e11264sgsf", // is the highest value uint64 can represent in base-36
			wantID:        "hw5e11264sgsf",
			wantPriority:  3,
			wantBaseKey:   "3w5e11264sgsf",
			wantIncrement: "e000000000000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			submissionId := createSubmissionID(test.baseKey, test.priority)
			assert.Equal(t, test.wantID, submissionId, "createSubmissionID(%s, %d)", test.baseKey, test.priority)

			priority := submissionIDPriority(submissionId)
			assert.Equal(t, test.wantPriority, priority, "submissionIDPriority(%s)", submissionId)

			base := submissionBaseKey(submissionId)
			assert.Equal(t, test.wantBaseKey, base, "submissionBaseKey(%s)", submissionId)

			next, err := incrementSubmissionID(submissionId)
			assert.NoError(t, err, "unexpected error for incrementSubmissionID(%s)", submissionId)
			assert.Equal(t, test.wantIncrement, next, "incrementSubmissionID(%s)", submissionId)
		})
	}
}
