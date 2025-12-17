package workmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type submissionExpectation struct {
	name                 string
	priority             int32
	baseKey              string
	expectedSubmissionId string
	expectedPriority     int32
	expectedIncrement    string
}

func TestSubmissionIDFunctions(t *testing.T) {

	tests := []submissionExpectation{
		{
			name:                 "test submissionId from previous release",
			priority:             0,
			baseKey:              "0000000000000",
			expectedSubmissionId: "a000000000000",
			expectedPriority:     3,
			expectedIncrement:    "a000000000001",
		},
		{
			name:                 "test lowest priority",
			priority:             1,
			baseKey:              "0000000000000",
			expectedSubmissionId: "0000000000000",
			expectedPriority:     1,
			expectedIncrement:    "4000000000001",
		},
		{
			name:                 "test highest priority",
			priority:             5,
			baseKey:              "0000000000000",
			expectedSubmissionId: "i000000000000",
			expectedPriority:     5,
			expectedIncrement:    "i000000000001",
		},
		{
			name:                 "test when submission id is the largest uint64 value and is incremented",
			priority:             3,
			baseKey:              "3w5e11264sgsf", // is the highest value uint64 can represent in base-36
			expectedSubmissionId: "dw5e11264sgsf",
			expectedPriority:     3,
			expectedIncrement:    "a000000000000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			submissionId, _ := CreateSubmissionId(test.baseKey, test.priority)
			assert.Equal(t, test.expectedSubmissionId, submissionId, "createSubmissionID(%s, %d)", test.baseKey, test.priority)

			workRequestPriority := submissionIdPriority(submissionId) + 1
			assert.Equal(t, test.expectedPriority, workRequestPriority, "submissionIDPriority(%s)", submissionId)

			base := submissionBaseKey(submissionId)
			assert.Equal(t, test.baseKey, base, "submissionBaseKey(%s)", submissionId)

			demoted, workRequestPriority := DemoteSubmissionId(submissionId)
			wantDemotedPriority := test.expectedPriority
			if wantDemotedPriority < priorityLevels {
				wantDemotedPriority++
			}
			assert.Equal(t, wantDemotedPriority, workRequestPriority, "submissionIDPriority(demoteSubmissionId(%s))", submissionId)
			assert.Equal(t, test.baseKey, submissionBaseKey(demoted), "submissionBaseKey(demoteSubmissionId(%s))", submissionId)

			promoted, workRequestPriority := PromoteSubmissionId(submissionId)
			wantPromotedPriority := test.expectedPriority
			if wantPromotedPriority > 1 {
				wantPromotedPriority--
			}
			assert.Equal(t, wantPromotedPriority, workRequestPriority, "submissionIDPriority(promoteSubmissionId(%s))", submissionId)
			assert.Equal(t, test.baseKey, submissionBaseKey(promoted), "submissionBaseKey(promoteSubmissionId(%s))", submissionId)
		})
	}
}
