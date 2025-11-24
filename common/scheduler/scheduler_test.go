package scheduler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type submissionExpectation struct {
	name                 string
	workRequestPriority  int32
	baseKey              string
	expectedSubmissionId string
	expectedPriority     int32
	expectedIncrement    string
}

func TestSubmissionIDFunctions(t *testing.T) {

	tests := []submissionExpectation{
		{
			name:                 "test submissionId from previous release",
			workRequestPriority:  0,
			baseKey:              "0000000000000",
			expectedSubmissionId: "a000000000000",
			expectedPriority:     3,
			expectedIncrement:    "a000000000001",
		},
		{
			name:                 "test lowest priority",
			workRequestPriority:  1,
			baseKey:              "0000000000000",
			expectedSubmissionId: "0000000000000",
			expectedPriority:     1,
			expectedIncrement:    "0000000000001",
		},
		{
			name:                 "test highest priority",
			workRequestPriority:  5,
			baseKey:              "0000000000000",
			expectedSubmissionId: "i000000000000",
			expectedPriority:     5,
			expectedIncrement:    "i000000000001",
		},
		{
			name:                 "test when submission id is the largest uint64 value and is incremented",
			workRequestPriority:  3,
			baseKey:              "3w5e11264sgsf", // is the highest value uint64 can represent in base-36
			expectedSubmissionId: "dw5e11264sgsf",
			expectedPriority:     3,
			expectedIncrement:    "a000000000000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			submissionId, _ := CreateSubmissionId(test.baseKey, test.workRequestPriority)
			assert.Equal(t, test.expectedSubmissionId, submissionId, "createSubmissionID(%s, %d)", test.baseKey, test.workRequestPriority)

			workRequestPriority := submissionIdPriority(submissionId) + 1
			assert.Equal(t, test.expectedPriority, workRequestPriority, "submissionIDPriority(%s)", submissionId)

			base := submissionBaseKey(submissionId)
			assert.Equal(t, test.baseKey, base, "submissionBaseKey(%s)", submissionId)

			next, workRequestPriority, err := IncrementSubmissionId(submissionId)
			assert.NoError(t, err, "unexpected error for incrementSubmissionID(%s)", submissionId)
			assert.Equal(t, test.expectedIncrement, next, "incrementSubmissionID(%s)", submissionId)

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
func TestGetNextPriority(t *testing.T) {
	nextPriority := getNextPriorityFunc()

	var ok bool
	var priority int
	for range 2 {
		var startPoints []int
		var counts [priorityLevels]int
		for range priorityLevels {
			start := true
			for priority, ok = nextPriority(); ok; priority, ok = nextPriority() {
				if start {
					startPoints = append(startPoints, priority)
					start = false
				}
				counts[priority]++
			}
		}

		assert.Equal(t, startPoints, []int{0, 1, 2, 3, 4})
		for priority = range priorityLevels {
			assert.Equal(t, counts[priority], priorityLevels)
		}
	}
}

func BenchmarkDistributeTokensEvenWork(b *testing.B) {
	weights := geometricFairnessWeights(STRONG)
	tokens := 100
	for exponent := 1; exponent <= 3; exponent++ {
		tokens *= 10
		b.Run(fmt.Sprintf("tokens_%d", tokens), func(b *testing.B) {
			s := &Scheduler{log: zap.NewNop()}
			for priority := range priorityLevels {
				s.workTokens[priority].Store(int32(tokens))
			}

			distributeTokens := s.getDistributeTokensFunc(weights)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				distributeTokens(tokens)
			}
		})
	}
}

func BenchmarkDistributeTokensUnevenWork(b *testing.B) {
	weights := geometricFairnessWeights(STRONG)
	testCases := []struct {
		name string
		work [priorityLevels]int32
	}{
		{name: "head_heavy", work: [priorityLevels]int32{10000, 2000, 500, 200, 100}},
		{name: "middle_hot", work: [priorityLevels]int32{1000, 2000, 4000, 2000, 1000}},
		{name: "tail_heavy", work: [priorityLevels]int32{100, 200, 500, 2000, 10000}},
		{name: "single_queue", work: [priorityLevels]int32{0, 0, 10000, 0, 0}},
		{name: "couple_queues", work: [priorityLevels]int32{10000, 0, 0, 0, 10000}},
		{name: "some_queues", work: [priorityLevels]int32{10000, 0, 10000, 0, 10000}},
	}

	tokens := 100
	for exponent := 1; exponent <= 3; exponent++ {
		tokens *= 10
		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				s := &Scheduler{log: zap.NewNop()}
				for priority := range priorityLevels {
					s.workTokens[priority].Store(tc.work[priority])
				}

				distributeTokens := s.getDistributeTokensFunc(weights)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					distributeTokens(tokens)
				}
			})
		}
	}
}
