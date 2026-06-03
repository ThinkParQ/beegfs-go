package scheduler

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

			next, _, err := IncrementSubmissionId(submissionId)
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

// TestSchedulerMetricGaugeCallback verifies that the OTel observable gauge callback reads
// directly from the scheduler's atomic fields and reports the stored values faithfully.
// It does NOT test that the scheduler's internal updateStats loop writes to those atomics —
// see TestGetUpdateStatsFuncWritesAtomics for that.
func TestSchedulerMetricGaugeCallback(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, mp.Shutdown(context.Background())) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := make(chan int, 10)
	s, closeFn := NewScheduler(ctx, zap.NewNop(), queue,
		WithMeter(mp.Meter("test")),
		// Use a 24h tick so the internal goroutine never fires updateStats during the test,
		// preventing it from overwriting the atomic values we store below.
		WithAverageWindow(24*time.Hour, 1, 1),
		WithAllowedTokensMin(1),
	)
	defer func() { require.NoError(t, closeFn()) }()

	// Store values directly to simulate processed work without timing dependencies.
	s.completedWorkHz.Store(math.Float64bits(42.0))
	s.tokensAllowedHz.Store(math.Float64bits(10.0))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var foundCompletedWork, foundTokensAllowed bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case metricCompletedWorkRate:
				foundCompletedWork = true
				data, ok := m.Data.(metricdata.Gauge[float64])
				require.True(t, ok, "metricCompletedWorkRate data must be a Gauge[float64]")
				require.Len(t, data.DataPoints, 1, "metricCompletedWorkRate must have exactly 1 data point")
				assert.Equal(t, 42.0, data.DataPoints[0].Value)
				assert.Equal(t, "Work completion rate (Hz)", m.Description,
					"metricCompletedWorkRate description mismatch — gauges may be swapped")
			case metricTokensAllowedRate:
				foundTokensAllowed = true
				data, ok := m.Data.(metricdata.Gauge[float64])
				require.True(t, ok, "metricTokensAllowedRate data must be a Gauge[float64]")
				require.Len(t, data.DataPoints, 1, "metricTokensAllowedRate must have exactly 1 data point")
				assert.Equal(t, 10.0, data.DataPoints[0].Value)
				assert.Equal(t, "Token release rate (Hz)", m.Description,
					"metricTokensAllowedRate description mismatch — gauges may be swapped")
			}
		}
	}
	assert.True(t, foundCompletedWork, "completed work rate gauge not found in collected metrics")
	assert.True(t, foundTokensAllowed, "tokens allowed rate gauge not found in collected metrics")
}

// TestGetUpdateStatsFuncWritesAtomics verifies that the scheduler's updateStats function —
// the production path that populates completedWorkHz and tokensAllowedHz — writes non-zero
// values to those atomics after processing work.
func TestGetUpdateStatsFuncWritesAtomics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := make(chan int, 10)
	s, closeFn := NewScheduler(ctx, zap.NewNop(), queue,
		WithAverageWindow(24*time.Hour, 1, 1),
		WithAllowedTokensMin(1),
	)
	defer func() { require.NoError(t, closeFn()) }()

	// getUpdateStatsFunc captures internal state; alpha=0.1 gives stable convergence.
	updateStats := s.getUpdateStatsFunc(1.0, 1, 0.5, 0.1)

	t0 := time.Now()
	// First call initializes previousTime; atomics are not written.
	updateStats(t0, 0, 0)

	// Second call: averageCompletedWorkPerMs starts at zero (≤ tolerance), so the function
	// takes the early-return path setting the average but not writing the atomics.
	t1 := t0.Add(100 * time.Millisecond)
	updateStats(t1, 0, 5)

	// Third call: averageCompletedWorkPerMs > tolerance now, so the function computes
	// tokensAllowedPerMs and writes both atomics.
	t2 := t1.Add(100 * time.Millisecond)
	updateStats(t2, 0, 5)

	completedWorkHz := math.Float64frombits(s.completedWorkHz.Load())
	tokensAllowedHz := math.Float64frombits(s.tokensAllowedHz.Load())

	assert.Greater(t, completedWorkHz, 0.0, "completedWorkHz should be positive after processing work")
	assert.Greater(t, tokensAllowedHz, 0.0, "tokensAllowedHz should be positive after processing work")
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
