package workermgr

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// workCounterKey identifies a work counter dimension (state + RST ID).
type workCounterKey struct {
	state string
	rstID int
}

// newWorkTestManager creates a Manager wired with a real OTel ManualReader.
// Workers are started. Call cleanup() to stop everything.
func newWorkTestManager(t *testing.T, workerConfigs []worker.Config) (*Manager, *sdkmetric.ManualReader, func()) {
	t.Helper()

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)

	rstConfigs := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}

	mgr, err := NewManager(
		context.Background(), log, Config{}, workerConfigs, rstConfigs,
		&flex.BeeRemoteNode{}, filesystem.NewMockFS(), map[string]*flex.Feature{},
	)
	require.NoError(t, err)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("workermgr")

	workActive, err := meter.Int64UpDownCounter("beeremote.work.active")
	require.NoError(t, err)
	workTerminal, err := meter.Int64Counter("beeremote.work.terminal")
	require.NoError(t, err)

	mgr.WorkActive = workActive
	mgr.WorkTerminal = workTerminal

	require.NoError(t, mgr.Start())

	cleanup := func() {
		mgr.Stop()
		require.NoError(t, mp.Shutdown(context.Background()))
	}
	return mgr, reader, cleanup
}

// workActiveValues returns a map of (state, rstID) → cumulative value for beeremote.work.active.
func workActiveValues(t *testing.T, reader *sdkmetric.ManualReader) map[workCounterKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	result := make(map[workCounterKey]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "beeremote.work.active" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "beeremote.work.active must be Sum[int64] (UpDownCounter)")
			for _, dp := range data.DataPoints {
				s, _ := dp.Attributes.Value(attrState)
				r, _ := dp.Attributes.Value(attrRSTID)
				result[workCounterKey{s.AsString(), int(r.AsInt64())}] = dp.Value
			}
		}
	}
	return result
}

// workTerminalValues returns a map of (state, rstID) → cumulative value for beeremote.work.terminal.
func workTerminalValues(t *testing.T, reader *sdkmetric.ManualReader) map[workCounterKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	result := make(map[workCounterKey]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "beeremote.work.terminal" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "beeremote.work.terminal must be Sum[int64] (Counter)")
			for _, dp := range data.DataPoints {
				s, _ := dp.Attributes.Value(attrState)
				r, _ := dp.Attributes.Value(attrRSTID)
				result[workCounterKey{s.AsString(), int(r.AsInt64())}] = dp.Value
			}
		}
	}
	return result
}

// mockWR builds a flex.WorkRequest for the given job/request IDs with Mock type and rst.id=1.
func mockWR(jobID, requestID string) *flex.WorkRequest {
	return flex.WorkRequest_builder{
		JobId: jobID, RequestId: requestID, Path: "/test/file",
		RemoteStorageTarget: 1,
		Mock:                flex.MockJob_builder{}.Build(),
	}.Build()
}

// unknownTypeWR builds a flex.WorkRequest with no type set, so it maps to worker.Unknown.
func unknownTypeWR(jobID, requestID string) *flex.WorkRequest {
	return flex.WorkRequest_builder{
		JobId: jobID, RequestId: requestID, Path: "/test/file",
		RemoteStorageTarget: 1,
		// No type field → WhichType() default → worker.Unknown → no pool → FAILED
	}.Build()
}

// schedulingWorkerConfig returns a mock worker config that schedules all submitted work.
func schedulingWorkerConfig() worker.Config {
	return worker.Config{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork", Args: []any{mock.Anything},
					ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}
}

// schedulingAndCancellingWorkerConfig returns a mock worker config that schedules submitted work
// and returns CANCELLED when asked to update it.
func schedulingAndCancellingWorkerConfig() worker.Config {
	return worker.Config{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork", Args: []any{mock.Anything},
					ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil},
				},
				{
					MethodName: "UpdateWork", Args: []any{mock.Anything},
					ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_CANCELLED}.Build(), nil},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}
}

// TestWorkCounterNoPool verifies that a WR with no matching pool is counted as a
// failed terminal immediately in SubmitJob. The cascade must not double-count it.
func TestWorkCounterNoPool(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mgr, reader, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()

	js := JobSubmission{
		JobID:        "job-1",
		WorkRequests: []*flex.WorkRequest{unknownTypeWR("job-1", "req-0")},
	}
	_, _, err := mgr.SubmitJob(js)
	require.Error(t, err)

	active := workActiveValues(t, reader)
	terminal := workTerminalValues(t, reader)

	assert.Empty(t, active, "FAILED WR starts terminal; never enters WorkActive")
	assert.Equal(t, int64(1), terminal[workCounterKey{"failed", 1}])
	assert.NotContains(t, terminal, workCounterKey{"unknown", 1}, "cascade must not double-count already-terminal FAILED WR")
}

// TestWorkCounterSubmitWorkError verifies that a SubmitWork communication failure
// records the WR as active "created" during assignment, then terminal "unknown" after
// the cascade fails with ErrWorkerNotInPool (AssignedNode is empty when assignment errors).
//
// NOTE: this test takes ~4s. assignToLeastBusyWorker retries 4×1s when all workers error.
func TestWorkCounterSubmitWorkError(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork", Args: []any{mock.Anything},
					ReturnArgs: []any{nil, fmt.Errorf("simulated submit failure")},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mgr, reader, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()

	js := JobSubmission{
		JobID:        "job-1",
		WorkRequests: []*flex.WorkRequest{mockWR("job-1", "req-0")},
	}
	workResults, _, err := mgr.SubmitJob(js)
	require.Error(t, err)

	// Verify the WR entered the SubmitWork-failure path: state=CREATED at assignment (pool
	// found, node ONLINE, SubmitWork returned error), then cascade sets it to UNKNOWN because
	// AssignedNode="" is not in the nodeMap (ErrWorkerNotInPool).
	// Both the SubmitWork-failure path and the ErrNoWorkersConnected path produce identical
	// metric outcomes (CREATED→UNKNOWN), so the WR state is what distinguishes them.
	require.Len(t, workResults, 1)
	wr := workResults["req-0"]
	assert.Equal(t, flex.Work_UNKNOWN, wr.Status().GetState())
	assert.Contains(t, wr.Status().GetMessage(), "error communicating to node")

	active := workActiveValues(t, reader)
	terminal := workTerminalValues(t, reader)

	// WorkActive{created} was +1 in SubmitJob, then -1 in cascade → net 0.
	// Cascade fails with ErrWorkerNotInPool (AssignedNode="" → not in nodeMap) → UNKNOWN terminal.
	// require.Contains verifies a data point was actually recorded (not just a missing-key zero).
	require.Contains(t, active, workCounterKey{"created", 1}, "WorkActive{created} must be present: decrement requires a prior increment")
	assert.Equal(t, int64(0), active[workCounterKey{"created", 1}])
	assert.Equal(t, int64(1), terminal[workCounterKey{"unknown", 1}])
}

// TestWorkCounterAllScheduled verifies that successfully scheduled WRs are counted
// in WorkActive with state "scheduled" and WorkTerminal stays empty.
func TestWorkCounterAllScheduled(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrCount int
	}{
		{"one_wr", 1},
		{"three_wrs", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, reader, cleanup := newWorkTestManager(t, []worker.Config{schedulingWorkerConfig()})
			defer cleanup()

			wrs := make([]*flex.WorkRequest, tc.wrCount)
			for i := range wrs {
				wrs[i] = mockWR("job-1", fmt.Sprintf("req-%d", i))
			}
			_, _, err := mgr.SubmitJob(JobSubmission{JobID: "job-1", WorkRequests: wrs})
			require.NoError(t, err)

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			assert.Equal(t, int64(tc.wrCount), active[workCounterKey{"scheduled", 1}])
			assert.Empty(t, terminal)
		})
	}
}

// TestWorkCounterPartialSchedulingWithUnknownType verifies partial scheduling:
// Mock-type WRs schedule successfully and are then cancelled by the cascade;
// unknown-type WRs fail immediately (no pool) and are not double-counted in the cascade.
func TestWorkCounterPartialSchedulingWithUnknownType(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mockWRCount       int
		unknownWRCount    int
		wantCancelled     int64
		wantFailed        int64
	}{
		// A5: 2 scheduled + 1 no-pool
		{"two_scheduled_one_no_pool", 2, 1, 2, 1},
		// A6: 1 scheduled + 1 no-pool
		{"one_scheduled_one_no_pool", 1, 1, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, reader, cleanup := newWorkTestManager(t, []worker.Config{schedulingAndCancellingWorkerConfig()})
			defer cleanup()

			var wrs []*flex.WorkRequest
			for i := range tc.mockWRCount {
				wrs = append(wrs, mockWR("job-1", fmt.Sprintf("req-mock-%d", i)))
			}
			for i := range tc.unknownWRCount {
				wrs = append(wrs, unknownTypeWR("job-1", fmt.Sprintf("req-unknown-%d", i)))
			}

			_, _, err := mgr.SubmitJob(JobSubmission{JobID: "job-1", WorkRequests: wrs})
			require.Error(t, err)

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			for k, v := range active {
				assert.Equal(t, int64(0), v, "WorkActive must be zero for key %v", k)
			}
			assert.Equal(t, tc.wantCancelled, terminal[workCounterKey{"cancelled", 1}], "scheduled WRs cancelled by cascade")
			assert.Equal(t, tc.wantFailed, terminal[workCounterKey{"failed", 1}], "no-pool WRs counted as failed at submit time")
		})
	}
}

// TestWorkCounterUpdateJobUnassigned verifies that UpdateJob cancels an unassigned CREATED WR
// and records the correct metric transition (WorkActive -1, WorkTerminal{cancelled} +1).
func TestWorkCounterUpdateJobUnassigned(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mgr, reader, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()

	// Pre-seed WorkActive to simulate the WR was previously recorded as active via SubmitJob.
	// Direct seeding is necessary here: the AssignedPool="" && AssignedNode="" guard in UpdateJob
	// is unreachable via SubmitJob because SubmitJob always sets AssignedPool to the resolved
	// node type. This is a unit test of that specific guard, not a full lifecycle test.
	mgr.WorkActive.Add(context.Background(), 1, metric.WithAttributes(
		attrState.String("created"), attrRSTID.Int(1),
	))

	workResults := map[string]worker.WorkResult{
		"req-0": {
			AssignedPool: "",
			AssignedNode: "",
			WorkResult: flex.Work_builder{
				RequestId: "req-0", JobId: "job-1",
				Status: flex.Work_Status_builder{State: flex.Work_CREATED}.Build(),
			}.Build(),
		},
	}
	mgr.UpdateJob(JobUpdate{
		JobID: "job-1", WorkResults: workResults,
		NewState: flex.UpdateWorkRequest_CANCELLED, RSTID: 1,
	})

	active := workActiveValues(t, reader)
	terminal := workTerminalValues(t, reader)

	// Net: WorkActive{created} +1 (pre-populate) -1 (transition) = 0.
	require.Contains(t, active, workCounterKey{"created", 1}, "WorkActive{created} must be present after transition")
	assert.Equal(t, int64(0), active[workCounterKey{"created", 1}])
	assert.Equal(t, int64(1), terminal[workCounterKey{"cancelled", 1}])
}

// TestWorkCounterUpdateJobPoolNotFound verifies that UpdateJob records a terminal "unknown"
// when the WR's AssignedPool is not in the manager's nodePools.
func TestWorkCounterUpdateJobPoolNotFound(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mgr, reader, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()

	// Pre-seed WorkActive to simulate the WR was previously recorded as active via SubmitJob.
	// Direct seeding is necessary here: to construct a pool-not-found scenario we need a WR
	// with AssignedPool=BeeSync, but the manager only has a Mock pool. This cannot be reached
	// through SubmitJob (which only creates WRs with the pool type it resolved). Unit test only.
	mgr.WorkActive.Add(context.Background(), 1, metric.WithAttributes(
		attrState.String("scheduled"), attrRSTID.Int(1),
	))

	// Manager has only a Mock pool; WR claims BeeSync pool (absent).
	workResults := map[string]worker.WorkResult{
		"req-0": {
			AssignedPool: worker.BeeSync,
			AssignedNode: "0",
			WorkResult: flex.Work_builder{
				RequestId: "req-0", JobId: "job-1",
				Status: flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(),
			}.Build(),
		},
	}
	mgr.UpdateJob(JobUpdate{
		JobID: "job-1", WorkResults: workResults,
		NewState: flex.UpdateWorkRequest_CANCELLED, RSTID: 1,
	})

	active := workActiveValues(t, reader)
	terminal := workTerminalValues(t, reader)

	// Net: WorkActive{scheduled} +1 (pre-populate) -1 (transition) = 0.
	require.Contains(t, active, workCounterKey{"scheduled", 1}, "WorkActive{scheduled} must be present after transition")
	assert.Equal(t, int64(0), active[workCounterKey{"scheduled", 1}])
	assert.Equal(t, int64(1), terminal[workCounterKey{"unknown", 1}])
}

// TestWorkCounterUpdateJobNodeResponse verifies UpdateJob metric recording for
// the three node-communication outcomes: ErrWorkRequestNotFound, generic comms
// error, and successful node acknowledgment.
//
// SubmitJob is called first so the mock node is ONLINE before UpdateJob fires.
func TestWorkCounterUpdateJobNodeResponse(t *testing.T) {
	for _, tc := range []struct {
		name          string
		updateReturn  []any
		wantTermState string
	}{
		// B3: node returns ErrWorkRequestNotFound → CANCELLED
		{"work_not_found", []any{nil, worker.ErrWorkRequestNotFound}, "cancelled"},
		// B4: node returns generic error → UNKNOWN
		{"comms_error", []any{nil, fmt.Errorf("simulated comms error")}, "unknown"},
		// B5: node acks CANCELLED
		{"node_acks_cancelled", []any{flex.Work_Status_builder{State: flex.Work_CANCELLED}.Build(), nil}, "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workerConfigs := []worker.Config{{
				ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
				MockConfig: worker.MockConfig{
					Expectations: []worker.MockExpectation{
						{MethodName: "connect", ReturnArgs: []any{false, nil}},
						{
							MethodName: "SubmitWork", Args: []any{mock.Anything},
							ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil},
						},
						{
							MethodName: "UpdateWork", Args: []any{mock.Anything},
							ReturnArgs: tc.updateReturn,
						},
						{MethodName: "disconnect", ReturnArgs: []any{nil}},
					},
				},
			}}
			mgr, reader, cleanup := newWorkTestManager(t, workerConfigs)
			defer cleanup()

			// SubmitJob first: ensures node is ONLINE and returns WorkResults with AssignedNode set.
			js := JobSubmission{
				JobID:        "job-1",
				WorkRequests: []*flex.WorkRequest{mockWR("job-1", "req-0")},
			}
			workResults, _, err := mgr.SubmitJob(js)
			require.NoError(t, err)

			mgr.UpdateJob(JobUpdate{
				JobID: "job-1", WorkResults: workResults,
				NewState: flex.UpdateWorkRequest_CANCELLED, RSTID: 1,
			})

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			// After SubmitJob: WorkActive{scheduled}=+1.
			// After UpdateJob: WorkActive{scheduled}=-1, WorkTerminal{tc.wantTermState}=+1.
			// Net: WorkActive{scheduled}=0, WorkTerminal{tc.wantTermState}=1.
			require.Contains(t, active, workCounterKey{"scheduled", 1}, "WorkActive{scheduled} must be present after SubmitJob+UpdateJob")
			assert.Equal(t, int64(0), active[workCounterKey{"scheduled", 1}])
			assert.Equal(t, int64(1), terminal[workCounterKey{tc.wantTermState, 1}])
		})
	}
}

func minimalWorkerConfig() worker.Config {
	return worker.Config{
		ID: "0", Name: "test-node-0", Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}
}

// TestRecordWorkTransitionNonTerminalToNonTerminal verifies that non-terminal → non-terminal
// transitions decrement WorkActive for the old state and increment it for the new state.
// Covers states that exist in workStateString but are not exercised via SubmitJob/UpdateJob.
func TestRecordWorkTransitionNonTerminalToNonTerminal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oldState flex.Work_State
		oldLabel string
		newState flex.Work_State
		newLabel string
	}{
		{"scheduled_to_running", flex.Work_SCHEDULED, "scheduled", flex.Work_RUNNING, "running"},
		{"created_to_error", flex.Work_CREATED, "created", flex.Work_ERROR, "error"},
		{"error_to_rescheduled", flex.Work_ERROR, "error", flex.Work_RESCHEDULED, "rescheduled"},
		{"rescheduled_to_scheduled", flex.Work_RESCHEDULED, "rescheduled", flex.Work_SCHEDULED, "scheduled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, reader, cleanup := newWorkTestManager(t, []worker.Config{minimalWorkerConfig()})
			defer cleanup()

			mgr.WorkActive.Add(context.Background(), 1, metric.WithAttributes(
				attrState.String(tc.oldLabel), attrRSTID.Int(1),
			))

			mgr.recordWorkTransition(tc.oldState, tc.newState, 1)

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			require.Contains(t, active, workCounterKey{tc.oldLabel, 1})
			assert.Equal(t, int64(0), active[workCounterKey{tc.oldLabel, 1}])
			assert.Equal(t, int64(1), active[workCounterKey{tc.newLabel, 1}])
			assert.Empty(t, terminal)
		})
	}
}

// TestRecordWorkTransitionNonTerminalToTerminal verifies that a non-terminal → terminal
// transition decrements WorkActive and increments WorkTerminal. Covers Work_COMPLETED which
// is the normal success terminal state and not reachable via UpdateJob (only via UpdateWork
// in Commit 2).
func TestRecordWorkTransitionNonTerminalToTerminal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		newState  flex.Work_State
		wantLabel string
	}{
		{"running_to_completed", flex.Work_COMPLETED, "completed"},
		{"running_to_failed", flex.Work_FAILED, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, reader, cleanup := newWorkTestManager(t, []worker.Config{minimalWorkerConfig()})
			defer cleanup()

			mgr.WorkActive.Add(context.Background(), 1, metric.WithAttributes(
				attrState.String("running"), attrRSTID.Int(1),
			))

			mgr.recordWorkTransition(flex.Work_RUNNING, tc.newState, 1)

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			require.Contains(t, active, workCounterKey{"running", 1})
			assert.Equal(t, int64(0), active[workCounterKey{"running", 1}])
			assert.Equal(t, int64(1), terminal[workCounterKey{tc.wantLabel, 1}])
		})
	}
}

// TestRecordWorkTransitionTerminalGuard verifies that calling recordWorkTransition with
// a terminal oldState is a no-op — the guard prevents double-counting WRs already in
// WorkTerminal. Uses Work_RUNNING as the transition target so the target label is always
// distinct from the seeded terminal label, making the "no new entries" assertion unambiguous.
func TestRecordWorkTransitionTerminalGuard(t *testing.T) {
	for _, terminalState := range []flex.Work_State{
		flex.Work_COMPLETED, flex.Work_CANCELLED, flex.Work_FAILED, flex.Work_UNKNOWN,
	} {
		t.Run(workStateString(terminalState), func(t *testing.T) {
			mgr, reader, cleanup := newWorkTestManager(t, []worker.Config{minimalWorkerConfig()})
			defer cleanup()

			// Seed WorkTerminal to simulate the WR was already counted.
			mgr.WorkTerminal.Add(context.Background(), 1, metric.WithAttributes(
				attrState.String(workStateString(terminalState)), attrRSTID.Int(1),
			))

			// Any transition from a terminal state must be ignored.
			mgr.recordWorkTransition(terminalState, flex.Work_RUNNING, 1)

			active := workActiveValues(t, reader)
			terminal := workTerminalValues(t, reader)

			// Guard must have fired: no WorkActive changes, WorkTerminal unchanged at seed value.
			assert.Empty(t, active, "WorkActive must not change when oldState is terminal")
			assert.Equal(t, int64(1), terminal[workCounterKey{workStateString(terminalState), 1}], "seed value must not increase")
			assert.Len(t, terminal, 1, "no new terminal entries must be added")
		})
	}
}
