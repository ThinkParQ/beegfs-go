package workermgr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
)

// drainingWorkerConfig returns a mock worker config that declines every work request because it is
// draining. The first refusal moves the node to DRAINING, after which the pool only offers it work
// once every online node has declined.
func drainingWorkerConfig(id, name string) worker.Config {
	return worker.Config{
		ID: id, Name: name, Type: worker.Mock, MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork", Args: []any{mock.Anything},
					ReturnArgs: []any{(*flex.Work_Status)(nil), worker.ErrNodeDraining},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}
}

// namedSchedulingWorkerConfig is schedulingWorkerConfig with a caller supplied identity, so a pool
// can hold more than one node.
func namedSchedulingWorkerConfig(id, name string) worker.Config {
	cfg := schedulingWorkerConfig()
	cfg.ID = id
	cfg.Name = name
	return cfg
}

// requireNodesConnected waits until every node in the mock pool has left UNKNOWN, so a test that
// measures how long an assignment takes is not really measuring the startup retries.
func requireNodesConnected(t *testing.T, mgr *Manager) {
	t.Helper()
	pool, ok := mgr.nodePools[worker.Mock]
	require.True(t, ok, "expected a mock node pool")
	require.Eventually(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		for _, node := range pool.nodes {
			if node.GetState() == worker.UNKNOWN {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "nodes never finished connecting")
}

// TestAssignSkipsDrainingWorker verifies a draining node is passed over in favor of a healthy peer,
// and that once it has reported draining it is demoted to a last resort rather than a candidate.
func TestAssignSkipsDrainingWorker(t *testing.T) {
	mgr, _, cleanup := newWorkTestManager(t, []worker.Config{
		drainingWorkerConfig("0", "draining-node"),
		namedSchedulingWorkerConfig("1", "healthy-node"),
	})
	defer cleanup()
	requireNodesConnected(t, mgr)

	pool := mgr.nodePools[worker.Mock]

	// Two assignments, so the second proves the draining node was excluded rather than merely
	// skipped once by the round robin cursor landing elsewhere.
	for i := range 2 {
		assigned, work, err := pool.assignToLeastBusyWorker(mockWR("job-1", "req-0"), "")
		require.NoErrorf(t, err, "assignment %d should have succeeded on the healthy node", i)
		assert.Equal(t, "1", assigned, "work must be assigned to the healthy node")
		assert.Equal(t, flex.Work_SCHEDULED, work.GetStatus().GetState())
	}

	var draining worker.Worker
	for _, node := range pool.nodes {
		if node.GetID() == "0" {
			draining = node
		}
	}
	require.NotNil(t, draining)
	assert.Equal(t, worker.DRAINING, draining.GetState(), "refusing a request must move the node to DRAINING")

	// Assert on the tiers directly. The call count on the mock cannot prove the ordering, because a
	// pool that offered the draining node work first and fell through to the healthy node would
	// produce the same assignments.
	candidates, drainingTier, poolSize, stillStarting := pool.assignmentCandidates("")
	assert.Equal(t, 2, poolSize)
	assert.False(t, stillStarting, "both nodes have connected")
	require.Len(t, candidates, 1, "the draining node must not be a candidate")
	assert.Equal(t, "1", candidates[0].GetID())
	require.Len(t, drainingTier, 1, "the draining node must still be available as a last resort")
	assert.Equal(t, "0", drainingTier[0].GetID())
}

// TestAssignPrefersOriginatingDrainingNode verifies that when the request has to fall through to a
// draining node, the node that submitted the job is tried first. That node is still processing the
// submission, so its work journal is guaranteed to be open, which is not true of a peer that may be
// further through its own shutdown.
func TestAssignPrefersOriginatingDrainingNode(t *testing.T) {
	mgr, _, cleanup := newWorkTestManager(t, []worker.Config{
		drainingWorkerConfig("0", "draining-node-0"),
		drainingWorkerConfig("1", "draining-node-1"),
	})
	defer cleanup()
	requireNodesConnected(t, mgr)

	pool := mgr.nodePools[worker.Mock]

	// Both nodes have to be offered work before they report draining, which is what moves them into
	// the last resort tier.
	_, _, err := pool.assignToLeastBusyWorker(mockWR("job-1", "req-0"), "")
	require.Error(t, err)
	_, _, err = pool.assignToLeastBusyWorker(mockWR("job-1", "req-1"), "")
	require.Error(t, err)

	for _, originNodeID := range []string{"0", "1"} {
		candidates, drainingTier, _, _ := pool.assignmentCandidates(originNodeID)
		require.Empty(t, candidates, "no node is online")
		require.Len(t, drainingTier, 2)
		assert.Equal(t, originNodeID, drainingTier[0].GetID(),
			"the node that submitted the job must be tried first among draining nodes")
	}

	// Without an origin the tier keeps whatever order the round robin cursor produced, so only its
	// membership is asserted.
	_, drainingTier, _, _ := pool.assignmentCandidates("")
	assert.Len(t, drainingTier, 2)
}

// TestAssignFailsFastWhenAllWorkersDraining verifies that once every node has connected, an
// assignment that cannot be placed fails immediately instead of sleeping through the startup
// retries. This is the regression that made every submission block for seconds after a sync node
// was shut down.
func TestAssignFailsFastWhenAllWorkersDraining(t *testing.T) {
	mgr, _, cleanup := newWorkTestManager(t, []worker.Config{
		drainingWorkerConfig("0", "draining-node"),
	})
	defer cleanup()
	requireNodesConnected(t, mgr)

	pool := mgr.nodePools[worker.Mock]

	// The first assignment is what discovers the node is draining.
	_, _, err := pool.assignToLeastBusyWorker(mockWR("job-1", "req-0"), "")
	require.Error(t, err)

	start := time.Now()
	_, _, err = pool.assignToLeastBusyWorker(mockWR("job-1", "req-1"), "")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFromAllWorkers,
		"a draining node that declines is now offered work and its refusal reported, so the operator can see why the request was not placed")
	assert.Less(t, elapsed, assignRetryInterval,
		"assignment must fail immediately once every node has connected, not sleep and retry")
}
