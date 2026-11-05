package workermgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func mockRST(id uint32) *flex.RemoteStorageTarget {
	return flex.RemoteStorageTarget_builder{Id: id, Name: fmt.Sprintf("rst-%d", id), Mock: new("test")}.Build()
}

func TestApplyRSTs(t *testing.T) {
	workerConfigs := []worker.Config{
		{
			ID: "0", Name: "accepts", Type: worker.Mock, MaxReconnectBackOff: 1,
			MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			}},
		},
		{
			ID: "1", Name: "rejects", Type: worker.Mock, MaxReconnectBackOff: 1,
			MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
				{MethodName: "UpdateConfig", Args: []any{mock.Anything}, ReturnArgs: []any{fmt.Errorf("%w: bad config", worker.ErrConfigRejected)}},
			}},
		},
		{
			ID: "2", Name: "offline", Type: worker.Mock, MaxReconnectBackOff: 1,
			MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{errors.New("unreachable")}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			}},
		},
	}
	mgr, _, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()

	pool := mgr.nodePools[worker.Mock]
	require.Eventually(t, func() bool {
		return pool.nodeMap["0"].GetState() == worker.ONLINE && pool.nodeMap["1"].GetState() == worker.ONLINE
	}, 5*time.Second, 10*time.Millisecond)

	ctx := context.Background()
	apply, err := mgr.ApplyRSTs(ctx, []*flex.RemoteStorageTarget{mockRST(1), mockRST(2)}, nil)
	require.NoError(t, err)
	outcomes := map[string]beeremote.ApplyResult_Outcome{}
	for _, n := range apply.GetNodes() {
		outcomes[n.GetNodeId()] = n.GetOutcome()
	}
	assert.Equal(t, map[string]beeremote.ApplyResult_Outcome{
		"0": beeremote.ApplyResult_APPLIED,
		"1": beeremote.ApplyResult_REJECTED,
		"2": beeremote.ApplyResult_OFFLINE,
	}, outcomes)
	// A mock node reconnects at once, so it cannot show the node leaving service. The BeeSync node
	// tests cover that (TestBeeSyncNodeUpdateConfigRejectionReconnects).

	live, deleted, ok := mgr.GetRST(2)
	require.True(t, ok)
	assert.NotNil(t, live)
	assert.Nil(t, deleted)

	// Delete RST 1. Its provider is retired, not dropped.
	before, _, _ := mgr.GetRST(1)
	_, err = mgr.ApplyRSTs(ctx, []*flex.RemoteStorageTarget{mockRST(2)}, map[uint32]rstdb.Deleted{1: {Config: mockRST(1), Reason: "gone"}})
	require.NoError(t, err)
	retired, d, ok := mgr.GetRST(1)
	require.True(t, ok)
	assert.Same(t, before, retired, "the retired provider is the one running jobs used")
	require.NotNil(t, d)
	assert.Equal(t, "gone", d.Reason)
	ids := []uint32{}
	for _, c := range mgr.LiveRSTs() {
		ids = append(ids, c.GetId())
	}
	assert.Equal(t, []uint32{2}, ids)
	assert.Contains(t, mgr.DeletedRSTs(), uint32(1))

	// A forced create reuses ID 1. Its retired provider is dropped and the live one is used.
	_, err = mgr.ApplyRSTs(ctx, []*flex.RemoteStorageTarget{mockRST(1), mockRST(2)}, map[uint32]rstdb.Deleted{})
	require.NoError(t, err)
	reused, d, ok := mgr.GetRST(1)
	require.True(t, ok)
	assert.Nil(t, d, "RST 1 is live again")
	assert.NotSame(t, before, reused, "the reused ID gets a new provider")
	assert.Empty(t, mgr.DeletedRSTs())

	// An invalid set changes nothing, including the retired providers.
	_, err = mgr.ApplyRSTs(ctx, []*flex.RemoteStorageTarget{{Id: 3}}, map[uint32]rstdb.Deleted{1: {Config: mockRST(1)}, 2: {Config: mockRST(2)}})
	require.Error(t, err)
	_, d, ok = mgr.GetRST(2)
	require.True(t, ok)
	assert.Nil(t, d, "RST 2 must still be live")
	assert.Empty(t, mgr.DeletedRSTs(), "the retired providers added before the failed swap are removed again")
}

// TestWorkerConfigFnHoldsBackChanges checks that a change, which takes the apply lock for writing,
// waits while a node's connect attempt holds its configuration, and proceeds once it is released.
func TestWorkerConfigFnHoldsBackChanges(t *testing.T) {
	applyMu := new(sync.RWMutex)
	configFn := workerConfigFn(func() *flex.UpdateConfigRequest { return &flex.UpdateConfigRequest{} }, applyMu)

	_, release := configFn()
	applied := make(chan struct{})
	go func() {
		applyMu.Lock()
		close(applied)
		applyMu.Unlock()
	}()
	select {
	case <-applied:
		t.Fatal("a change landed while a node was connecting")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	release() // A second release must not unlock twice.
	select {
	case <-applied:
	case <-time.After(5 * time.Second):
		t.Fatal("the change did not proceed after the node released its configuration")
	}
}
