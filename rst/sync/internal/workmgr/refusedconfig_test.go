package workmgr

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// The tests in this file cover a node that refused a configuration from Remote. Such a node may
// hold RSTs Remote no longer serves, so it must start no work request until it accepts one.

var testRemote = flex.BeeRemoteNode_builder{Id: "0", Address: "mock:0"}.Build()

// refusedRSTs is a set every node refuses, because two RSTs share an ID.
func refusedRSTs() []*flex.RemoteStorageTarget {
	return []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build(),
		flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build(),
	}
}

func acceptedRSTs() []*flex.RemoteStorageTarget {
	return []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
}

func TestRefusedConfigMakesNodeNotReady(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()
	require.True(t, mgr.IsReady())

	require.Error(t, mgr.UpdateConfig(context.Background(), refusedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))
	assert.False(t, mgr.IsReady(), "a node that refused a configuration is not ready")

	require.NoError(t, mgr.UpdateConfig(context.Background(), acceptedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))
	assert.True(t, mgr.IsReady(), "accepting a configuration makes the node ready again")
}

// Rescheduled work is not pulled again while the node is not ready, and is pulled once it accepts a
// configuration. The worker would also refuse to start it, but pulling it would reschedule it and
// report it to Remote again every workDelayMinimum.
func TestRefusedConfigPausesQueuedWork(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	checks := make(chan struct{}, 10)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("0", "0")).
		Run(func(mock.Arguments) { checks <- struct{}{} }).
		Return(false, workDelayMinimum, nil)
	var updates atomic.Int32
	mockBeeRemote.On("updateWork", mock.Anything).Run(func(mock.Arguments) { updates.Add(1) }).Return(nil)

	_, err = mgr.SubmitWorkRequest(proto.Clone(baseTestRequest).(*flex.WorkRequest))
	require.NoError(t, err)
	select {
	case <-checks:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first readiness check")
	}

	require.Error(t, mgr.UpdateConfig(context.Background(), refusedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))
	require.Eventually(t, func() bool { return updates.Load() == 1 }, 5*time.Second, 10*time.Millisecond, "the first reschedule is reported")
	time.Sleep(3 * workDelayMinimum)
	assert.Empty(t, checks, "no work request starts while the node is not ready")
	assert.Equal(t, int32(1), updates.Load(), "the paused request is not pulled and reported again")

	require.NoError(t, mgr.UpdateConfig(context.Background(), acceptedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))
	select {
	case <-checks:
	case <-time.After(5 * time.Second):
		t.Fatal("the work request did not resume after the node accepted a configuration")
	}
}

// A request already in the work queue when the node refuses a configuration is rescheduled by the
// worker instead of started.
func TestRefusedConfigReschedulesWorkAlreadyQueued(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	// The test manager has one worker. The first request holds it until release is closed, so the
	// second request waits in the work queue.
	started := make(chan struct{})
	release := make(chan struct{})
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("0", "0")).
		Run(func(mock.Arguments) { close(started); <-release }).
		Return(false, time.Hour, nil).Once()
	var secondChecked atomic.Bool
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "0")).
		Run(func(mock.Arguments) { secondChecked.Store(true) }).
		Return(false, time.Hour, nil)

	rescheduled := make(chan *flex.Work, 1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "0", flex.Work_RESCHEDULED)).
		Run(func(args mock.Arguments) { rescheduled <- args.Get(0).(*flex.Work) }).
		Return(nil)
	mockBeeRemote.On("updateWork", mock.Anything).Return(nil)

	_, err = mgr.SubmitWorkRequest(proto.Clone(baseTestRequest).(*flex.WorkRequest))
	require.NoError(t, err)
	<-started
	second := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	second.SetJobId("1")
	second.SetPath("/foo/baz")
	_, err = mgr.SubmitWorkRequest(second)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mgr.activeWorkMu.Lock()
		defer mgr.activeWorkMu.Unlock()
		return len(mgr.activeWork) == 2
	}, 5*time.Second, 10*time.Millisecond, "the second request must be queued before the node refuses")

	require.Error(t, mgr.UpdateConfig(context.Background(), refusedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))
	close(release)

	select {
	case work := <-rescheduled:
		assert.Contains(t, work.GetStatus().GetMessage(), "waiting for this node to accept")
	case <-time.After(5 * time.Second):
		t.Fatal("the queued request was not rescheduled")
	}
	assert.False(t, secondChecked.Load(), "the queued request must not start")
}

// TestUnreachableRemoteWithChangedRSTsMakesNodeNotReady checks a push that cannot be applied
// because the node cannot reach Remote to check its features. With the RSTs the node already holds,
// as after every Remote restart, the node stays ready. With different RSTs, Remote has changed them,
// so the node must not go on with the ones it holds.
func TestUnreachableRemoteWithChangedRSTsMakesNodeNotReady(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()
	require.NoError(t, mgr.UpdateConfig(context.Background(), acceptedRSTs(), testRemote, "test-sync-node", rst.DefaultStateRoot))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := flex.BeeRemoteNode_builder{Id: "0", Address: listener.Addr().String()}.Build()
	require.NoError(t, listener.Close())

	err = mgr.UpdateConfig(context.Background(), acceptedRSTs(), unreachable, "test-sync-node", "")
	require.ErrorIs(t, err, beeremote.ErrUnavailable)
	assert.True(t, mgr.IsReady(), "the same RSTs, with the default state root spelled as empty, keep the node ready")

	changed := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 2, Mock: proto.String("test")}.Build()}
	err = mgr.UpdateConfig(context.Background(), changed, unreachable, "test-sync-node", rst.DefaultStateRoot)
	require.ErrorIs(t, err, beeremote.ErrUnavailable)
	assert.False(t, mgr.IsReady(), "changed RSTs that could not be applied make the node not ready")

	require.NoError(t, mgr.UpdateConfig(context.Background(), changed, testRemote, "test-sync-node", rst.DefaultStateRoot))
	assert.True(t, mgr.IsReady(), "applying them makes the node ready again")
}
