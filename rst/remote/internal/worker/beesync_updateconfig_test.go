package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// badRSTID marks a configuration the configServer refuses.
const badRSTID = 99

// configServer is a sync node that accepts every configuration except one naming badRSTID, and
// remembers the last configuration it received.
type configServer struct {
	testWorkerNodeServer
	mu   sync.Mutex
	last *flex.UpdateConfigRequest
}

func (s *configServer) UpdateConfig(ctx context.Context, request *flex.UpdateConfigRequest) (*flex.UpdateConfigResponse, error) {
	s.mu.Lock()
	s.last = request
	s.mu.Unlock()
	for _, r := range request.GetRsts() {
		if r.GetId() == badRSTID {
			return flex.UpdateConfigResponse_builder{Result: flex.UpdateConfigResponse_FAILURE, Message: "bad rst"}.Build(), nil
		}
	}
	return flex.UpdateConfigResponse_builder{Result: flex.UpdateConfigResponse_SUCCESS}.Build(), nil
}

func (s *configServer) BulkUpdateWork(ctx context.Context, request *flex.BulkUpdateWorkRequest) (*flex.BulkUpdateWorkResponse, error) {
	return flex.BulkUpdateWorkResponse_builder{Success: true}.Build(), nil
}

func (s *configServer) Heartbeat(ctx context.Context, request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	return flex.HeartbeatResponse_builder{State: flex.HeartbeatResponse_READY}.Build(), nil
}

func (s *configServer) lastRSTs() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := []uint32{}
	for _, r := range s.last.GetRsts() {
		ids = append(ids, r.GetId())
	}
	return ids
}

func configWith(ids ...uint32) *flex.UpdateConfigRequest {
	rsts := []*flex.RemoteStorageTarget{}
	for _, id := range ids {
		rsts = append(rsts, flex.RemoteStorageTarget_builder{Id: id}.Build())
	}
	return flex.UpdateConfigRequest_builder{
		Rsts:      rsts,
		BeeRemote: flex.BeeRemoteNode_builder{Address: "127.0.0.1:12345"}.Build(),
	}.Build()
}

// A node that rejects a pushed configuration leaves service, and reconnects with whatever
// configuration its ConfigFn returns then. It rejoins only once that configuration is accepted.
func TestBeeSyncNodeUpdateConfigRejectionReconnects(t *testing.T) {
	server := &configServer{}
	addr, cleanup := startWorkerNodeServer(t, server)
	t.Cleanup(cleanup)

	n, err := newWorkerNodeFromConfig(zap.NewNop(), Config{
		ID: "node-1", Name: "node-1", Type: BeeSync, Address: addr, TlsDisable: true,
		HeartbeatInterval: 1, MaxReconnectBackOff: 1,
	})
	require.NoError(t, err)
	node := n.(*BeeSyncNode)
	t.Cleanup(node.Stop)

	var current atomic.Pointer[flex.UpdateConfigRequest]
	current.Store(configWith(1))
	go node.Handle(&sync.WaitGroup{}, func() (*flex.UpdateConfigRequest, func()) { return current.Load(), func() {} }, flex.BulkUpdateWorkRequest_builder{}.Build(), map[string]*flex.Feature{})
	require.Eventually(t, func() bool { return node.GetState() == ONLINE }, 5*time.Second, 10*time.Millisecond)

	// An accepted push keeps the node online and carries its ID.
	require.NoError(t, node.UpdateConfig(configWith(1, 2)))
	assert.Equal(t, []uint32{1, 2}, server.lastRSTs())
	assert.Equal(t, ONLINE, node.GetState())

	// A rejected push takes the node out of service. Its reconnects send the current
	// configuration, which BeeRemote now serves with the bad RST, so they keep failing.
	current.Store(configWith(1, 2, badRSTID))
	require.ErrorIs(t, node.UpdateConfig(current.Load()), ErrConfigRejected)
	require.Eventually(t, func() bool { return node.GetState() == OFFLINE }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	assert.Equal(t, OFFLINE, node.GetState(), "the node must stay out of service while the configuration is rejected")

	// Once BeeRemote serves a configuration the node accepts, the next reconnect brings it back.
	current.Store(configWith(1, 3))
	require.Eventually(t, func() bool { return node.GetState() == ONLINE }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []uint32{1, 3}, server.lastRSTs())
}

func TestBeeSyncNodeUpdateConfigNeedsConnection(t *testing.T) {
	n, err := newWorkerNodeFromConfig(zap.NewNop(), Config{ID: "node-1", Name: "node-1", Type: BeeSync, Address: "127.0.0.1:1", TlsDisable: true})
	require.NoError(t, err)
	assert.ErrorIs(t, n.UpdateConfig(configWith(1)), ErrNodeNotConnected)
}
