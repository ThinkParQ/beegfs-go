package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// hangingWorkerNodeServer accepts connections like a healthy sync node, but can be told to never
// answer UpdateConfig or Heartbeat. A hung handler returns only when its RPC is cancelled, which
// models a node that accepts the TCP connection and then stops responding.
type hangingWorkerNodeServer struct {
	testWorkerNodeServer
	hangUpdateConfig  bool
	hangHeartbeat     bool
	updateConfigCalls atomic.Int32
	heartbeatCalls    atomic.Int32
}

func (s *hangingWorkerNodeServer) UpdateConfig(ctx context.Context, request *flex.UpdateConfigRequest) (*flex.UpdateConfigResponse, error) {
	s.updateConfigCalls.Add(1)
	if s.hangUpdateConfig {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return flex.UpdateConfigResponse_builder{Result: flex.UpdateConfigResponse_SUCCESS}.Build(), nil
}

func (s *hangingWorkerNodeServer) BulkUpdateWork(ctx context.Context, request *flex.BulkUpdateWorkRequest) (*flex.BulkUpdateWorkResponse, error) {
	return flex.BulkUpdateWorkResponse_builder{Success: true}.Build(), nil
}

func (s *hangingWorkerNodeServer) Heartbeat(ctx context.Context, request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	s.heartbeatCalls.Add(1)
	if s.hangHeartbeat {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return flex.HeartbeatResponse_builder{State: flex.HeartbeatResponse_READY}.Build(), nil
}

// startHandle runs Handle for a BeeSync node pointed at server, with a one second heartbeat
// interval so a hung heartbeat times out quickly. It returns the node and a channel that is closed
// when Handle returns.
func startHandle(t *testing.T, server *hangingWorkerNodeServer) (*BeeSyncNode, chan struct{}) {
	t.Helper()
	addr, cleanup := startWorkerNodeServer(t, server)
	t.Cleanup(cleanup)

	node, err := newWorkerNodeFromConfig(zap.NewNop(), Config{
		ID: "node-1", Name: "node-1", Type: BeeSync, Address: addr, TlsDisable: true,
		HeartbeatInterval: 1,
	})
	require.NoError(t, err)
	beeSyncNode, ok := node.(*BeeSyncNode)
	require.True(t, ok)
	t.Cleanup(beeSyncNode.Stop)

	config, bulk := connectInputs()
	handleDone := make(chan struct{})
	go func() {
		beeSyncNode.Handle(&sync.WaitGroup{}, func() (*flex.UpdateConfigRequest, func()) { return config, func() {} }, bulk, map[string]*flex.Feature{})
		close(handleDone)
	}()
	return beeSyncNode, handleDone
}

// Stop() must end Handle while connect is blocked on a node that never answers UpdateConfig.
func TestBeeSyncHandleStopsDuringHungConnect(t *testing.T) {
	server := &hangingWorkerNodeServer{hangUpdateConfig: true}
	node, handleDone := startHandle(t, server)

	require.Eventually(t, func() bool { return server.updateConfigCalls.Load() > 0 }, 5*time.Second,
		time.Millisecond, "connect never sent UpdateConfig")
	node.Stop()

	select {
	case <-handleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after Stop while connect was blocked")
	}
}

// Stop() must end Handle while a heartbeat is blocked, without waiting for the heartbeat timeout.
func TestBeeSyncHandleStopsDuringHungHeartbeat(t *testing.T) {
	server := &hangingWorkerNodeServer{hangHeartbeat: true}
	node, handleDone := startHandle(t, server)

	require.Eventually(t, func() bool { return server.heartbeatCalls.Load() > 0 }, 5*time.Second,
		time.Millisecond, "Handle never sent a heartbeat")
	node.Stop()

	// The heartbeat timeout is one second, so returning well inside it shows Stop interrupted the
	// heartbeat rather than the timeout ending it.
	select {
	case <-handleDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Handle did not return promptly after Stop while a heartbeat was blocked")
	}
}

// A heartbeat that never answers must time out and send the node through a reconnect, instead of
// leaving it ONLINE and getting work while it cannot respond.
func TestBeeSyncHandleReconnectsAfterHungHeartbeat(t *testing.T) {
	server := &hangingWorkerNodeServer{hangHeartbeat: true}
	startHandle(t, server)

	// The first UpdateConfig is the initial connect. A second one means the heartbeat timed out,
	// the node was placed offline, and Handle reconnected.
	require.Eventually(t, func() bool { return server.updateConfigCalls.Load() >= 2 }, 5*time.Second,
		10*time.Millisecond, "a hung heartbeat should have sent the node through a reconnect")
}

// A connect that never gets an answer must time out, so the node leaves UNKNOWN and is retried. A
// node stuck in UNKNOWN makes the pool wait on every request no other node accepts.
func TestBeeSyncHandleRetriesAfterHungConnect(t *testing.T) {
	defaultConnectTimeout := connectTimeout
	connectTimeout = 200 * time.Millisecond
	t.Cleanup(func() { connectTimeout = defaultConnectTimeout })

	server := &hangingWorkerNodeServer{hangUpdateConfig: true}
	node, _ := startHandle(t, server)

	require.Eventually(t, func() bool { return node.GetState() == OFFLINE }, 2*time.Second,
		time.Millisecond, "a hung connect should time out and leave the node offline")
	// The first retry comes two to three seconds after the failed attempt.
	require.Eventually(t, func() bool { return server.updateConfigCalls.Load() >= 2 }, 5*time.Second,
		10*time.Millisecond, "a hung connect should be retried")
}
