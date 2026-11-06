package worker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Stopping a node while it is waiting to retry a failed connect must end Handle. The node is left
// offline, and its connection is closed both after the failed connect and when it goes offline.
func TestHandleStopsWhileConnecting(t *testing.T) {
	node, err := newWorkerNodeFromConfig(zap.NewNop(), Config{ID: "0", Name: "test-node-0", Type: Mock})
	require.NoError(t, err)
	mockNode, ok := node.(*MockNode)
	require.True(t, ok)

	connectAttempted := make(chan struct{}, 1)
	mockNode.On("connect").Return(errors.New("simulated connect failure")).Run(func(mock.Arguments) {
		select {
		case connectAttempted <- struct{}{}:
		default:
		}
	})
	mockNode.On("disconnect").Return(nil)

	handleDone := make(chan struct{})
	go func() {
		mockNode.Handle(&sync.WaitGroup{}, func() (*flex.UpdateConfigRequest, func()) { return nil, func() {} }, nil, nil)
		close(handleDone)
	}()

	select {
	case <-connectAttempted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first connect attempt")
	}

	// A failed attempt must move the node out of UNKNOWN while it backs off, so the pool stops
	// waiting for it as if it were still starting.
	require.Eventually(t, func() bool { return mockNode.GetState() == OFFLINE }, time.Second, time.Millisecond,
		"node should be offline after a failed connect")

	// The first retry waits at least two seconds, so Stop arrives while Handle is backing off.
	mockNode.Stop()
	select {
	case <-handleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after the node was stopped")
	}

	assert.Equal(t, OFFLINE, mockNode.GetState())
	mockNode.AssertNumberOfCalls(t, "connect", 1)
	mockNode.AssertNumberOfCalls(t, "disconnect", 2)
}

// A node that keeps rejecting its config is retried until it is stopped. The rejection is logged
// as an error only once, and every later rejection uses the normal retry warning.
func TestHandleLogsConfigRejectedOnce(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	// The smallest backoff cap keeps each retry under a second.
	node, err := newWorkerNodeFromConfig(zap.New(core), Config{ID: "0", Name: "test-node-0", Type: Mock, MaxReconnectBackOff: 1})
	require.NoError(t, err)
	mockNode, ok := node.(*MockNode)
	require.True(t, ok)

	const rejections = 3
	connectAttempts := make(chan struct{}, rejections)
	mockNode.On("connect").Return(fmt.Errorf("%w: simulated", ErrConfigRejected)).Run(func(mock.Arguments) {
		select {
		case connectAttempts <- struct{}{}:
		default:
		}
	})
	mockNode.On("disconnect").Return(nil)

	handleDone := make(chan struct{})
	go func() {
		mockNode.Handle(&sync.WaitGroup{}, func() (*flex.UpdateConfigRequest, func()) { return nil, func() {} }, nil, nil)
		close(handleDone)
	}()

	for range rejections {
		select {
		case <-connectAttempts:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a connect attempt")
		}
	}

	mockNode.Stop()
	select {
	case <-handleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after the node was stopped")
	}

	// Each rejection is logged on the Handle goroutine before it can return, so every attempt seen
	// above has been logged by now.
	errorLogs := logs.FilterLevelExact(zapcore.ErrorLevel)
	require.Equal(t, 1, errorLogs.Len())
	assert.Contains(t, errorLogs.All()[0].Message, "rejected its configuration")
	warnLogs := logs.FilterLevelExact(zapcore.WarnLevel).FilterMessageSnippet("unable to connect")
	assert.Equal(t, rejections-1, warnLogs.Len())
}

// RPCs attempted on an offline node must not touch rpcWG. setOffline waits on rpcWG while the node
// is offline, and sync.WaitGroup panics when an Add from zero runs concurrently with a Wait. The
// waits here stand in for setOffline, and the RPCs stand in for the pool still calling the node.
func TestRPCsOnOfflineNodeDoNotRaceWithWait(t *testing.T) {
	node, err := newWorkerNodeFromConfig(zap.NewNop(), Config{ID: "0", Name: "test-node-0", Type: Mock})
	require.NoError(t, err)
	mockNode, ok := node.(*MockNode)
	require.True(t, ok)
	mockNode.setState(OFFLINE)

	var stop atomic.Bool
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() {
			for !stop.Load() {
				_, err := mockNode.SubmitWork(&flex.WorkRequest{})
				assert.Error(t, err)
				_, err = mockNode.UpdateWork(&flex.UpdateWorkRequest{})
				assert.Error(t, err)
			}
		})
	}

	// An RPC that calls Add before it checks the state makes this panic within a few milliseconds.
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		mockNode.rpcWG.Wait()
	}
	stop.Store(true)
	callers.Wait()
}

// TestHandleReleasesConfigAfterEachAttempt checks the contract of ConfigFn: every connect attempt
// releases what it took, and a successful attempt releases only once the node is ONLINE. A release
// that never comes would block every later RST change. An early release would let a change land
// while the node is still connecting, and be skipped.
func TestHandleReleasesConfigAfterEachAttempt(t *testing.T) {
	node, err := newWorkerNodeFromConfig(zap.NewNop(), Config{ID: "0", Name: "test-node-0", Type: Mock, MaxReconnectBackOff: 1})
	require.NoError(t, err)
	mockNode, ok := node.(*MockNode)
	require.True(t, ok)
	mockNode.On("connect").Return(errors.New("simulated connect failure")).Once()
	mockNode.On("connect").Return(nil)
	mockNode.On("disconnect").Return(nil)

	var mu sync.Mutex
	taken, released := 0, 0
	var stateAtLastRelease State
	config := func() (*flex.UpdateConfigRequest, func()) {
		mu.Lock()
		taken++
		mu.Unlock()
		return nil, func() {
			mu.Lock()
			released++
			stateAtLastRelease = mockNode.GetState()
			mu.Unlock()
		}
	}

	handleDone := make(chan struct{})
	go func() {
		mockNode.Handle(&sync.WaitGroup{}, config, nil, nil)
		close(handleDone)
	}()
	require.Eventually(t, func() bool { return mockNode.GetState() == ONLINE }, 10*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, 2, taken, "a failed and a successful attempt")
	assert.Equal(t, taken, released, "every attempt releases")
	assert.Equal(t, ONLINE, stateAtLastRelease, "the successful attempt releases after the node is ONLINE")
	mu.Unlock()

	mockNode.Stop()
	select {
	case <-handleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after the node was stopped")
	}
}
