package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestHandleExitsOnStop verifies that Handle() exits promptly when Stop() is
// called before the initial connect attempt fires. The bug: when connectLoop()
// returns false due to context cancellation, done is never set, causing Handle()
// to spin in a tight loop indefinitely.
func TestHandleExitsOnStop(t *testing.T) {
	w, err := newWorkerNodeFromConfig(zap.NewNop(), Config{
		ID:   "0",
		Name: "test-node",
		Type: Mock,
		// Keep reconnect backoff at 1s so Stop() can race the timer.
		MaxReconnectBackOff: 5,
		MockConfig: MockConfig{
			Expectations: []MockExpectation{
				// connect may or may not fire depending on timing; allow it.
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	returned := make(chan struct{})
	go func() {
		w.Handle(&wg, nil, nil, nil)
		close(returned)
	}()

	// Give Handle() time to enter connectLoop before stopping. The connect timer
	// is 1s, so 20ms is enough to enter the select without letting it fire.
	time.Sleep(20 * time.Millisecond)
	w.Stop()

	select {
	case <-returned:
		// Handle() exited cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("Handle() did not exit after Stop() — goroutine spinning in infinite loop")
	}
}
