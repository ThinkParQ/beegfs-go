package subscriber

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A failed Connect must close the connection it opened. The handler calls Connect again after every
// failure, so a connection left open would leak once per attempt. Each open connection keeps
// goroutines running, so a leak shows up as goroutines that never exit.
func TestGRPCSubscriberConnectClosesConnectionOnFailure(t *testing.T) {
	// Listening on port 0 and closing again yields a local address that refuses connections.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())

	const attempts = 20
	goroutinesBefore := runtime.NumGoroutine()
	s := newGRPCSubscriber(GrpcConfig{Address: address, TlsDisable: true})
	for range attempts {
		retry, err := s.Connect(1)
		require.Error(t, err)
		require.True(t, retry)
	}

	assert.Nil(t, s.conn)
	// A closed connection stops its goroutines shortly after Close returns, so wait for them. The
	// wait polls here rather than in assert.Eventually, which would add its own goroutines to the
	// count.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > goroutinesBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), goroutinesBefore, "goroutines of failed connections still run")
}
