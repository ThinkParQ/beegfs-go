package beemsg

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/gobee/beemsg/msg"
)

func TestNodeTypeFromString(t *testing.T) {
	assert.Equal(t, Meta, NodeTypeFromString("meta"))
	assert.Equal(t, Storage, NodeTypeFromString("StOrage"))
	assert.Equal(t, Client, NodeTypeFromString("CLIENT"))
	assert.Equal(t, Invalid, NodeTypeFromString("Management"))
	assert.Equal(t, Invalid, NodeTypeFromString("garbage"))
}

// Test establishing new connections to a node
func TestConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan any)

	go func() {
		sock, err := net.Listen("tcp", "")
		assert.NoError(t, err)

		addr := sock.Addr().String()
		sync := make(chan any)

		// The listener / receiver routine
		go func() {
			req := &msg.AuthenticateChannel{}

			// Accept connection without authentication
			conn, err := sock.Accept()
			assert.NoError(t, err)
			defer conn.Close()

			// Accept connection with authentication
			conn2, err := sock.Accept()
			assert.NoError(t, err)
			defer conn2.Close()

			// Read authenticate message and check secret value
			err = ReadFrom(ctx, conn2, req)
			assert.NoError(t, err)
			assert.EqualValues(t, 1234, req.AuthSecret)

			sock.Close()

			close(sync)
		}()

		// Connect without authentication and first addr being invalid
		_, err = (&Node{Addrs: []string{"thishostdoesntexist111", addr}}).connect(ctx, 0, 200*time.Millisecond)
		assert.NoError(t, err)

		// Connect with authentication
		_, err = (&Node{Addrs: []string{addr}}).connect(ctx, 1234, 100*time.Millisecond)
		assert.NoError(t, err)

		// Wait until the accepting / reading goroutine is done and the listener is closed
		<-sync

		// Check that connecting fails now
		_, err = (&Node{Addrs: []string{addr}}).connect(ctx, 0, 100*time.Millisecond)
		assert.Error(t, err)

		close(done)
	}()

	// Fail and end the test if it hangs after a timeout
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

// Test making TCP requests to a node. This also implicitly test establishing connections
func TestRequestTCP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	done := make(chan any)

	go func() {
		listener, err := net.Listen("tcp", "")
		assert.NoError(t, err)

		addr := listener.Addr().String()

		// The listener / receiver routine
		go func() {
			conn, err := listener.Accept()
			assert.NoError(t, err)

			req := &testMsg{}
			ReadFrom(ctx, conn, req)
			WriteTo(ctx, conn, req)

			// Handle second request, which should use the same connection
			ReadFrom(ctx, conn, req)
			WriteTo(ctx, conn, req)

			conn.Close()
			listener.Close()
		}()

		// Just an arbitrary message to send
		req := &testMsg{fieldA: 1234}
		resp := &testMsg{}

		node := &Node{Addrs: []string{"thishostdoesntexist", addr}}

		// Make the request
		err = node.requestTCP(ctx, 0, 100*time.Millisecond, req, resp)
		assert.NoError(t, err)
		assert.Equal(t, req.fieldA, resp.fieldA)

		req.fieldA = 2345

		// Make another request. The connection should have been stored and should be reused
		err = node.requestTCP(ctx, 0, 100*time.Millisecond, req, resp)
		assert.NoError(t, err)
		assert.Equal(t, req.fieldA, resp.fieldA)

		close(done)
	}()

	// Fail and end the test if it hangs after a timeout
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout")
	}

}

// Test making UDP requests to a node and receiving a response. This also implicitly test establishing connections
func TestRequestUDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan any)

	go func() {
		listener, _ := net.ListenUDP("udp", &net.UDPAddr{})
		addr := listener.LocalAddr().String()

		// The listener / receiver routine
		go func() {
			respBuf := make([]byte, MaxDatagramSize)
			_, from, err := listener.ReadFrom(respBuf)
			assert.NoError(t, err)
			listener.WriteTo(respBuf, from)
		}()

		// Just an arbitrary message to send
		req := &testMsg{fieldA: 1234}
		resp := &testMsg{}

		// Make the request
		err := (&Node{Addrs: []string{"thishostdoesntexist", addr}}).requestUDP(ctx, req, resp)
		assert.NoError(t, err)
		assert.Equal(t, req.fieldA, resp.fieldA)

		listener.Close()
		close(done)
	}()

	// Fail and end the test if it hangs after a timeout
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}
