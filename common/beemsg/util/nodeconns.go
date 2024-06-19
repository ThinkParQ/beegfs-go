package util

import (
	"context"
	"net"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
)

// A wrapper around the queue that stores connection handles of type net.Conn
type NodeConns struct {
	conns queue
}

// Create new NodeConns
func NewNodeConns() *NodeConns {
	return &NodeConns{}
}

// Tries to pop an element from the front of the queue and converts it to a net.Conn.
// Returns nil if the queue is empty.
func (nc *NodeConns) TryGet() net.Conn {
	e := nc.conns.tryGet()
	if e == nil {
		return nil
	}

	// Since Put() only accepts net.Conn, this should never fail
	return e.(net.Conn)
}

// Puts a connection to the back of the queue
func (nc *NodeConns) Put(conn net.Conn) {
	nc.conns.put(conn)
}

// Empties the queue and closes all connections. The user must ensure that this is called when the
// queue is no longer needed.
func (nc *NodeConns) CleanUp() {
	for {
		conn := nc.TryGet()
		if conn != nil {
			conn.Close()
			continue
		}
		break
	}
}

// Sends a BeeMsg to one of the given node addresses and receives a response into resp if
// it is not set to nil. The function tries to reuse existing connections from the NodeConns queue
// first. If there are none or none of them worked, a new connection is opened. After successfully
// sending and optionally receiving a BeeMsg, the connection is pushed into the queue for reuse.
// Setting authSecret to 0 disables BeeMsg authentication.
// Note that timeout controls the timeout per connection attempt while the context controls the
// global timeout. Thus, timeout should be significantly shorter than the global timeout.
func (conns *NodeConns) RequestTCP(ctx context.Context, addrs []string, authSecret int64,
	timeout time.Duration, req msg.SerializableMsg, resp msg.DeserializableMsg) error {

	// Loop through established connections in the connection queue until one works
	for {
		conn := conns.TryGet()
		if conn == nil {
			break
		}

		// found an established connection, try making a request
		err := WriteRead(ctx, conn, req, resp)
		if err == nil {
			// Request successful, push connection back to store
			conns.Put(conn)
			return nil
		}

		// If the request was not successful (e.g. due to closed connection, we just try the next
		// one and do not push it back to the store)
		conn.Close()
	}

	// No established connection left in store. If we are not yet over the connection limit,
	// establish a new one. This allows for an "infinite" amount of connections to be opened,
	// which is fine for the current use case. If a connection limit must be implemented (a.k.a
	// waiting for existing connections to become available), this should probably happen here.

	conn, err := ConnectTCP(ctx, addrs, authSecret, timeout)
	if err != nil {
		return err
	}

	err = WriteRead(ctx, conn, req, resp)
	if err != nil {
		conn.Close()
		return err
	}

	// Request successful, push connection to store
	conns.Put(conn)
	return nil
}
