package beemsg

import (
	"net"
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
// Returns nil  if the queue is empty.
func (nc *NodeConns) TryGet() net.Conn {
	e := nc.conns.tryGet()
	if e == nil {
		return nil
	}

	// Since Put() only accepts net.Conn, this should never fail
	return e.(net.Conn)
}

func (nc *NodeConns) Put(conn net.Conn) {
	nc.conns.put(conn)
}

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
