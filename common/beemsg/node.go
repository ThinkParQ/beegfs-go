package beemsg

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
	"github.com/thinkparq/gobee/types"
)

// The BeeGFS nodetype "enum"
type NodeType int

const (
	Invalid NodeType = iota
	Meta
	Storage
	Client
)

// The string representation of NodeType
func (nt NodeType) String() string {
	switch nt {
	case Meta:
		return "Meta"
	case Storage:
		return "Storage"
	case Client:
		return "Client"
	default:
		return "Invalid"
	}
}

// Creates NodeType from string (e.g. user input)
func NodeTypeFromString(s string) NodeType {
	switch strings.ToUpper(s) {
	case "META":
		return Meta
	case "STORAGE":
		return Storage
	case "CLIENT":
		return Client
	}

	return Invalid
}

// The node stores entries
type Node struct {
	Uid   int64
	Id    uint32
	Type  NodeType
	Alias string
	Addrs []string
}

func (node *Node) String() string {
	return fmt.Sprintf("Node{Id: %d, Type: %s, Alias: %s}", node.Id, node.Type, node.Alias)
}

// Makes a BeeMsg request using a TCP connection from the queue or opens a new one
func (node *Node) requestTCP(ctx context.Context, conns *NodeConns, authSecret int64, timeout time.Duration,
	req msg.SerializableMsg, resp msg.DeserializableMsg) error {

	// Loop through established connections from the store until one works
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
	// establish a new one.
	//
	// This allows for an "infinite" amount of connections to be opened, which is fine
	// for the current use case. If a connection limit must be implemented (a.k.a waiting for
	// existing connections to become available), this should probably happen here.

	conn, err := node.connect(ctx, authSecret, timeout)
	if err != nil {
		return err
	}

	err = WriteRead(ctx, conn, req, resp)
	if err != nil {
		conn.Close()
		return fmt.Errorf("request to node %s failed: %w", node, err)
	}

	// Request successful, push connection to store
	conns.Put(conn)
	return nil
}

type connResult struct {
	conn net.Conn
	err  error
}

// Attempt to TCP connect to one address at a time with the given timeout
func connectLoop(ctx context.Context, addrs []string, timeout time.Duration, ch chan connResult) {
	defer close(ch)
	dialer := net.Dialer{Timeout: timeout}
	errs := &types.MultiError{}

	// Try to connect to the node using its addresses in order
	for _, addr := range addrs {
		conn, err := dialer.Dial("tcp", addr)
		if err == nil {
			// Connection established
			select {
			// return the connection
			case ch <- connResult{conn: conn, err: nil}:
			// if the context has been canceled during connect, make sure connection is closed
			case <-ctx.Done():
				conn.Close()
			}
			return
		}

		errs.Errors = append(errs.Errors, err)
	}

	// Ensure we are not getting stuck here if nobody is receiving from that channel
	select {
	// All known addresses failed.
	case ch <- connResult{conn: nil, err: errs}:
	case <-ctx.Done():
	}
}

// Connects to the node by TCP
func (node *Node) connect(ctx context.Context, authSecret int64, timeout time.Duration) (net.Conn, error) {
	if len(node.Addrs) == 0 {
		return nil, fmt.Errorf("no known addresses of %s", node)
	}

	ch := make(chan connResult)
	// Try to connect
	go connectLoop(ctx, node.Addrs, timeout, ch)

	// Wait for the connection attempts to complete or the context being cancelled
	var conn net.Conn
	select {
	case res := <-ch:
		if res.err != nil {
			return nil, fmt.Errorf("no response from %s on all known addresses: %w", node, res.err)
		}
		conn = res.conn
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Authenticate the connection if requested
	if authSecret != 0 {
		if err := WriteTo(ctx, conn, &msg.AuthenticateChannel{AuthSecret: authSecret}); err != nil {
			return nil, fmt.Errorf("authenticating to %s failed: %w", node, err)
		}
	}

	return conn, nil
}

const (
	MaxDatagramSize = 65507
)

// Sends a message to the node via UDP and optionally waits for a response if resp is not `nil`
func (node *Node) requestUDP(ctx context.Context, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return fmt.Errorf("creating UDP socket for %s failed: %w", node, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	defer conn.Close()

	// Serialize message into buffer for multiple sends
	reqBytes := bytes.Buffer{}
	err = WriteTo(ctx, &reqBytes, req)
	if err != nil {
		return err
	}

	errs := &types.MultiError{}

	// Broadcast message to all known addresses at once
	for _, a := range node.Addrs {
		addr, err := net.ResolveUDPAddr("udp", a)
		if err != nil {
			errs.Errors = append(errs.Errors, err)
			continue
		}

		conn.WriteTo(reqBytes.Bytes(), addr)
		if err != nil {
			errs.Errors = append(errs.Errors, err)
		}
	}

	// We only want to error out if all sends failed
	if len(errs.Errors) == len(node.Addrs) {
		return fmt.Errorf("failed to send UDP message to %s on all known addresses: %w", node, errs)
	}

	// Optionally receive the response. Multiple duplicate responses might come in, we only care
	// about the first - the rest is silently dropped with the socket
	if resp != nil {
		respBuf := make([]byte, MaxDatagramSize)
		_, err := conn.Read(respBuf)
		if err != nil {
			return fmt.Errorf("reading UDP response from %s failed: %w", node, err)
		}

		respBytes := bytes.NewBuffer(respBuf)
		err = ReadFrom(ctx, respBytes, resp)
		if err != nil {
			return err
		}
	}

	return nil
}
