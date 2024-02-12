package beemsg

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
	"github.com/thinkparq/gobee/types"
)

const (
	MaxDatagramSize = 65507
)

// Sends a message to the node via UDP and optionally waits for a response if resp is not `nil`
func RequestUDP(ctx context.Context, addrs []string, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return fmt.Errorf("creating UDP socket failed: %w", err)
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
	for _, a := range addrs {
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
	if len(errs.Errors) == len(addrs) {
		return fmt.Errorf("failed to send UDP message to all given addresses: %w", errs)
	}

	// Optionally receive the response. Multiple duplicate responses might come in, we only care
	// about the first - the rest is silently dropped with the socket
	if resp != nil {
		respBuf := make([]byte, MaxDatagramSize)
		_, err := conn.Read(respBuf)
		if err != nil {
			return fmt.Errorf("reading UDP response failed: %w", err)
		}

		respBytes := bytes.NewBuffer(respBuf)
		err = ReadFrom(ctx, respBytes, resp)
		if err != nil {
			return err
		}
	}

	return nil
}

// Makes a BeeMsg request using a TCP connection from the connection queue or opens a new one
func RequestTCP(ctx context.Context, addrs []string, conns *NodeConns, authSecret int64,
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
	// establish a new one.
	//
	// This allows for an "infinite" amount of connections to be opened, which is fine
	// for the current use case. If a connection limit must be implemented (a.k.a waiting for
	// existing connections to become available), this should probably happen here.

	conn, err := ConnectTCP(ctx, addrs, authSecret, timeout)
	if err != nil {
		return err
	}

	err = WriteRead(ctx, conn, req, resp)
	if err != nil {
		conn.Close()
		return fmt.Errorf("request failed: %w", err)
	}

	// Request successful, push connection to store
	conns.Put(conn)
	return nil
}

type connResult struct {
	conn net.Conn
	err  error
}

// Connects to a node by TCP, trying the provided addresses in order. Setting authSecret to 0
// disables authentication.
func ConnectTCP(ctx context.Context, addrs []string, authSecret int64, timeout time.Duration) (net.Conn, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	ch := make(chan connResult)
	// Try to connect
	go connectLoop(ctx, addrs, timeout, ch)

	// Wait for the connection attempts to complete or the context being cancelled
	var conn net.Conn
	select {
	case res := <-ch:
		if res.err != nil {
			return nil, fmt.Errorf("no response from any address %v: %w", addrs, res.err)
		}
		conn = res.conn
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Authenticate the connection if requested
	if authSecret != 0 {
		if err := WriteTo(ctx, conn, &msg.AuthenticateChannel{AuthSecret: authSecret}); err != nil {
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	return conn, nil
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
