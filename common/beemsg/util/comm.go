package util

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/types"
)

const (
	MaxDatagramSize = 65507
)

// Sends a BeeMsg to all given addrs via UDP and receives a response into resp if that is not set
// to nil. This function is meant to be used with multiple addresses of a single node, therefore it
// will only receive one response on the same socket and discard the rest of them.
func RequestUDP(ctx context.Context, addrs []string,
	req msg.SerializableMsg, resp msg.DeserializableMsg) error {

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return fmt.Errorf("creating UDP socket failed: %w", err)
	}
	defer conn.Close()

	// Serialize message into buffer for multiple sends
	reqBytes, err := AssembleBeeMsg(req)
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

		_, err = conn.WriteTo(reqBytes, addr)
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

		recvCh := recvResponse(conn, respBuf)

		// Wait for receive or the context cancels
		select {
		case err = <-recvCh:
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("reading UDP response failed: %w", err)
		}

		msgLen, err := msg.ExtractMsgLen(respBuf[0:msg.HeaderLen])
		if err != nil {
			return err
		}

		err = DisassembleBeeMsg(respBuf[0:msg.HeaderLen], respBuf[msg.HeaderLen:msgLen], resp)
		if err != nil {
			return err
		}
	}

	return nil
}

func recvResponse(conn *net.UDPConn, buf []byte) <-chan error {
	ch := make(chan error, 1)

	go func() {
		_, err := conn.Read(buf)
		ch <- err
	}()

	return ch
}

type connResult struct {
	conn net.Conn
	err  error
}

// Connects to a one of the given node addresses by TCP, trying the provided addresses in order.
// Setting authSecret to 0 disables authentication.
// Note that timeout controls the timeout per connection attempt while the context controls the
// global timeout. Thus, timeout should be significantly shorter than the global timeout.
func ConnectTCP(ctx context.Context, addrs []string, authSecret uint64, timeout time.Duration) (net.Conn, error) {
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

// Attempt to TCP connect to one address at a time with the given timeout.
// Note that timeout controls the timeout per connection attempt while the context controls the
// global timeout.
func connectLoop(ctx context.Context, addrs []string, timeout time.Duration, ch chan connResult) {
	defer close(ch)
	dialer := net.Dialer{Timeout: timeout}
	errs := &types.MultiError{}

	// Try to connect to the node using its addresses in order
	for _, addr := range addrs {
		conn, err := dialer.DialContext(ctx, "tcp", addr)

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
