package util

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/thinkparq/beegfs-crypto/go/beecrypt"
)

type FramedConn struct {
	net.Conn
	transport *beecrypt.Transport
	peerID    beecrypt.KeyPairID
	sendBuf   []byte
	recvBuf   []byte
}

func NewFramedConn(conn net.Conn, transport *beecrypt.Transport, peerID beecrypt.KeyPairID) *FramedConn {
	return &FramedConn{Conn: conn, transport: transport, peerID: peerID}
}

func (c *FramedConn) PeerID() beecrypt.KeyPairID {
	return c.peerID
}

func (c *FramedConn) Read([]byte) (int, error) {
	return 0, errors.ErrUnsupported
}

func (c *FramedConn) Write(b []byte) (int, error) {
	if err := c.WriteMsg(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *FramedConn) WriteMsg(b []byte) error {
	offset := 0
	for {
		chunkLen := min(len(b)-offset, beecrypt.MaxPlaintextLen)
		needed := beecrypt.FrameHdrLen + chunkLen + beecrypt.AuthTagLen
		if len(c.sendBuf) < needed {
			c.sendBuf = make([]byte, needed)
		}

		copy(c.sendBuf[beecrypt.FrameHdrLen:], b[offset:offset+chunkLen])
		offset += chunkLen

		frameLen, err := c.transport.Seal(c.sendBuf, chunkLen, offset == len(b))
		if err != nil {
			return fmt.Errorf("sealing frame failed: %w", err)
		}
		if _, err := c.Conn.Write(c.sendBuf[:frameLen]); err != nil {
			return err
		}

		if offset == len(b) {
			return nil
		}
	}
}

func (c *FramedConn) ReadMsg(maxLen int) ([]byte, error) {
	var msg []byte
	for {
		if len(c.recvBuf) < beecrypt.FrameHdrLen {
			c.recvBuf = make([]byte, beecrypt.FrameHdrLen)
		}
		if _, err := io.ReadFull(c.Conn, c.recvBuf[:beecrypt.FrameHdrLen]); err != nil {
			return nil, err
		}

		info, err := beecrypt.PeekFrame(c.recvBuf[:beecrypt.FrameHdrLen])
		if err != nil {
			return nil, fmt.Errorf("invalid frame: %w", err)
		}
		if len(c.recvBuf) < info.Len {
			newBuf := make([]byte, info.Len)
			copy(newBuf, c.recvBuf[:beecrypt.FrameHdrLen])
			c.recvBuf = newBuf
		}
		if _, err := io.ReadFull(c.Conn, c.recvBuf[beecrypt.FrameHdrLen:info.Len]); err != nil {
			return nil, err
		}

		plainLen, final, err := c.transport.Open(c.recvBuf[:info.Len])
		if err != nil {
			return nil, fmt.Errorf("opening frame failed: %w", err)
		}
		if len(msg)+plainLen > maxLen {
			return nil, fmt.Errorf("received message exceeds %d bytes", maxLen)
		}

		msg = append(msg, c.recvBuf[beecrypt.FrameHdrLen:beecrypt.FrameHdrLen+plainLen]...)
		if final {
			return msg, nil
		}
	}
}
