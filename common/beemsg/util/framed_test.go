package util

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-crypto/go/beecrypt"
)

func testKeyPair(t *testing.T) *beecrypt.KeyPair {
	t.Helper()
	pem, err := beecrypt.GenerateKeyPair()
	require.NoError(t, err)
	kp, err := beecrypt.LoadKeyPair(pem)
	require.NoError(t, err)
	return kp
}

func TestConnectTCPFramed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, server := testKeyPair(t), testKeyPair(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tp, peerID, err := beecrypt.Handshake(ctx, conn, beecrypt.RoleResponder,
					beecrypt.ModeEncrypted, server, nil)
				if err != nil || peerID != client.ID() {
					return
				}
				fc := NewFramedConn(conn, tp, peerID)
				req := &testMsg{}
				if ReadFrom(ctx, fc, req) == nil {
					WriteTo(ctx, fc, req)
				}
			}()
		}
	}()

	auth := ConnAuth{
		Crypto: &ConnCrypto{KeyPair: client, Mode: beecrypt.ModeEncrypted},
		PeerID: server.ID(),
	}

	conn, err := ConnectTCP(ctx, []string{ln.Addr().String()}, auth, time.Second)
	require.NoError(t, err)
	defer conn.Close()

	in := testMsg{fieldA: 123, fieldB: make([]byte, beecrypt.MaxPlaintextLen), flags: 50000}
	out := testMsg{}
	require.NoError(t, WriteRead(ctx, conn, &in, &out))
	assert.Equal(t, in, out)

	auth.PeerID = client.ID()
	_, err = ConnectTCP(ctx, []string{ln.Addr().String()}, auth, time.Second)
	assert.ErrorIs(t, err, beecrypt.ErrPeerMismatch)
}
