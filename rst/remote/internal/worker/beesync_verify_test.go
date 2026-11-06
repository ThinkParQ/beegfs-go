package worker

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type verifyingWorkerNodeServer struct {
	flex.UnimplementedWorkerNodeServer
}

func (s *verifyingWorkerNodeServer) VerifyBackend(ctx context.Context, request *flex.VerifyBackendRequest) (*flex.VerifyBackendResponse, error) {
	return &flex.VerifyBackendResponse{Ok: false, Message: "the store rejected the credentials"}, nil
}

// onlineNode returns a node Remote counts as connected, whose client dials addr.
func onlineNode(t *testing.T, addr string) *BeeSyncNode {
	t.Helper()
	node := newTestBeeSyncNode(t, addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	node.client = flex.NewWorkerNodeClient(conn)
	node.setState(ONLINE)
	return node
}

// TestBeeSyncNodeVerifyBackend checks how a node's answer to a verify is reported. A node that
// cannot be reached, although Remote still counts it as connected, is not connected rather than a
// failed check, because it says nothing about the backend.
func TestBeeSyncNodeVerifyBackend(t *testing.T) {
	ctx := context.Background()

	addr, cleanup := startWorkerNodeServer(t, &verifyingWorkerNodeServer{})
	defer cleanup()
	_, err := onlineNode(t, addr).VerifyBackend(ctx, &flex.RemoteStorageTarget{})
	require.EqualError(t, err, "the store rejected the credentials")
	assert.NotErrorIs(t, err, ErrNodeNotConnected)

	oldAddr, oldCleanup := startWorkerNodeServer(t, &flex.UnimplementedWorkerNodeServer{})
	defer oldCleanup()
	_, err = onlineNode(t, oldAddr).VerifyBackend(ctx, &flex.RemoteStorageTarget{})
	require.ErrorIs(t, err, ErrVerifyUnsupported)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gone := listener.Addr().String()
	listener.Close()
	_, err = onlineNode(t, gone).VerifyBackend(ctx, &flex.RemoteStorageTarget{})
	require.ErrorIs(t, err, ErrNodeNotConnected)
}
