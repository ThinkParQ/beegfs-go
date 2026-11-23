package worker

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// verifyingConfigServer is a Sync node that accepts every connect and passes every verify.
type verifyingConfigServer struct {
	configServer
}

func (s *verifyingConfigServer) VerifyBackend(ctx context.Context, request *flex.VerifyBackendRequest) (*flex.VerifyBackendResponse, error) {
	return &flex.VerifyBackendResponse{Ok: true, Message: "reached the store"}, nil
}

// TestBeeSyncNodeVerifyBackendSendsAuthSecret checks that a verify goes over the connection that
// connect opened, which carries Remote's secret. The Sync node requires the secret on every call.
// A client that does not send it is rejected, which shows the node really checks VerifyBackend.
func TestBeeSyncNodeVerifyBackendSendsAuthSecret(t *testing.T) {
	ctx := context.Background()
	secret := []byte("secret")
	fake := &verifyingConfigServer{configServer{testWorkerNodeServer: testWorkerNodeServer{
		capabilities: map[string]*flex.Feature{registry.FeatureFilterFiles: nil},
	}}}
	addr, cleanup := startWorkerNodeServer(t, fake, beegrpc.AuthServerOptions(secret, zap.NewNop())...)
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	t.Cleanup(func() { _ = node.disconnect() })
	node.config.SetAuthSecret(secret)
	config, bulk := connectInputs()
	require.NoError(t, node.connect(ctx, config, bulk, map[string]*flex.Feature{registry.FeatureFilterFiles: nil}))
	node.setState(ONLINE)
	msg, err := node.VerifyBackend(ctx, &flex.RemoteStorageTarget{})
	require.NoError(t, err)
	assert.Equal(t, "reached the store", msg)

	_, err = onlineNode(t, addr).VerifyBackend(ctx, &flex.RemoteStorageTarget{})
	assert.ErrorContains(t, err, codes.Unauthenticated.String())
}
