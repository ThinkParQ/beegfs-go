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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testWorkerNodeServer struct {
	flex.UnimplementedWorkerNodeServer
	capabilities map[string]*flex.Feature
}

func (s *testWorkerNodeServer) GetCapabilities(ctx context.Context, request *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	return &flex.GetCapabilitiesResponse{
		BuildInfo:      &flex.BuildInfo{BinaryName: "sync", Version: "1.0.0"},
		Features:       s.capabilities,
		StartTimestamp: timestamppb.Now(),
	}, nil
}

type unimplementedCapabilitiesServer struct {
	flex.UnimplementedWorkerNodeServer
}

func startWorkerNodeServer(t *testing.T, server flex.WorkerNodeServer, opts ...grpc.ServerOption) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer(opts...)
	flex.RegisterWorkerNodeServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
	}

	return listener.Addr().String(), cleanup
}

func newTestBeeSyncNode(t *testing.T, addr string) *BeeSyncNode {
	t.Helper()
	config := Config{
		ID:         "node-1",
		Name:       "node-1",
		Type:       BeeSync,
		Address:    addr,
		TlsDisable: true,
	}

	node, err := newWorkerNodeFromConfig(zap.NewNop(), config)
	require.NoError(t, err)

	beeSyncNode, ok := node.(*BeeSyncNode)
	require.True(t, ok)
	return beeSyncNode
}

func connectInputs() (*flex.UpdateConfigRequest, *flex.BulkUpdateWorkRequest) {
	config := flex.UpdateConfigRequest_builder{
		BeeRemote: flex.BeeRemoteNode_builder{Address: "127.0.0.1:12345"}.Build(),
	}.Build()
	bulk := flex.BulkUpdateWorkRequest_builder{}.Build()
	return config, bulk
}

func TestBeeSyncNodeConnectRequiresAllFeatures(t *testing.T) {
	addr, cleanup := startWorkerNodeServer(t, &testWorkerNodeServer{
		capabilities: map[string]*flex.Feature{
			registry.FeatureFilterFiles: nil,
		},
	})
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	t.Cleanup(func() {
		_ = node.disconnect()
	})

	required := map[string]*flex.Feature{
		registry.FeatureFilterFiles: nil,
		"extra-feature":             nil,
	}
	config, bulk := connectInputs()
	err := node.connect(context.Background(), config, bulk, required)

	assert.Error(t, err)
	assert.ErrorContains(t, err, "incompatible sync node")
	assert.ErrorIs(t, err, registry.ErrUnsupportedFeature)
}

func TestBeeSyncNodeConnectHandlesUnimplementedCapabilities(t *testing.T) {
	addr, cleanup := startWorkerNodeServer(t, &unimplementedCapabilitiesServer{})
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	t.Cleanup(func() {
		_ = node.disconnect()
	})

	required := map[string]*flex.Feature{
		registry.FeatureFilterFiles: nil,
	}
	config, bulk := connectInputs()
	err := node.connect(context.Background(), config, bulk, required)

	assert.ErrorIs(t, err, registry.ErrCapabilitiesNotSupported)
}

// A connect that fails after opening its connection is disconnected once before the retry, and
// again when the node goes offline. The second disconnect must not report an error.
func TestBeeSyncNodeDisconnectAfterFailedConnect(t *testing.T) {
	// The test server does not implement UpdateConfig, so connect fails after it opens the
	// connection and checks capabilities.
	addr, cleanup := startWorkerNodeServer(t, &testWorkerNodeServer{})
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	config, bulk := connectInputs()
	require.Error(t, node.connect(context.Background(), config, bulk, nil))
	require.NotNil(t, node.conn)

	assert.NoError(t, node.disconnect())
	assert.Nil(t, node.conn)
	assert.NoError(t, node.disconnect())
}

// TestBeeSyncNodeConnectSendsAuthSecret checks that Remote authenticates to a Sync node with the
// secret set on the worker config. The server requires an extra feature, so a call that passes
// authentication fails the feature check instead.
func TestBeeSyncNodeConnectSendsAuthSecret(t *testing.T) {
	secret := []byte("secret")
	testCases := []struct {
		name         string
		remoteSecret []byte
		wantAuthErr  bool
	}{
		{"secretMissing", nil, true},
		{"secretWrong", []byte("other"), true},
		{"secretRight", secret, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			addr, cleanup := startWorkerNodeServer(t, &testWorkerNodeServer{
				capabilities: map[string]*flex.Feature{registry.FeatureFilterFiles: nil},
			}, beegrpc.AuthServerOptions(secret, zap.NewNop())...)
			defer cleanup()

			node := newTestBeeSyncNode(t, addr)
			t.Cleanup(func() { _ = node.disconnect() })

			node.config.SetAuthSecret(tc.remoteSecret)
			config, bulk := connectInputs()
			required := map[string]*flex.Feature{
				registry.FeatureFilterFiles: nil,
				"extra-feature":             nil,
			}
			// The worker loop retries both failures with backoff. A Sync node that rejects
			// Remote's secret may be fixed by an operator without restarting Remote.
			err := node.connect(context.Background(), config, bulk, required)
			require.Error(t, err)
			if tc.wantAuthErr {
				assert.ErrorContains(t, err, codes.Unauthenticated.String())
			} else {
				assert.ErrorIs(t, err, registry.ErrUnsupportedFeature)
				assert.NotEqual(t, codes.Unauthenticated, status.Code(err))
			}
		})
	}
}

// rejectingWorkerNodeServer is a Sync node that passes the feature check but refuses every
// configuration, the way a real Sync node does when Remote rejects the secret it calls back with.
// It records the requests it gets.
type rejectingWorkerNodeServer struct {
	testWorkerNodeServer
	message string
	got     []*flex.UpdateConfigRequest
}

func (s *rejectingWorkerNodeServer) UpdateConfig(_ context.Context, req *flex.UpdateConfigRequest) (*flex.UpdateConfigResponse, error) {
	s.got = append(s.got, req)
	return &flex.UpdateConfigResponse{Result: flex.UpdateConfigResponse_FAILURE, Message: s.message}, nil
}

// TestBeeSyncNodeConnectConfigRejected checks how Remote reacts when a Sync node accepts Remote's
// secret but refuses the configuration because Remote rejected the secret the node called back
// with. The call fails with ErrConfigRejected, and the error carries the node's reason. The worker
// loop then retries the connect with backoff, so an operator can fix the secret without restarting
// Remote.
func TestBeeSyncNodeConnectConfigRejected(t *testing.T) {
	secret := []byte("secret")
	reason := "rpc error: code = Unauthenticated desc = request requires authentication but provided secret doesn't match (hint: the BeeGFS clients of Remote and this Sync node must use the same connAuthFile)"
	fake := &rejectingWorkerNodeServer{
		testWorkerNodeServer: testWorkerNodeServer{capabilities: map[string]*flex.Feature{registry.FeatureFilterFiles: nil}},
		message:              reason,
	}
	addr, cleanup := startWorkerNodeServer(t, fake, beegrpc.AuthServerOptions(secret, zap.NewNop())...)
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	t.Cleanup(func() { _ = node.disconnect() })
	node.config.SetAuthSecret(secret)

	config, bulk := connectInputs()
	err := node.connect(context.Background(), config, bulk, map[string]*flex.Feature{registry.FeatureFilterFiles: nil})
	assert.ErrorIs(t, err, ErrConfigRejected)
	assert.ErrorContains(t, err, "connAuthFile", "the node's hint reaches Remote's log")

	require.Len(t, fake.got, 1)
	assert.Empty(t, fake.got[0].GetBeeRemote().GetAuthSecret(), "Remote must never push its secret to a Sync node")
	assert.False(t, fake.got[0].GetBeeRemote().GetAuthDisable())
}
