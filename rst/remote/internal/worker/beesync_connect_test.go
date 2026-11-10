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
	retry, err := node.connect(config, bulk, required)

	assert.True(t, retry)
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
	retry, err := node.connect(config, bulk, required)

	assert.True(t, retry)
	assert.ErrorIs(t, err, registry.ErrCapabilitiesNotSupported)
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
			_, err := node.connect(config, bulk, required)
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
