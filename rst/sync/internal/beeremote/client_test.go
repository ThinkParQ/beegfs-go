package beeremote

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// capabilitiesServer is a remote that answers GetCapabilities with features. A nil features map
// makes it a remote that predates capabilities, so GetCapabilities is unimplemented.
type capabilitiesServer struct {
	beeremote.UnimplementedBeeRemoteServer
	features map[string]*flex.Feature
}

func (s *capabilitiesServer) GetCapabilities(ctx context.Context, request *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	if s.features == nil {
		return s.UnimplementedBeeRemoteServer.GetCapabilities(ctx, request)
	}
	return &flex.GetCapabilitiesResponse{
		BuildInfo:      &flex.BuildInfo{BinaryName: "beegfs-remote", Version: "test"},
		Features:       s.features,
		StartTimestamp: timestamppb.Now(),
	}, nil
}

// startRemote serves srv on a local port without TLS and returns its address.
func startRemote(t *testing.T, srv beeremote.BeeRemoteServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	beeremote.RegisterBeeRemoteServer(server, srv)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func newTestClient() *Client {
	return &Client{config: Config{TlsDisable: true}, readyMu: &sync.RWMutex{}}
}

// TestRequireFeatures checks a remote's features over a real connection, the way UpdateConfig does
// before it switches the node to that remote.
func TestRequireFeatures(t *testing.T) {
	feature := registry.FeatureRemoteLockRelease

	t.Run("a remote with the feature is accepted", func(t *testing.T) {
		addr := startRemote(t, &capabilitiesServer{features: map[string]*flex.Feature{feature: nil}})
		err := newTestClient().RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{Address: addr}.Build(), feature)
		assert.NoError(t, err)
	})

	t.Run("a remote without the feature is refused", func(t *testing.T) {
		addr := startRemote(t, &capabilitiesServer{features: map[string]*flex.Feature{registry.FeatureSyncDrain: nil}})
		err := newTestClient().RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{Address: addr}.Build(), feature)
		assert.ErrorIs(t, err, ErrIncompatibleRemote)
		assert.ErrorIs(t, err, registry.ErrUnsupportedFeature)
	})

	t.Run("a remote that predates capabilities is refused", func(t *testing.T) {
		addr := startRemote(t, &capabilitiesServer{})
		err := newTestClient().RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{Address: addr}.Build(), feature)
		assert.ErrorIs(t, err, ErrIncompatibleRemote)
		assert.ErrorIs(t, err, registry.ErrCapabilitiesNotSupported)
	})

	t.Run("a remote that is not listening yet is unavailable", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		err = newTestClient().RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{Address: addr}.Build(), feature)
		assert.ErrorIs(t, err, ErrUnavailable)
		assert.NotErrorIs(t, err, ErrIncompatibleRemote)
	})

	t.Run("the mock remote is not checked", func(t *testing.T) {
		err := newTestClient().RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{Address: "mock:0"}.Build(), feature)
		assert.NoError(t, err)
	})

	t.Run("a missing configuration or address is refused", func(t *testing.T) {
		client := newTestClient()
		assert.ErrorIs(t, client.RequireFeatures(context.Background(), nil, feature), ErrNilConfiguration)
		assert.ErrorIs(t, client.RequireFeatures(context.Background(), flex.BeeRemoteNode_builder{}.Build(), feature), ErrInvalidAddress)
	})
}

// fakeRegistryGetter answers GetCapabilities with err.
type fakeRegistryGetter struct {
	err error
}

func (f fakeRegistryGetter) GetCapabilities(context.Context, *flex.GetCapabilitiesRequest, ...grpc.CallOption) (*flex.GetCapabilitiesResponse, error) {
	return nil, f.err
}

// TestRequireFeaturesErrors pins how a failed capabilities call is reported. Only an unreachable
// remote is ErrUnavailable, which the sync server turns into a retry for remote. Any other failure
// is neither unavailable nor a missing feature.
func TestRequireFeaturesErrors(t *testing.T) {
	cases := []struct {
		name             string
		err              error
		wantUnavailable  bool
		wantIncompatible bool
	}{
		{"remote is unreachable", status.Error(codes.Unavailable, "connection refused"), true, false},
		{"the check timed out", status.Error(codes.DeadlineExceeded, "deadline exceeded"), true, false},
		{"remote predates capabilities", status.Error(codes.Unimplemented, "unknown method"), false, true},
		{"remote hit an internal error", status.Error(codes.Internal, "internal"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := requireFeatures(context.Background(), fakeRegistryGetter{err: tc.err}, registry.FeatureRemoteLockRelease)
			require.Error(t, err)
			assert.Equal(t, tc.wantUnavailable, errors.Is(err, ErrUnavailable), "ErrUnavailable")
			assert.Equal(t, tc.wantIncompatible, errors.Is(err, ErrIncompatibleRemote), "ErrIncompatibleRemote")
		})
	}
}
