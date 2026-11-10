package subscriber

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	wsub "github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	pb "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handlerServer is a subscriber whose ReceiveEvents runs handle.
type handlerServer struct {
	pb.UnimplementedSubscriberServer
	handle func(stream pb.Subscriber_ReceiveEventsServer) error
}

func (h *handlerServer) ReceiveEvents(stream pb.Subscriber_ReceiveEventsServer) error {
	return h.handle(stream)
}

// acceptStream sends headers, then reads events until Watch closes the stream.
func acceptStream(stream pb.Subscriber_ReceiveEventsServer) error {
	if err := stream.SendHeader(nil); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

// silentStream accepts the stream but sends nothing, like a subscriber that only answers with its
// first ack.
func silentStream(stream pb.Subscriber_ReceiveEventsServer) error {
	<-stream.Context().Done()
	return nil
}

// startServer registers srv on a new gRPC server with opts, serves it on a local port without TLS,
// and returns its address.
func startServer(t *testing.T, register func(*grpc.Server), opts ...grpc.ServerOption) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(opts...)
	register(server)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

func startHandlerServer(t *testing.T, handle func(pb.Subscriber_ReceiveEventsServer) error, opts ...grpc.ServerOption) string {
	t.Helper()
	return startServer(t, func(s *grpc.Server) {
		pb.RegisterSubscriberServer(s, &handlerServer{handle: handle})
	}, opts...)
}

// newTestSubscriber returns a subscriber for addr that sends secret, or no secret when it is nil.
func newTestSubscriber(t *testing.T, addr string, secret []byte) *GRPCSubscriber {
	t.Helper()
	s := newGRPCSubscriber(GrpcConfig{Address: addr, TlsDisable: true, DisconnectTimeout: 1, SendAuthSecret: secret != nil}, secret)
	t.Cleanup(func() { _ = s.Disconnect() })
	return s
}

// TestConnectProbe checks that Connect reports a stream the subscriber rejected, and accepts one
// the subscriber accepted. A subscriber that sends headers is accepted at once. A silent one is
// accepted after streamProbeTimeout.
func TestConnectProbe(t *testing.T) {
	secret := []byte("secret")
	requireSecret := beegrpc.AuthServerOptions(secret, zap.NewNop())
	endAtOnce := func(pb.Subscriber_ReceiveEventsServer) error { return nil }

	testCases := []struct {
		name         string
		handle       func(pb.Subscriber_ReceiveEventsServer) error
		opts         []grpc.ServerOption
		clientSecret []byte
		wantErr      []string // substrings of the error; nil means Connect succeeds
		wantSlow     bool     // Connect waits out streamProbeTimeout
	}{
		{"acceptedWithHeaders", acceptStream, nil, nil, nil, false},
		{"authenticated", acceptStream, requireSecret, secret, nil, false},
		{"secretMissing", acceptStream, requireSecret, nil, []string{codes.Unauthenticated.String(), "grpc-send-auth-secret"}, false},
		{"secretWrong", acceptStream, requireSecret, []byte("other"), []string{codes.Unauthenticated.String(), "grpc-send-auth-secret"}, false},
		{"endedWithoutHeaders", endAtOnce, nil, nil, []string{"as soon as it was opened"}, false},
		{"silentSubscriber", silentStream, nil, nil, nil, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			addr := startHandlerServer(t, tc.handle, tc.opts...)
			s := newTestSubscriber(t, addr, tc.clientSecret)

			start := time.Now()
			retry, err := s.Connect(1)
			elapsed := time.Since(start)

			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, retry, "a rejected stream is retried with backoff")
				for _, want := range tc.wantErr {
					assert.ErrorContains(t, err, want)
				}
			}
			if tc.wantSlow {
				assert.GreaterOrEqual(t, elapsed, streamProbeTimeout)
			} else {
				assert.Less(t, elapsed, streamProbeTimeout)
			}
		})
	}
}

// TestReceiveAfterProbeTimeout checks Receive on a stream the probe timed out on. Receive waits
// for the probe's Header() call, still gets the subscriber's first response, and keeps the error
// that ends the stream for the handler.
func TestReceiveAfterProbeTimeout(t *testing.T) {
	release := make(chan struct{})
	addr := startHandlerServer(t, func(stream pb.Subscriber_ReceiveEventsServer) error {
		<-release
		if err := stream.Send(&pb.Response{CompletedSeq: 7}); err != nil {
			return err
		}
		return status.Error(codes.Unauthenticated, "secret changed")
	})
	s := newTestSubscriber(t, addr, nil)

	_, err := s.Connect(1)
	require.NoError(t, err)
	responses := s.Receive()
	close(release)

	select {
	case resp, ok := <-responses:
		require.True(t, ok, "the channel closed before the first response")
		assert.Equal(t, uint64(7), resp.CompletedSeq)
	case <-time.After(5 * time.Second):
		t.Fatal("no response from the subscriber")
	}
	select {
	case _, ok := <-responses:
		require.False(t, ok, "got a second response, want the channel closed")
	case <-time.After(5 * time.Second):
		t.Fatal("the channel did not close after the stream ended")
	}
	err = s.ReceiveErr()
	assert.ErrorContains(t, err, codes.Unauthenticated.String())
	assert.ErrorContains(t, err, "grpc-send-auth-secret")
}

// TestReceiveErrCleanClose checks that a stream the subscriber closes cleanly leaves no error.
func TestReceiveErrCleanClose(t *testing.T) {
	addr := startHandlerServer(t, func(stream pb.Subscriber_ReceiveEventsServer) error {
		return stream.SendHeader(nil)
	})
	s := newTestSubscriber(t, addr, nil)

	_, err := s.Connect(1)
	require.NoError(t, err)
	for range s.Receive() {
	}
	assert.NoError(t, s.ReceiveErr())
}

// TestConnectToSubscriberService connects to the subscriber service Remote runs, behind the same
// authentication check. The service sends headers as soon as it accepts a stream, so Connect never
// waits out streamProbeTimeout, whether the stream is accepted or rejected.
func TestConnectToSubscriberService(t *testing.T) {
	secret := []byte("secret")
	addr := startServer(t, func(s *grpc.Server) {
		ds, err := wsub.NewDiskStore(filepath.Join(t.TempDir(), "acks.json"))
		require.NoError(t, err)
		// An ack frequency of an hour means the service sends no ack during the test, so only its
		// headers can end the probe early.
		_, err = wsub.NewService(zap.NewNop(), time.Hour, s, ds)
		require.NoError(t, err)
	}, beegrpc.AuthServerOptions(secret, zap.NewNop())...)

	for _, tc := range []struct {
		name    string
		secret  []byte
		wantErr bool
	}{
		{"authenticated", secret, false},
		{"secretMissing", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSubscriber(t, addr, tc.secret)
			start := time.Now()
			_, err := s.Connect(1)
			assert.Less(t, time.Since(start), streamProbeTimeout)
			if tc.wantErr {
				assert.ErrorContains(t, err, codes.Unauthenticated.String())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
