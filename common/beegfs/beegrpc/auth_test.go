package beegrpc_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// startHealthServer starts an in-process server that requires serverSecret and logs rejections to
// log, and returns a health client that sends clientSecret.
func startHealthServer(t *testing.T, serverSecret, clientSecret []byte, log *zap.Logger) healthpb.HealthClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(beegrpc.AuthServerOptions(serverSecret, log)...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	conn, err := beegrpc.NewClientConn(lis.Addr().String(), beegrpc.WithTLSDisable(true), beegrpc.WithAuthSecret(clientSecret))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func TestAuthServerOptions(t *testing.T) {
	right := []byte("secret")
	testCases := []struct {
		name         string
		serverSecret []byte
		clientSecret []byte
		wantCode     codes.Code
	}{
		{"noSecretRequired", nil, nil, codes.OK},
		{"unneededSecretSent", nil, right, codes.OK},
		{"secretMissing", right, nil, codes.Unauthenticated},
		{"secretWrong", right, []byte("other"), codes.Unauthenticated},
		{"secretRight", right, right, codes.OK},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := startHealthServer(t, tc.serverSecret, tc.clientSecret, zap.NewNop())
			ctx := context.Background()

			_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("unary: code = %s, want %s (err: %v)", got, tc.wantCode, err)
			}

			stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
			if err == nil {
				_, err = stream.Recv()
			}
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("stream: code = %s, want %s (err: %v)", got, tc.wantCode, err)
			}
		})
	}
}

// TestAuthServerOptionsLogsRejections checks that the server logs a rejected call once, names the
// method, and never logs the secret. Later rejections inside the log interval are only counted.
func TestAuthServerOptionsLogsRejections(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	secret := []byte("secret")
	client := startHealthServer(t, secret, []byte("other"), zap.New(core))

	for range 3 {
		_, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("code = %s, want %s (err: %v)", got, codes.Unauthenticated, err)
		}
	}

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := fields["method"]; got != "/grpc.health.v1.Health/Check" {
		t.Errorf("method = %v, want /grpc.health.v1.Health/Check", got)
	}
	if got := fields["caller"]; got == "" || got == "unknown" {
		t.Errorf("caller = %v, want the client's address", got)
	}
	// Print every field, whatever its type, so a secret or hash inside a list is found too.
	for _, raw := range []string{"other", "secret"} {
		hash := strconv.FormatUint(util.GenerateAuthSecret([]byte(raw)), 10)
		for key, value := range fields {
			if printed := fmt.Sprint(value); strings.Contains(printed, hash) || printed == raw {
				t.Errorf("field %s logs the secret %q or its hash: %q", key, raw, printed)
			}
		}
	}
}

// countingHealth is a health server that counts the calls that reach it.
type countingHealth struct {
	healthpb.UnimplementedHealthServer
	calls atomic.Int32
}

func (c *countingHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	c.calls.Add(1)
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (c *countingHealth) Watch(_ *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	c.calls.Add(1)
	return stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING})
}

// TestAuthServerOptionsStopsBeforeHandler checks that a rejected call never reaches the handler, on
// unary and streaming calls alike. A handler that ran first could act on an unauthenticated call
// even though the caller sees Unauthenticated.
func TestAuthServerOptionsStopsBeforeHandler(t *testing.T) {
	secret := []byte("secret")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(beegrpc.AuthServerOptions(secret, zap.NewNop())...)
	handler := &countingHealth{}
	healthpb.RegisterHealthServer(server, handler)
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	for _, clientSecret := range [][]byte{nil, []byte("other")} {
		conn, err := beegrpc.NewClientConn(lis.Addr().String(), beegrpc.WithTLSDisable(true), beegrpc.WithAuthSecret(clientSecret))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		client := healthpb.NewHealthClient(conn)
		ctx := context.Background()

		_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Errorf("unary with secret %q: code = %s, want %s", clientSecret, got, codes.Unauthenticated)
		}
		stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
		if err == nil {
			_, err = stream.Recv()
		}
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Errorf("stream with secret %q: code = %s, want %s", clientSecret, got, codes.Unauthenticated)
		}
	}
	if got := handler.calls.Load(); got != 0 {
		t.Errorf("handler ran %d times for rejected calls, want 0", got)
	}
}
