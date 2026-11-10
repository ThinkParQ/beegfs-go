package beegrpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
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
	for key, value := range fields {
		if s, ok := value.(string); ok && (s == "other" || s == "secret") {
			t.Errorf("field %s logs a secret: %q", key, s)
		}
	}
}
