package beegrpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// startHealthServer starts an in-process server that requires serverSecret, and returns a health
// client that sends clientSecret.
func startHealthServer(t *testing.T, serverSecret, clientSecret []byte) healthpb.HealthClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(beegrpc.AuthServerOptions(serverSecret)...)
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
			client := startHealthServer(t, tc.serverSecret, tc.clientSecret)
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
