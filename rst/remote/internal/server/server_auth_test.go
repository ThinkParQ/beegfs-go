package server_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/server"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const rejectMsg = "rejected a call that failed BeeGFS connection authentication"

// startRemoteServer builds the real Remote gRPC server with server.New and serves it on a local port
// without TLS. The job manager is nil, so only calls that never reach a handler that uses it are
// safe: GetCapabilities, and any call the auth check rejects. A health service is attached through
// GetGRPCServer() after New, the way other services are attached to Remote's server. It returns the
// server's address and the log the server writes to.
func startRemoteServer(t *testing.T, secret []byte) (string, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.WarnLevel)
	features := map[string]*flex.Feature{registry.FeatureRemoteLockRelease: nil}
	srv, err := server.New(&logger.Logger{Logger: zap.New(core)}, server.Config{TlsDisable: true}, nil, &flex.BuildInfo{}, features, secret)
	require.NoError(t, err)
	healthpb.RegisterHealthServer(srv.GetGRPCServer(), health.NewServer())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.GetGRPCServer().Serve(lis)
	t.Cleanup(srv.GetGRPCServer().Stop)
	return lis.Addr().String(), logs
}

// dial returns a client connection to addr that sends the hash of secret, or no secret when it is
// nil.
func dial(t *testing.T, addr string, secret []byte) *grpc.ClientConn {
	t.Helper()
	conn, err := beegrpc.NewClientConn(addr, beegrpc.WithTLSDisable(true), beegrpc.WithAuthSecret(secret))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// rejections returns the log entries for rejected calls. New also warns that TLS is off, so the
// other entries are skipped.
func rejections(logs *observer.ObservedLogs) []observer.LoggedEntry {
	return logs.FilterMessageSnippet(rejectMsg).All()
}

// TestServerRequiresAuth checks that server.New makes Remote's own server check the secret. It
// covers a unary call, a streaming call, and a service attached later through GetGRPCServer().
func TestServerRequiresAuth(t *testing.T) {
	secret := []byte("secret")
	addr, _ := startRemoteServer(t, secret)

	testCases := []struct {
		name         string
		clientSecret []byte
		wantCode     codes.Code
	}{
		{"secretMissing", nil, codes.Unauthenticated},
		{"secretWrong", []byte("other"), codes.Unauthenticated},
		{"secretRight", secret, codes.OK},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dial(t, addr, tc.clientSecret)
			ctx := context.Background()

			// require, not assert: if the check is missing, GetJobs below would reach a handler that
			// uses the nil job manager and crash the whole test binary.
			resp, err := beeremote.NewBeeRemoteClient(conn).GetCapabilities(ctx, &flex.GetCapabilitiesRequest{})
			require.Equal(t, tc.wantCode, status.Code(err), "GetCapabilities: %v", err)
			if tc.wantCode == codes.OK {
				require.NotNil(t, resp)
				assert.Contains(t, resp.GetFeatures(), registry.FeatureRemoteLockRelease)
			}

			_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			assert.Equal(t, tc.wantCode, status.Code(err), "health service attached after New: %v", err)

			// GetJobs needs the job manager, so only the rejected calls are safe to make. The
			// rejection arrives on Recv, not when the stream opens.
			if tc.wantCode == codes.Unauthenticated {
				stream, err := beeremote.NewBeeRemoteClient(conn).GetJobs(ctx, &beeremote.GetJobsRequest{})
				if err == nil {
					_, err = stream.Recv()
				}
				assert.Equal(t, codes.Unauthenticated, status.Code(err), "GetJobs: %v", err)
			}
		})
	}
}

// TestServerWithoutAuth checks that a nil secret leaves Remote's server open. That is the state
// when the BeeGFS client of Remote's mount does not use connection authentication.
func TestServerWithoutAuth(t *testing.T) {
	addr, logs := startRemoteServer(t, nil)
	for _, clientSecret := range [][]byte{nil, []byte("any")} {
		_, err := beeremote.NewBeeRemoteClient(dial(t, addr, clientSecret)).GetCapabilities(context.Background(), &flex.GetCapabilitiesRequest{})
		assert.NoError(t, err, "client secret %q", clientSecret)
	}
	assert.Empty(t, rejections(logs))
}

// TestServerLogsRejections checks that Remote's server logs rejected calls once inside the log
// interval, names the method and the caller, and never logs the secret or its hash.
func TestServerLogsRejections(t *testing.T) {
	serverSecret, callerSecret := []byte("remote-secret-7f3a"), []byte("caller-secret-c41d")
	addr, logs := startRemoteServer(t, serverSecret)
	client := beeremote.NewBeeRemoteClient(dial(t, addr, callerSecret))
	for range 3 {
		_, err := client.GetCapabilities(context.Background(), &flex.GetCapabilitiesRequest{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}

	entries := rejections(logs)
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	assert.Equal(t, beeremote.BeeRemote_GetCapabilities_FullMethodName, fields["method"])
	assert.True(t, strings.HasPrefix(fields["caller"].(string), "127.0.0.1:"), "caller = %v", fields["caller"])
	assert.Equal(t, "server", fields["component"], "the log carries the server's component field")
	var forbidden []string
	for _, s := range [][]byte{serverSecret, callerSecret} {
		forbidden = append(forbidden, string(s), strconv.FormatUint(util.GenerateAuthSecret(s), 10))
	}
	// Print every field, whatever its type, so a secret inside a list or a nested value is found.
	for key, value := range fields {
		for _, f := range forbidden {
			assert.NotContains(t, fmt.Sprint(value), f, "field %s logs a secret or its hash", key)
		}
	}
}
