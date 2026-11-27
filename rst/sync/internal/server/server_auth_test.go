package server

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/workmgr"
	beeremotepb "github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const rejectMsg = "rejected a call that failed BeeGFS connection authentication"

// startSyncServer builds the real Sync gRPC server with New and serves it on a local port without
// TLS. New does not report the port it listens on, so the test serves s.grpcServer on its own
// listener instead of calling ListenAndServe. workMgr may be nil. Then only GetCapabilities,
// BulkUpdateWork and calls the auth check rejects are safe, because the other handlers use it. It
// returns the server's address and the log the server writes to.
func startSyncServer(t *testing.T, secret []byte, workMgr *workmgr.Manager) (string, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.InfoLevel)
	features := map[string]*flex.Feature{registry.FeatureFilterFiles: nil}
	s, err := New(&logger.Logger{Logger: zap.New(core)}, Config{TlsDisable: true}, workMgr, &flex.BuildInfo{}, features, secret)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go s.grpcServer.Serve(lis)
	t.Cleanup(s.grpcServer.Stop)
	return lis.Addr().String(), logs
}

// syncClient returns a WorkerNode client for addr that sends the hash of secret, or no secret when
// it is nil. Remote calls Sync the same way.
func syncClient(t *testing.T, addr string, secret []byte) flex.WorkerNodeClient {
	t.Helper()
	conn, err := beegrpc.NewClientConn(addr, beegrpc.WithTLSDisable(true), beegrpc.WithAuthSecret(secret))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return flex.NewWorkerNodeClient(conn)
}

// TestServerRequiresAuth checks that New makes Sync's server check the secret. A Sync node runs as
// root on a mount that skips file permission checks, so an unchecked call could read or overwrite
// any file in BeeGFS.
func TestServerRequiresAuth(t *testing.T) {
	secret := []byte("secret")
	addr, _ := startSyncServer(t, secret, nil)

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
			client := syncClient(t, addr, tc.clientSecret)
			ctx := context.Background()

			// require, not assert: if the check is missing, the calls below would reach handlers
			// that use the nil work manager and crash the whole test binary.
			resp, err := client.GetCapabilities(ctx, &flex.GetCapabilitiesRequest{})
			require.Equal(t, tc.wantCode, status.Code(err), "GetCapabilities: %v", err)
			if tc.wantCode == codes.OK {
				assert.Contains(t, resp.GetFeatures(), registry.FeatureFilterFiles)
			}

			_, err = client.BulkUpdateWork(ctx, &flex.BulkUpdateWorkRequest{})
			assert.Equal(t, tc.wantCode, status.Code(err), "BulkUpdateWork: %v", err)

			// These handlers use the work manager, which is nil here. Only the rejected calls are
			// safe, and they show the check runs before the handler.
			if tc.wantCode == codes.Unauthenticated {
				_, err = client.UpdateConfig(ctx, &flex.UpdateConfigRequest{})
				assert.Equal(t, codes.Unauthenticated, status.Code(err), "UpdateConfig: %v", err)
				_, err = client.SubmitWork(ctx, &flex.SubmitWorkRequest{})
				assert.Equal(t, codes.Unauthenticated, status.Code(err), "SubmitWork: %v", err)
				_, err = client.Heartbeat(ctx, &flex.HeartbeatRequest{})
				assert.Equal(t, codes.Unauthenticated, status.Code(err), "Heartbeat: %v", err)
			}
		})
	}
}

// TestServerWithoutAuth checks that a nil secret leaves Sync's server open. That is the state when
// the BeeGFS client of the node's mount does not use connection authentication.
func TestServerWithoutAuth(t *testing.T) {
	addr, logs := startSyncServer(t, nil, nil)
	for _, clientSecret := range [][]byte{nil, []byte("any")} {
		_, err := syncClient(t, addr, clientSecret).GetCapabilities(context.Background(), &flex.GetCapabilitiesRequest{})
		assert.NoError(t, err, "client secret %q", clientSecret)
	}
	assert.Zero(t, logs.FilterMessageSnippet(rejectMsg).Len())
}

// TestServerLogsRejections checks that Sync's server logs rejected calls once inside the log
// interval, names the method and the caller, and never logs the secret or its hash.
func TestServerLogsRejections(t *testing.T) {
	serverSecret, callerSecret := []byte("sync-secret-2b9e"), []byte("caller-secret-d013")
	addr, logs := startSyncServer(t, serverSecret, nil)
	client := syncClient(t, addr, callerSecret)
	for range 3 {
		_, err := client.GetCapabilities(context.Background(), &flex.GetCapabilitiesRequest{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}

	entries := logs.FilterMessageSnippet(rejectMsg).All()
	require.Len(t, entries, 1)
	assert.Equal(t, zapcore.WarnLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	assert.Equal(t, flex.WorkerNode_GetCapabilities_FullMethodName, fields["method"])
	assert.True(t, strings.HasPrefix(fields["caller"].(string), "127.0.0.1:"), "caller = %v", fields["caller"])
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

// fakeRemote is a Remote that only answers GetCapabilities, with every feature a Sync node needs.
type fakeRemote struct {
	beeremotepb.UnimplementedBeeRemoteServer
}

func (fakeRemote) GetCapabilities(context.Context, *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	return &flex.GetCapabilitiesResponse{
		BuildInfo: &flex.BuildInfo{BinaryName: "beegfs-remote", Version: "test"},
		Features: map[string]*flex.Feature{
			registry.FeatureRemoteLockRelease: nil,
			registry.FeatureReserveBeforeLock: nil,
		},
		StartTimestamp: timestamppb.Now(),
	}, nil
}

// startFakeRemote serves a fakeRemote that requires secret on a local port without TLS, and
// returns its address.
func startFakeRemote(t *testing.T, secret []byte) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(beegrpc.AuthServerOptions(secret, zap.NewNop())...)
	beeremotepb.RegisterBeeRemoteServer(server, fakeRemote{})
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

// newWorkManager starts a real work manager whose BeeRemote client sends nodeSecret, as if the
// node's BeeGFS client used that secret. The manager is stopped when the test ends.
func newWorkManager(t *testing.T, nodeSecret []byte) *workmgr.Manager {
	t.Helper()
	dir := t.TempDir()
	clientCfg := beeremote.Config{TlsDisable: true}
	clientCfg.SetAuth(nodeSecret, "/unused")
	client, err := beeremote.New(clientCfg)
	require.NoError(t, err)
	// The manager needs a logger built by logger.New, which also sets up telemetry.
	log, err := logger.New(logger.Config{Type: "stdout", Level: 3}, nil)
	require.NoError(t, err)
	mgr, err := workmgr.NewAndStart(log, workmgr.Config{
		WorkJournalPath:     filepath.Join(dir, "journal"),
		JobStorePath:        filepath.Join(dir, "jobs"),
		ActiveWorkQueueSize: 10,
		NumWorkers:          1,
	}, client, filesystem.NewMockFS())
	require.NoError(t, err)
	t.Cleanup(mgr.Stop)
	return mgr
}

// TestUpdateConfigAuth checks the answer Remote gets when it configures a Sync node. Before the
// node accepts a configuration it calls back to the Remote the configuration names, with the
// secret of its own BeeGFS client:
//   - Remote rejects the secret: the node answers FAILURE with the reason, and no gRPC error.
//     Remote treats that as fatal for this attempt and reconnects from the start.
//   - Remote cannot be reached: the node answers Unavailable, so Remote retries with backoff.
//   - Remote accepts the secret: the node applies the configuration and answers SUCCESS.
//
// Remote's own secret on this call is the same as the node's in every case, so the node's server
// accepts it. Only the call back to Remote differs.
func TestUpdateConfigAuth(t *testing.T) {
	nodeSecret := []byte("node-secret")
	mgr := newWorkManager(t, nodeSecret)
	addr, _ := startSyncServer(t, nodeSecret, mgr)
	client := syncClient(t, addr, nodeSecret)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := lis.Addr().String()
	require.NoError(t, lis.Close())

	request := func(remoteAddr string) *flex.UpdateConfigRequest {
		return &flex.UpdateConfigRequest{
			NodeId:    "node-1",
			StateRoot: rst.DefaultStateRoot,
			Rsts:      []*flex.RemoteStorageTarget{{Id: 1, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}},
			BeeRemote: &flex.BeeRemoteNode{Id: "0", Address: remoteAddr, MgmtdAddress: "127.0.0.1:1", MgmtdTlsDisable: true},
		}
	}
	ctx := context.Background()

	t.Run("remoteRejectsSecret", func(t *testing.T) {
		resp, err := client.UpdateConfig(ctx, request(startFakeRemote(t, []byte("remote-secret"))))
		require.NoError(t, err, "a rejected secret is reported in the response, not as a gRPC error")
		assert.Equal(t, flex.UpdateConfigResponse_FAILURE, resp.GetResult())
		assert.Contains(t, resp.GetMessage(), codes.Unauthenticated.String())
		assert.Contains(t, resp.GetMessage(), "connAuthFile")
		assert.False(t, mgr.IsReady(), "the node must not accept a Remote it cannot report back to")
	})

	t.Run("remoteUnreachable", func(t *testing.T) {
		_, err := client.UpdateConfig(ctx, request(unreachable))
		assert.Equal(t, codes.Unavailable, status.Code(err))
		assert.False(t, mgr.IsReady())
	})

	// This applies the configuration, which initializes the CTL library. CTL can be initialized
	// only once per process, so this must stay the last case and the only one in the package that
	// succeeds.
	t.Run("remoteAcceptsSecret", func(t *testing.T) {
		resp, err := client.UpdateConfig(ctx, request(startFakeRemote(t, nodeSecret)))
		require.NoError(t, err)
		assert.Equal(t, flex.UpdateConfigResponse_SUCCESS, resp.GetResult(), resp.GetMessage())
		assert.True(t, mgr.IsReady())
	})
}
