package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/rst/internal/connauth"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/config"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/server"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestWarnDeprecatedMgmtdAuth checks when Remote warns that the deprecated management.auth-file
// and management.auth-disable settings are ignored. It warns only when the settings would have
// given Remote a different secret than the one its BeeGFS client gives it.
func TestWarnDeprecatedMgmtdAuth(t *testing.T) {
	dir := t.TempDir()
	const clientSecret = "client-secret-91b2"
	clientFile := filepath.Join(dir, "client.auth")
	require.NoError(t, os.WriteFile(clientFile, []byte(clientSecret), 0600))
	sameContent := filepath.Join(dir, "copy.auth")
	require.NoError(t, os.WriteFile(sameContent, []byte(clientSecret), 0600))
	const otherSecret = "other-secret-5e07"
	otherFile := filepath.Join(dir, "other.auth")
	require.NoError(t, os.WriteFile(otherFile, []byte(otherSecret), 0600))
	absent := filepath.Join(dir, "absent.auth")

	clientOn := connauth.State{Secret: []byte(clientSecret), AuthFile: clientFile, Source: "BeeGFS client"}
	clientOff := connauth.State{Source: "BeeGFS client"}

	testCases := []struct {
		name        string
		authFile    string
		authDisable bool
		state       connauth.State
		wantWarn    bool
	}{
		{"sameFile", clientFile, false, clientOn, false},
		{"sameContentOtherPath", sameContent, false, clientOn, false},
		{"otherContent", otherFile, false, clientOn, true},
		{"fileUnreadableClientOn", absent, false, clientOn, true},
		{"fileUnreadableClientOff", absent, false, clientOff, false},
		{"fileReadableClientOff", clientFile, false, clientOff, true},
		{"disableClientOn", clientFile, true, clientOn, true},
		{"disableClientOff", clientFile, true, clientOff, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			cfg := config.MgmtdConfig{}
			//lint:ignore SA1019 the test sets the deprecated settings it checks.
			cfg.AuthFile, cfg.AuthDisable = tc.authFile, tc.authDisable

			warnDeprecatedMgmtdAuth(&logger.Logger{Logger: zap.New(core)}, cfg, tc.state)

			if !tc.wantWarn {
				assert.Zero(t, logs.Len())
				return
			}
			require.Equal(t, 1, logs.Len())
			entry := logs.All()[0]
			assert.Equal(t, zapcore.WarnLevel, entry.Level)
			assert.Contains(t, entry.Message, "ignoring the deprecated")
			fields := entry.ContextMap()
			assert.Equal(t, tc.authFile, fields["management.auth-file"])
			assert.Equal(t, tc.authDisable, fields["management.auth-disable"])
			assert.Equal(t, tc.state.Enabled(), fields["clientUsesConnAuth"])
			for key, value := range fields {
				if s, ok := value.(string); ok {
					assert.NotContains(t, s, clientSecret, "field %s logs a secret", key)
					assert.NotContains(t, s, otherSecret, "field %s logs a secret", key)
				}
			}
		})
	}
}

// TestCtlAuth checks the auth settings Remote hands to the CTL library.
func TestCtlAuth(t *testing.T) {
	file, disable := ctlAuth(connauth.State{Secret: []byte("s"), AuthFile: "/etc/beegfs/conn.auth"})
	assert.Equal(t, "/etc/beegfs/conn.auth", file)
	assert.False(t, disable)

	file, disable = ctlAuth(connauth.State{})
	assert.Empty(t, file)
	assert.True(t, disable)
}

// authSecretOf reads the secret SetAuthSecret stored on a worker config. The field is unexported,
// so the test reads it through reflection, which allows reads but not writes.
func authSecretOf(cfg worker.Config) []byte {
	return reflect.ValueOf(cfg).FieldByName("authSecret").Bytes()
}

// TestSetWorkerAuth checks that every Sync worker gets the secret, and that a state without
// connection authentication clears it.
func TestSetWorkerAuth(t *testing.T) {
	workers := []worker.Config{{ID: "1", Type: worker.BeeSync}, {ID: "2", Type: worker.BeeSync}}
	setWorkerAuth(workers, connauth.State{Secret: []byte("secret"), AuthFile: "/f"})
	for _, w := range workers {
		assert.Equal(t, []byte("secret"), authSecretOf(w), "worker %s", w.ID)
	}
	setWorkerAuth(workers, connauth.State{})
	for _, w := range workers {
		assert.Nil(t, authSecretOf(w), "worker %s", w.ID)
	}
}

// TestNewBeeRemoteNode checks the description of Remote pushed to Sync nodes. It carries Remote's
// address and management settings, and never the secret.
func TestNewBeeRemoteNode(t *testing.T) {
	cfg := &config.AppConfig{
		Server: server.Config{Address: "remote:9010"},
		Management: config.MgmtdConfig{
			Address:                "mgmtd:8010",
			TLSDisableVerification: true,
			UseProxy:               true,
		},
	}
	//lint:ignore SA1019 a deprecated auth-file must not leak into the pushed description.
	cfg.Management.AuthFile = "/etc/beegfs/conn.auth"

	node := newBeeRemoteNode("0", cfg, []byte("cert"))
	assert.Equal(t, "0", node.GetId())
	assert.Equal(t, "remote:9010", node.GetAddress())
	assert.Equal(t, "mgmtd:8010", node.GetMgmtdAddress())
	assert.Equal(t, []byte("cert"), node.GetMgmtdTlsCert())
	assert.True(t, node.GetMgmtdTlsDisableVerification())
	assert.False(t, node.GetMgmtdTlsDisable())
	assert.True(t, node.GetMgmtdUseProxy())
	assert.Empty(t, node.GetAuthSecret(), "the secret must never be pushed to Sync nodes")
	assert.False(t, node.GetAuthDisable())
}
