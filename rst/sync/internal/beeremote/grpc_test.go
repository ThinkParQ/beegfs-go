package beeremote

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// useLegacyAuthFile points legacySyncAuthFile at path for the rest of the test.
func useLegacyAuthFile(t *testing.T, path string) {
	t.Helper()
	saved := legacySyncAuthFile
	legacySyncAuthFile = path
	t.Cleanup(func() { legacySyncAuthFile = saved })
}

// TestRemoveLegacyAuthFile checks the cleanup of the raw secret earlier versions wrote. A missing
// file is the normal case on a fresh install, so it must not be an error.
func TestRemoveLegacyAuthFile(t *testing.T) {
	t.Run("fileExists", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "legacy")
		require.NoError(t, os.WriteFile(path, []byte("raw secret"), 0600))
		useLegacyAuthFile(t, path)
		require.NoError(t, RemoveLegacyAuthFile())
		assert.NoFileExists(t, path)
	})
	t.Run("fileAbsent", func(t *testing.T) {
		useLegacyAuthFile(t, filepath.Join(t.TempDir(), "legacy"))
		assert.NoError(t, RemoveLegacyAuthFile())
	})
	t.Run("removeFails", func(t *testing.T) {
		// A directory that is not empty cannot be removed, so os.Remove fails with an error that
		// is not ErrNotExist. The caller logs it as a warning.
		path := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(path, "child"), nil, 0600))
		useLegacyAuthFile(t, path)
		assert.Error(t, RemoveLegacyAuthFile())
		assert.DirExists(t, path)
	})
}

// TestClassifyRPCErrorUnauthenticated checks the error a Sync node gets when Remote rejects its
// secret. The node must not retry it as if Remote were down, and the operator needs the hint.
func TestClassifyRPCErrorUnauthenticated(t *testing.T) {
	err := classifyRPCError(status.Error(codes.Unauthenticated, "provided secret doesn't match"))
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "the status code must survive the hint")
	assert.NotErrorIs(t, err, ErrUnavailable)
	assert.ErrorContains(t, err, "connAuthFile")
}

// TestInitUsesNodeAuth checks the auth settings a Sync node hands to the CTL library when Remote
// configures it. CTL must use the auth file of this node's own BeeGFS client. The AuthSecret and
// AuthDisable fields Remote may still push, from an older Remote, are ignored, and the node writes
// no copy of the secret.
//
// CTL keeps its settings in a process-wide singleton that can be set only once, so this must be
// the only test in the package that initializes a grpcProvider.
func TestInitUsesNodeAuth(t *testing.T) {
	for _, key := range []string{"BEEGFS_AUTH_FILE", "BEEGFS_AUTH_DISABLE"} {
		// t.Setenv restores the variable after the test. Unset it so CTL reads only its config.
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
	legacy := filepath.Join(t.TempDir(), "legacy")
	useLegacyAuthFile(t, legacy)

	const nodeAuthFile = "/etc/beegfs/node-client.auth"
	cfg := Config{TlsDisable: true}
	cfg.SetAuth([]byte("node secret"), nodeAuthFile)
	client, err := New(cfg)
	require.NoError(t, err)

	pushed := &flex.BeeRemoteNode{
		Id:              "0",
		Address:         "127.0.0.1:1",
		MgmtdAddress:    "127.0.0.1:2",
		MgmtdTlsDisable: true,
		AuthSecret:      []byte("pushed secret"),
		AuthDisable:     true,
	}
	require.NoError(t, client.UpdateConfig(pushed, "node-1"))
	t.Cleanup(func() { _ = client.disconnect() })

	assert.Equal(t, nodeAuthFile, viper.GetString(config.AuthFileKey))
	assert.False(t, viper.GetBool(config.AuthDisableKey), "CTL must keep authentication on while the node's client uses it")
	assert.NoFileExists(t, legacy, "the node must not write the secret Remote pushed")
}
