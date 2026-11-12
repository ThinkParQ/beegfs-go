package connauth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestResolve walks the table in the package comment. An error row names a substring of the
// error, because operators act on its text and hints.
func TestResolve(t *testing.T) {
	dir := t.TempDir()
	clientFile := filepath.Join(dir, "client.auth")
	require.NoError(t, os.WriteFile(clientFile, []byte("client"), 0600))
	settingsFile := filepath.Join(dir, "settings.auth")
	require.NoError(t, os.WriteFile(settingsFile, []byte("settings"), 0600))
	empty := filepath.Join(dir, "empty.auth")
	require.NoError(t, os.WriteFile(empty, nil, 0600))
	absent := filepath.Join(dir, "absent.auth")

	on := map[string]string{disableAuthKey: "false", authFileKey: clientFile}
	onFileHidden := map[string]string{disableAuthKey: "false", authFileKey: absent}
	off := map[string]string{disableAuthKey: "true"}
	tooOld := map[string]string{authFileKey: clientFile}

	file := func(p string) Settings { return Settings{AuthFile: &p} }
	disable := func(b bool) Settings { return Settings{AuthDisable: &b} }
	both := Settings{AuthFile: &settingsFile, AuthDisable: new(bool)}
	*both.AuthDisable = true

	const unreadableHint = "make /proc/fs/beegfs readable"

	testCases := []struct {
		name       string
		client     map[string]string
		settings   Settings
		wantSecret string // "" means ConnAuth off
		wantFile   string
		wantSource string
		wantErr    string // a substring of the error; "" means no error
	}{
		// The client is readable and decides.
		{"clientOn", on, Settings{}, "client", clientFile, sourceClient, ""},
		{"clientOff", off, Settings{}, "", "", sourceClient, ""},
		{"clientOnFileOverridden", on, file(settingsFile), "settings", settingsFile, sourceAuthFile, ""},
		{"clientOnFileHiddenOverridden", onFileHidden, file(settingsFile), "settings", settingsFile, sourceAuthFile, ""},
		{"clientOnFileHidden", onFileHidden, Settings{}, "", "", "", "must be readable at the path the BeeGFS client reports"},
		{"clientOnDisableContradicts", on, disable(true), "", "", "", "auth.auth-disable is set, but"},
		{"clientOffFileContradicts", off, file(settingsFile), "", "", "", "auth.auth-file is set, but"},
		{"clientOffDisableAgrees", off, disable(true), "", "", sourceClient, ""},
		{"disableFalseIsUnset", on, disable(false), "client", clientFile, sourceClient, ""},
		{"clientOnNoFileReported", map[string]string{disableAuthKey: "false"}, Settings{}, "", "", "", "reports no " + authFileKey},
		{"clientOnEmptyFile", map[string]string{disableAuthKey: "false", authFileKey: empty}, Settings{}, "", "", "", "is empty"},
		{"clientValueUnknown", map[string]string{disableAuthKey: "1"}, Settings{}, "", "", "", "unexpected value"},
		{"clientOnSettingsFileEmpty", on, file(empty), "", "", "", "is empty"},

		// The client is not readable, so only the settings can decide.
		{"noClientNoSettings", nil, Settings{}, "", "", "", unreadableHint},
		{"noClientFile", nil, file(settingsFile), "settings", settingsFile, sourceAuthFile, ""},
		{"noClientFileMissing", nil, file(absent), "", "", "", "reading the auth file from " + sourceAuthFile},
		{"noClientFileEmpty", nil, file(empty), "", "", "", "is empty"},
		{"noClientDisable", nil, disable(true), "", "", sourceAuthDisable, ""},
		{"noClientDisableFalse", nil, disable(false), "", "", "", unreadableHint},
		{"clientTooOldCountsAsUnreadable", tooOld, disable(true), "", "", sourceAuthDisable, ""},
		{"clientTooOldNoSettings", tooOld, Settings{}, "", "", "", unreadableHint},

		{"bothSettings", on, both, "", "", "", "cannot both be set"},
		{"bothSettingsNoClient", nil, both, "", "", "", "cannot both be set"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := resolve(tc.client, tc.settings, "/mnt/beegfs")
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, state.Secret, "a failed resolve returns no secret")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSecret != "", state.Enabled())
			assert.Equal(t, tc.wantSecret, string(state.Secret))
			assert.Equal(t, tc.wantFile, state.AuthFile)
			assert.Equal(t, tc.wantSource, state.Source)
		})
	}
}

// stubClients makes getClients return clients and err for the rest of the test. It records the
// config of each call in calls.
func stubClients(t *testing.T, clients []procfs.Client, err error) *[]procfs.GetBeeGFSClientsConfig {
	t.Helper()
	calls := &[]procfs.GetBeeGFSClientsConfig{}
	saved := getClients
	getClients = func(_ context.Context, cfg procfs.GetBeeGFSClientsConfig, _ *logger.Logger) ([]procfs.Client, error) {
		*calls = append(*calls, cfg)
		return clients, err
	}
	t.Cleanup(func() { getClients = saved })
	return calls
}

// TestFromMount checks how FromMount turns the clients procfs reports into the input of resolve,
// and what it logs. getClients is stubbed, so no BeeGFS mount is needed.
func TestFromMount(t *testing.T) {
	dir := t.TempDir()
	clientFile := filepath.Join(dir, "client.auth")
	const secret = "client-secret"
	require.NoError(t, os.WriteFile(clientFile, []byte(secret), 0600))
	const mount = "/mnt/beegfs"

	clientOn := procfs.Client{ID: "c1", Config: map[string]string{disableAuthKey: "false", authFileKey: clientFile}}
	clientOff := procfs.Client{ID: "c2", Config: map[string]string{disableAuthKey: "true"}}
	disableSetting := true

	testCases := []struct {
		name       string
		mount      string
		clients    []procfs.Client
		clientsErr error
		settings   Settings
		wantSecret string
		wantSource string
		wantErr    string
		wantLevel  zapcore.Level
	}{
		{name: "mountPathEmpty", mount: "", wantErr: "mount-point must be set"},
		{name: "procfsError", mount: mount, clientsErr: errors.New("boom"), wantErr: "reading the BeeGFS client configuration from /proc/fs/beegfs: boom"},
		{name: "twoClients", mount: mount, clients: []procfs.Client{clientOn, clientOff}, wantErr: "found 2 BeeGFS clients"},
		{name: "noClientNoSettings", mount: mount, wantErr: "make /proc/fs/beegfs readable"},
		{name: "noClientDisable", mount: mount, settings: Settings{AuthDisable: &disableSetting}, wantSource: sourceAuthDisable, wantLevel: zapcore.WarnLevel},
		{name: "clientOn", mount: mount, clients: []procfs.Client{clientOn}, wantSecret: secret, wantSource: sourceClient, wantLevel: zapcore.InfoLevel},
		{name: "clientOff", mount: mount, clients: []procfs.Client{clientOff}, wantSource: sourceClient, wantLevel: zapcore.WarnLevel},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := stubClients(t, tc.clients, tc.clientsErr)
			core, logs := observer.New(zapcore.DebugLevel)
			log := &logger.Logger{Logger: zap.New(core)}

			state, err := FromMount(context.Background(), tc.mount, tc.settings, log)

			if tc.mount == "" {
				assert.Empty(t, *calls, "an empty mount path must fail before procfs is read")
			} else {
				require.Len(t, *calls, 1)
				assert.Equal(t, []string{tc.mount}, (*calls)[0].FilterByMounts, "only the client of the mount point counts")
			}
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				assert.Zero(t, logs.Len(), "a failed FromMount logs nothing, the caller logs the error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSecret, string(state.Secret))
			assert.Equal(t, tc.wantSource, state.Source)

			entries := logs.All()
			require.Len(t, entries, 1)
			assert.Equal(t, tc.wantLevel, entries[0].Level)
			fields := entries[0].ContextMap()
			assert.Equal(t, tc.wantSource, fields["source"])
			if state.Enabled() {
				assert.Equal(t, clientFile, fields["authFile"])
			}
			for key, value := range fields {
				assert.NotEqual(t, secret, value, "field %s logs the secret", key)
			}
			assert.NotContains(t, entries[0].Message, secret)
		})
	}
}
