package connauth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	testCases := []struct {
		name       string
		client     map[string]string
		settings   Settings
		wantSecret string // "" means ConnAuth off
		wantSource string
		wantErr    bool
	}{
		// The client is readable and decides.
		{"clientOn", on, Settings{}, "client", sourceClient, false},
		{"clientOff", off, Settings{}, "", sourceClient, false},
		{"clientOnFileOverridden", on, file(settingsFile), "settings", sourceAuthFile, false},
		{"clientOnFileHiddenOverridden", onFileHidden, file(settingsFile), "settings", sourceAuthFile, false},
		{"clientOnFileHidden", onFileHidden, Settings{}, "", "", true},
		{"clientOnDisableContradicts", on, disable(true), "", "", true},
		{"clientOffFileContradicts", off, file(settingsFile), "", "", true},
		{"clientOffDisableAgrees", off, disable(true), "", sourceClient, false},
		{"disableFalseIsUnset", on, disable(false), "client", sourceClient, false},
		{"clientOnNoFileReported", map[string]string{disableAuthKey: "false"}, Settings{}, "", "", true},
		{"clientOnEmptyFile", map[string]string{disableAuthKey: "false", authFileKey: empty}, Settings{}, "", "", true},
		{"clientValueUnknown", map[string]string{disableAuthKey: "1"}, Settings{}, "", "", true},

		// The client is not readable, so only the settings can decide.
		{"noClientNoSettings", nil, Settings{}, "", "", true},
		{"noClientFile", nil, file(settingsFile), "settings", sourceAuthFile, false},
		{"noClientFileMissing", nil, file(absent), "", "", true},
		{"noClientDisable", nil, disable(true), "", sourceAuthDisable, false},
		{"noClientDisableFalse", nil, disable(false), "", "", true},
		{"clientTooOldCountsAsUnreadable", tooOld, disable(true), "", sourceAuthDisable, false},
		{"clientTooOldNoSettings", tooOld, Settings{}, "", "", true},

		{"bothSettings", on, both, "", "", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := resolve(tc.client, tc.settings, "/mnt/beegfs")
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSecret != "", state.Enabled())
			assert.Equal(t, tc.wantSecret, string(state.Secret))
			assert.Equal(t, tc.wantSource, state.Source)
		})
	}
}
