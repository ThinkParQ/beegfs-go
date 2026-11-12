package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/common/configmgr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
)

func TestValidateServiceName(t *testing.T) {
	t.Run("empty service-name with OTLP enabled returns error", func(t *testing.T) {
		cfg := AppConfig{
			Telemetry: telemetry.Config{
				OTLP: telemetry.OTLPConfig{Enabled: true},
			},
		}
		err := cfg.ValidateConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service-name must be set when telemetry is enabled")
	})

	t.Run("empty service-name with Logs enabled returns error", func(t *testing.T) {
		cfg := AppConfig{
			Telemetry: telemetry.Config{
				Logs: telemetry.LogsConfig{Enabled: true},
			},
		}
		err := cfg.ValidateConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service-name must be set when telemetry is enabled")
	})

	t.Run("non-empty service-name passes service-name check", func(t *testing.T) {
		cfg := AppConfig{
			ServiceName: "test-svc",
			Telemetry: telemetry.Config{
				OTLP: telemetry.OTLPConfig{Enabled: true},
			},
		}
		err := cfg.ValidateConfig()
		// Some other validation may fail, but not the service-name check.
		if err != nil {
			assert.NotContains(t, err.Error(), "service-name must be set")
		}
	})

	t.Run("no telemetry enabled does not require service-name", func(t *testing.T) {
		cfg := AppConfig{
			// ServiceName intentionally empty; no telemetry enabled.
			Telemetry: telemetry.Config{},
		}
		err := cfg.ValidateConfig()
		// Other fields may fail, but not the service-name check.
		if err != nil {
			assert.NotContains(t, err.Error(), "service-name must be set")
		}
	})
}

// TestAuthSettingsDecodeUnsetAsNil checks that the [auth] settings have no defaults, so connauth can
// tell an absent key from false. This only holds while nothing registers a flag for them, because
// a flag always supplies a default.
func TestAuthSettingsDecodeUnsetAsNil(t *testing.T) {
	const baseCfg = `
[manager]
num-workers = 1
active-work-queue-size = 1
`
	loadCfg := func(t *testing.T, body string) *AppConfig {
		t.Helper()
		cfgFile := filepath.Join(t.TempDir(), "beegfs-sync.toml")
		require.NoError(t, os.WriteFile(cfgFile, []byte(body), 0644))
		flags := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)
		flags.String(configmgr.FlagConfigFile, cfgFile, "")
		cfgMgr, err := configmgr.New(flags, "BEESYNC_", &AppConfig{})
		require.NoError(t, err)
		cfg, ok := cfgMgr.Get().(*AppConfig)
		require.True(t, ok)
		return cfg
	}

	t.Run("absent keys decode to nil", func(t *testing.T) {
		cfg := loadCfg(t, baseCfg)
		assert.Nil(t, cfg.Auth.AuthFile)
		assert.Nil(t, cfg.Auth.AuthDisable)
	})

	t.Run("keys in the config file decode to set values", func(t *testing.T) {
		cfg := loadCfg(t, baseCfg+"[auth]\nauth-file = \"/run/secrets/conn.auth\"\nauth-disable = false\n")
		require.NotNil(t, cfg.Auth.AuthFile)
		assert.Equal(t, "/run/secrets/conn.auth", *cfg.Auth.AuthFile)
		require.NotNil(t, cfg.Auth.AuthDisable)
		assert.False(t, *cfg.Auth.AuthDisable)
	})

	t.Run("environment variables decode to set values", func(t *testing.T) {
		t.Setenv("BEESYNC_AUTH__AUTH_DISABLE", "true")
		t.Setenv("BEESYNC_AUTH__AUTH_FILE", "/run/secrets/env.auth")
		cfg := loadCfg(t, baseCfg)
		require.NotNil(t, cfg.Auth.AuthDisable)
		assert.True(t, *cfg.Auth.AuthDisable)
		require.NotNil(t, cfg.Auth.AuthFile)
		assert.Equal(t, "/run/secrets/env.auth", *cfg.Auth.AuthFile)
	})
}
