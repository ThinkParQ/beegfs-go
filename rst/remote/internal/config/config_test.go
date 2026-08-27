package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/configmgr"
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

// TestDeprecatedRequestQueueDepthStillParses guards backwards compatibility for configuration files
// written against v8.4.1 and earlier, which shipped a job.request-queue-depth setting.
//
// The setting no longer does anything, but configuration is decoded with UnmarshalExact, which
// rejects any key that has no matching struct field. Deleting job.Config.RequestQueueDepth would
// therefore turn a harmlessly ignored setting into a startup failure for every user who set it.
// The field is retained solely to keep those files parsing.
//
// If this test fails because the field was removed, that removal is a breaking change: it belongs
// in a major release, with a note telling users to drop the setting first.
func TestDeprecatedRequestQueueDepthStillParses(t *testing.T) {
	// A minimal configuration that satisfies ValidateConfig, which configmgr runs after decoding.
	const baseCfg = `
[job]
path-db = "/var/lib/beegfs/remote/path.badger"
min-job-entries-per-rst = 2
max-job-entries-per-rst = 4
`

	// loadCfg writes body to a temporary config file and loads it the way beegfs-remote does.
	loadCfg := func(t *testing.T, body string) (*AppConfig, error) {
		t.Helper()
		cfgFile := filepath.Join(t.TempDir(), "beegfs-remote.toml")
		require.NoError(t, os.WriteFile(cfgFile, []byte(body), 0644))
		flags := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)
		flags.String(configmgr.FlagConfigFile, cfgFile, "")

		cfgMgr, err := configmgr.New(flags, "BEEREMOTE_", &AppConfig{}, SetRSTTypeHook())
		if err != nil {
			return nil, err
		}
		cfg, ok := cfgMgr.Get().(*AppConfig)
		require.True(t, ok, "expected the config manager to return an *AppConfig")
		return cfg, nil
	}

	t.Run("a config file setting the deprecated key still parses", func(t *testing.T) {
		cfg, err := loadCfg(t, baseCfg+"request-queue-depth = 2048\n")
		require.NoError(t, err)
		//lint:ignore SA1019 asserting the deprecated field still decodes is the point of this test.
		assert.Equal(t, 2048, cfg.Job.RequestQueueDepth)
	})

	// Without this case the one above would keep passing if strict decoding were ever turned off,
	// which would leave the guard above asserting nothing.
	t.Run("an unknown key in the same section is still rejected", func(t *testing.T) {
		_, err := loadCfg(t, baseCfg+"not-a-real-setting = 1\n")
		require.Error(t, err)
	})
}
