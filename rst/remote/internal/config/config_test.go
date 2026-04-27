package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
)

func TestValidateServiceName(t *testing.T) {
	t.Run("empty service-name with Prometheus enabled returns error", func(t *testing.T) {
		cfg := AppConfig{
			Telemetry: telemetry.Config{
				Prometheus: telemetry.PrometheusConfig{Enabled: true},
			},
		}
		err := cfg.ValidateConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service-name must be set when telemetry is enabled")
	})

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
				Prometheus: telemetry.PrometheusConfig{Enabled: true},
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
