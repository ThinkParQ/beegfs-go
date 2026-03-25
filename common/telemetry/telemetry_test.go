package telemetry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
)

// findFreePort finds an available TCP port by binding to :0.
func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// TestNewDisabled verifies that a disabled provider returns a no-op meter.
func TestNewDisabled(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{Enabled: false})
	require.NoError(t, err)
	require.NotNil(t, p)

	meter := p.Meter("test")
	counter, err := meter.Int64Counter("test.counter")
	require.NoError(t, err)
	// Should not panic.
	counter.Add(context.Background(), 1)

	hist, err := meter.Float64Histogram("test.histogram")
	require.NoError(t, err)
	hist.Record(context.Background(), 1.5)

	require.NoError(t, p.Shutdown(context.Background()))
}

// TestNewEnabledNoExporters verifies that enabling telemetry with no exporters returns an error.
func TestNewEnabledNoExporters(t *testing.T) {
	_, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "test",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no exporter")
}

// TestValidateConfig tests all validation rules.
func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     telemetry.Config
		wantErr bool
		errMsg  string
	}{
		{
			name:    "disabled config always valid",
			cfg:     telemetry.Config{Enabled: false},
			wantErr: false,
		},
		{
			name:    "enabled with no exporters",
			cfg:     telemetry.Config{Enabled: true},
			wantErr: true,
			errMsg:  "no exporter",
		},
		{
			name: "invalid OTLP protocol",
			cfg: telemetry.Config{
				Enabled: true,
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "tcp",
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
				},
			},
			wantErr: true,
			errMsg:  "protocol must be 'grpc' or 'http'",
		},
		{
			name: "missing OTLP endpoint",
			cfg: telemetry.Config{
				Enabled: true,
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Interval: 30 * time.Second,
				},
			},
			wantErr: true,
			errMsg:  "endpoint must be set",
		},
		{
			name: "OTLP interval too short",
			cfg: telemetry.Config{
				Enabled: true,
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Interval: 500 * time.Millisecond,
				},
			},
			wantErr: true,
			errMsg:  "interval must be at least 1s",
		},
		{
			name: "Prometheus port 0",
			cfg: telemetry.Config{
				Enabled: true,
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    0,
					Path:    "/metrics",
				},
			},
			wantErr: true,
			errMsg:  "port must be between 1 and 65535",
		},
		{
			name: "Prometheus port too high",
			cfg: telemetry.Config{
				Enabled: true,
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    70000,
					Path:    "/metrics",
				},
			},
			wantErr: true,
			errMsg:  "port must be between 1 and 65535",
		},
		{
			name: "empty Prometheus path",
			cfg: telemetry.Config{
				Enabled: true,
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    9090,
					Path:    "",
				},
			},
			wantErr: true,
			errMsg:  "path must be set",
		},
		{
			name: "valid OTLP-only config",
			cfg: telemetry.Config{
				Enabled: true,
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
				},
			},
			wantErr: false,
		},
		{
			name: "valid Prometheus-only config",
			cfg: telemetry.Config{
				Enabled: true,
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    9090,
					Path:    "/metrics",
				},
			},
			wantErr: false,
		},
		{
			name: "valid both enabled",
			cfg: telemetry.Config{
				Enabled: true,
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "http",
					Endpoint: "localhost:4318",
					Interval: 60 * time.Second,
				},
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    9090,
					Path:    "/metrics",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateConfig()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestWithOptions verifies that WithInstanceID and WithVersion set resource
// attributes that appear in exported metrics (via the Prometheus target_info metric).
func TestWithOptions(t *testing.T) {
	port := findFreePort(t)
	p, err := telemetry.New(
		telemetry.Config{
			Enabled:     true,
			ServiceName: "options-test",
			Prometheus: telemetry.PrometheusConfig{
				Enabled: true,
				Port:    port,
				Path:    "/metrics",
			},
		},
		telemetry.WithInstanceID("my-host:9000"),
		telemetry.WithVersion("v2.0.0"),
	)
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// Record a metric so the response is non-empty.
	counter, err := p.Meter("test").Int64Counter("options_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	// The OTel Prometheus exporter exposes resource attributes as target_info labels.
	// Label names use underscores (OTel attribute key normalization); values are verbatim.
	assert.Contains(t, bodyStr, "options_test_counter", "recorded counter should appear in output")
	assert.Contains(t, bodyStr, `service_instance_id="my-host:9000"`, "service.instance.id should appear in target_info")
	assert.Contains(t, bodyStr, `service_version="v2.0.0"`, "service.version should appear in target_info")
}

// TestShutdownDisabled verifies that Shutdown on a disabled provider returns nil.
func TestShutdownDisabled(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{Enabled: false})
	require.NoError(t, err)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// TestPrometheusEndpoint starts a provider with Prometheus enabled on a random
// port, then GETs the metrics path and verifies a 200 response.
func TestPrometheusEndpoint(t *testing.T) {
	port := findFreePort(t)
	cfg := telemetry.Config{
		Enabled:     true,
		ServiceName: "test-service",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	}
	p, err := telemetry.New(cfg)
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// Record a metric so the response is non-empty.
	meter := p.Meter("test")
	counter, err := meter.Int64Counter("test_prom_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 42)

	// The port is pre-bound by New(), so the server is ready immediately.
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotEmpty(t, body)
	assert.Contains(t, string(body), "test_prom_counter", "recorded counter should appear in Prometheus response")
}

// TestPrometheusPortAlreadyInUse verifies that New() returns an error immediately
// when the Prometheus port is already bound by another listener.
func TestPrometheusPortAlreadyInUse(t *testing.T) {
	// Hold the port open for the duration of the test.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	cfg := telemetry.Config{
		Enabled:     true,
		ServiceName: "test-service",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	}
	_, err = telemetry.New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind")
}

// TestShutdownEnabled verifies that Shutdown on an enabled Prometheus provider
// returns nil and tears down resources cleanly.
func TestShutdownEnabled(t *testing.T) {
	port := findFreePort(t)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "shutdown-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	})
	require.NoError(t, err)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// testTelemetryConfig implements telemetry.Configurer for testing UpdateConfiguration.
type testTelemetryConfig struct {
	cfg telemetry.Config
}

func (c *testTelemetryConfig) GetTelemetryConfig() telemetry.Config {
	return c.cfg
}

// TestUpdateConfiguration verifies the UpdateConfiguration behavior for various scenarios.
func TestUpdateConfiguration(t *testing.T) {
	baseCfg := telemetry.Config{Enabled: false}

	t.Run("non-Configurer returns error", func(t *testing.T) {
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		err = p.UpdateConfiguration("not a configurer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to get telemetry configuration")
	})

	t.Run("unchanged config on disabled provider is silent", func(t *testing.T) {
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: baseCfg})
		assert.NoError(t, err)
	})

	t.Run("toggling enabled on disabled provider accepts config", func(t *testing.T) {
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		// UpdateConfiguration stores the new config but does NOT restart the
		// provider, so no server is actually started here.
		newCfg := telemetry.Config{
			Enabled:     true,
			ServiceName: "toggled",
			Prometheus: telemetry.PrometheusConfig{
				Enabled: true,
				Port:    findFreePort(t),
				Path:    "/metrics",
			},
		}
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: newCfg})
		assert.NoError(t, err)
	})

	t.Run("changed config on enabled provider warns but succeeds", func(t *testing.T) {
		port := findFreePort(t)
		cfg := telemetry.Config{
			Enabled:     true,
			ServiceName: "update-test",
			Prometheus: telemetry.PrometheusConfig{
				Enabled: true,
				Port:    port,
				Path:    "/metrics",
			},
		}
		p, err := telemetry.New(cfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())

		changedCfg := cfg
		changedCfg.ServiceName = "changed-name"
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: changedCfg})
		assert.NoError(t, err)
	})
}

// TestHistogramBucketBoundaries verifies that custom DurationBoundaries are
// applied to *.duration histograms via the Prometheus exporter.
func TestHistogramBucketBoundaries(t *testing.T) {
	port := findFreePort(t)
	customBounds := []float64{0.01, 0.05, 0.1, 0.5, 1.0}
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "histogram-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
		Histograms: telemetry.HistogramConfig{
			DurationBoundaries: customBounds,
		},
	})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	hist, err := p.Meter("test").Float64Histogram("test.duration")
	require.NoError(t, err)
	hist.Record(context.Background(), 0.03)
	hist.Record(context.Background(), 0.75)

	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	// Verify the custom bucket boundaries appear in the Prometheus output.
	for _, bound := range customBounds {
		le := fmt.Sprintf("le=\"%g\"", bound)
		assert.True(t, strings.Contains(bodyStr, le),
			"expected bucket boundary %s in Prometheus output", le)
	}
}

// TestOTLPReaderConstruction verifies that OTLP exporter construction succeeds
// for both gRPC and HTTP protocols (connection is deferred, so no server needed).
func TestOTLPReaderConstruction(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Enabled:     true,
				ServiceName: "otlp-test",
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: protocol,
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
					Insecure: true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
	}
}
