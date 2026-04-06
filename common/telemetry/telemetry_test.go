package telemetry_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
			cfg:     telemetry.Config{Enabled: true, ServiceName: "test"},
			wantErr: true,
			errMsg:  "no exporter",
		},
		{
			name: "metrics enabled without service name",
			cfg: telemetry.Config{
				Enabled: true,
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    9090,
					Path:    "/metrics",
				},
			},
			wantErr: true,
			errMsg:  "telemetry.service-name must be set when metrics are enabled",
		},
		{
			name: "invalid OTLP protocol",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
			name: "OTLP compression gzip valid",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "gzip",
				},
			},
			wantErr: false,
		},
		{
			name: "OTLP compression none valid",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "none",
				},
			},
			wantErr: false,
		},
		{
			name: "OTLP compression empty valid",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "",
				},
			},
			wantErr: false,
		},
		{
			name: "OTLP compression invalid",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "zstd",
				},
			},
			wantErr: true,
			errMsg:  "telemetry.otlp.compression must be 'gzip' or 'none'",
		},
		{
			name: "OTLP timeout too short",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
					Timeout:  500 * time.Millisecond,
				},
			},
			wantErr: true,
			errMsg:  "telemetry.otlp.timeout must be at least 1s",
		},
		{
			name: "OTLP timeout zero valid (SDK default)",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
					Timeout:  0,
				},
			},
			wantErr: false,
		},
		{
			name: "valid Prometheus-only config",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test",
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
				Enabled:     true,
				ServiceName: "test",
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
		{
			name: "logs only (metrics disabled)",
			cfg: telemetry.Config{
				Enabled:     false,
				ServiceName: "test-service",
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
				},
			},
			wantErr: false,
		},
		{
			name: "logs and metrics both enabled",
			cfg: telemetry.Config{
				Enabled:     true,
				ServiceName: "test-service",
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    9090,
					Path:    "/metrics",
				},
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "http",
					Endpoint: "localhost:4318",
				},
			},
			wantErr: false,
		},
		{
			name: "logs enabled without service name",
			cfg: telemetry.Config{
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
				},
			},
			wantErr: true,
			errMsg:  "telemetry.service-name must be set",
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
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "test_prom_counter", "recorded counter should appear in Prometheus response")
	// Verify that Go runtime and process collectors registered on the custom
	// registry are included in the output.
	assert.Contains(t, bodyStr, "go_goroutines", "Go runtime metrics should appear in Prometheus response")
	assert.Contains(t, bodyStr, "process_resident_memory_bytes", "process metrics should appear in Prometheus response")
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

// observeGlobalLogger replaces the global zap logger with an observed one for
// the duration of the test, restoring the original on cleanup. Only log entries
// at level or above are captured.
func observeGlobalLogger(t *testing.T, level zapcore.Level) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(level)
	observed := zap.New(core)
	restore := zap.ReplaceGlobals(observed)
	t.Cleanup(restore)
	return logs
}

// assertLogContains fails the test if no captured log entry's Message contains substr.
// Note: only entry.Message is searched; structured zap fields in entry.Context are not.
// If production code ever moves the key text into a Field, update this helper accordingly.
func assertLogContains(t *testing.T, logs *observer.ObservedLogs, substr string, msgAndArgs ...interface{}) {
	t.Helper()
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, substr) {
			return
		}
	}
	assert.Fail(t, fmt.Sprintf("expected a log entry containing %q but none found", substr), msgAndArgs...)
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

	t.Run("disabled provider suppresses restart warning on config change", func(t *testing.T) {
		logs := observeGlobalLogger(t, zapcore.WarnLevel)
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

		// A second UpdateConfiguration on a still-disabled provider (sdkProvider
		// and logProvider remain nil) must also succeed without a restart warning,
		// regardless of whether the config changed.
		differentCfg := newCfg
		differentCfg.ServiceName = "toggled-again"
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: differentCfg})
		assert.NoError(t, err)
		for _, entry := range logs.All() {
			assert.NotContains(t, entry.Message, "telemetry configuration changes require a restart",
				"disabled provider must not warn on config change")
		}
	})

	t.Run("changed config on enabled provider warns but succeeds", func(t *testing.T) {
		logs := observeGlobalLogger(t, zapcore.WarnLevel)
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

		// A live provider must warn when its config changes, because the change
		// only takes effect after a restart.
		assertLogContains(t, logs, "telemetry configuration changes require a restart",
			"expected restart warning when config changes on a live provider")
	})
}

// TestHistogramBucketBoundaries verifies that custom DurationBoundaries and
// BytesBoundaries are applied to their respective histograms via the Prometheus exporter.
func TestHistogramBucketBoundaries(t *testing.T) {
	t.Run("DurationBoundaries", func(t *testing.T) {
		t.Parallel()
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
	})

	t.Run("BytesBoundaries", func(t *testing.T) {
		t.Parallel()
		port := findFreePort(t)
		customBounds := []float64{1024, 65536, 1048576}
		p, err := telemetry.New(telemetry.Config{
			Enabled:     true,
			ServiceName: "histogram-bytes-test",
			Prometheus: telemetry.PrometheusConfig{
				Enabled: true,
				Port:    port,
				Path:    "/metrics",
			},
			Histograms: telemetry.HistogramConfig{
				BytesBoundaries: customBounds,
			},
		})
		require.NoError(t, err)
		defer p.Shutdown(context.Background())

		hist, err := p.Meter("test").Float64Histogram("test.bytes.transferred")
		require.NoError(t, err)
		hist.Record(context.Background(), 2048)
		hist.Record(context.Background(), 131072)

		url := fmt.Sprintf("http://localhost:%d/metrics", port)
		resp, err := http.Get(url)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		bodyStr := string(body)

		// Verify the custom byte bucket boundaries appear in the Prometheus output.
		for _, bound := range customBounds {
			le := fmt.Sprintf("le=\"%g\"", bound)
			assert.True(t, strings.Contains(bodyStr, le),
				"expected byte bucket boundary %s in Prometheus output", le)
		}
	})
}

// TestOTLPReaderNewFields verifies that url-path, compression, and timeout fields are accepted
// by exporter construction for both protocols (connection is deferred, so no server needed).
func TestOTLPReaderNewFields(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol+"/compression-gzip", func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Enabled:     true,
				ServiceName: "otlp-fields-test",
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    protocol,
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "gzip",
					TLSDisable:  true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
		t.Run(protocol+"/timeout", func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Enabled:     true,
				ServiceName: "otlp-fields-test",
				OTLP: telemetry.OTLPConfig{
					Enabled:    true,
					Protocol:   protocol,
					Endpoint:   "localhost:4317",
					Interval:   30 * time.Second,
					Timeout:    5 * time.Second,
					TLSDisable: true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
	}
	t.Run("http/url-path", func(t *testing.T) {
		p, err := telemetry.New(telemetry.Config{
			Enabled:     true,
			ServiceName: "otlp-fields-test",
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "http",
				Endpoint:   "localhost:4318",
				Interval:   30 * time.Second,
				URLPath:    "/otlp/v1/metrics",
				TLSDisable: true,
			},
		})
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
	})
}

// TestLogsDisabledWhenMetricsEnabled verifies that when metrics are enabled but logs are not
// configured, LogProvider returns nil — metrics running does not activate logs.
func TestLogsDisabledWhenMetricsEnabled(t *testing.T) {
	port := findFreePort(t)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "logs-disabled-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	})
	require.NoError(t, err)
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Nil(t, p.LogProvider())
}

// TestLogsValidation verifies logs config validation rules.
func TestLogsValidation(t *testing.T) {
	t.Run("disabled logs skips validation even with invalid values", func(t *testing.T) {
		// When Logs.Enabled is false, bad protocol/endpoint values must be ignored.
		cfg := telemetry.Config{
			Logs: telemetry.LogsConfig{
				Enabled:  false,
				Protocol: "not-a-protocol",
			},
		}
		require.NoError(t, cfg.ValidateConfig())
	})

	errTests := []struct {
		name   string
		cfg    telemetry.Config
		errMsg string
	}{
		{
			name: "invalid protocol",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "udp",
					Endpoint: "localhost:4317",
				},
			},
			errMsg: "telemetry.logs.protocol must be 'grpc' or 'http'",
		},
		{
			name: "missing endpoint",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "grpc",
				},
			},
			errMsg: "telemetry.logs.endpoint must be set",
		},
		{
			name: "invalid compression",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Compression: "zstd",
				},
			},
			errMsg: "telemetry.logs.compression must be 'gzip' or 'none'",
		},
		{
			name: "timeout too short",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Timeout:  500 * time.Millisecond,
				},
			},
			errMsg: "telemetry.logs.timeout must be at least 1s",
		},
	}

	validTests := []struct {
		name string
		cfg  telemetry.Config
	}{
		{
			name: "compression gzip valid",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Compression: "gzip",
				},
			},
		},
		{
			name: "compression none valid",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Compression: "none",
				},
			},
		},
		{
			name: "compression empty valid",
			cfg: telemetry.Config{
				ServiceName: "test-svc",
				Logs: telemetry.LogsConfig{
					Enabled:     true,
					Protocol:    "grpc",
					Endpoint:    "localhost:4317",
					Compression: "",
				},
			},
		},
	}

	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.cfg.ValidateConfig())
		})
	}

	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateConfig()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestLogsIndependentOfMetrics verifies that logs can be enabled without enabling metrics.
// This is the key use case: logs-only mode where metrics telemetry is disabled.
func TestLogsIndependentOfMetrics(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{
		Enabled:     false,
		ServiceName: "logs-only",
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)

	assert.NotNil(t, p.LogProvider(), "log provider must be non-nil when logs are enabled")
	// Meter should still work (returns noop) even though metrics are disabled.
	meter := p.Meter("test")
	counter, err := meter.Int64Counter("test.counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1) // must not panic

	assert.NoError(t, p.Shutdown(context.Background()))
}

// TestLogsAndMetricsEnabled verifies that both log and metrics providers are initialized
// when both signals are enabled simultaneously.
func TestLogsAndMetricsEnabled(t *testing.T) {
	port := findFreePort(t)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "both-signals",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)

	assert.NotNil(t, p.LogProvider(), "log provider must be non-nil")

	counter, err := p.Meter("test").Int64Counter("both_signals_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	// Verify the metrics side is actually wired — the counter must appear in Prometheus output.
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "both_signals_counter")

	assert.NoError(t, p.Shutdown(context.Background()))
}

// TestShutdownWithLogsEnabled verifies that Shutdown flushes the log provider cleanly.
func TestShutdownWithLogsEnabled(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{
		Enabled:     false,
		ServiceName: "log-shutdown-test",
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// TestLogsExporterConstruction verifies that log exporter construction succeeds
// for both gRPC and HTTP protocols (connection is deferred, so no server needed).
func TestLogsExporterConstruction(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Enabled:     false,
				ServiceName: "log-exporter-test",
				Logs: telemetry.LogsConfig{
					Enabled:    true,
					Protocol:   protocol,
					Endpoint:   "localhost:4317",
					TLSDisable: true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
			assert.NotNil(t, p.LogProvider())
		})
	}
}

// TestTLSDisableVerification verifies that TLSDisableVerification=true (without TLSDisable)
// succeeds for construction of both OTLP metric and log exporters across protocols.
// Connection is deferred so no server is needed.
func TestTLSDisableVerification(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run("otlp/"+protocol, func(t *testing.T) {
			logs := observeGlobalLogger(t, zapcore.WarnLevel)
			p, err := telemetry.New(telemetry.Config{
				Enabled:     true,
				ServiceName: "tls-skip-verify-test",
				OTLP: telemetry.OTLPConfig{
					Enabled:                true,
					Protocol:               protocol,
					Endpoint:               "localhost:4317",
					Interval:               30 * time.Second,
					TLSDisableVerification: true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
			// A security-relevant warning must be emitted when verification is disabled.
			assertLogContains(t, logs, "TLS certificate verification is disabled",
				"expected InsecureSkipVerify warning for otlp/%s", protocol)
		})
		t.Run("logs/"+protocol, func(t *testing.T) {
			logs := observeGlobalLogger(t, zapcore.WarnLevel)
			p, err := telemetry.New(telemetry.Config{
				Enabled:     false,
				ServiceName: "tls-skip-verify-test",
				Logs: telemetry.LogsConfig{
					Enabled:                true,
					Protocol:               protocol,
					Endpoint:               "localhost:4317",
					TLSDisableVerification: true,
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
			assert.NotNil(t, p.LogProvider())
			assertLogContains(t, logs, "TLS certificate verification is disabled",
				"expected InsecureSkipVerify warning for logs/%s", protocol)
		})
	}
}

// TestTLSCertFileErrors verifies error handling in buildClientTLSConfig for bad cert file inputs.
func TestTLSCertFileErrors(t *testing.T) {
	t.Run("non-existent cert file returns error", func(t *testing.T) {
		_, err := telemetry.New(telemetry.Config{
			Enabled:     true,
			ServiceName: "cert-err-test",
			OTLP: telemetry.OTLPConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				Interval:    30 * time.Second,
				TLSCertFile: "/nonexistent/cert.pem",
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reading certificate file failed")
	})

	t.Run("invalid PEM content returns error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "bad-cert-*.pem")
		require.NoError(t, err)
		_, err = f.WriteString("-----BEGIN CERTIFICATE-----\nnot a real cert\n-----END CERTIFICATE-----\n")
		require.NoError(t, err)
		f.Close()

		_, err = telemetry.New(telemetry.Config{
			Enabled:     false,
			ServiceName: "cert-err-test",
			Logs: telemetry.LogsConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				TLSCertFile: f.Name(),
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "appending provided certificate to pool failed")
	})

	t.Run("valid self-signed cert file succeeds construction", func(t *testing.T) {
		cert := generateSelfSignedCert(t)
		f, err := os.CreateTemp(t.TempDir(), "valid-cert-*.pem")
		require.NoError(t, err)
		_, err = f.Write(cert)
		require.NoError(t, err)
		f.Close()

		p, err := telemetry.New(telemetry.Config{
			Enabled:     false,
			ServiceName: "cert-ok-test",
			Logs: telemetry.LogsConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				TLSCertFile: f.Name(),
			},
		})
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		assert.NotNil(t, p.LogProvider())
	})
}

// TestPrometheusEndpointTLS starts a provider with TLS configured, verifies that a plain
// HTTP client is rejected, then verifies an HTTPS client with the self-signed cert succeeds.
func TestPrometheusEndpointTLS(t *testing.T) {
	certPEM, keyPEM := generateServerCert(t)

	certFile, err := os.CreateTemp(t.TempDir(), "cert-*.pem")
	require.NoError(t, err)
	_, err = certFile.Write(certPEM)
	require.NoError(t, err)
	certFile.Close()

	keyFile, err := os.CreateTemp(t.TempDir(), "key-*.pem")
	require.NoError(t, err)
	_, err = keyFile.Write(keyPEM)
	require.NoError(t, err)
	keyFile.Close()

	port := findFreePort(t)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "tls-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled:     true,
			Port:        port,
			Path:        "/metrics",
			TLSCertFile: certFile.Name(),
			TLSKeyFile:  keyFile.Name(),
		},
	})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// Record a metric so we can assert body content on the TLS path.
	counter, err := p.Meter("test").Int64Counter("tls_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 7)

	// Plain HTTP must not succeed — the server speaks TLS. When a plain HTTP/1.1
	// request hits a TLS listener, Go's TLS stack fails the handshake. Depending
	// on the platform and Go version, the client sees either a connection error
	// or an HTTP error status (e.g. 400). Both outcomes prove TLS is enforced.
	plainURL := fmt.Sprintf("http://localhost:%d/metrics", port)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, plainURL, nil)
	require.NoError(t, err)
	plainResp, plainErr := http.DefaultClient.Do(req)
	if plainErr == nil {
		defer plainResp.Body.Close()
		assert.NotEqual(t, http.StatusOK, plainResp.StatusCode, "plain HTTP client should not get 200 from a TLS server")
	}

	// HTTPS client with the self-signed cert in its trust pool must succeed
	// and return the recorded metric.
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(certPEM)
	require.True(t, ok)
	tlsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: certPool},
		},
	}
	url := fmt.Sprintf("https://localhost:%d/metrics", port)
	resp, err := tlsClient.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "tls_test_counter", "recorded counter should appear in TLS-secured Prometheus response")
}

// TestPrometheusPartialTLSConfig verifies that ValidateConfig rejects configs where
// only one of tls-cert-file and tls-key-file is set.
func TestPrometheusPartialTLSConfig(t *testing.T) {
	for _, tt := range []struct {
		name    string
		certSet bool
		keySet  bool
	}{
		{"cert only", true, false},
		{"key only", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := telemetry.Config{
				Enabled:     true,
				ServiceName: "partial-tls-test",
				Prometheus: telemetry.PrometheusConfig{
					Enabled: true,
					Port:    findFreePort(t),
					Path:    "/metrics",
				},
			}
			if tt.certSet {
				cfg.Prometheus.TLSCertFile = "/some/cert.pem"
			}
			if tt.keySet {
				cfg.Prometheus.TLSKeyFile = "/some/key.pem"
			}
			err := cfg.ValidateConfig()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "tls-cert-file and tls-key-file must both be set or both be empty")
		})
	}
}

// TestOTLPDefaultTLSConstruction verifies that OTLP exporter construction succeeds when
// neither a cert file nor disable-verification is configured. In this case the OTel SDK
// uses its default TLS transport (system cert pool). Connection is deferred so no server
// is needed. Note: this is a construction smoke test; it does not verify TLS handshake
// behavior.
func TestOTLPDefaultTLSConstruction(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Enabled:     true,
				ServiceName: "tls-nil-test",
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: protocol,
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
					// No TLSDisable, no TLSCertFile, no TLSDisableVerification —
					// exercises buildClientTLSConfig("", false) -> nil, nil.
				},
			})
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
	}
}

// TestOTLPReaderGRPCURLPathWarning verifies that configuring a URL path for gRPC
// protocol emits a warning, since gRPC transport ignores url-path.
func TestOTLPReaderGRPCURLPathWarning(t *testing.T) {
	logs := observeGlobalLogger(t, zapcore.WarnLevel)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "grpc-urlpath-test",
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			URLPath:    "/custom",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	assertLogContains(t, logs, "telemetry.otlp.url-path is ignored for gRPC protocol",
		"expected url-path warning for OTLP gRPC protocol")
}

// TestLogsGRPCURLPathWarning verifies that configuring a URL path for gRPC log
// export emits a warning, since gRPC transport ignores url-path.
func TestLogsGRPCURLPathWarning(t *testing.T) {
	logs := observeGlobalLogger(t, zapcore.WarnLevel)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     false,
		ServiceName: "logs-grpc-urlpath-test",
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			URLPath:    "/custom",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	assertLogContains(t, logs, "telemetry.logs.url-path is ignored for gRPC protocol",
		"expected url-path warning for logs gRPC protocol")
}

// TestPrometheusServerTLSDisableLog verifies that starting a Prometheus server
// with TLSDisable=true logs an info-level message indicating TLS was explicitly disabled.
func TestPrometheusServerTLSDisableLog(t *testing.T) {
	logs := observeGlobalLogger(t, zapcore.InfoLevel)
	port := findFreePort(t)
	p, err := telemetry.New(telemetry.Config{
		Enabled:     true,
		ServiceName: "tls-disable-log-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled:    true,
			Port:       port,
			Path:       "/metrics",
			TLSDisable: true,
		},
	})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	assertLogContains(t, logs, "prometheus metrics server TLS explicitly disabled",
		"expected TLS-disabled info log when Prometheus TLS is explicitly disabled")
}

// generateSelfSignedCert creates a minimal self-signed CA cert in PEM format for testing.
func generateSelfSignedCert(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// generateServerCert creates a self-signed cert and private key pair in PEM format for testing.
func generateServerCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}
