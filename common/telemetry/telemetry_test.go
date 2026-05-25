package telemetry_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
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

// TestNewNoopWhenNoExporters verifies that a provider with no exporters configured
// returns a no-op meter and shuts down cleanly.
func TestNewNoopWhenNoExporters(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{})
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

// TestValidateConfig tests all validation rules.
func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     telemetry.Config
		wantErr bool
		errMsg  string
	}{
		{
			name:    "no exporters configured is valid",
			cfg:     telemetry.Config{},
			wantErr: false,
		},
		{
			name: "invalid OTLP protocol",
			cfg: telemetry.Config{
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
			name: "valid OTLP-only config",
			cfg: telemetry.Config{
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
			name: "logs only (metrics disabled)",
			cfg: telemetry.Config{
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
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
				},
				Logs: telemetry.LogsConfig{
					Enabled:  true,
					Protocol: "grpc",
					Endpoint: "localhost:4317",
				},
			},
			wantErr: false,
		},
		{
			name: "OTLP TLS disable and disable-verification are mutually exclusive",
			cfg: telemetry.Config{
				OTLP: telemetry.OTLPConfig{
					Enabled:                true,
					Protocol:               "grpc",
					Endpoint:               "localhost:4317",
					Interval:               30 * time.Second,
					TLSDisable:             true,
					TLSDisableVerification: true,
				},
			},
			wantErr: true,
			errMsg:  "telemetry.otlp: tls-disable and tls-disable-verification are mutually exclusive",
		},
		{
			name: "logs TLS disable and disable-verification are mutually exclusive",
			cfg: telemetry.Config{
				Logs: telemetry.LogsConfig{
					Enabled:                true,
					Protocol:               "grpc",
					Endpoint:               "localhost:4317",
					TLSDisable:             true,
					TLSDisableVerification: true,
				},
			},
			wantErr: true,
			errMsg:  "telemetry.logs: tls-disable and tls-disable-verification are mutually exclusive",
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

// TestWithOptions verifies that WithInstanceID, WithVersion, and WithServiceName options
// are accepted without error when constructing a provider.
func TestWithOptions(t *testing.T) {
	p, err := telemetry.New(
		telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "grpc",
				Endpoint:   "localhost:4317",
				Interval:   30 * time.Second,
				TLSDisable: true,
			},
		},
		telemetry.WithServiceName("options-test"),
		telemetry.WithInstanceID("my-host:9000"),
		telemetry.WithVersion("v2.0.0"),
	)
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	counter, err := p.Meter("test").Int64Counter("options_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)
}

// TestWithServiceName verifies that WithServiceName is accepted without error when
// constructing a provider.
func TestWithServiceName(t *testing.T) {
	p, err := telemetry.New(
		telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "grpc",
				Endpoint:   "localhost:4317",
				Interval:   30 * time.Second,
				TLSDisable: true,
			},
		},
		telemetry.WithServiceName("my-service"),
	)
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	counter, err := p.Meter("test").Int64Counter("service_name_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)
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
	baseCfg := telemetry.Config{}

	t.Run("non-Configurer returns error", func(t *testing.T) {
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		err = p.UpdateConfiguration("not a configurer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to get telemetry configuration")
	})

	t.Run("unchanged config on disabled provider is silent", func(t *testing.T) {
		logs := observeGlobalLogger(t, zapcore.WarnLevel)
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: baseCfg})
		assert.NoError(t, err)
		assert.Empty(t, logs.All(), "noop provider must emit no warnings on UpdateConfiguration")
	})

	t.Run("disabled provider suppresses restart warning on config change", func(t *testing.T) {
		logs := observeGlobalLogger(t, zapcore.WarnLevel)
		p, err := telemetry.New(baseCfg)
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		// UpdateConfiguration stores the new config but does NOT restart the
		// provider, so no exporter is actually started here.
		newCfg := telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "grpc",
				Endpoint:   "localhost:4317",
				Interval:   30 * time.Second,
				TLSDisable: true,
			},
		}
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: newCfg})
		assert.NoError(t, err)

		// A second UpdateConfiguration on a still-disabled provider (sdkProvider
		// and logProvider remain nil) must also succeed without a restart warning,
		// regardless of whether the config changed.
		differentCfg := newCfg
		differentCfg.OTLP.Endpoint = "localhost:4318"
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: differentCfg})
		assert.NoError(t, err)
		for _, entry := range logs.All() {
			assert.NotContains(t, entry.Message, "telemetry configuration changes require a restart",
				"disabled provider must not warn on config change")
		}
	})

	t.Run("changed config on enabled provider warns but succeeds", func(t *testing.T) {
		logs := observeGlobalLogger(t, zapcore.WarnLevel)
		cfg := telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "grpc",
				Endpoint:   "localhost:4317",
				Interval:   30 * time.Second,
				TLSDisable: true,
			},
		}
		p, err := telemetry.New(cfg, telemetry.WithServiceName("update-test"))
		require.NoError(t, err)
		defer p.Shutdown(context.Background())

		changedCfg := cfg
		changedCfg.OTLP.Endpoint = "localhost:4318"
		err = p.UpdateConfiguration(&testTelemetryConfig{cfg: changedCfg})
		assert.NoError(t, err)

		// A live provider must warn when its config changes, because the change
		// only takes effect after a restart.
		assertLogContains(t, logs, "telemetry configuration changes require a restart",
			"expected restart warning when config changes on a live provider")
	})
}

// TestHistogramBucketBoundaries verifies that custom DurationBoundaries and
// BytesBoundaries configuration is accepted without error.
func TestHistogramBucketBoundaries(t *testing.T) {
	otlpCfg := telemetry.OTLPConfig{
		Enabled:    true,
		Protocol:   "grpc",
		Endpoint:   "localhost:4317",
		Interval:   30 * time.Second,
		TLSDisable: true,
	}

	t.Run("DurationBoundaries", func(t *testing.T) {
		t.Parallel()
		p, err := telemetry.New(telemetry.Config{
			OTLP: otlpCfg,
			Histograms: telemetry.HistogramConfig{
				DurationBoundaries: []float64{0.01, 0.05, 0.1, 0.5, 1.0},
			},
		}, telemetry.WithServiceName("histogram-test"))
		require.NoError(t, err)
		defer p.Shutdown(context.Background())

		hist, err := p.Meter("test").Float64Histogram("test.duration")
		require.NoError(t, err)
		hist.Record(context.Background(), 0.03)
		hist.Record(context.Background(), 0.75)
	})

	t.Run("BytesBoundaries", func(t *testing.T) {
		t.Parallel()
		p, err := telemetry.New(telemetry.Config{
			OTLP: otlpCfg,
			Histograms: telemetry.HistogramConfig{
				BytesBoundaries: []float64{1024, 65536, 1048576},
			},
		}, telemetry.WithServiceName("histogram-bytes-test"))
		require.NoError(t, err)
		defer p.Shutdown(context.Background())

		hist, err := p.Meter("test").Float64Histogram("test.bytes.transferred")
		require.NoError(t, err)
		hist.Record(context.Background(), 2048)
		hist.Record(context.Background(), 131072)
	})
}

// TestOTLPReaderNewFields verifies that url-path, compression, and timeout fields are accepted
// by exporter construction for both protocols (connection is deferred, so no server needed).
func TestOTLPReaderNewFields(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol+"/compression-gzip", func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				OTLP: telemetry.OTLPConfig{
					Enabled:     true,
					Protocol:    protocol,
					Endpoint:    "localhost:4317",
					Interval:    30 * time.Second,
					Compression: "gzip",
					TLSDisable:  true,
				},
			}, telemetry.WithServiceName("otlp-fields-test"))
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
		t.Run(protocol+"/timeout", func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				OTLP: telemetry.OTLPConfig{
					Enabled:    true,
					Protocol:   protocol,
					Endpoint:   "localhost:4317",
					Interval:   30 * time.Second,
					Timeout:    5 * time.Second,
					TLSDisable: true,
				},
			}, telemetry.WithServiceName("otlp-fields-test"))
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
		})
	}
	t.Run("http/url-path", func(t *testing.T) {
		p, err := telemetry.New(telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:    true,
				Protocol:   "http",
				Endpoint:   "localhost:4318",
				Interval:   30 * time.Second,
				URLPath:    "/otlp/v1/metrics",
				TLSDisable: true,
			},
		}, telemetry.WithServiceName("otlp-fields-test"))
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
	})
}

// TestLogsDisabledWhenMetricsEnabled verifies that when metrics are enabled but logs are not
// configured, LogProvider returns nil — metrics running does not activate logs.
func TestLogsDisabledWhenMetricsEnabled(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("logs-disabled-test"))
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
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
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("logs-only"))
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
	p, err := telemetry.New(telemetry.Config{
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			TLSDisable: true,
		},
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("both-signals"))
	require.NoError(t, err)

	defer p.Shutdown(context.Background())

	assert.NotNil(t, p.LogProvider(), "log provider must be non-nil")

	counter, err := p.Meter("test").Int64Counter("both_signals_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)
}

// TestShutdownWithLogsEnabled verifies that Shutdown flushes the log provider cleanly.
func TestShutdownWithLogsEnabled(t *testing.T) {
	p, err := telemetry.New(telemetry.Config{
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("log-shutdown-test"))
	require.NoError(t, err)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// TestLogsExporterConstruction verifies that log exporter construction succeeds
// for both gRPC and HTTP protocols (connection is deferred, so no server needed).
func TestLogsExporterConstruction(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			p, err := telemetry.New(telemetry.Config{
				Logs: telemetry.LogsConfig{
					Enabled:    true,
					Protocol:   protocol,
					Endpoint:   "localhost:4317",
					TLSDisable: true,
				},
			}, telemetry.WithServiceName("log-exporter-test"))
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
				OTLP: telemetry.OTLPConfig{
					Enabled:                true,
					Protocol:               protocol,
					Endpoint:               "localhost:4317",
					Interval:               30 * time.Second,
					TLSDisableVerification: true,
				},
			}, telemetry.WithServiceName("tls-skip-verify-test"))
			require.NoError(t, err)
			defer p.Shutdown(context.Background())
			// A security-relevant warning must be emitted when verification is disabled.
			assertLogContains(t, logs, "TLS certificate verification is disabled",
				"expected InsecureSkipVerify warning for otlp/%s", protocol)
		})
		t.Run("logs/"+protocol, func(t *testing.T) {
			logs := observeGlobalLogger(t, zapcore.WarnLevel)
			p, err := telemetry.New(telemetry.Config{
				Logs: telemetry.LogsConfig{
					Enabled:                true,
					Protocol:               protocol,
					Endpoint:               "localhost:4317",
					TLSDisableVerification: true,
				},
			}, telemetry.WithServiceName("tls-skip-verify-test"))
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
			OTLP: telemetry.OTLPConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				Interval:    30 * time.Second,
				TLSCertFile: "/nonexistent/cert.pem",
			},
		}, telemetry.WithServiceName("cert-err-test"))
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
			Logs: telemetry.LogsConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				TLSCertFile: f.Name(),
			},
		}, telemetry.WithServiceName("cert-err-test"))
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
			Logs: telemetry.LogsConfig{
				Enabled:     true,
				Protocol:    "grpc",
				Endpoint:    "localhost:4317",
				TLSCertFile: f.Name(),
			},
		}, telemetry.WithServiceName("cert-ok-test"))
		require.NoError(t, err)
		defer p.Shutdown(context.Background())
		assert.NotNil(t, p.LogProvider())
	})
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
				OTLP: telemetry.OTLPConfig{
					Enabled:  true,
					Protocol: protocol,
					Endpoint: "localhost:4317",
					Interval: 30 * time.Second,
					// No TLSDisable, no TLSCertFile, no TLSDisableVerification —
					// exercises buildClientTLSConfig("", false) -> nil, nil.
				},
			}, telemetry.WithServiceName("tls-nil-test"))
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
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			URLPath:    "/custom",
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("grpc-urlpath-test"))
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
		Logs: telemetry.LogsConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			URLPath:    "/custom",
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("logs-grpc-urlpath-test"))
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	assertLogContains(t, logs, "telemetry.logs.url-path is ignored for gRPC protocol",
		"expected url-path warning for logs gRPC protocol")
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

