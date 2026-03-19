package telemetry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// TestMetricRecording verifies that counters and histograms record correctly
// using an in-memory manual reader.
func TestMetricRecording(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	sdkProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer sdkProvider.Shutdown(context.Background()) //nolint:errcheck

	meter := sdkProvider.Meter("test")

	counter, err := meter.Int64Counter("test.requests")
	require.NoError(t, err)
	counter.Add(context.Background(), 5,
		metric.WithAttributeSet(attribute.NewSet(attribute.String("state", "new"))),
	)

	hist, err := meter.Float64Histogram("test.duration")
	require.NoError(t, err)
	hist.Record(context.Background(), 1.5)
	hist.Record(context.Background(), 2.5)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.NotEmpty(t, rm.ScopeMetrics)

	var foundCounter, foundHist bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "test.requests":
				foundCounter = true
				data, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				require.Len(t, data.DataPoints, 1)
				assert.Equal(t, int64(5), data.DataPoints[0].Value)
			case "test.duration":
				foundHist = true
				data, ok := m.Data.(metricdata.Histogram[float64])
				require.True(t, ok)
				require.Len(t, data.DataPoints, 1)
				assert.Equal(t, uint64(2), data.DataPoints[0].Count)
			}
		}
	}
	assert.True(t, foundCounter, "counter metric not found")
	assert.True(t, foundHist, "histogram metric not found")
}

// TestWithOptions verifies that WithInstanceID and WithVersion can be applied
// without error.
func TestWithOptions(t *testing.T) {
	p, err := telemetry.New(
		telemetry.Config{Enabled: false},
		telemetry.WithInstanceID("my-host:9000"),
		telemetry.WithVersion("v2.0.0"),
	)
	require.NoError(t, err)
	require.NotNil(t, p)
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
	defer p.Shutdown(context.Background()) //nolint:errcheck

	// Record a metric so the response is non-empty.
	meter := p.Meter("test")
	counter, err := meter.Int64Counter("test_prom_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 42)

	// The port is pre-bound by New(), so the server is ready immediately.
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotEmpty(t, body)
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
