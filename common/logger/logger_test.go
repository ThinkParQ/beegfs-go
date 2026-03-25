package logger

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

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

// TestNew performs some sanity checks around log initialization. For now it
// just checks if the log level was set correctly and that a valid logger was
// returned.
func TestNew(t *testing.T) {

	devLogger, err := New(Config{
		Developer: true,
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "debug", devLogger.level.String())
	assert.NotNil(t, devLogger.Logger)
	assert.NotNil(t, devLogger.Telemetry, "developer logger should have noop telemetry")

	logger1, err := New(Config{
		Type:  "stdout",
		Level: 3,
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "info", logger1.level.String())
	assert.NotNil(t, logger1.Logger)
	assert.NotNil(t, logger1.Telemetry, "production logger should have noop telemetry")

	_, err = New(Config{Type: "foo", Level: 3}, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsupported log type")
}

func TestNewWithTelemetryEnabled(t *testing.T) {
	// An enabled Prometheus config should create a real provider whose metrics
	// endpoint is reachable.
	port := findFreePort(t)
	telCfg := &telemetry.Config{
		Enabled:     true,
		ServiceName: "logger-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	}
	log, err := New(Config{Type: "stdout", Level: 3}, telCfg)
	require.NoError(t, err)
	require.NotNil(t, log.Telemetry)
	defer log.Shutdown(context.Background())

	// Create an instrument and record a value so the endpoint has data.
	meter := log.Telemetry.Meter("test")
	counter, err := meter.Int64Counter("logger_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 42)

	// Verify the Prometheus endpoint is reachable and contains the counter.
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "logger_test_counter")
}

func TestNewWithTelemetryError(t *testing.T) {
	// An invalid telemetry config (enabled but no exporters) should propagate
	// the telemetry.New error.
	_, err := New(Config{Type: "stdout", Level: 3}, &telemetry.Config{
		Enabled:     true,
		ServiceName: "bad-config",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to initialize telemetry")
}

func TestWith(t *testing.T) {
	log, err := New(Config{Type: "stdout", Level: 3}, nil)
	require.NoError(t, err)

	child := log.With(zap.String("component", "test"))

	// Child should be a different Logger instance.
	assert.NotSame(t, log, child)
	// But share the same Telemetry provider.
	assert.Same(t, log.Telemetry, child.Telemetry)
	// And share the same atomic level.
	assert.Equal(t, log.level, child.level)
}

func TestShutdownWithTelemetry(t *testing.T) {
	// Verify Shutdown tears down an active telemetry provider cleanly.
	port := findFreePort(t)
	log, err := New(Config{Type: "stdout", Level: 3}, &telemetry.Config{
		Enabled:     true,
		ServiceName: "shutdown-test",
		Prometheus: telemetry.PrometheusConfig{
			Enabled: true,
			Port:    port,
			Path:    "/metrics",
		},
	})
	require.NoError(t, err)

	// Endpoint should be reachable before shutdown.
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// After shutdown, the Prometheus server should be stopped.
	// Sync may error on stdout, so we only check for telemetry-related errors.
	_ = log.Shutdown(context.Background())

	_, err = http.Get(url)
	assert.Error(t, err, "Prometheus endpoint should be unreachable after shutdown")
}

type testConfig struct {
	logConfig Config
}

func (c *testConfig) GetLoggingConfig() Config {
	return c.logConfig
}

var _ Configurer = &testConfig{}

func TestUpdateConfiguration(t *testing.T) {

	logger1, err := New(Config{
		Type:  "stdout",
		Level: 2,
	}, nil)

	assert.NoError(t, err)
	assert.Equal(t, "warn", logger1.level.String())

	// We can update the log level and anything else is ignored.
	newConfig := &testConfig{
		logConfig: Config{Level: 5, Type: "stdout"},
	}

	err = logger1.UpdateConfiguration(newConfig)
	assert.NoError(t, err)
	assert.Equal(t, "debug", logger1.level.String())
}

func TestUpdateConfigurationWithoutConfigurer(t *testing.T) {
	// When the config doesn't implement Configurer, UpdateConfiguration should
	// return an error (this indicates a bug in the application).
	log, err := New(Config{Type: "stdout", Level: 3}, nil)
	require.NoError(t, err)

	err = log.UpdateConfiguration("not a configurer")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get log configuration")
}

// testFullConfig implements both logger.Configurer and telemetry.Configurer.
type testFullConfig struct {
	logConfig       Config
	telemetryConfig telemetry.Config
}

func (c *testFullConfig) GetLoggingConfig() Config {
	return c.logConfig
}

func (c *testFullConfig) GetTelemetryConfig() telemetry.Config {
	return c.telemetryConfig
}

func TestUpdateConfigurationDelegatesToTelemetry(t *testing.T) {
	// Use a zap observer so we can verify both the log level change AND that
	// telemetry delegation happened without error (no warning logged).
	core, logs := observer.New(zapcore.DebugLevel)
	tp, err := telemetry.New(telemetry.Config{Enabled: false})
	require.NoError(t, err)
	log := &Logger{
		Logger:    zap.New(core),
		level:     zap.NewAtomicLevelAt(zapcore.InfoLevel),
		Telemetry: tp,
	}

	cfg := &testFullConfig{
		logConfig:       Config{Level: 4, Type: "stdout"},
		telemetryConfig: telemetry.Config{Enabled: false},
	}

	err = log.UpdateConfiguration(cfg)
	assert.NoError(t, err)
	assert.Equal(t, zapcore.DebugLevel, log.level.Level())

	// Verify no "unable to update telemetry configuration" warning was logged,
	// which means the telemetry delegation succeeded.
	for _, entry := range logs.All() {
		assert.NotEqual(t, "unable to update telemetry configuration", entry.Message,
			"telemetry delegation should succeed when config implements telemetry.Configurer")
	}
}

func TestUpdateConfigurationSkipsTelemetryWithoutConfigurer(t *testing.T) {
	// When the config implements logger.Configurer but NOT telemetry.Configurer,
	// UpdateConfiguration should skip telemetry delegation silently (no warning).
	core, logs := observer.New(zapcore.DebugLevel)
	tp, err := telemetry.New(telemetry.Config{Enabled: false})
	require.NoError(t, err)
	log := &Logger{
		Logger:    zap.New(core),
		level:     zap.NewAtomicLevelAt(zapcore.InfoLevel),
		Telemetry: tp,
	}

	logOnlyConfig := &testConfig{
		logConfig: Config{Level: 3, Type: "stdout"},
	}
	err = log.UpdateConfiguration(logOnlyConfig)
	assert.NoError(t, err)

	// Verify no telemetry warning was emitted — delegation was skipped cleanly.
	for _, entry := range logs.All() {
		assert.NotEqual(t, "unable to update telemetry configuration", entry.Message,
			"should not warn about telemetry when config doesn't implement telemetry.Configurer")
	}
}

func TestGetLevel(t *testing.T) {
	tests := []struct {
		newLevel  int8
		wantLevel zapcore.Level
		wantErr   error
	}{
		{0, zapcore.FatalLevel, nil},
		{1, zapcore.ErrorLevel, nil},
		{2, zapcore.WarnLevel, nil},
		{3, zapcore.InfoLevel, nil},
		{4, zapcore.DebugLevel, nil},
		{5, zapcore.DebugLevel, nil},
		{6, zapcore.InfoLevel, fmt.Errorf("the provided log.level (6) is invalid (must be 0-5)")},
	}

	for _, tt := range tests {
		gotLevel, gotErr := getLevel(tt.newLevel)
		assert.Equal(t, tt.wantLevel, gotLevel, fmt.Sprintf("getLevel(%d) level mismatch", tt.newLevel))
		assert.Equal(t, tt.wantErr, gotErr, fmt.Sprintf("getLevel(%d) error mismatch", tt.newLevel))
	}
}
