package logger

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

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
	assert.NotNil(t, devLogger.telemetry, "developer logger should have noop telemetry")

	logger1, err := New(Config{
		Type:  "stdout",
		Level: 3,
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "info", logger1.level.String())
	assert.NotNil(t, logger1.Logger)
	assert.NotNil(t, logger1.telemetry, "production logger should have noop telemetry")

	_, err = New(Config{Type: "foo", Level: 3}, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsupported log type")
}

func TestNewWithTelemetryEnabled(t *testing.T) {
	telCfg := &telemetry.Config{
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			TLSDisable: true,
		},
	}
	log, err := New(Config{Type: "stdout", Level: 3}, telCfg, telemetry.WithServiceName("logger-test"))
	require.NoError(t, err)
	defer log.Shutdown(context.Background())

	meter := log.Meter("test")
	counter, err := meter.Int64Counter("logger_test_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 42)
}

func TestWith(t *testing.T) {
	log, err := New(Config{Type: "stdout", Level: 3}, nil)
	require.NoError(t, err)

	child := log.With(zap.String("component", "test"))

	// Child should be a different Logger instance.
	assert.NotSame(t, log, child)
	// Both must share the same telemetry provider pointer so that instruments
	// created through either logger contribute to the same metric pipeline.
	// This assertion is structural (pointer equality) by design: the sharing
	// contract is enforced by With() copying the pointer, not by cloning.
	assert.Same(t, log.telemetry, child.telemetry)
	// And share the same atomic level.
	assert.Equal(t, log.level, child.level)
}

func TestShutdownWithTelemetry(t *testing.T) {
	// Verify Shutdown tears down an active telemetry provider cleanly.
	log, err := New(Config{Type: "stdout", Level: 3}, &telemetry.Config{
		OTLP: telemetry.OTLPConfig{
			Enabled:    true,
			Protocol:   "grpc",
			Endpoint:   "localhost:4317",
			Interval:   30 * time.Second,
			TLSDisable: true,
		},
	}, telemetry.WithServiceName("shutdown-test"))
	require.NoError(t, err)

	// zap.Logger.Sync() returns EINVAL on stdout sinks; allow that specific error.
	err = log.Shutdown(context.Background())
	if err != nil && !errors.Is(err, syscall.EINVAL) {
		t.Errorf("unexpected shutdown error: %v", err)
	}
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
	tp, err := telemetry.New(telemetry.Config{})
	require.NoError(t, err)
	log := &Logger{
		Logger:    zap.New(core),
		level:     zap.NewAtomicLevelAt(zapcore.InfoLevel),
		telemetry: tp,
	}

	cfg := &testFullConfig{
		logConfig:       Config{Level: 4, Type: "stdout"},
		telemetryConfig: telemetry.Config{},
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
	// UpdateConfiguration should skip telemetry delegation and still apply the
	// log-level change from the config.
	core, logs := observer.New(zapcore.DebugLevel)
	tp, err := telemetry.New(telemetry.Config{})
	require.NoError(t, err)
	log := &Logger{
		Logger:    zap.New(core),
		level:     zap.NewAtomicLevelAt(zapcore.InfoLevel),
		telemetry: tp,
	}

	logOnlyConfig := &testConfig{
		logConfig: Config{Level: 4, Type: "stdout"}, // level 4 = debug
	}
	err = log.UpdateConfiguration(logOnlyConfig)
	assert.NoError(t, err)
	// The log level must be updated even though telemetry delegation was skipped.
	assert.Equal(t, zapcore.DebugLevel, log.level.Level())
	// No telemetry-related error should be logged when delegation is skipped.
	for _, entry := range logs.All() {
		assert.NotContains(t, entry.Message, "unable to update telemetry configuration",
			"telemetry skip must not log an error")
	}
}

func TestUpdateConfigurationTelemetryDelegationError(t *testing.T) {
	// When telemetry config delegation fails (invalid config), UpdateConfiguration
	// must return an error rather than swallowing it.
	log, err := New(Config{Type: "stdout", Level: 3}, nil)
	require.NoError(t, err)

	// An OTLP config with an invalid protocol fails ValidateConfig inside
	// telemetry.UpdateConfiguration, which should propagate back to the caller.
	badTelemetryCfg := &testFullConfig{
		logConfig: Config{Level: 3, Type: "stdout"},
		telemetryConfig: telemetry.Config{
			OTLP: telemetry.OTLPConfig{
				Enabled:  true,
				Protocol: "invalid",
				Endpoint: "localhost:4317",
			},
		},
	}

	err = log.UpdateConfiguration(badTelemetryCfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to update telemetry configuration")
	assert.Contains(t, err.Error(), "protocol")
}

func TestNewWithInvalidTelemetryConfigReturnsError(t *testing.T) {
	// Verify that logger.New propagates a telemetry construction error to the caller.
	// OTLP with an invalid protocol fails ValidateConfig inside telemetry.New.
	_, err := New(Config{Type: "stdout", Level: 3}, &telemetry.Config{
		OTLP: telemetry.OTLPConfig{
			Enabled:  true,
			Protocol: "invalid",
			Endpoint: "localhost:4317",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to initialize telemetry")
	assert.Contains(t, err.Error(), "protocol")
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
