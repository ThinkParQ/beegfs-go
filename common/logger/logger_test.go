package logger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

// TestNew() performs some sanity checks around log initialization. For now it
// just checks if the log level was set correctly and that a valid logger was
// returned.
func TestNew(t *testing.T) {

	devLogger, err := New(Config{
		Developer: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, "debug", devLogger.level.String())
	assert.NotNil(t, devLogger.Logger)

	logger1, err := New(Config{
		Type:  "stdout",
		Level: 3,
	})
	assert.NoError(t, err)
	assert.Equal(t, "info", logger1.level.String())
	assert.NotNil(t, logger1.Logger)

	_, err = New(Config{Type: "foo", Level: 3})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsupported log type")

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
		Level: 1,
	})

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

func TestGetLevel(t *testing.T) {
	tests := []struct {
		newLevel  int8
		wantLevel zapcore.Level
		wantErr   error
	}{
		{1, zapcore.WarnLevel, nil},
		{3, zapcore.InfoLevel, nil},
		{5, zapcore.DebugLevel, nil},
		{2, zapcore.InfoLevel, fmt.Errorf("the provided log.level (2) is invalid (must be 1, 3, or 5)")},
	}

	for _, tt := range tests {
		gotLevel, gotErr := getLevel(tt.newLevel)
		assert.Equal(t, tt.wantLevel, gotLevel, fmt.Sprintf("getLevel(%d) level mismatch", tt.newLevel))
		assert.Equal(t, tt.wantErr, gotErr, fmt.Sprintf("getLevel(%d) error mismatch", tt.newLevel))
	}
}
