// Logger implements provides a common logging implementation for Go
// projects at ThinkParQ. Its goal is to allowing logging capabilities be easily
// extended in the future while requiring minimal changes inside individual
// applications.
package logger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/telemetry"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger is a wrapper around zap.Logger. It allows different aspects of the
// Logger to be reconfigured after the application has started. Notably the log
// level. It optionally embeds a telemetry Provider so components can obtain
// meters via log.Meter("name").
type Logger struct {
	*zap.Logger
	level     zap.AtomicLevel
	telemetry *telemetry.Provider
}

// Verify all interfaces that depend on Logger are satisfied:
var _ configmgr.Listener = &Logger{}

// Config represents the configuration for a Logger.
type Config struct {
	Type              supportedLogTypes `mapstructure:"type"`
	File              string            `mapstructure:"file"`
	Level             int8              `mapstructure:"level"`
	MaxSize           int               `mapstructure:"max-size"`
	NumRotatedFiles   int               `mapstructure:"num-rotated-files"`
	IncomingEventRate bool              `mapstructure:"incoming-event-rate"`
	Developer         bool              `mapstructure:"developer"`
}

type supportedLogTypes string

const (
	StdOut  supportedLogTypes = "stdout"
	StdErr  supportedLogTypes = "stderr"
	LogFile supportedLogTypes = "logfile"
	// The syslog type is the slowest logging option due to how zap log messages
	// need to be translated to syslog messages and severity levels.
	Syslog supportedLogTypes = "syslog"
)

// SupportedLogTypes is a slice of supported log types. Any log types added in
// the future must be added to this slice. It is used for printing help text,
// for example if an invalid type is specified.
var SupportedLogTypes = []supportedLogTypes{
	StdOut,
	StdErr,
	LogFile,
	Syslog,
}

// New returns new logger based on the provided configuration. When
// telemetryCfg is non-nil, the Logger initializes and owns a telemetry
// Provider; otherwise it uses a noop Provider (zero overhead).
func New(newConfig Config, telemetryCfg *telemetry.Config, telemetryOpts ...telemetry.Option) (*Logger, error) {

	logMgr := Logger{}

	effectiveCfg := telemetry.Config{Enabled: false}
	if telemetryCfg != nil {
		effectiveCfg = *telemetryCfg
	}
	tp, err := telemetry.New(effectiveCfg, telemetryOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize telemetry: %w", err)
	}
	logMgr.telemetry = tp

	// Use the opinionated Zap development configuration.
	// This notably gives us stack traces at warn and error levels.
	if newConfig.Developer {
		// Ignore the configuration setting and set the log level to five for
		// developer configurations.
		zapLevel, err := getLevel(5)
		if err != nil {
			return nil, err
		}
		logMgr.level = zap.NewAtomicLevelAt(zapLevel)

		cfg := zap.NewDevelopmentConfig()
		cfg.Level = logMgr.level
		l, err := cfg.Build()
		if err != nil {
			return nil, err
		}
		logMgr.Logger = l
		// Developer mode uses zap's built-in development config which does not
		// support adding a tee core. OTLP log export is intentionally skipped in
		// this mode even when telemetry.logs is enabled.
		if logMgr.telemetry.LogProvider() != nil {
			logMgr.Logger.Warn("developer mode is active: OTLP log export is disabled (logs will not be sent to the configured endpoint)")
		}
		return &logMgr, nil
	}

	// Otherwise build a production config based on the user settings:
	zapConfig := zap.NewProductionEncoderConfig()
	zapConfig.TimeKey = "timestamp"
	zapConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	// Then setup the encoder that will turn our log entries into byte slices.
	// For now just log in plaintext and don't expose an option to log using
	// JSON. We can always add an option later with zapcore.NewJSONEncoder() if
	// needed. IMPORTANT: If the encoding type ever changes then the way we
	// handle writing to syslog in SyslogWriteSyncer.Write() MUST be updated
	// accordingly.
	zapEncoder := zapcore.NewConsoleEncoder(zapConfig)

	// The use of an atomic level means we can update the log level later on.
	// However we have to keep a reference to the atomic level if we want to
	// adjust it later (which is why we add it to the logger struct).
	zapLevel, err := getLevel(newConfig.Level)
	if err != nil {
		return nil, err
	}
	logMgr.level = zap.NewAtomicLevelAt(zapLevel)

	// zapcore.WriteSyncers are what handle writing the byte slices from the
	// encoder somewhere. This means we can easily add support for new types of
	// logging (i.e., log destinations) by simply swapping out the WriteSyncer.
	var logDestination zapcore.WriteSyncer
	switch newConfig.Type {
	case StdOut:
		logDestination = zapcore.AddSync(os.Stdout)
	case StdErr:
		logDestination = zapcore.AddSync(os.Stderr)
	case LogFile:
		// Just being able to write to the provided log file is not sufficient
		// if we want to rotate log files. Make sure the directory selected for
		// logging exists and we can write to it.
		if err := ensureLogsAreWritable(newConfig.File); err != nil {
			return nil, err
		}

		logDestination = zapcore.AddSync(&lumberjack.Logger{
			Filename:   newConfig.File,
			MaxSize:    newConfig.MaxSize,
			MaxBackups: newConfig.NumRotatedFiles,
		})
	case Syslog:
		// By default we'll log at severity level info. Typically we'll be able
		// to parse out the log level and log at the appropriate severity level.
		// We'll use the process name as the prefix tag in case there are multiple
		// instances of BeeWatch running on the same server.
		l, err := NewSyslogWriteSyncer(syslog.LOG_INFO|syslog.LOG_LOCAL0, os.Args[0])
		if err != nil {
			return nil, fmt.Errorf("unable to initialize syslog destination: %w", err)
		}
		logDestination = l
	default:
		return nil, fmt.Errorf("unsupported log type: %s", newConfig.Type)
	}

	baseCore := zapcore.NewCore(zapEncoder, logDestination, logMgr.level)

	if lp := logMgr.telemetry.LogProvider(); lp != nil {
		otelCore := otelzap.NewCore("github.com/thinkparq/beegfs-go/common/logger", otelzap.WithLoggerProvider(lp))
		// Gate the otelzap core on the same atomic level as the console
		// sink so OTLP does not receive records below the configured threshold.
		leveledOtelCore, err := zapcore.NewIncreaseLevelCore(otelCore, logMgr.level)
		if err != nil {
			return nil, fmt.Errorf("failed to apply log level to otelzap core: %w", err)
		}
		baseCore = zapcore.NewTee(baseCore, leveledOtelCore)
	}

	logMgr.Logger = zap.New(baseCore)
	return &logMgr, nil
}

// With returns a child logger with the given fields permanently attached to
// every log entry it produces. The child shares the same telemetry provider
// and log level as the parent.
func (lm *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{
		Logger:    lm.Logger.With(fields...),
		level:     lm.level,
		telemetry: lm.telemetry,
	}
}

// Meter returns a named OTel Meter for creating instruments.
// When telemetry is disabled, returns a no-op Meter (zero overhead).
func (lm *Logger) Meter(name string) metric.Meter {
	return lm.telemetry.Meter(name)
}

// LogProvider returns the OTel LoggerProvider, or nil when log export is disabled.
// The concrete *sdklog.LoggerProvider type is returned (rather than an interface)
// because otelzap.NewCore requires it directly.
func (lm *Logger) LogProvider() *sdklog.LoggerProvider {
	return lm.telemetry.LogProvider()
}

// Shutdown flushes pending telemetry metrics and syncs the logger. It should be
// called during graceful service shutdown (typically via defer).
func (lm *Logger) Shutdown(ctx context.Context) error {
	var errs []error
	if lm.telemetry != nil {
		if err := lm.telemetry.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := lm.Sync(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// DeferredShutdown returns a function that shuts down the logger within the
// given timeout. Intended for use with defer:
//
//	defer logger.DeferredShutdown(10 * time.Second)()
//
// Falls back to stdlib log on error because the zap logger may already be shut
// down by the time the deferred function runs.
func (lm *Logger) DeferredShutdown(timeout time.Duration) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := lm.Shutdown(ctx); err != nil {
			log.Printf("error during logger shutdown: %v", err)
		}
	}
}

// Configurer interface is used to get the logging configuration. It allows the
// UpdateConfiguration() method to be provided any application configuration and
// the logger to extract its configuration without requiring knowledge of a
// particular application. Typically this works by embedding the logger's Config
// struct as part of the overall "AppConfig" struct that must implement the
// Configurer interface.
type Configurer interface {
	GetLoggingConfig() Config
}

// UpdateConfiguration satisfies the [configmgr.Listener] interface and is used
// to dynamically update supported aspects of the logger and its embedded
// telemetry Provider. For logging it currently only supports dynamically
// updating the log level. Telemetry configuration changes are delegated to the
// Provider when the config implements telemetry.Configurer.
func (lm *Logger) UpdateConfiguration(newConfig any) error {

	// Use type assertion to verify the newConfig interface variable is of the
	// correct type so we can use it to get the new configuration.
	configurer, ok := newConfig.(Configurer)
	if !ok {
		return fmt.Errorf("unable to get log configuration from the application configuration (most likely this indicates a bug and a report should be filed)")
	}

	newLogConfig := configurer.GetLoggingConfig()

	// We don't set the component on the logging struct because then it would be
	// included in every log message. So instead set it up whenever we need to
	// log from the logging package.
	log := lm.Logger.With(zap.String("component", path.Base(reflect.TypeFor[Logger]().PkgPath())))

	newLevel, err := getLevel(newLogConfig.Level)
	if err != nil {
		return err
	}

	// If developer logging is enabled ignore the provided log level and set it to debug.
	if newLogConfig.Developer {
		newLevel = zapcore.DebugLevel
	}

	if lm.level.Level() != newLevel {
		lm.level.SetLevel(newLevel)
		log.Log(lm.level.Level(), "set log level", zap.Any("logLevel", lm.level.Level()))
	} else {
		log.Debug("no change to log level")
	}

	// Forward the config reload to the telemetry Provider if the application
	// has telemetry configuration (i.e. its AppConfig implements telemetry.Configurer).
	if _, ok := newConfig.(telemetry.Configurer); ok {
		if err := lm.telemetry.UpdateConfiguration(newConfig); err != nil {
			log.Warn("unable to update telemetry configuration", zap.Error(err))
		}
	}

	return nil
}

// getLevel maps Zap log levels to BeeGFS log levels.
func getLevel(newLevel int8) (zapcore.Level, error) {

	// We'll map Zap levels to standard BeeGFS log levels. The use of an atomic level means we can
	// change this after the application has started.
	switch newLevel {
	case 0:
		return zapcore.FatalLevel, nil
	case 1:
		return zapcore.ErrorLevel, nil
	case 2:
		return zapcore.WarnLevel, nil
	case 3:
		return zapcore.InfoLevel, nil
	case 4, 5:
		return zapcore.DebugLevel, nil
	default:
		// If we used zapcore.InvalidLevel we could cause a panic. So instead return a sane level
		// just in case something decides to ignore the error and use the level we return anyway.
		return zapcore.InfoLevel, fmt.Errorf("the provided log.level (%d) is invalid (must be 0-5)", newLevel)
	}
}
