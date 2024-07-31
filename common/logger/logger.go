// Logger implements provides a common logging implementation for Go
// projects at ThinkParQ. Its goal is to allowing logging capabilities be easily
// extended in the future while requiring minimal changes inside individual
// applications.
package logger

import (
	"fmt"
	"log/syslog"
	"os"
	"path"
	"reflect"

	"github.com/thinkparq/gobee/configmgr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger is a wrapper around zap.Logger. It allows different aspects of the
// Logger to be reconfigured after the application has started. Notably the log
// level.
type Logger struct {
	*zap.Logger
	level zap.AtomicLevel
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
	LogFile,
	Syslog,
}

// New returns new logger based on the provided configuration.
func New(newConfig Config) (*Logger, error) {

	logMgr := Logger{}

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

	logMgr.Logger = zap.New(zapcore.NewCore(zapEncoder, logDestination, logMgr.level))
	return &logMgr, nil

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

// UpdateConfiguration satisfies the [gobee.configmgr.ConfigListener] interface
// and is used to dynamically update supported aspects of the logger. Currently
// it only supports dynamically updating the log level. So it can be used with
// any application configuration it accepts an interface{} but it is expected
// that interface satisfy the [Configurer] interface.
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
	log := lm.Logger.With(zap.String("component", path.Base(reflect.TypeOf(Logger{}).PkgPath())))

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

	return nil
}

// getLevel maps Zap log levels to BeeGFS log levels.
func getLevel(newLevel int8) (zapcore.Level, error) {

	// We'll map Zap levels to standard BeeGFS log levels. The use of an atomic
	// level means we can change this after the application has started.
	//var zapLevel zap.AtomicLevel
	switch newLevel {
	case 1:
		return zapcore.WarnLevel, nil
	case 3:
		return zapcore.InfoLevel, nil
	case 5:
		return zapcore.DebugLevel, nil
	default:
		// If we used zapcore.InvalidLevel we could cause a panic.
		// So instead return a sane level just in case something decides to
		// ignore the error and use the level we return anyway.
		return zapcore.InfoLevel, fmt.Errorf("the provided log.level (%d) is invalid (must be 1, 3, or 5)", newLevel)
	}
}
