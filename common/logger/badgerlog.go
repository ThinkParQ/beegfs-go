package logger

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// BadgerDB doesn't directly work with Zap. This bridge type wraps zap.Logger
// and implements the badger.Logger interface so we can log from Badger using
// Zap: https://pkg.go.dev/github.com/dgraph-io/badger#Logger This should not be
// used anywhere high performance logging is required, because it uses
// fmt.Sprintf to transform output but should be sufficient since Badger doesn't
// need to log very often (unless debug logging is enabled).
type BadgerLoggerBridge struct {
	logger *zap.Logger
}

// NewBadgerLoggerBridge takes a subComponent name and an existing zap.Logger.
// It returns a Logger compatible with the badger.Logger interface and sets up
// Zap so each message logged will include the provided sub component name.
func NewBadgerLoggerBridge(subComponent string, logger *zap.Logger) *BadgerLoggerBridge {
	logger = logger.With(zap.String("database", subComponent))
	return &BadgerLoggerBridge{
		logger: logger,
	}
}

func (z *BadgerLoggerBridge) Errorf(format string, args ...interface{}) {
	z.logger.Error(fmt.Sprintf(strings.TrimSuffix(format, "\n"), args...))
}

func (z *BadgerLoggerBridge) Warningf(format string, args ...interface{}) {
	z.logger.Warn(fmt.Sprintf(strings.TrimSuffix(format, "\n"), args...))
}

func (z *BadgerLoggerBridge) Infof(format string, args ...interface{}) {
	z.logger.Info(fmt.Sprintf(strings.TrimSuffix(format, "\n"), args...))
}

func (z *BadgerLoggerBridge) Debugf(format string, args ...interface{}) {
	z.logger.Debug(fmt.Sprintf(strings.TrimSuffix(format, "\n"), args...))
}
