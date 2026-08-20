package iotest

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newIOTraceLogger builds a zap logger for IO-level tracing. level 0 returns a no-op logger.
// Levels 1–5 follow the BeeGFS log scale: 1=Error, 2=Warn, 3=Info, 4/5=Debug.
// logFile is the output path; empty means stderr.
func newIOTraceLogger(level int, logFile string) (*zap.Logger, func(), error) {
	if level <= 0 {
		return zap.NewNop(), func() {}, nil
	}

	encCfg := zapcore.EncoderConfig{
		TimeKey:     "T",
		LevelKey:    "L",
		MessageKey:  "M",
		EncodeTime:  zapcore.ISO8601TimeEncoder,
		EncodeLevel: zapcore.CapitalLevelEncoder,
	}
	enc := zapcore.NewConsoleEncoder(encCfg)

	var sink zapcore.WriteSyncer
	cleanup := func() {}
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, nil, fmt.Errorf("trace: open %s: %w", logFile, err)
		}
		sink = zapcore.AddSync(f)
		cleanup = func() { _ = f.Close() }
	} else {
		sink = zapcore.AddSync(os.Stderr)
	}

	zapLevel := traceToZapLevel(level)
	core := zapcore.NewCore(enc, sink, zap.NewAtomicLevelAt(zapLevel))
	log := zap.New(core)
	return log, func() { _ = log.Sync(); cleanup() }, nil
}

func traceToZapLevel(level int) zapcore.Level {
	switch level {
	case 1:
		return zapcore.ErrorLevel
	case 2:
		return zapcore.WarnLevel
	case 3:
		return zapcore.InfoLevel
	default:
		return zapcore.DebugLevel
	}
}
