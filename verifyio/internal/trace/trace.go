// Package trace configures IO-level tracing for verifyio tools.
// It is separate from operational logging so tools can enable verbose
// per-operation traces independently of their normal log level.
package trace

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TraceConfig controls IO-level tracing. Level 0 disables tracing.
// Levels 1–5 follow the BeeGFS log scale (1=Error, 2=Warn, 3=Info,
// 4/5=Debug). LogFile is the output path; empty means stderr.
type TraceConfig struct {
	Level   int8
	LogFile string
}

// TraceLoggers holds one logger per traceable subsystem. Today all
// subsystems share the same logger; the struct exists so subsystems
// can be given independent levels later without changing call sites.
type TraceLoggers struct {
	IO *zap.Logger
}

// Sync flushes any buffered log entries. Call before process exit.
func (t TraceLoggers) Sync() {
	_ = t.IO.Sync()
}

// NewTraceLoggers builds TraceLoggers from cfg. If cfg.Level is 0,
// all loggers are no-ops.
func NewTraceLoggers(cfg TraceConfig) (TraceLoggers, error) {
	if cfg.Level <= 0 {
		return TraceLoggers{IO: zap.NewNop()}, nil
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
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return TraceLoggers{}, fmt.Errorf("trace: open %s: %w", cfg.LogFile, err)
		}
		sink = zapcore.AddSync(f)
	} else {
		sink = zapcore.AddSync(os.Stderr)
	}

	level := zap.NewAtomicLevelAt(toZapLevel(cfg.Level))
	core := zapcore.NewCore(enc, sink, level)

	return TraceLoggers{IO: zap.New(core)}, nil
}

func toZapLevel(level int8) zapcore.Level {
	switch level {
	case 1:
		return zapcore.ErrorLevel
	case 2:
		return zapcore.WarnLevel
	case 3:
		return zapcore.InfoLevel
	default: // 4, 5
		return zapcore.DebugLevel
	}
}
