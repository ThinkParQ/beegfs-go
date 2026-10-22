// Package trace configures IO-level tracing for verifyio consumers.
// It is separate from operational logging so tools can enable verbose
// per-operation traces independently of their normal log level.
//
// Deliberately not under internal/: the beegfs iotest CLI command tree needs it
// too. That tree is not part of this library slice and lands separately, so no
// consumer here exercises the placement -- but while trace was internal, that
// package could not import it and carried a byte-for-byte copy instead,
// including its own copy of the BeeGFS log-level mapping, which is a third
// transcription of the one in common/logger. One of the copies then drifted
// (only one closed its log file), which is the usual way duplicated setup code
// fails.
package trace

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// DefaultMaxSizeMB and DefaultMaxBackups bound an IO trace on disk.
	//
	// These exist because a trace is per-operation: a soak at ~80k ops/sec with
	// tracing on produces log lines faster than anything reads them, and both
	// earlier copies of this code wrote to a plain O_APPEND file with no
	// rotation at all. A multi-hour soak -- the case the tool is for -- could
	// fill the filesystem it was testing, turning the diagnostic into the
	// outage. common/logger already rotates for exactly this reason.
	DefaultMaxSizeMB  = 100
	DefaultMaxBackups = 3
)

// TraceConfig controls IO-level tracing. Level 0 disables tracing.
// Levels 1–5 follow the BeeGFS log scale (1=Error, 2=Warn, 3=Info,
// 4/5=Debug). LogFile is the output path; empty means stderr.
//
// MaxSizeMB and MaxBackups bound the on-disk trace when LogFile is set; zero
// selects DefaultMaxSizeMB / DefaultMaxBackups. They are ignored for stderr.
type TraceConfig struct {
	Level      int8
	LogFile    string
	MaxSizeMB  int
	MaxBackups int
}

// TraceLoggers holds one logger per traceable subsystem. Today all
// subsystems share the same logger; the struct exists so subsystems
// can be given independent levels later without changing call sites.
type TraceLoggers struct {
	IO *zap.Logger

	// closer releases the log file when one was opened. nil for stderr and for
	// a disabled tracer.
	closer func() error
}

// Sync flushes any buffered log entries. Call before process exit.
//
// The nil check is what makes the zero value safe, and Close's doc promises
// exactly that. NewTraceLoggers returns TraceLoggers{} on both of its error
// paths, and zap.Logger.Sync dereferences the logger's core, so without it the
// idiomatic `tl, err := NewTraceLoggers(cfg); defer tl.Close()` panics in the
// defer and the panic replaces the trace-path error the caller was about to
// report.
func (t TraceLoggers) Sync() {
	if t.IO == nil {
		return
	}
	_ = t.IO.Sync()
}

// Close flushes and releases the log file, if any. Safe to call on a
// zero-value or disabled TraceLoggers, and safe to call more than once.
func (t *TraceLoggers) Close() error {
	t.Sync()
	if t.closer == nil {
		return nil
	}
	c := t.closer
	t.closer = nil
	return c()
}

// NewTraceLoggers builds TraceLoggers from cfg. If cfg.Level is 0,
// all loggers are no-ops.
//
// Call Close (not just Sync) when finished, or the log file stays open.
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
	var closer func() error
	if cfg.LogFile != "" {
		maxSize := cfg.MaxSizeMB
		if maxSize <= 0 {
			maxSize = DefaultMaxSizeMB
		}
		maxBackups := cfg.MaxBackups
		if maxBackups <= 0 {
			maxBackups = DefaultMaxBackups
		}
		// Fail now if the path is unusable, rather than discovering it on the
		// first trace line: lumberjack opens lazily on first write.
		//
		// O_NOFOLLOW matches every other O_CREATE in this tree (fileops.Open,
		// xattrstore.OpenStore, posixbench.preallocate, iotest-util's xattrcap):
		// these tools run as root on test boxes, so refuse to follow a symlink
		// planted at the destination rather than append to whatever it names.
		// Note lumberjack's own later opens are not covered by this probe.
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND|unix.O_NOFOLLOW, 0644)
		if err != nil {
			return TraceLoggers{}, fmt.Errorf("trace: open %s: %w", cfg.LogFile, err)
		}
		if err := f.Close(); err != nil {
			return TraceLoggers{}, fmt.Errorf("trace: close probe of %s: %w", cfg.LogFile, err)
		}
		lj := &lumberjack.Logger{
			Filename:   cfg.LogFile,
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
		}
		sink = zapcore.AddSync(lj)
		closer = lj.Close
	} else {
		sink = zapcore.AddSync(os.Stderr)
	}

	level := zap.NewAtomicLevelAt(ToZapLevel(cfg.Level))
	core := zapcore.NewCore(enc, sink, level)

	return TraceLoggers{IO: zap.New(core), closer: closer}, nil
}

// ToZapLevel maps the BeeGFS 1-5 log scale onto zap levels. Exported so
// consumers do not transcribe the mapping a fourth time; see the package doc.
//
// Out-of-range values fall through to Debug rather than erroring, matching what
// both previous copies did. Note common/logger's equivalent differs -- it maps 0
// to Fatal and rejects out-of-range input -- so do not treat the two as
// interchangeable.
func ToZapLevel(level int8) zapcore.Level {
	switch level {
	case 1:
		return zapcore.ErrorLevel
	case 2:
		return zapcore.WarnLevel
	case 3:
		return zapcore.InfoLevel
	default: // 4, 5, and anything unexpected
		return zapcore.DebugLevel
	}
}
