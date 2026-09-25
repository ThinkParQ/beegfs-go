// This is a unit test.
//
// Coverage: that a trace line reaches a file-backed log, that the log rotates
// rather than growing without bound, that Close is idempotent and safe on the
// zero value, on a disabled tracer and after a failed NewTraceLoggers, that an
// unusable LogFile is rejected up front, and the BeeGFS 1-5 level mapping.
//
// Rotation is the one that matters: a trace is per-operation, so a soak at tens
// of thousands of ops per second emits lines faster than anything consumes them.
// Both earlier copies of this code wrote to a plain O_APPEND file with no
// rotation, so a long soak could fill the filesystem it was testing and the
// diagnostic became the outage.
//
// NOT covered: that Close actually releases the log file -- the property the
// package doc names, since one of those two copies never closed it. Setting
// closer = nil at trace.go:138 leaves every test in this file passing: Close
// returns nil with no closer installed, and zap writes through an unbuffered
// AddSync so the file has content either way. Pinning it needs a descriptor
// count over /proc/self/fd; that was prototyped and judged not worth carrying,
// because every consumer opens one trace per process and then exits, so a leaked
// handle costs a descriptor that was about to be released anyway. Revisit if a
// consumer ever opens a trace per file or per worker.
package trace

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestNewTraceLoggersDisabledIsNoop(t *testing.T) {
	tl, err := NewTraceLoggers(TraceConfig{Level: 0})
	if err != nil {
		t.Fatalf("NewTraceLoggers: %v", err)
	}
	if tl.IO == nil {
		t.Fatal("IO logger is nil")
	}
	// Close must be safe with no file behind it.
	if err := tl.Close(); err != nil {
		t.Errorf("Close on a disabled tracer: %v", err)
	}
	// And idempotent.
	if err := tl.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestNewTraceLoggersWritesAndCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "io.log")
	tl, err := NewTraceLoggers(TraceConfig{Level: 4, LogFile: path})
	if err != nil {
		t.Fatalf("NewTraceLoggers: %v", err)
	}

	tl.IO.Debug("hello trace")
	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Closing twice must not error -- callers wire this into a defer and may
	// also call it explicitly.
	if err := tl.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if len(b) == 0 {
		t.Error("log file is empty; the trace line did not reach it")
	}
}

func TestNewTraceLoggersRejectsUnusablePath(t *testing.T) {
	// lumberjack opens lazily on first write, so without an upfront probe an
	// unwritable path would only surface as a silently-missing trace much later.
	dir := t.TempDir()
	if _, err := NewTraceLoggers(TraceConfig{Level: 4, LogFile: dir}); err == nil {
		t.Error("want an error when LogFile names a directory")
	}
}

func TestNewTraceLoggersRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "io.log")
	// 1 MB is lumberjack's minimum meaningful size; write comfortably past it.
	tl, err := NewTraceLoggers(TraceConfig{Level: 4, LogFile: path, MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatalf("NewTraceLoggers: %v", err)
	}
	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'x'
	}
	for range 600 { // ~2.4 MB of payload, so at least one rotation
		tl.IO.Debug(string(big))
	}
	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) < 2 {
		var names []string
		var total int64
		for _, e := range entries {
			names = append(names, e.Name())
			if info, ierr := e.Info(); ierr == nil {
				total += info.Size()
			}
		}
		t.Errorf("got %d file(s) %v totalling %d bytes; want a rotated backup alongside "+
			"the active log -- an unrotated trace can fill the filesystem under test",
			len(entries), names, total)
	}
}

func TestToZapLevel(t *testing.T) {
	for _, c := range []struct {
		in   int8
		want zapcore.Level
	}{
		{1, zapcore.ErrorLevel},
		{2, zapcore.WarnLevel},
		{3, zapcore.InfoLevel},
		{4, zapcore.DebugLevel},
		{5, zapcore.DebugLevel},
		// Out of range falls through to Debug, matching both previous copies.
		// common/logger's equivalent behaves differently on purpose (0 -> Fatal,
		// out-of-range rejected), so the two are not interchangeable.
		{99, zapcore.DebugLevel},
	} {
		if got := ToZapLevel(c.in); got != c.want {
			t.Errorf("ToZapLevel(%d) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestZeroValueTraceLoggersAreSafe pins Close's documented promise that the
// zero value is safe -- it was not.
//
// NewTraceLoggers returns TraceLoggers{} on both of its error paths, so the
// idiomatic acquire-then-defer sequence ran Close on a nil *zap.Logger.
// zap.Logger.Sync dereferences the logger's core, so the deferred Close
// panicked, and the panic replaced the trace-path error the caller was about to
// report. Nothing in-repo caught it when this was written, because the shared
// helper exited on the error itself; it was waiting for the first external
// consumer, which the package doc explicitly invites. That helper is now
// climain.NewTraceLoggers and RETURNS the error, so cmd/iotest-smoke is an
// in-repo consumer of this path -- it checks the error before registering the
// deferred Close, which is the ordering this test makes safe either way.
func TestZeroValueTraceLoggersAreSafe(t *testing.T) {
	var tl TraceLoggers
	tl.Sync() // must not panic
	if err := tl.Close(); err != nil {
		t.Errorf("Close on the zero value: %v", err)
	}
	// Close is documented as safe more than once, too.
	if err := tl.Close(); err != nil {
		t.Errorf("second Close on the zero value: %v", err)
	}
}

// TestNewTraceLoggersErrorPathIsCloseable walks the actual failing sequence
// rather than a hand-built zero value: an unusable trace path, then the
// deferred Close that used to panic instead of letting the caller report err.
func TestNewTraceLoggersErrorPathIsCloseable(t *testing.T) {
	// A path whose parent is a regular file can never be opened.
	notADir := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(notADir, nil, 0644); err != nil {
		t.Fatalf("seeding the blocking file: %v", err)
	}

	tl, err := NewTraceLoggers(TraceConfig{Level: 3, LogFile: filepath.Join(notADir, "trace.log")})
	if err == nil {
		t.Fatal("NewTraceLoggers accepted a trace path under a regular file")
	}
	if cerr := tl.Close(); cerr != nil {
		t.Errorf("Close after a failed NewTraceLoggers: %v", cerr)
	}
}
