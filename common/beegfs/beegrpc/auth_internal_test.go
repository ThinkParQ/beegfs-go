package beegrpc

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRejectLogRateLimit checks that rejectLog logs the first rejection, counts the ones inside
// authRejectLogInterval without logging them, and reports that count in the next log.
func TestRejectLogRateLimit(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	r := &rejectLog{log: zap.New(core)}
	ctx := context.Background()
	err := status.Error(codes.Unauthenticated, "no secret")

	r.record(ctx, "/svc/A", err)
	r.record(ctx, "/svc/B", err)
	r.record(ctx, "/svc/C", err)
	if got := logs.Len(); got != 1 {
		t.Fatalf("got %d log entries inside the interval, want 1", got)
	}
	if got := logs.All()[0].ContextMap()["rejectedSinceLastLog"]; got != int64(0) {
		t.Errorf("first log: rejectedSinceLastLog = %v, want 0", got)
	}

	// Move the last log back past the interval, so the next rejection logs again.
	r.mu.Lock()
	r.lastLogged = time.Now().Add(-authRejectLogInterval)
	r.mu.Unlock()

	r.record(ctx, "/svc/D", err)
	if got := logs.Len(); got != 2 {
		t.Fatalf("got %d log entries after the interval, want 2", got)
	}
	second := logs.All()[1].ContextMap()
	if got := second["rejectedSinceLastLog"]; got != int64(2) {
		t.Errorf("second log: rejectedSinceLastLog = %v, want 2", got)
	}
	if got := second["method"]; got != "/svc/D" {
		t.Errorf("second log: method = %v, want /svc/D", got)
	}
	if got := second["caller"]; got != "unknown" {
		t.Errorf("second log: caller = %v, want unknown for a context without a peer", got)
	}
}
