package beegrpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// TestRejectLogRateLimit checks that rejectLog logs the first rejection, counts the ones inside
// authRejectLogInterval without logging them, and reports that count in the next log. A context
// without a peer counts as the host "unknown".
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
	r.hosts["unknown"].lastLogged = time.Now().Add(-authRejectLogInterval)
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

// peerCtx returns a context whose peer is the TCP address addr, as gRPC sets it for a real call.
func peerCtx(t *testing.T, addr string) context.Context {
	t.Helper()
	a, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatalf("resolve %s: %v", addr, err)
	}
	return peer.NewContext(context.Background(), &peer.Peer{Addr: a})
}

// TestRejectLogPerHost checks that a host that retries in a loop does not hide another host, and
// that a new port on the same host does not reset that host's limit.
func TestRejectLogPerHost(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	r := &rejectLog{log: zap.New(core)}
	err := status.Error(codes.Unauthenticated, "no secret")

	for i := range 10 {
		r.record(peerCtx(t, fmt.Sprintf("10.0.0.1:%d", 40000+i)), "/svc/A", err)
	}
	r.record(peerCtx(t, "10.0.0.2:40000"), "/svc/A", err)

	if got := logs.Len(); got != 2 {
		t.Fatalf("got %d log entries, want 2 (one per host)", got)
	}
	if got := logs.All()[1].ContextMap()["caller"]; got != "10.0.0.2:40000" {
		t.Errorf("second log: caller = %v, want 10.0.0.2:40000", got)
	}
	if got := r.hosts["10.0.0.1"].skipped; got != 9 {
		t.Errorf("10.0.0.1 skipped = %d, want 9 (the port must not be part of the key)", got)
	}
}

// TestRejectLogHostCap checks that rejectLog tracks at most authRejectMaxHosts hosts plus the
// overflow entry, and that it drops hosts whose last log is older than the interval to make room.
func TestRejectLogHostCap(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	r := &rejectLog{log: zap.New(core)}
	err := status.Error(codes.Unauthenticated, "no secret")

	for i := range authRejectMaxHosts {
		r.record(peerCtx(t, fmt.Sprintf("10.1.%d.%d:1", i/256, i%256)), "/svc/A", err)
	}
	// The map is full and every host logged just now, so new hosts share the overflow entry.
	r.record(peerCtx(t, "10.9.0.1:1"), "/svc/A", err)
	r.record(peerCtx(t, "10.9.0.2:1"), "/svc/A", err)
	if got := len(r.hosts); got != authRejectMaxHosts+1 {
		t.Fatalf("tracked %d hosts, want %d", got, authRejectMaxHosts+1)
	}
	if got := r.hosts[authRejectOverflowHost].skipped; got != 1 {
		t.Errorf("overflow skipped = %d, want 1", got)
	}
	if got := logs.Len(); got != authRejectMaxHosts+1 {
		t.Errorf("got %d log entries, want %d", got, authRejectMaxHosts+1)
	}

	// Once the old hosts' logs expire, a new host gets its own entry again.
	r.mu.Lock()
	for _, state := range r.hosts {
		state.lastLogged = time.Now().Add(-authRejectLogInterval)
	}
	r.mu.Unlock()
	r.record(peerCtx(t, "10.9.0.3:1"), "/svc/A", err)
	if _, ok := r.hosts["10.9.0.3"]; !ok || len(r.hosts) != 1 {
		t.Errorf("after expiry: tracked %d hosts, want only 10.9.0.3", len(r.hosts))
	}
}

// TestRejectHint checks that a rejected Watch event stream gets the Watch hint, and every other
// method gets the generic one.
func TestRejectHint(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	r := &rejectLog{log: zap.New(core)}
	err := status.Error(codes.Unauthenticated, "no secret")

	r.record(peerCtx(t, "10.0.0.1:1"), beewatch.Subscriber_ReceiveEvents_FullMethodName, err)
	r.record(peerCtx(t, "10.0.0.2:1"), "/svc/A", err)

	if got := logs.All()[0].ContextMap()["hint"]; got != authRejectWatchHint {
		t.Errorf("Watch stream: hint = %v, want the Watch hint", got)
	}
	if got := logs.All()[1].ContextMap()["hint"]; got != authRejectHint {
		t.Errorf("other method: hint = %v, want the generic hint", got)
	}
}
