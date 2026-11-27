package beegrpc

// BeeGFS connection authentication ("ConnAuth") proves that a caller holds the file system's
// shared secret file, usually /etc/beegfs/conn.auth. The file itself never travels. Every caller
// sends a 64-bit hash of it instead (see util.GenerateAuthSecret). Over gRPC the hash travels as a
// decimal string in the AuthSecretMetadataKey metadata key of every request. The management
// service checks that key. AuthServerOptions lets a Go gRPC server check it the same way.
//
// Clients add the key with WithAuthSecret on NewClientConn. Servers that do not check the key
// ignore it, so a client may always send a secret it holds.

import (
	"context"
	"crypto/subtle"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	// AuthSecretMetadataKey is the gRPC metadata key that carries the hashed auth secret.
	AuthSecretMetadataKey = "auth-secret"
	// authRejectLogInterval is the shortest time between two logs of rejected calls from one caller
	// host. A caller with the wrong secret usually retries in a loop, so logging every rejection
	// would flood the log.
	authRejectLogInterval = time.Minute
	// authRejectMaxHosts caps how many caller hosts rejectLog tracks. Callers are unauthenticated,
	// so the cap keeps a caller that cycles through addresses from growing the map without bound.
	authRejectMaxHosts = 1024
	// authRejectOverflowHost is the key rejectLog uses for every new host once it tracks
	// authRejectMaxHosts hosts that all logged within authRejectLogInterval.
	authRejectOverflowHost = "other"
	// authRejectHint is the operator hint in a log of a rejected call (see rejectHint).
	authRejectHint = "the caller must send the hash of the same auth file this service uses"
	// authRejectWatchHint is the hint for a rejected Watch event stream (see rejectHint).
	authRejectWatchHint = "the caller is BeeGFS Watch delivering file system events: set " +
		"grpc-send-auth-secret = true for this subscriber in the Watch configuration, and make " +
		"sure management.auth-file there holds the same secret as this service. Older Watch " +
		"versions cannot send the secret and must be upgraded"
)

// AuthServerOptions returns the server options that make a gRPC server require secret from every
// caller, on unary and streaming calls alike. Secret is the raw content of the auth file. A nil or
// empty secret returns no options, so the server checks nothing. A call without the key, or with a
// different hash, fails with codes.Unauthenticated before it reaches the handler.
//
// The server logs rejected calls to log as warnings, at most once per authRejectLogInterval for each
// caller host (see rejectLog). Log must not be nil when secret is set.
func AuthServerOptions(secret []byte, log *zap.Logger) []grpc.ServerOption {
	if len(secret) == 0 {
		return nil
	}
	want := []byte(strconv.FormatUint(util.GenerateAuthSecret(secret), 10))
	rejects := &rejectLog{log: log}
	check := func(ctx context.Context, method string) error {
		md, _ := metadata.FromIncomingContext(ctx)
		got := md.Get(AuthSecretMetadataKey)
		var err error
		if len(got) == 0 {
			err = status.Error(codes.Unauthenticated, "request requires authentication but no secret was provided")
		} else if subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			err = status.Error(codes.Unauthenticated, "request requires authentication but provided secret doesn't match")
		}
		if err != nil {
			rejects.record(ctx, method, err)
		}
		return err
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := check(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}

// rejectLog logs calls that fail the auth check. Without it the caller sees the rejection, but the
// operator of the server sees nothing. It never logs the secret or the hash the caller sent.
//
// The rate limit applies to each caller host on its own. One shared limit would let a single host
// that retries in a loop take every log slot, and hide the address of every other rejected host.
// The rules:
//   - The key is the caller's host without the port. A client that reconnects gets a new ephemeral
//     port, so a key with the port would not limit a client that reconnects on every retry.
//   - The first rejection from a host is logged. Later ones from that host are logged at most once
//     per authRejectLogInterval. Each log counts the rejections from that host it skipped since
//     the previous log.
//   - At most authRejectMaxHosts hosts are tracked. When the map is full, hosts whose last log is
//     older than the interval are dropped. If none can be dropped, new hosts share the
//     authRejectOverflowHost entry. A dropped host loses its skipped count, and so does a host
//     that stops calling. Neither is ever logged.
type rejectLog struct {
	log   *zap.Logger
	mu    sync.Mutex
	hosts map[string]*hostRejects
}

// hostRejects is the rate limit state of one caller host.
type hostRejects struct {
	lastLogged time.Time
	skipped    int
}

// record runs on every rejected call. ctx is the call's context, which carries the caller's
// address. method is the full gRPC method name. err is the status returned to the caller.
func (r *rejectLog) record(ctx context.Context, method string, err error) {
	caller := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		caller = p.Addr.String()
	}
	host := caller
	if h, _, splitErr := net.SplitHostPort(caller); splitErr == nil {
		host = h
	}

	now := time.Now()
	r.mu.Lock()
	state := r.stateFor(host, now)
	if !state.lastLogged.IsZero() && now.Sub(state.lastLogged) < authRejectLogInterval {
		state.skipped++
		r.mu.Unlock()
		return
	}
	skipped := state.skipped
	state.lastLogged = now
	state.skipped = 0
	r.mu.Unlock()

	r.log.Warn("rejected a call that failed BeeGFS connection authentication",
		zap.String("caller", caller), zap.String("method", method),
		zap.String("reason", status.Convert(err).Message()),
		zap.Int("rejectedSinceLastLog", skipped),
		zap.String("hint", rejectHint(method)))
}

// rejectHint returns the hint for a log of a rejected call to method, the full gRPC method name.
// Watch does not send the secret unless its subscriber entry opts in, so a missing opt-in is the
// usual reason Watch is rejected. The fix is in the Watch configuration, not on this service, so
// the Watch event stream gets a hint that names it. Every other method gets the generic hint.
func rejectHint(method string) string {
	if method == beewatch.Subscriber_ReceiveEvents_FullMethodName {
		return authRejectWatchHint
	}
	return authRejectHint
}

// stateFor returns the state of host, and adds it when it is new. It applies the authRejectMaxHosts
// cap described on rejectLog. The caller must hold r.mu.
func (r *rejectLog) stateFor(host string, now time.Time) *hostRejects {
	if r.hosts == nil {
		r.hosts = make(map[string]*hostRejects)
	}
	if state, ok := r.hosts[host]; ok {
		return state
	}
	if len(r.hosts) >= authRejectMaxHosts {
		for h, state := range r.hosts {
			if now.Sub(state.lastLogged) >= authRejectLogInterval {
				delete(r.hosts, h)
			}
		}
	}
	if len(r.hosts) >= authRejectMaxHosts {
		host = authRejectOverflowHost
		if state, ok := r.hosts[host]; ok {
			return state
		}
	}
	state := &hostRejects{}
	r.hosts[host] = state
	return state
}
