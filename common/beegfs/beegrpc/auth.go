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
	"strconv"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beemsg/util"
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
	// authRejectLogInterval is the shortest time between two logs of rejected calls. A caller with
	// the wrong secret usually retries in a loop, so logging every rejection would flood the log.
	authRejectLogInterval = time.Minute
)

// AuthServerOptions returns the server options that make a gRPC server require secret from every
// caller, on unary and streaming calls alike. Secret is the raw content of the auth file. A nil or
// empty secret returns no options, so the server checks nothing. A call without the key, or with a
// different hash, fails with codes.Unauthenticated before it reaches the handler.
//
// The server logs rejected calls to log as warnings, at most once per authRejectLogInterval (see
// rejectLog). Log must not be nil when secret is set.
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
// operator of the server sees nothing. It logs the first rejection, then at most one per
// authRejectLogInterval. Each log names the rejected call and counts the rejections it skipped
// since the previous log. It never logs the secret or the hash the caller sent.
type rejectLog struct {
	log        *zap.Logger
	mu         sync.Mutex
	lastLogged time.Time
	skipped    int
}

// record runs on every rejected call. ctx is the call's context, which carries the caller's
// address. method is the full gRPC method name. err is the status returned to the caller.
func (r *rejectLog) record(ctx context.Context, method string, err error) {
	r.mu.Lock()
	now := time.Now()
	if !r.lastLogged.IsZero() && now.Sub(r.lastLogged) < authRejectLogInterval {
		r.skipped++
		r.mu.Unlock()
		return
	}
	skipped := r.skipped
	r.lastLogged = now
	r.skipped = 0
	r.mu.Unlock()

	caller := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		caller = p.Addr.String()
	}
	r.log.Warn("rejected a call that failed BeeGFS connection authentication",
		zap.String("caller", caller), zap.String("method", method),
		zap.String("reason", status.Convert(err).Message()),
		zap.Int("rejectedSinceLastLog", skipped),
		zap.String("hint", "the caller must send the hash of the same auth file this service uses"))
}
