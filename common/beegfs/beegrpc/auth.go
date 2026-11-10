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

	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// AuthSecretMetadataKey is the gRPC metadata key that carries the hashed auth secret.
	AuthSecretMetadataKey = "auth-secret"
)

// AuthServerOptions returns the server options that make a gRPC server require secret from every
// caller, on unary and streaming calls alike. Secret is the raw content of the auth file. A nil or
// empty secret returns no options, so the server checks nothing. A call without the key, or with a
// different hash, fails with codes.Unauthenticated before it reaches the handler.
func AuthServerOptions(secret []byte) []grpc.ServerOption {
	if len(secret) == 0 {
		return nil
	}
	want := []byte(strconv.FormatUint(util.GenerateAuthSecret(secret), 10))
	check := func(ctx context.Context) error {
		md, _ := metadata.FromIncomingContext(ctx)
		got := md.Get(AuthSecretMetadataKey)
		if len(got) == 0 {
			return status.Error(codes.Unauthenticated, "request requires authentication but no secret was provided")
		}
		if subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			return status.Error(codes.Unauthenticated, "request requires authentication but provided secret doesn't match")
		}
		return nil
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := check(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := check(ss.Context()); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}
