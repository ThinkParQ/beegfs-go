package config

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func connHint(err error, nonLegacy bool) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return err
	}

	msg := st.Message()
	var hint string
	switch {
	case strings.Contains(msg, "no application protocol") && nonLegacy:
		hint = fmt.Sprintf("the management uses a different connection protocol, check --%s", ProtocolKey)
	case strings.Contains(msg, "no application protocol"):
		hint = fmt.Sprintf("the management uses the plain or encrypted connection protocol, set --%s and --%s", ProtocolKey, MgmtdIdKey)
	case nonLegacy && strings.Contains(msg, "key pair ID doesn't match"):
		hint = fmt.Sprintf("the key pair ID of the management differs from --%s, or the management uses the legacy connection protocol", MgmtdIdKey)
	case nonLegacy && strings.Contains(msg, "does not look like a TLS handshake"):
		hint = fmt.Sprintf("the management does not use TLS, it probably uses the legacy connection protocol with TLS disabled, check --%s", ProtocolKey)
	case strings.Contains(msg, "does not look like a TLS handshake"):
		hint = fmt.Sprintf("the management does not use TLS, check --%s", TlsDisableKey)
	case !nonLegacy && strings.Contains(msg, "server preface"):
		hint = fmt.Sprintf("the TLS configuration or the connection protocol may not match the management, check --%s and --%s", TlsDisableKey, ProtocolKey)
	default:
		return err
	}

	return status.Error(st.Code(), fmt.Sprintf("%s (hint: %s)", msg, hint))
}

func connHintUnaryInterceptor(nonLegacy bool) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return connHint(invoker(ctx, method, req, reply, cc, opts...), nonLegacy)
	}
}

func connHintStreamInterceptor(nonLegacy bool) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		return stream, connHint(err, nonLegacy)
	}
}
