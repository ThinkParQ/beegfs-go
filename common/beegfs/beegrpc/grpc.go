package beegrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type connOpts struct {
	TLSDisableVerification bool
	TLSDisable             bool
	UseProxy               bool
	// TLSCaCert is the contents of a certificate file or nil if no extra certs should be applied.
	TLSCaCert []byte
	// AuthSecret is the contents of an auth file or nil if authentication should be disabled.
	AuthSecret []byte
}

// applyConnOpts is a common constructor for connOpts. It exists because originally only
// NewClientConn() needed the connOpts, but later it was determined NewMgmtd() needs the auth
// secret and changing the signature of NewClientConn to return connOpts was not ideal.
func applyConnOpts(cOpts ...connOpt) *connOpts {
	opts := &connOpts{
		TLSDisableVerification: false,
		TLSDisable:             false,
		UseProxy:               false,
		TLSCaCert:              nil,
		AuthSecret:             nil,
	}
	for _, opt := range cOpts {
		opt(opts)
	}
	return opts
}

type connOpt func(*connOpts)

func WithTLSDisableVerification(disable bool) connOpt {
	return func(co *connOpts) {
		co.TLSDisableVerification = disable
	}
}

func WithTLSDisable(disable bool) connOpt {
	return func(co *connOpts) {
		co.TLSDisable = disable
	}
}

func WithTLSCaCert(cert []byte) connOpt {
	return func(co *connOpts) {
		co.TLSCaCert = cert
	}
}

func WithAuthSecret(secret []byte) connOpt {
	return func(co *connOpts) {
		co.AuthSecret = secret
	}
}

func WithProxy(enable bool) connOpt {
	return func(co *connOpts) {
		co.UseProxy = enable
	}
}

// NewClientConn provides a standard method to configure TLS and BeeGFS connection authentication
// when setting up a gRPC client connection for use with a BeeGFS gRPC client.
func NewClientConn(address string, cOpts ...connOpt) (*grpc.ClientConn, error) {
	if strings.Contains(address, "://") {
		return nil, fmt.Errorf("specifying a URI scheme in the address is not supported: %q", address)
	}

	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, fmt.Errorf(
			"invalid target address %q; bare addresses must be in 'host:port' form (for IPv6, use the '[address]:port' notation): %w",
			address, err,
		)
	}

	config := applyConnOpts(cOpts...)

	var opts []grpc.DialOption
	// NOTE: grpc.WithNoProxy() is marked as experimental, but has been for a long time. In the
	// unlikely case it gets removed, we can easily replace it with a custome DialOption generator
	if !config.UseProxy {
		opts = append(opts, grpc.WithNoProxy())
	}
	// The mgmtd expects the conn auth secret to be included in the metadata with each request. We
	// use an interceptor to inject this automatically when authentication is enabled. References:
	// https://github.com/grpc/grpc-go/tree/master/examples/features/metadata_interceptor and
	// https://github.com/grpc/grpc-go/blob/master/examples/features/interceptor/README.md#client-side
	if config.AuthSecret != nil {
		connAuthUnaryInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			newCtx := metadata.AppendToOutgoingContext(ctx, "auth-secret", fmt.Sprint(util.GenerateAuthSecret(config.AuthSecret)))
			return invoker(newCtx, method, req, reply, cc, opts...)
		}
		opts = append(opts, grpc.WithUnaryInterceptor(connAuthUnaryInterceptor))
		connAuthStreamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			newCtx := metadata.AppendToOutgoingContext(ctx, "auth-secret", fmt.Sprint(util.GenerateAuthSecret(config.AuthSecret)))
			return streamer(newCtx, desc, cc, method, opts...)
		}
		opts = append(opts, grpc.WithStreamInterceptor(connAuthStreamInterceptor))
	}
	// If the user explicitly disabled TLS that should take precedence. Note a mismatch between the
	// client and server TLS configuration will likely return a vague `error reading server preface:
	// EOF`. It is up to the caller to intercept this error and display a meaningful error/hint.
	if config.TLSDisable {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("couldn't load system cert pool: %w", err)
		}
		// Append custom ca certificate if provided
		if config.TLSCaCert != nil {
			if !certPool.AppendCertsFromPEM(config.TLSCaCert) {
				return nil, fmt.Errorf("appending provided certificate to pool failed")
			}
		}
		creds := credentials.NewTLS(&tls.Config{
			RootCAs:            certPool,
			InsecureSkipVerify: config.TLSDisableVerification,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	c, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("error creating gRPC client: %w", err)
	}
	return c, nil
}
