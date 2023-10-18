package internal

import (
	"context"
	"crypto/tls"
	"fmt"

	pb "github.com/thinkparq/protobuf/go/beegfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// The key used for Context.Value() to retrieve the configuration
type CtxConfigKey struct{}

// The global configuration, accessible via Context.Value(CtxConfigKey{})
type Config struct {
	ManagementAddr string
	mgmtdClient    pb.ManagementClient
}

// Returns a gRPC management client. Opens connection the first time used.
func ManagementClient(ctx context.Context) (pb.ManagementClient, error) {
	cfg := ctx.Value(CtxConfigKey{}).(*Config)

	if cfg.mgmtdClient == nil {
		client, err := connectToManagement(cfg.ManagementAddr)
		if err != nil {
			return nil, err
		}

		cfg.mgmtdClient = client
	}

	return cfg.mgmtdClient, nil
}

func connectToManagement(addr string) (pb.ManagementClient, error) {
	g, err := grpc.Dial(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		return nil, fmt.Errorf("Connecting to management service on %s failed: %w", addr, err)
	}

	return pb.NewManagementClient(g), nil
}
