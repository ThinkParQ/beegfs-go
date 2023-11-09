package config

import (
	"crypto/tls"
	"fmt"

	"github.com/thinkparq/protobuf/go/beegfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var globalConfig Config

// Connection set
type Config struct {
	ManagementAddr string
	BeeRemoteAddr  string
}

// Returns a pointer to the global config singleton
func Get() *Config {
	return &globalConfig
}

// Try to establish a connection to the managements gRPC service
func ManagementClient() (beegfs.ManagementClient, error) {
	if globalConfig.ManagementAddr == "" {
		return nil, fmt.Errorf("management address not set")
	}

	// Open gRPC connection to management.
	// Since the connection is opened asynchronously in the background, a context for timeouts is
	// not needed here. A potential timeout on connection will be checked when making an actual
	// request
	g, err := grpc.Dial(globalConfig.ManagementAddr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		return nil, fmt.Errorf("connecting to management service on %s failed: %w", globalConfig.ManagementAddr, err)
	}

	return beegfs.NewManagementClient(g), nil
}

// func BeeRemoteClient() (beeremote.BeeRemoteClient, error) {
// 	if globalConfig.ManagementAddr == "" {
// 		return nil, fmt.Errorf("bee-remote address not set")
// 	}
//
// 	g, err := grpc.Dial(globalConfig.BeeRemoteAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		return nil, fmt.Errorf("connecting to bee remote on %s failed: %w", globalConfig.BeeRemoteAddr, err)
// 	}
//
// 	return beeremote.NewBeeRemoteClient(g), nil
// }
