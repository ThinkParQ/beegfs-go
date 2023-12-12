package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/thinkparq/gobee/beemsg"
	"github.com/thinkparq/protobuf/go/beegfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// The global config singleton
var globalConfig Config

// Connection settings
type Config struct {
	ManagementAddr string
	BeeRemoteAddr  string

	// The timeout for a single(!) connection attempt
	ConnTimeout time.Duration
	// The authentication secret to use for BeeMsg communication. 0 means authentication is
	// disabled.
	AuthenticationSecret int64
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

// The global node store singleton
var nodeStore *beemsg.NodeStore

// Return a pointer to the global node store. Initializes and fetches node list on first call.
func NodeStore(ctx context.Context) (*beemsg.NodeStore, error) {
	if nodeStore != nil {
		return nodeStore, nil
	}

	// Create a node store using the current settings. These are copied, so later changes to
	// globalConfig don't affect them!
	s := beemsg.NewNodeStore(globalConfig.ConnTimeout, globalConfig.AuthenticationSecret)
	nodeStore = &s

	c, err := ManagementClient()
	if err != nil {
		return nil, err
	}

	// Fetch the node list from management
	nodes, err := c.GetNodeList(ctx, &beegfs.GetNodeListReq{
		IncludeNics: true,
	})
	if err != nil {
		return nil, err
	}

	// Loop through the node entries
	for _, n := range nodes.GetNodes() {
		addrs := []string{}
		// Collect the nodes addresses as strings
		for _, a := range n.Nics {
			addrs = append(addrs, fmt.Sprintf("%s:%d", a.Addr, n.BeemsgPort))
		}

		t := beemsg.Invalid
		switch n.Type {
		case beegfs.GetNodeListResp_Node_META:
			t = beemsg.Meta
		case beegfs.GetNodeListResp_Node_STORAGE:
			t = beemsg.Storage
		case beegfs.GetNodeListResp_Node_CLIENT:
			t = beemsg.Client
		}

		// Add node to store
		nodeStore.AddNode(&beemsg.Node{
			Uid:   n.Uid,
			Id:    n.NodeId,
			Type:  t,
			Alias: n.Alias,
			Addrs: addrs,
		})
	}

	return nodeStore, nil
}
