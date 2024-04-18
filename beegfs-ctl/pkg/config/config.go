package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/thinkparq/gobee/beegfs"
	"github.com/thinkparq/gobee/beemsg"
	pb "github.com/thinkparq/protobuf/go/beegfs"
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

	// Prints values in their raw, base form, without adding units and SI/IEC prefixes. Durations
	// excluded.
	Raw bool

	// Tells the command to print additional, normally hidden info. An example would be the entity
	// UIDs which currently are only used internally and hidden to avoid user confusion.
	Debug bool
}

// Returns a pointer to the global config singleton
func Get() *Config {
	return &globalConfig
}

// Try to establish a connection to the managements gRPC service
func ManagementClient() (pb.ManagementClient, error) {
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

	return pb.NewManagementClient(g), nil
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
	nodeStore := beemsg.NewNodeStore(globalConfig.ConnTimeout, globalConfig.AuthenticationSecret)

	c, err := ManagementClient()
	if err != nil {
		return nil, err
	}

	// Fetch the node list from management
	nodes, err := c.GetNodes(ctx, &pb.GetNodesRequest{
		IncludeNics: true,
	})
	if err != nil {
		return nil, err
	}

	// Loop through the node entries
	for _, n := range nodes.GetNodes() {
		nics := []beegfs.Nic{}
		for _, a := range n.Nics {
			nict := beegfs.InvalidNicType
			switch a.GetNicType() {
			case pb.NicType_ETHERNET:
				nict = beegfs.Ethernet
			case pb.NicType_RDMA:
				nict = beegfs.Rdma
			}

			nics = append(nics, beegfs.Nic{Addr: a.Addr, Name: a.Name, Type: nict})
		}

		t := beegfs.InvalidNodeType
		switch n.GetId().GetLegacyId().GetNodeType() {
		case pb.NodeType_META:
			t = beegfs.Meta
		case pb.NodeType_STORAGE:
			t = beegfs.Storage
		case pb.NodeType_CLIENT:
			t = beegfs.Client
		case pb.NodeType_MANAGEMENT:
			t = beegfs.Management
		}

		// Add node to store
		nodeStore.AddNode(&beegfs.Node{
			Uid: beegfs.Uid(n.Id.Uid),
			Id: beegfs.LegacyId{
				NumId:    beegfs.NumId(n.Id.LegacyId.NumId),
				NodeType: t,
			},
			Alias: beegfs.Alias(n.Id.Alias),
			Nics:  nics,
		})
	}

	metaRoot, err := beegfs.EntityIdSetFromProto(nodes.GetMetaRootNode())
	if err != nil {
		return nil, err
	}

	nodeStore.SetMetaRootNode(metaRoot.Uid)

	return nodeStore, nil
}

// Resets the global state and frees resources
func Cleanup() {
	if nodeStore != nil {
		nodeStore.Cleanup()
	}

	globalConfig = Config{}
	nodeStore = nil
}
