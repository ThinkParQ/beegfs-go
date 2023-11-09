package node

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	pb "github.com/thinkparq/protobuf/go/beegfs"
)

// NOTE
//
// This file is meant as an example to create your own commands. Please follow the general
// structure, but DO NOT copy the excessive generic comments. Only write meaningful, command specific
// comments.

// The configuration passed to the GetNodeList function. Is built from command line flags in the
// command line tool.
type GetNodeList_Config struct {
	// Include the network addresses amd extra info for all the nodes in the response. Causes extra work on management
	IncludeAddrs bool
	// Check all nodes for reachability
	CheckReachability bool
}

// A GetNodeList result entry.
type GetNodeList_Node struct {
	Uid        int64
	Id         uint32
	Type       string
	Alias      string
	BeemsgPort uint16
	// List of network addresses the node should be available on. Ordered as delivered from
	// management (e.g. highest priority first)
	Addrs []string
	// Empty if Ping is false or node is unreachable. A string in the form addr:port if the node
	// responded.
	ReachableOn string
}

// Get the complete list of nodes from the mananagement
func GetNodeList(ctx context.Context, cfg GetNodeList_Config) ([]GetNodeList_Node, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	// Send request to the management via gRPC.
	res, err := mgmtd.GetNodeList(ctx, &pb.GetNodeListReq{IncludeNics: cfg.IncludeAddrs || cfg.CheckReachability})
	if err != nil {
		return nil, fmt.Errorf("requesting node list failed: %w", err)
	}

	// Transform into result struct
	var nodes []GetNodeList_Node
	for _, e := range res.Nodes {
		var addrs []string
		for _, a := range e.Nics {
			addrs = append(addrs, a.Addr)
		}

		node := GetNodeList_Node{
			Uid:         e.Uid,
			Id:          e.NodeId,
			Type:        e.Type.String(),
			Alias:       e.Alias,
			BeemsgPort:  uint16(e.BeemsgPort),
			Addrs:       addrs,
			ReachableOn: "",
		}
		nodes = append(nodes, node)
	}

	// Check all nodes for reachability if requested
	if cfg.CheckReachability {
		var wg sync.WaitGroup
		for i, node := range nodes {
			wg.Add(1)

			// For servers we want to show which of the nodes address was used for a TCP connection,
			// so we want to try connect via TCP.
			// Clients, on the other hand, are only reachable via UDP, so they need extra treatment.
			// TODO use enum?
			if node.Type == "CLIENT" {
				go pingClient(&node)
			} else {
				// We share a pointer to the original struct, not the copy made for the for loop.
				// This allows modification
				go connectToServer(&wg, &nodes[i])
			}
		}

		// Wait for all goroutines completion
		wg.Wait()
	}

	return nodes, nil
}

// Tries to connect to a server node via TCP using the supplied network addresses in order.
// TODO timeout, do we need to send an actual message?
func connectToServer(wg *sync.WaitGroup, node *GetNodeList_Node) {
	defer wg.Done()

	for _, addr := range node.Addrs {
		addrString := fmt.Sprintf("%s:%d", addr, node.BeemsgPort)
		conn, err := net.Dial("tcp", addrString)

		if err == nil {
			defer conn.Close()
			// Node is available, update the structs member accordingly
			node.ReachableOn = addr
			return
		}
	}
}

// Sends a heartbeat msg to a client node via UDP and waits for a response
// TODO broadcast to all addresses or not?, shared socket? probably need to wait for responses in a
// separate goroutine and map them
func pingClient(node *GetNodeList_Node) {
	// TODO
}
