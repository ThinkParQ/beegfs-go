package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/gobee/beemsg/msg"
	"github.com/thinkparq/gobee/beemsg/util"
	"github.com/thinkparq/gobee/types/node"
	"github.com/thinkparq/gobee/types/nodetype"
)

// The configuration passed to the GetNodeList function. Is built from command line flags in the
// command line tool.
type GetNodeList_Config struct {
	// Only process and print nodes of the given type.
	FilterByNodeType nodetype.NodeType
	// Include the network interface names and addresses and extra info for all the nodes in the
	// response. Causes extra work on management.
	WithNics bool
	// Check all nodes for reachability
	ReachabilityCheck bool
	// Waiting time for node responses. This defines how long the reachability check will take if
	// at least one pinged nic does not respond.
	ReachabilityTimeout time.Duration
}

// Wraps a Nic from the nodestore and provides additional reachability info.
type GetNodeList_Nic struct {
	Nic       node.Nic
	Reachable bool
}

// A GetNodeList result entry wrapping a Node from the nodestore together with a list of Nics.
type GetNodeList_Node struct {
	Node node.Node
	Nics []*GetNodeList_Nic
}

// Get the complete list of nodes from the mananagement
func GetNodeList(ctx context.Context, cfg GetNodeList_Config) ([]*GetNodeList_Node, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, err
	}

	addrMap := make(map[string]*GetNodeList_Nic)

	nodes := make([]*GetNodeList_Node, 0)
	for _, node := range store.GetNodes() {
		if cfg.FilterByNodeType != nodetype.Invalid && node.Id.Type != cfg.FilterByNodeType {
			continue
		}

		nics := make([]*GetNodeList_Nic, 0, len(node.Nics))
		for _, nic := range node.Nics {
			gn := &GetNodeList_Nic{Nic: nic}
			nics = append(nics, gn)
			addrMap[nic.Addr] = gn
		}

		nodes = append(nodes, &GetNodeList_Node{
			Node: node,
			Nics: nics,
		})
	}

	// Check all nics of all nodes for reachability if requested
	if cfg.ReachabilityCheck {
		// If the context has no deadline yet, we set it to the given timeout.
		// Note that this ignores cfg.ReachabilityTimeout if there is already a deadline on
		// the context.
		if _, ok := ctx.Deadline(); !ok {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, cfg.ReachabilityTimeout)
			defer cancel()
		}

		err := checkReachability(ctx, addrMap)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// Checks all of the given Nics for reachability by sending a HeartbeatRequest and waiting for
// response. The result is directly written to the *GetNodeList_Nic.Reachable. The map may not be
// touched until this function returns.
func checkReachability(ctx context.Context, addrMap map[string]*GetNodeList_Nic) error {
	// Create UDP socket - used for sending out the requests and collecting the response
	sock, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return err
	}
	defer sock.Close()

	// Start the receiver goroutine. We run this concurrently to sending out requests to avoid
	// too many pending responses (I don't know how big the kernel buffer for incoming datagrams is)
	doneCh := recvDatagrams(sock, addrMap)

	// Create the BeeMsg
	buf, err := util.AssembleBeeMsg(&msg.HeartbeatRequest{})
	if err != nil {
		return err
	}

	// Send a request one by one. No concurrency here since we can only receive one response at a
	// time anyway, so this wouldn't make too much sense.
	for addr := range addrMap {
		addr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return err
		}

		// Write the BeeMsg
		_, err = sock.WriteTo(buf, addr)
		if err != nil {
			return err
		}
	}

	// Wait till all responses have been received or we hit the context deadline
	select {
	case err = <-doneCh:
		return err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil
		}
		return ctx.Err()
	}
}

// Receive datagrams on the given socket. Set the corresponding *GetNodeList_Nic.Reachable to true
// for each received one.
func recvDatagrams(sock *net.UDPConn, addrMap map[string]*GetNodeList_Nic) <-chan error {
	// This channel is just used to signal that the receiver is done
	closeCh := make(chan error)

	go func() {
		defer close(closeCh)
		buf := make([]byte, 0, util.MaxDatagramSize)

		// check if all nodes in addrMap are marked as reachable
		allReachable := func() bool {
			for _, v := range addrMap {
				if !v.Reachable {
					return false
				}
			}
			return true
		}

		// while we didn't get a response from each nic
		for !allReachable() {
			_, from, err := sock.ReadFrom(buf)
			if err != nil {
				// If the deadline is hit, this is not an error
				if errors.Is(err, os.ErrDeadlineExceeded) {
					break
				}

				closeCh <- fmt.Errorf("could not read response: %w", err)
				return
			}

			addrMap[from.String()].Reachable = true
		}

		closeCh <- nil
	}()

	return closeCh
}
