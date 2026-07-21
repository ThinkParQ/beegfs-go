package health

import (
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

// This file defines the structured detail attached to check/section results. Each detail type
// carries a clean, JSON-serialized projection plus (where the human renderer reuses an existing
// table printer) a Raw field that is not serialized. Both are built from the same collected data.

// BusyNode reports a single server node's queued requests. The degraded/critical thresholds it is
// evaluated against are reported once in the check summary (see QueuedReqsDegradedThreshold /
// QueuedReqsCriticalThreshold); per-node thresholds can be added here if they become configurable.
type BusyNode struct {
	Alias          string `json:"alias"`
	NumID          uint64 `json:"numID"`
	NodeType       string `json:"nodeType"`
	QueuedRequests uint32 `json:"queuedRequests"`
}

// BusyDetail is the detail for a busy-nodes check: every node of that type, not only the busy ones.
type BusyDetail struct {
	Nodes []BusyNode        `json:"nodes"`
	Raw   []stats.NodeStats `json:"-"`
}

func newBusyDetail(nodes []stats.NodeStats) BusyDetail {
	projected := make([]BusyNode, 0, len(nodes))
	for _, n := range nodes {
		projected = append(projected, BusyNode{
			Alias:          string(n.Node.Alias),
			NumID:          uint64(n.Node.Id.NumId),
			NodeType:       n.Node.Id.NodeType.String(),
			QueuedRequests: n.Stats.QueuedRequests,
		})
	}
	return BusyDetail{Nodes: projected, Raw: nodes}
}

// TargetCapacity is the per-target capacity/state projection shown by the target/df table.
type TargetCapacity struct {
	Target          beegfs.EntityIdSet  `json:"target"`
	NodeType        string              `json:"nodeType"`
	Node            *beegfs.EntityIdSet `json:"node,omitempty"`
	StoragePool     *beegfs.EntityIdSet `json:"storagePool,omitempty"`
	Reachability    string              `json:"reachability"`
	Consistency     string              `json:"consistency"`
	CapacityPool    string              `json:"capacityPool"`
	TotalSpaceBytes *uint64             `json:"totalSpaceBytes,omitempty"`
	FreeSpaceBytes  *uint64             `json:"freeSpaceBytes,omitempty"`
	TotalInodes     *uint64             `json:"totalInodes,omitempty"`
	FreeInodes      *uint64             `json:"freeInodes,omitempty"`
}

// TargetsDetail is the detail for the Targets section: every target's capacity and state.
type TargetsDetail struct {
	Targets []TargetCapacity               `json:"targets"`
	Raw     []tgtBackend.GetTargets_Result `json:"-"`
}

func newTargetsDetail(targets []tgtBackend.GetTargets_Result) TargetsDetail {
	projected := make([]TargetCapacity, 0, len(targets))
	for _, t := range targets {
		projected = append(projected, TargetCapacity{
			Target:          t.Target,
			NodeType:        t.NodeType.String(),
			Node:            t.Node,
			StoragePool:     t.StoragePool,
			Reachability:    t.ReachabilityState,
			Consistency:     t.ConsistencyState,
			CapacityPool:    t.CapacityPool,
			TotalSpaceBytes: t.TotalSpaceBytes,
			FreeSpaceBytes:  t.FreeSpaceBytes,
			TotalInodes:     t.TotalInodes,
			FreeInodes:      t.FreeInodes,
		})
	}
	return TargetsDetail{Targets: projected, Raw: targets}
}

// PeerConn is a single established connection to a server node from a client mount.
type PeerConn struct {
	Type        string `json:"type"`
	IP          string `json:"ip"`
	Connections int    `json:"connections"`
	Fallback    bool   `json:"fallback"`
}

// NodeConn groups the connections to one server node.
type NodeConn struct {
	Alias string     `json:"alias"`
	NumID uint64     `json:"numID"`
	Peers []PeerConn `json:"peers"`
}

// ClientConn is the per-client connection result. Raw is retained for the human renderer.
type ClientConn struct {
	ID           string        `json:"id"`
	Mount        string        `json:"mount"`
	Status       Status        `json:"fallbackStatus"`
	Summary      string        `json:"fallbackSummary"`
	MgmtdNodes   []NodeConn    `json:"mgmtdNodes"`
	MetaNodes    []NodeConn    `json:"metaNodes"`
	StorageNodes []NodeConn    `json:"storageNodes"`
	Raw          procfs.Client `json:"-"`
}

// ConnectionsDetail is the detail for the Connections section: one entry per client mount.
type ConnectionsDetail struct {
	Clients []ClientConn `json:"clients"`
}

// ClientConnections is the network-connection topology for one client mount, used by the
// `health network` command's JSON output. (The health check reuses NodeConn/PeerConn via ClientConn,
// which additionally carries a fallback verdict.)
type ClientConnections struct {
	ID           string     `json:"id"`
	Mount        string     `json:"mount"`
	MgmtdNodes   []NodeConn `json:"mgmtdNodes"`
	MetaNodes    []NodeConn `json:"metaNodes"`
	StorageNodes []NodeConn `json:"storageNodes"`
}

// ClientConnectionsFor projects a set of clients into their connection topology for JSON output.
func ClientConnectionsFor(clients []procfs.Client) []ClientConnections {
	result := make([]ClientConnections, 0, len(clients))
	for _, c := range clients {
		result = append(result, ClientConnections{
			ID:           c.ID,
			Mount:        c.Mount.Path,
			MgmtdNodes:   nodeConns(c.MgmtdNodes),
			MetaNodes:    nodeConns(c.MetaNodes),
			StorageNodes: nodeConns(c.StorageNodes),
		})
	}
	return result
}

func nodeConns(nodes []procfs.Node) []NodeConn {
	result := make([]NodeConn, 0, len(nodes))
	for _, n := range nodes {
		peers := make([]PeerConn, 0, len(n.Peers))
		for _, p := range n.Peers {
			peers = append(peers, PeerConn{
				Type:        p.Type.String(),
				IP:          p.IP,
				Connections: p.Connections,
				Fallback:    p.Fallback,
			})
		}
		result = append(result, NodeConn{
			Alias: string(n.Alias),
			NumID: uint64(n.NumID),
			Peers: peers,
		})
	}
	return result
}
