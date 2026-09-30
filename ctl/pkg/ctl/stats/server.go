package stats

import (
	"context"
	"fmt"
	"slices"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

// Contains one stat entry and the node it belongs to.
type NodeStats struct {
	Node  beegfs.Node
	Stats Stats
	// Err is set when the stats for Node could not be read. Stats is then the zero value and must
	// not be read as the node being idle.
	Err error
}

type Stats struct {
	StatsTime      uint64
	BusyWorkers    uint32
	QueuedRequests uint32
	DiskWriteBytes uint64
	DiskReadBytes  uint64
	NetSendBytes   uint64
	NetRecvBytes   uint64
	WorkRequests   uint32
}

func (s Stats) add(other Stats) Stats {
	s.DiskReadBytes += other.DiskReadBytes
	s.DiskWriteBytes += other.DiskWriteBytes
	s.NetSendBytes += other.NetSendBytes
	s.NetRecvBytes += other.NetRecvBytes
	s.WorkRequests += other.WorkRequests
	s.BusyWorkers += other.BusyWorkers
	s.QueuedRequests += other.QueuedRequests
	s.StatsTime = other.StatsTime
	return s
}

func statsFromHighResolutionStats(h msg.HighResolutionStats) Stats {
	return Stats{
		StatsTime:      h.StatsTimeMS / 1000,
		BusyWorkers:    h.BusyWorkers,
		QueuedRequests: h.QueuedRequests,
		DiskWriteBytes: h.DiskWriteBytes,
		DiskReadBytes:  h.DiskReadBytes,
		NetSendBytes:   h.NetSendBytes,
		NetRecvBytes:   h.NetRecvBytes,
		WorkRequests:   h.WorkRequests,
	}
}

// Queries the stats for one node. The returned slice is chronologically sorted in ascending order.
func SingleServerNode(ctx context.Context, id beegfs.EntityId) (beegfs.Node, []Stats, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return beegfs.Node{}, []Stats{}, err
	}

	n, err := store.GetNode(id)
	if err != nil {
		return beegfs.Node{}, []Stats{}, err
	}

	sRes := <-getServerStats(ctx, n)
	if sRes.err != nil {
		return beegfs.Node{}, []Stats{}, sRes.err
	}

	slices.Reverse(sRes.stats)

	return n, sRes.stats, nil
}

// Queries latest stat entry for multiple nodes separately. The returned error is only set when the
// node list cannot be read. A node whose stats cannot be read is still returned, with its Err set, so
// callers can tell an unreachable node from an idle one.
//
// A node returns its entries newest first. The newest entry is skipped and the second newest is
// used, like MultiServerNodesAggregated skips the newest entries. A node that returns fewer than two
// entries is treated as unreadable.
func MultiServerNodes(ctx context.Context, nt beegfs.NodeType) ([]NodeStats, error) {
	nodes, err := getNodeList(ctx, nt)
	if err != nil {
		return []NodeStats{}, err
	}

	channels := make([]<-chan getServerStatsResult, 0)
	for _, n := range nodes {
		channels = append(channels, getServerStats(ctx, n))
	}

	stats := make([]NodeStats, 0)
	for _, c := range channels {
		sRes := <-c

		serverStatResult := NodeStats{
			Node: sRes.node,
		}

		switch {
		case sRes.err != nil:
			serverStatResult.Err = sRes.err
		case len(sRes.stats) < 2:
			serverStatResult.Err = fmt.Errorf("node returned %d stats entries, at least 2 are required", len(sRes.stats))
		default:
			serverStatResult.Stats = sRes.stats[1]
		}

		stats = append(stats, serverStatResult)
	}

	return stats, nil
}

// Queries and sums the stats for multiple nodes. The returned slice is chronologically sorted in ascending order.
// The second return value is the number of nodes used for the sum. A node that returns no entries
// beyond the 2 newest ones is left out of the sum and the count. If the stats of any node cannot be
// read, an error is returned.
func MultiServerNodesAggregated(ctx context.Context, nt beegfs.NodeType) ([]Stats, int, error) {
	nodes, err := getNodeList(ctx, nt)
	if err != nil {
		return []Stats{}, 0, err
	}

	channels := make([]<-chan getServerStatsResult, 0)
	for _, n := range nodes {
		channels = append(channels, getServerStats(ctx, n))
	}

	// This map maps a timestamp to stats entry. It is used to sum up stat entries with the same timestamp.
	var m = make(map[uint64]Stats)
	summedNodes := 0
	for _, c := range channels {
		sRes := <-c

		if sRes.err != nil {
			return []Stats{}, 0, fmt.Errorf("reading stats from node %s: %w", sRes.node.Alias, sRes.err)
		}

		// The old ctl cuts off the latest 2 values. Its called "inaccuracy time". We are not sure if that makes sense but implement it
		// here as well for the time being.
		if len(sRes.stats) <= 2 {
			continue
		}
		for _, s := range sRes.stats[2:] {
			m[s.StatsTime] = m[s.StatsTime].add(s)
		}
		summedNodes++
	}

	var result = make([]Stats, 0)
	for _, v := range m {
		result = append(result, v)
	}

	slices.SortFunc(result, func(a, b Stats) int {
		return int(a.StatsTime) - int(b.StatsTime)
	})

	return result, summedNodes, nil
}

// Fetches the list of nodes from nodestore filter by the given node type
func getNodeList(ctx context.Context, nt beegfs.NodeType) ([]beegfs.Node, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return []beegfs.Node{}, err
	}

	nodes := store.GetNodes()
	res := make([]beegfs.Node, 0, len(nodes))

	for _, n := range nodes {
		if n.Id.NodeType != beegfs.Meta && n.Id.NodeType != beegfs.Storage {
			continue
		}

		if nt != beegfs.InvalidNodeType && n.Id.NodeType != nt {
			continue
		}

		res = append(res, n)
	}

	return res, nil
}

type getServerStatsResult struct {
	stats []Stats
	node  beegfs.Node
	err   error
}

// Fetches the list of stats from one node.
func getServerStats(ctx context.Context, n beegfs.Node) <-chan getServerStatsResult {
	var ch = make(chan getServerStatsResult, 1)
	go func() {
		store, err := config.NodeStore(ctx)
		if err != nil {
			ch <- getServerStatsResult{err: err, node: n}
			return
		}

		var resp = msg.GetHighResStatsResp{}
		err = store.RequestTCP(ctx, n.Uid, &msg.GetHighResStats{LastStatTime: 0}, &resp)
		if err != nil {
			ch <- getServerStatsResult{err: err, node: n}
			return
		}

		stats := []Stats{}
		for _, s := range resp.Stats {
			stats = append(stats, statsFromHighResolutionStats(s))
		}

		ch <- getServerStatsResult{stats: stats, node: n}
		close(ch)
	}()

	return ch
}
