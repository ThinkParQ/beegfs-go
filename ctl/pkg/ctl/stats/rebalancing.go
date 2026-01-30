package stats

import (
	"context"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type ChunkBalanceNodeStatus struct {
	Node  beegfs.Node
	Stats msg.GetChunkBalanceJobStatsRespMsg
	Err   error
}

func ChunkBalanceStatusForNodes(ctx context.Context, nt beegfs.NodeType) ([]ChunkBalanceNodeStatus, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodeList(ctx, nt)
	if err != nil {
		return nil, err
	}

	statuses := []ChunkBalanceNodeStatus{}
	for _, node := range nodes {
		nodeStatus, err := chunkBalanceStatusForNode(ctx, node, store)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, nodeStatus)
	}
	return statuses, nil
}

func ChunkBalanceStatusForNode(ctx context.Context, id beegfs.EntityId) (ChunkBalanceNodeStatus, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return ChunkBalanceNodeStatus{}, err
	}
	node, err := store.GetNode(id)
	if err != nil {
		return ChunkBalanceNodeStatus{}, err
	}
	return chunkBalanceStatusForNode(ctx, node, store)
}

func chunkBalanceStatusForNode(ctx context.Context, node beegfs.Node, store *beemsg.NodeStore) (ChunkBalanceNodeStatus, error) {
	if node.Id.NodeType != beegfs.Meta && node.Id.NodeType != beegfs.Storage {
		return ChunkBalanceNodeStatus{}, fmt.Errorf("the specified node %q is not a metadata or storage node", node)
	}
	resp := &msg.GetChunkBalanceJobStatsRespMsg{}
	err := store.RequestTCP(ctx, node.Uid, &msg.GetChunkBalanceJobStatsMsg{}, resp)
	if err != nil {
		err = fmt.Errorf("%w (possibly this node is not running %s)", err, msg.GetChunkBalanceJobStatsMsgVersions)
	}
	return ChunkBalanceNodeStatus{
		Node:  node,
		Stats: *resp,
		Err:   err,
	}, nil
}
