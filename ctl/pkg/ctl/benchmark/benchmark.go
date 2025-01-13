package benchmark

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

// NewStorageBenchConfig() handles initializing a new StorageBenchConfig. Unfortunately in the C++
// equivalents for some of these fields "0" has meaning instead of indicating "unset" or "invalid".
// This function ensures those fields are initialized  to a value that indicates they are unset.
func NewStorageBenchConfig() StorageBenchConfig {
	return StorageBenchConfig{
		Action: beegfs.BenchUnspecified,
		Type:   beegfs.NoBench,
	}
}

// Used to execute a storage bench action. Ensure to set all fields to the expected values or use
// NewStorageBenchConfig() to set fields where the default value for a particular type has meaning,
// to a value that indicates the field is unset.
type StorageBenchConfig struct {
	// Specify either TargetIDs or StorageNodes to filter the target list for the benchmark action.
	// Omit both to execute the action against all targets.
	TargetIDs    []beegfs.EntityId
	StorageNodes []beegfs.EntityId
	// Controls what storage bench request is issued to the specified targets.
	Action beegfs.StorageBenchAction
	// All remaining fields are only required when the action is BenchStart.
	Type      beegfs.StorageBenchType
	ODirect   bool
	BlockSize int64
	FileSize  int64
	Threads   int32
}

// The result of a storage bench action for a single storage node. Equivalent of
// StorageBenchResponseInfo in the C++ codebase.
type StorageBenchResult struct {
	Node      beegfs.EntityIdSet
	ErrorCode beegfs.StorageBenchError
	Status    beegfs.StorageBenchStatus
	Action    beegfs.StorageBenchAction
	Type      beegfs.StorageBenchType
	// Map of targets to their results.
	TargetResults map[beegfs.EntityIdSet]int64
}

// ExecuteStorageBenchAction is used to interact with the storage bench functionality of storage
// nodes. It takes action based on the provided benchConfig.
func ExecuteStorageBenchAction(ctx context.Context, benchConfig *StorageBenchConfig) ([]StorageBenchResult, error) {

	log, _ := config.GetLogger()
	log.Debug("initializing storage bench mode", zap.Any("benchConfig", benchConfig))

	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, err
	}

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, err
		}
		log.Debug("remote storage target mappings are not available (ignoring)", zap.Any("error", err))
	}

	filteredTargetsByNode, err := filterTargetsByNode(ctx, mappings, benchConfig)
	if err != nil {
		return nil, fmt.Errorf("error filtering target list: %w", err)
	}
	log.Debug("initialized storage bench mode", zap.String("filteredTargetsByNode", fmt.Sprintf("%+v", filteredTargetsByNode)))

	baseRequest := msg.StorageBenchControlMsg{
		Action:    benchConfig.Action,
		Type:      benchConfig.Type,
		BlockSize: benchConfig.BlockSize,
		FileSize:  benchConfig.FileSize,
		Threads:   benchConfig.Threads,
		ODirect:   benchConfig.ODirect,
	}

	results := make([]StorageBenchResult, 0, len(filteredTargetsByNode))
	for node, targets := range filteredTargetsByNode {
		req := baseRequest
		req.TargetIDs = targets
		resp := &msg.StorageBenchControlMsgResp{}
		err = store.RequestTCP(ctx, node.Uid, &req, resp)
		if err != nil {
			return nil, fmt.Errorf("requesting storage bench %s failed on node %s: %w", req.Action, node, err)
		}

		if len(resp.TargetIDs) != len(resp.TargetResults) {
			return nil, fmt.Errorf("the number of targets (%d) and number of target results (%d) do not match for node %s (this is probably a bug on the storage node)", len(resp.TargetIDs), len(resp.TargetResults), node)
		}

		targetResults := make(map[beegfs.EntityIdSet]int64, len(resp.TargetIDs))
		for i, id := range resp.TargetIDs {
			target, err := mappings.TargetToEntityIdSet.Get(beegfs.LegacyId{NumId: beegfs.NumId(id), NodeType: beegfs.Storage})
			if err != nil {
				return nil, fmt.Errorf("unknown target found in results for node %s: %w", node, err)
			}
			targetResults[target] = resp.TargetResults[i]
		}

		results = append(results, StorageBenchResult{
			Node:          node,
			ErrorCode:     resp.ErrorCode,
			Status:        resp.Status,
			Action:        resp.Action,
			Type:          resp.Type,
			TargetResults: targetResults,
		})
	}

	return results, nil
}

// filterTargetsByNode maps storage nodes to their target IDs. If the user specified storage nodes
// or specific targets in the provided benchConfig, only those nodes+targets are returned. If the
// user specifies the same storage node or target node multiple times it filters out the duplicates.
func filterTargetsByNode(ctx context.Context, mappings *util.Mappings, benchConfig *StorageBenchConfig) (map[beegfs.EntityIdSet][]uint16, error) {

	filteredTargetsByNode := map[beegfs.EntityIdSet][]uint16{}

	if len(benchConfig.TargetIDs) > 0 && len(benchConfig.StorageNodes) > 0 {
		return nil, fmt.Errorf("cannot specify both target IDs and storage nodes")
	} else if len(benchConfig.TargetIDs) > 0 {
		// Filter target list by specified storage targets and filter out duplicate target IDs:
		seenTargetIDs := make(map[beegfs.Uid]struct{})
		for _, id := range benchConfig.TargetIDs {
			node, err := mappings.TargetToNode.Get(id)
			if err != nil {
				return nil, fmt.Errorf("unable to map specified target to a storage node: %w", err)
			}
			target, err := mappings.TargetToEntityIdSet.Get(id)
			if err != nil {
				return nil, fmt.Errorf("unable to map specified target entity ID to a storage target: %w", err)
			}
			if _, ok := seenTargetIDs[target.Uid]; ok {
				continue
			}
			seenTargetIDs[target.Uid] = struct{}{}
			filteredTargetsByNode[node] = append(filteredTargetsByNode[node], uint16(target.LegacyId.NumId))
		}
	} else if len(benchConfig.StorageNodes) > 0 {
		// Filter target list by specified storage nodes:
		store, err := config.NodeStore(ctx)
		if err != nil {
			return nil, err
		}
		for _, node := range benchConfig.StorageNodes {
			targets, err := mappings.NodeToTargets.Get(node)
			if err != nil {
				return nil, fmt.Errorf("unable to map storage node to its targets: %w", err)
			}
			u16Targets := make([]uint16, 0, len(targets))
			for _, target := range targets {
				u16Targets = append(u16Targets, uint16(target.LegacyId.NumId))
			}
			node, err := store.GetNode(node)
			if err != nil {
				return nil, fmt.Errorf("unable to retrieve storage node from the node store: %w", err)
			}
			filteredTargetsByNode[beegfs.EntityIdSet{
				Uid: node.Uid,
				LegacyId: beegfs.LegacyId{
					NumId:    node.Id.NumId,
					NodeType: node.Id.NodeType,
				},
				Alias: node.Alias,
			}] = u16Targets
		}
	} else {
		// No filtering, return all targets:
		allTargets, err := target.GetTargets(ctx)
		if err != nil {
			return nil, err
		}
		for _, target := range allTargets {
			if target.NodeType == beegfs.Storage {
				filteredTargetsByNode[target.Node] = append(filteredTargetsByNode[target.Node], uint16(target.Target.LegacyId.NumId))
			}
		}
	}

	return filteredTargetsByNode, nil
}
