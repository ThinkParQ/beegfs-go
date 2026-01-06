package stats

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
)

type RebalanceStatsCfg struct {
	Node     beegfs.EntityId
	NodeType beegfs.NodeType
	Interval time.Duration
}

func newRebalancingStatsCmd() *cobra.Command {
	cfg := RebalanceStatsCfg{Node: beegfs.InvalidEntityId{}}

	var cmd = &cobra.Command{
		Use:   "rebalance [<node>]",
		Short: "Show rebalancing statistics for BeeGFS servers",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				id, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
				if err != nil {
					return err
				}
				cfg.Node = id
				// TODO: Do we want to support this?
				// if cfg.Sum {
				// 	return errors.New("cannot summarize results for a single node")
				// }
			} else if len(args) > 1 {
				return errors.New("cannot specify multiple nodes (zero or one node must be specified)")
			}
			return runRebalanceStatsCommand(cmd.Context(), cfg)
		},
		// TODO: Pending clarification/final naming of the different stats, states, etc.
		Long: "",
	}
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"The node type to query (meta, storage).")
	cmd.Flags().DurationVar(&cfg.Interval, "interval", 1*time.Second,
		"Interval to automatically refresh and print updated stats.")
	return cmd
}

func runRebalanceStatsCommand(ctx context.Context, cfg RebalanceStatsCfg) error {

	// Collection errors will be printed in the last unlabled column (i.e., if a node is offline).
	defaultColumns := []string{"status", "start_time", "end_time", "queued", "error_count", "locked_inodes", "migrated_chunks", "active_workers", ""}
	defaultwithNode := []string{"id", "type", "alias", "status", "start_time", "end_time", "queued", "error_count", "locked_inodes", "migrated_chunks", "active_workers", ""}
	allColumns := []string{"uid", "id", "type", "alias", "status", "start_time", "end_time", "queued", "error_count", "locked_inodes", "migrated_chunks", "active_workers", ""}

	var collectStatsFunc func(context.Context, RebalanceStatsCfg, *cmdfmt.Printomatic) error
	if _, ok := cfg.Node.(beegfs.InvalidEntityId); ok {
		defaultColumns = defaultwithNode
		collectStatsFunc = cbStatsMultiNode
	} else {
		collectStatsFunc = cbStatsSingleNode
	}

	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	}

	tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)

	for {
		if err := collectStatsFunc(ctx, cfg, &tbl); err != nil {
			return err
		}
		tbl.PrintRemaining()
		if cfg.Interval <= 0 {
			break
		}
		select {
		case <-time.After(cfg.Interval):
			continue
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func cbStatsSingleNode(ctx context.Context, cfg RebalanceStatsCfg, tbl *cmdfmt.Printomatic) error {
	status, err := stats.ChunkBalanceStatusForNode(ctx, cfg.Node)
	if err != nil {
		return err
	}
	printCBData(tbl, status)
	return nil
}

func cbStatsMultiNode(ctx context.Context, cfg RebalanceStatsCfg, tbl *cmdfmt.Printomatic) error {

	statuses, err := stats.ChunkBalanceStatusForNodes(ctx, cfg.NodeType)
	if err != nil {
		return err
	}

	// Same sorting as node list.
	slices.SortFunc(statuses, func(a, b stats.ChunkBalanceNodeStatus) int {
		if a.Node.Id.NodeType == b.Node.Id.NodeType {
			return int(a.Node.Id.NumId - b.Node.Id.NumId)
		} else {
			return int(a.Node.Id.NodeType - b.Node.Id.NodeType)
		}
	})

	for _, status := range statuses {
		printCBData(tbl, status)
	}

	return nil
}

func printCBData(tbl *cmdfmt.Printomatic, status stats.ChunkBalanceNodeStatus) {
	if status.Err != nil {
		tbl.AddItem(
			status.Node.Uid,
			status.Node.Id,
			status.Node.Id.NodeType,
			status.Node.Alias,
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			status.Err,
		)
	} else {
		tbl.AddItem(
			status.Node.Uid,
			status.Node.Id,
			status.Node.Id.NodeType,
			status.Node.Alias,
			status.Stats.Status,
			status.Stats.StartTime,
			status.Stats.EndTime,
			status.Stats.ItemsInQueue,
			status.Stats.ErrorCount,
			status.Stats.LockedInodes,
			status.Stats.MigratedChunks,
			status.Stats.ActiveWorkerNum,
			"",
		)
	}
}
