package stats

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
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
		Short: fmt.Sprintf("Show statistics related to background data rebalancing in BeeGFS (%s)", msg.GetChunkBalanceJobStatsMsgVersions),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				id, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
				if err != nil {
					return err
				}
				cfg.Node = id
			} else if len(args) > 1 {
				return errors.New("cannot specify multiple nodes (zero or one node must be specified)")
			}
			return runRebalanceStatsCommand(cmd.Context(), cfg)
		},
		Long: fmt.Sprintf(`Show statistics related to background data rebalancing in BeeGFS (%s).

The 'beegfs entry migrate' command is used to restripe files and migrate file contents between
storage targets. When the rebalancing migration mode is used, a chunk balancer job is created on all
metadata and storage nodes that own files (inodes) and chunks on the source/destination targets. One
or more work items representing each inode to be restriped and chunk to be migrated are added
asynchronously to each balancer job. If the migrate command is run multiple times while the same
balancer job is active, those work items will be added to the existing job.

The status of a chunk balancer job will indicate one of the following states:

* Not Started: When a node first starts and no job has ever been executed.
* Starting: Work was submitted to a node and a new job is starting up.
* Running: Work is actively being executed on a node.
* Idle: All work has been completed, but the job is still active and could accept additional work.
* Interrupted: Node shutdown was requested before the job could complete.
* Failure: The job was unable to spawn new worker thread(s) (terminal state).
* Errors: The job finished but one or more work items did not complete successfully (terminal state).
* Success: The job finished and all work items completed successfully (terminal state). 

The state of a rebalancer job is "Running" while that node has work that is being executed. Once the
node has completed all assigned work the state transitions to "Idle". If additional work is added
before the configured idle timeout (default, meta: 480 seconds; default, storage: 600 seconds) the
state will transition back to "Running". After the idle timeout the state will transition to
"Success" if all work was completed successfully, or "Errors" if any errors occurred. Once the
rebalancer job reaches a terminal state all associated work threads on the metadata and storage
nodes will be shutdown. If additional work is submitted after the rebalancer job reaches a terminal
state it will create a new rebalancer job and reset all cumulative counters.

The following statistics are tracked for each chunk balancer job:

* Start Time: The time the job started.
* End Time: The time the job entered a terminal state.
* Work Queue: The number of work items currently assigned to the node.
* Error Count: Work items that could not be completed successfully (cumulative).
* Locked Inodes: The number of inodes currently locked on a metadata node (not applicable to storage nodes).
* Migrated Chunks: The number of chunks the metadata or storage node was involved in migrating (cumulative).
* Worker Num (hidden): The number of threads available to process work items. The number of threads
  increases automatically (to a fixed limit) based on the size of the work queue.
`, msg.GetChunkBalanceJobStatsMsgVersions),
	}
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"The node type to query (meta, storage).")
	cmd.Flags().DurationVar(&cfg.Interval, "interval", 1*time.Second,
		"Interval to automatically refresh and print updated stats.")
	return cmd
}

func runRebalanceStatsCommand(ctx context.Context, cfg RebalanceStatsCfg) error {

	// Collection errors will be printed in the last unlabled column (i.e., if a node is offline).
	defaultColumns := []string{"status", "start_time", "end_time", "work_queue", "error_count", "locked_inodes", "migrated_chunks", ""}
	defaultwithNode := []string{"id", "type", "alias", "status", "start_time", "end_time", "work_queue", "error_count", "locked_inodes", "migrated_chunks", ""}
	allColumns := []string{"uid", "id", "type", "alias", "status", "start_time", "end_time", "work_queue", "error_count", "locked_inodes", "migrated_chunks", "worker_num", ""}

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

	lockedInodes := "-"
	if status.Node.Id.NodeType == beegfs.Meta {
		lockedInodes = strconv.Itoa(int(status.Stats.LockedInodes))
	}

	startTime := "-"
	if status.Stats.StartTime != 0 {
		startTime = time.Unix(status.Stats.StartTime, 0).Format(time.RFC3339)
	}

	endTime := "-"
	if status.Stats.EndTime != 0 {
		endTime = time.Unix(status.Stats.EndTime, 0).Format(time.RFC3339)
	}

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
			startTime,
			endTime,
			status.Stats.WorkQueue,
			status.Stats.ErrorCount,
			lockedInodes,
			status.Stats.MigratedChunks,
			status.Stats.WorkerNum,
			"",
		)
	}
}
