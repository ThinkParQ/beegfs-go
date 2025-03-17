package stats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
)

type serverStats_Config struct {
	Node     beegfs.EntityId
	History  time.Duration
	NodeType beegfs.NodeType
	Interval time.Duration
	Sum      bool
}

func newServerStatsCmd() *cobra.Command {
	cfg := serverStats_Config{Node: beegfs.InvalidEntityId{}}

	var cmd = &cobra.Command{
		Use:   "server [<node>]",
		Short: "Show IO statistics for BeeGFS servers",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				id, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
				if err != nil {
					return err
				}
				cfg.Node = id
				if cfg.Sum {
					return errors.New("cannot summarize results for a single node")
				}
			} else if len(args) > 1 {
				return errors.New("cannot specify multiple nodes (zero or one node must be specified)")
			}

			return runServerstatsCmd(cmd, &cfg)
		},
		Long: `Show IO statistics for BeeGFS servers.
  This command shows the number of network requests that were processed per second
  by the servers, the number of requests currently pending in the queue and the
  number of worker threads that are currently busy processing requests at the
  time of measurement on the servers, and the amount of read/written data per
  second for storage servers. The time field shows Unix timestamps in seconds since
  the epoch.

  By default prints out stats for all server types refreshing every 1s until Ctrl+C.

  When stats are requested for a single server by specifying <node>, the output displays 
  a history of the last few seconds in rows, with the most recent values at the bottom.

  When stats are requested for multiple servers (by omitting <node>), individual 
  statistics for each server are displayed, with one row per server.

Example: Print individual stats of all servers, refreshed every second.
  $ beegfs stats server --interval 1s

Example: Print aggregate stats of metadata servers, refreshed every 3 seconds.
  $ beegfs stats server --node-type meta --interval 3s --sum

Example: Print stats for a single metadata server:
  $ beegfs stats server meta:1
`,
	}

	cmd.Flags().DurationVar(&cfg.History, "history", 10*time.Second,
		"Include historical stats for this duration.")
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"The node type to query (meta, storage).")
	cmd.Flags().DurationVar(&cfg.Interval, "interval", 1*time.Second,
		"Interval to automatically refresh and print updated stats.")
	cmd.Flags().BoolVar(&cfg.Sum, "sum", false,
		"Summarized stats for multiple nodes.")
	return cmd
}

func runServerstatsCmd(cmd *cobra.Command, cfg *serverStats_Config) error {

	defaultColumns := []string{"time", "queue_length", "requests", "busy_workers", "written", "read", "sent", "received"}
	defaultWithNode := []string{"node id", "alias", "queue_length", "requests", "busy_workers", "written", "read", "sent", "received"}
	allColumns := []string{"time", "uid", "node_id", "alias", "queue_length", "requests", "busy_workers", "written", "read", "sent", "received"}

	var collectStatsFunc func(context.Context, *serverStats_Config, *cmdfmt.Printomatic) error
	if _, ok := cfg.Node.(beegfs.InvalidEntityId); ok {
		if cfg.Sum {
			collectStatsFunc = multiNodeAggregated
		} else {
			defaultColumns = defaultWithNode
			collectStatsFunc = multiNode
		}
	} else {
		collectStatsFunc = singleNode
	}

	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	}

	tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)

	// incase if the interval is given we loop here until the user presses ctrl + c
	for {
		if err := collectStatsFunc(cmd.Context(), cfg, &tbl); err != nil {
			return err
		}
		// Constantly print output as it is available:
		tbl.PrintRemaining()
		if cfg.Interval <= 0 {
			break
		}

		select {
		case <-time.After(cfg.Interval):
			continue
		case <-cmd.Context().Done():
			return nil
		}
	}
	return nil
}

// Queries and prints the stats for one node
func singleNode(ctx context.Context, cfg *serverStats_Config, w *cmdfmt.Printomatic) error {
	node, stats, err := stats.SingleServerNode(ctx, cfg.Node)
	if err != nil {
		return err
	}

	// Only show the latest entries from the user specified history length
	numToKeep := len(stats) - int(cfg.History.Seconds())
	if numToKeep > 0 {
		stats = stats[numToKeep:]
	}

	for _, stat := range stats {
		printData(w, &node, stat)
	}

	return nil
}

// Queries and prints latest stat entry for multiple nodes separately
func multiNode(ctx context.Context, cfg *serverStats_Config, w *cmdfmt.Printomatic) error {
	perServerstatsResult, err := stats.MultiServerNodes(ctx, cfg.NodeType)
	if err != nil {
		return err
	}

	slices.SortFunc(perServerstatsResult, func(a, b stats.NodeStats) int {
		if a.Node.Id.NodeType == b.Node.Id.NodeType {
			return int(a.Node.Id.NumId - b.Node.Id.NumId)
		}
		return int(a.Node.Id.NodeType - b.Node.Id.NodeType)
	})

	for _, serverStats := range perServerstatsResult {
		printData(w, &serverStats.Node, serverStats.Stats)
	}

	return nil
}

// Queries, sums up and prints the summarized stats for multiple nodes
func multiNodeAggregated(ctx context.Context, cfg *serverStats_Config, w *cmdfmt.Printomatic) error {
	totalStats, numberOfNodes, err := stats.MultiServerNodesAggregated(ctx, cfg.NodeType)
	if err != nil {
		return err
	}

	// Only show the latest entries from the user specified history length
	l := len(totalStats) - int(cfg.History.Seconds())
	if l < 0 {
		l = 0
	}
	totalStats = totalStats[l:]

	fmt.Printf("Total results for nodes: %d\n", numberOfNodes)
	for _, stat := range totalStats {
		printData(w, nil, stat)
	}

	return nil
}

// Prints one line of stat entry
func printData(w *cmdfmt.Printomatic, node *beegfs.Node, stat stats.Stats) {

	uid := "n/a"
	id := "n/a"
	alias := "n/a"

	if node != nil {
		uid = fmt.Sprintf("%v", node.Uid)
		id = fmt.Sprintf("%v", node.Id)
		alias = fmt.Sprintf("%v", node.Alias)
	}
	queuedReqs := fmt.Sprintf("%d", stat.QueuedRequests)
	workReqs := fmt.Sprintf("%d", stat.WorkRequests)
	busyWorkers := fmt.Sprintf("%d", stat.BusyWorkers)

	if viper.GetBool(config.RawKey) {
		w.AddItem(
			stat.StatsTime,
			uid,
			id,
			alias,
			queuedReqs,
			workReqs,
			busyWorkers,
			fmt.Sprintf("%d", stat.DiskWriteBytes),
			fmt.Sprintf("%d", stat.DiskReadBytes),
			fmt.Sprintf("%d", stat.NetSendBytes),
			fmt.Sprintf("%d", stat.NetRecvBytes),
		)
	} else {
		var statsTime string
		if stat.StatsTime > math.MaxInt64 {
			// If the timestamp is out-of-range just print it as is.
			statsTime = fmt.Sprintf("%d", stat.StatsTime)
		} else {
			statsTime = time.Unix(int64(stat.StatsTime), 0).Format(time.RFC3339)
		}
		w.AddItem(
			statsTime,
			uid,
			id,
			alias,
			queuedReqs,
			workReqs,
			busyWorkers,
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(stat.DiskWriteBytes), unitconv.IEC, 0)),
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(stat.DiskReadBytes), unitconv.IEC, 0)),
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(stat.NetSendBytes), unitconv.IEC, 0)),
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(stat.NetRecvBytes), unitconv.IEC, 0)),
		)
	}
}
