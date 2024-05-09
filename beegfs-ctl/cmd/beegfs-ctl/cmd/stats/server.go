package stats

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/stats"
	"github.com/thinkparq/gobee/beegfs"
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
		Use:   "server",
		Short: "Show IO statistics for BeeGFS servers.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				id, err := beegfs.NewNodeParser().Parse(args[0])
				if err != nil {
					return err
				}
				cfg.Node = id
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

  When stats are requested for a single server, the output displays a history of the
  last few seconds in rows, with the most recent values at the bottom.

  When stats are requested for multiple servers, individual statistics for each 
  server are displayed, with one row per server.

Example: Print individual stats of all servers, refresh every second.
  $ beegfs stats server --interval 1s

Example: Print aggregate stats of metadata servers, refresh every 3 seconds.
  $ beegfs stats server --node-type meta --interval 3s
`,
	}

	cmd.Flags().DurationVar(&cfg.History, "history", 10*time.Second,
		"Include historical stats for this duration.")
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"The node type to query (meta, storage).")
	cmd.Flags().DurationVar(&cfg.Interval, "interval", 0*time.Second,
		"Interval to automatically refresh and print updated stats.")
	cmd.Flags().BoolVar(&cfg.Sum, "sum", false,
		"Summarized stats for multiple nodes.")
	return cmd
}

func runServerstatsCmd(cmd *cobra.Command, cfg *serverStats_Config) error {
	// incase if the interval is given we loop here until the user presses ctrl + c
	for {
		w := cmdfmt.NewTableWriter(os.Stdout)

		var err error
		if _, ok := cfg.Node.(beegfs.InvalidEntityId); ok {
			if cfg.Sum {
				err = multiNodeAggregated(cmd.Context(), cfg, &w)
			} else {
				err = multiNode(cmd.Context(), cfg, &w)
			}
		} else {
			err = singleNode(cmd.Context(), cfg, &w)
		}

		if err != nil {
			return err
		}

		w.Flush()

		if cfg.Interval <= 0 {
			break
		}

		time.Sleep(cfg.Interval)
	}
	return nil
}

// Queries and prints the stats for one node
func singleNode(ctx context.Context, cfg *serverStats_Config, w *tabwriter.Writer) error {
	stats, err := stats.SingleServerNode(ctx, cfg.Node)
	if err != nil {
		return err
	}

	// Only show the latest entries from the user specified history length
	numToKeep := len(stats) - int(cfg.History.Seconds())
	if numToKeep > 0 {
		stats = stats[numToKeep:]
	}

	printHeader(w, false, false)
	for _, stat := range stats {
		printData(w, stat)
	}
	fmt.Fprintf(w, "\n")

	return nil
}

// Queries and prints latest stat entry for multiple nodes separately
func multiNode(ctx context.Context, cfg *serverStats_Config, w *tabwriter.Writer) error {
	perServerstatsResult, err := stats.MultiServerNodes(ctx, cfg.NodeType)
	if err != nil {
		return err
	}

	printHeader(w, true, config.Get().Debug)
	for _, serverStats := range perServerstatsResult {
		cmdfmt.PrintNodeInfoRow(w, serverStats.Node, config.Get().Debug)
		printData(w, serverStats.Stats)
	}

	return nil
}

// Queries, sums up and prints the summarized stats for multiple nodes
func multiNodeAggregated(ctx context.Context, cfg *serverStats_Config, w *tabwriter.Writer) error {
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

	fmt.Fprintf(w, "Total results for nodes: %d\n", numberOfNodes)
	printHeader(w, false, false)
	for _, stat := range totalStats {
		printData(w, stat)
	}
	fmt.Fprintf(w, "\n")

	return nil
}

// Prints the tables header
func printHeader(w *tabwriter.Writer, includeNodeInfo bool, includeUid bool) {
	if includeNodeInfo {
		cmdfmt.PrintNodeInfoHeader(w, includeUid)
	}

	fmt.Fprintf(w, "Time\tQueue length\tRequests\tBusy workers\tWritten\tRead\tSent\tReceived\t\n")
}

// Prints one line of stat entry
func printData(w *tabwriter.Writer, stat stats.Stats) {
	fmt.Fprintf(w, "%d\t", stat.StatsTime)
	fmt.Fprintf(w, "%d\t", stat.QueuedRequests)
	fmt.Fprintf(w, "%d\t", stat.WorkRequests)
	fmt.Fprintf(w, "%d\t", stat.BusyWorkers)

	if config.Get().Raw {
		fmt.Fprintf(w, "%d\t", stat.DiskWriteBytes)
		fmt.Fprintf(w, "%d\t", stat.DiskReadBytes)
		fmt.Fprintf(w, "%d\t", stat.NetSendBytes)
		fmt.Fprintf(w, "%d\t", stat.NetRecvBytes)
	} else {
		fmt.Fprintf(w, "%sB\t", unitconv.FormatPrefix(float64(stat.DiskWriteBytes), unitconv.IEC, 0))
		fmt.Fprintf(w, "%sB\t", unitconv.FormatPrefix(float64(stat.DiskReadBytes), unitconv.IEC, 0))
		fmt.Fprintf(w, "%sB\t", unitconv.FormatPrefix(float64(stat.NetSendBytes), unitconv.IEC, 0))
		fmt.Fprintf(w, "%sB\t", unitconv.FormatPrefix(float64(stat.NetRecvBytes), unitconv.IEC, 0))
	}

	fmt.Fprintf(w, "\n")
}
