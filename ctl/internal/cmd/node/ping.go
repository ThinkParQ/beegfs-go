package node

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/node"
	"go.uber.org/zap"
)

const (
	// ioctl ping times are reported in nanoseconds; human output displays microseconds.
	pingDisplayDivisor = 1000.0
	pingDisplayUnit    = "µs"
)

// Creates new "ping" command
func newPingCmd() *cobra.Command {
	cfg := node.PingConfig{NodeType: beegfs.InvalidNodeType, NodeIDs: []beegfs.EntityId{}}

	cmd := &cobra.Command{
		Use:   "ping [nodeType | [node [node ...]]]",
		Short: "Ping BeeGFS nodes through the client module",
		Long: fmt.Sprintf(`Ping uses a mounted BeeGFS client to ping nodes that are part of the BeeGFS
instance managed by the configured mgmtd.

If no mount point is supplied, the first mount point for the configured BeeGFS
instance is used automatically.

Accepted positional arguments are either a single node type (all nodes of that
type are pinged) or one or multiple individual node aliases or IDs (only nodes
that match are pinged). If no positional arguments are passe, all nodes known
to the configured mgmtd will be pinged.

Optionally, the ping count (-c/--count) for each node and the interval
(-i/--interval) between individual pings can be supplied. In most cases
the defaults (10 pings, 1ms interval) should work fine.

If -p/--parallel is supplied, nodes will be pinged in parallel instead of
sequentially. The global --%s flag will be used to determine how
many pings to do in parallel. `, config.NumWorkersKey),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPingCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVarP(&cfg.Mountpoint, "mount-point", "m", "",
		"Mount point of the client that will be used to ping the node. Optional.\nIf not configured the first mount point found for the configured mgmtd will be used.")
	cmd.Flags().Uint32VarP(&cfg.Count, "count", "c", 10, "How many pings to send.")
	cmd.Flags().DurationVarP(&cfg.Interval, "interval", "i", 1*time.Millisecond, "Time in ms to wait between subsequent pings.")
	cmd.Flags().BoolVarP(&cfg.Parallel, "parallel", "p", false,
		fmt.Sprintf("Ping nodes in parallel. Uses --%s to determine how many parallel pings to do.", config.NumWorkersKey))

	return cmd
}

// pingSample is one ping measurement for a node. Timing is in nanoseconds.
type pingSample struct {
	TimeNs   uint32 `json:"time_ns"`
	Protocol string `json:"protocol"`
}

// pingStats summarizes the timing of a node's successful pings. All values are in nanoseconds.
type pingStats struct {
	AverageNs float64 `json:"average_ns"`
	MedianNs  float64 `json:"median_ns"`
	MinNs     float64 `json:"min_ns"`
	MaxNs     float64 `json:"max_ns"`
}

// pingReport is the structured result of pinging a single node. The same value backs both the
// human-readable output and the --output json representation so the two cannot drift. A node that
// could not be pinged at all carries a non-empty Error and leaves the timing fields zero/empty;
// otherwise Error is empty and Succeeded/Failed/Samples/Stats describe the run.
type pingReport struct {
	Node      string       `json:"node"`
	Succeeded uint32       `json:"succeeded"`
	Failed    uint32       `json:"failed"`
	Samples   []pingSample `json:"samples,omitempty"`
	Stats     *pingStats   `json:"stats,omitempty"`
	Error     string       `json:"error,omitempty"`
}

// newPingReport builds the structured report for a node that was reached and pinged.
func newPingReport(res *node.PingResult) pingReport {
	report := pingReport{
		Node:      res.Node.Id.StringLong(),
		Succeeded: res.OutSuccess,
		Failed:    res.OutErrors,
	}

	// OutSuccess is reported by the kernel; clamp to the fixed-size ping arrays so a bogus count
	// can never index them out of bounds and panic.
	n := min(int(res.OutSuccess), len(res.OutPingTime), len(res.OutPingType))
	if n <= 0 {
		return report
	}

	total := 0.0
	minNs := math.MaxFloat64
	maxNs := 0.0
	times := make([]float64, 0, n)
	for i := range n {
		t := res.OutPingTime[i]
		report.Samples = append(report.Samples, pingSample{TimeNs: t, Protocol: res.OutPingType[i]})
		ft := float64(t)
		minNs = math.Min(minNs, ft)
		maxNs = math.Max(maxNs, ft)
		times = append(times, ft)
		total += ft
	}
	slices.Sort(times)
	report.Stats = &pingStats{
		AverageNs: total / float64(n),
		MedianNs:  times[n/2],
		MinNs:     minNs,
		MaxNs:     maxNs,
	}
	return report
}

// newPingErrorReport builds the structured report for a node that could not be pinged.
func newPingErrorReport(pingErr *node.PingError) pingReport {
	return pingReport{
		Node:  fmt.Sprintf("%v", pingErr.NodeID),
		Error: pingErr.Error(),
	}
}

// printPingReport renders a single node's report in the human-readable format on stdout (a per-node
// result, including a failure, is result content, not an operational error — see STYLE.md §2).
func printPingReport(report pingReport) {
	if report.Error != "" {
		fmt.Printf("Error pinging %s\n", report.Node)
		fmt.Printf("================================================================================\n")
		fmt.Println(report.Error)
		fmt.Printf("Is the node online and reachable by the client?\n\n")
		return
	}

	fmt.Printf("Pinged node %s. Successful: %d, failed: %d.\n", report.Node, report.Succeeded, report.Failed)
	fmt.Printf("================================================================================\n")
	for _, s := range report.Samples {
		t := float64(s.TimeNs)
		fmt.Printf("Ping time:\t%6.2f%s\t(%6.0fns) (protocol: %s)\n", t/pingDisplayDivisor, pingDisplayUnit, t, s.Protocol)
	}
	if report.Stats != nil {
		fmt.Printf("Average: %.2f%s, Median: %.2f%s, Min: %.2f%s, Max: %.2f%s\n\n",
			report.Stats.AverageNs/pingDisplayDivisor, pingDisplayUnit,
			report.Stats.MedianNs/pingDisplayDivisor, pingDisplayUnit,
			report.Stats.MinNs/pingDisplayDivisor, pingDisplayUnit,
			report.Stats.MaxNs/pingDisplayDivisor, pingDisplayUnit)
	} else {
		fmt.Printf("No successful pings; no timing statistics available.\n\n")
	}
}

func runPingCmd(cmd *cobra.Command, cfg node.PingConfig) error {
	log, _ := config.GetLogger()

	if cfg.Count == 0 {
		return fmt.Errorf("ping count can not be zero")
	}

	if cmd.Flags().NArg() > 0 {
		nodeType := beegfs.NodeTypeFromString(cmd.Flags().Arg(0))
		if cmd.Flags().NArg() > 1 || nodeType == beegfs.InvalidNodeType {
			// Multiple arguments were provided or nodeType was not valid so assume nodes.
			idParser := beegfs.NewEntityIdSliceParser(16, beegfs.Management, beegfs.Meta, beegfs.Storage)
			var err error
			cfg.NodeIDs, err = idParser.Parse(strings.Join(cmd.Flags().Args(), ","))
			if err != nil {
				return fmt.Errorf("unable to parse args: %w", err)
			}
			log.Debug("Parsed nodeIDs:", zap.Any("nodeIDs", cfg.NodeIDs))
			if len(cfg.NodeIDs) == 0 {
				return fmt.Errorf("node list empty after parsing")
			}
		} else {
			cfg.NodeType = nodeType
		}
	}

	outputType := config.OutputType(viper.GetString(config.OutputKey))

	results, errs, err := node.PingNodes(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	successful := 0
	failed := 0
	// JSON and JSON-pretty emit a single document once every node has been pinged; NDJSON and human
	// output stream one node at a time as results arrive. Initialized non-nil so a run that pings
	// no nodes still marshals to an empty array `[]` rather than `null`.
	collected := []pingReport{}
	emit := func(report pingReport) error {
		switch outputType {
		case config.OutputJSON, config.OutputJSONPretty:
			collected = append(collected, report)
		case config.OutputNDJSON:
			data, err := json.Marshal(report)
			if err != nil {
				return fmt.Errorf("marshaling ping report: %w", err)
			}
			fmt.Println(string(data))
		default:
			printPingReport(report)
		}
		return nil
	}

	// Drain both channels until each is closed (setting a closed channel to nil disables its select
	// case) so a result buffered at close time is never dropped.
	for results != nil || errs != nil {
		select {
		case res, ok := <-results:
			if !ok {
				log.Debug("Results channel closed")
				results = nil
				continue
			}
			successful++
			if err := emit(newPingReport(res)); err != nil {
				return err
			}
		case pingErr, ok := <-errs:
			if !ok {
				log.Debug("Errors channel closed")
				errs = nil
				continue
			}
			failed++
			if err := emit(newPingErrorReport(pingErr)); err != nil {
				return err
			}
		}
	}

	switch outputType {
	case config.OutputJSON:
		data, err := json.Marshal(collected)
		if err != nil {
			return fmt.Errorf("marshaling ping report: %w", err)
		}
		fmt.Println(string(data))
	case config.OutputJSONPretty:
		data, err := json.MarshalIndent(collected, "", " ")
		if err != nil {
			return fmt.Errorf("marshaling ping report: %w", err)
		}
		fmt.Println(string(data))
	}

	if failed > 0 {
		if successful > 0 {
			return util.NewCtlError(fmt.Errorf("ping failed for at least one node"), util.PartialSuccess)
		} else {
			return fmt.Errorf("ping failed for all nodes")
		}
	}
	return nil
}
