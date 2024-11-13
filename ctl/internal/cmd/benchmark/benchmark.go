package benchmark

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/benchmark"
)

type frontendCfg struct {
	refreshInterval time.Duration
	unadornedTable  bool
	wait            bool
	watch           bool
	verbose         bool
}

func NewBenchmarkCmd() *cobra.Command {
	frontendCfg := frontendCfg{}
	backendCfg := benchmark.NewStorageBenchConfig()
	cmd := &cobra.Command{
		Use:     "benchmark",
		Aliases: []string{"bench"},
		Short:   "Run, manage, and query file system benchmarks",
		Long: `Run, manage, and query file system benchmarks.
By default operations are performed against all nodes and targets. Optionally a subset of nodes or targets can be specified instead.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("watch") {
				frontendCfg.watch = true
			} else if cmd.Flags().Changed("wait") {
				frontendCfg.wait = true
			}
			if len(args) != 0 {
				return fmt.Errorf("this mode does not support positional argument (HINT: nodes and targets should be separated by commas not spaces)")
			}
			return nil
		},
	}
	// Common flags that apply to all benchmark sub-commands are defined on the parent benchmark
	// command and provided to sub-commands as a FlagSet. Individual commands must then add the
	// parent flags in addition to any command-specific flags. These could also be defined here as
	// "PersistentFlags" but then they end up buried in the list of global flags instead of being
	// listed under the flags for the specific command.
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.StorageNodes, 32, beegfs.Storage), "nodes", "Only interact with the specified storage nodes.")
	cmd.Flags().Var(beegfs.NewEntityIdSlicePFlag(&backendCfg.TargetIDs, 16, beegfs.Storage), "targets", "Only interact with the specified storage targets.")
	cmd.MarkFlagsMutuallyExclusive("nodes", "targets")
	cmd.Flags().DurationVar(&frontendCfg.refreshInterval, "wait", 0, "Wait and periodically poll for status updates in the background until Ctrl+C or the benchmark completes.")
	cmd.Flags().DurationVarP(&frontendCfg.refreshInterval, "watch", "w", 0, "Periodically poll and print the current status and results until Ctrl+C or the benchmark completes.")
	cmd.MarkFlagsMutuallyExclusive("wait", "watch")
	cmd.Flags().BoolVar(&frontendCfg.unadornedTable, "unadorned", false, "Print output as simple tables with no borders.")
	cmd.Flags().BoolVarP(&frontendCfg.verbose, "verbose", "v", false, "Print results for individual targets.")
	cmd.AddCommand(
		newStartCmd(&frontendCfg, &backendCfg, cmd.Flags()),
		newStatusCmd(&frontendCfg, &backendCfg, cmd.Flags()),
		newStopCmd(&frontendCfg, &backendCfg, cmd.Flags()),
		newCleanupCmd(&frontendCfg, &backendCfg, cmd.Flags()))
	return cmd
}

func newStartCmd(frontendCfg *frontendCfg, backendCfg *benchmark.StorageBenchConfig, parentFlags *pflag.FlagSet) *cobra.Command {
	readBench := false
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a benchmark on one or more storage targets.",
		Long: `Start a benchmark on one or more storage targets.

Storage nodes will run I/O directly to their target(s) without any data transfer to or from client nodes.
This allows storage performance to be measured independent of network performance.

Note benchmark files will not be deleted automatically. Use the "cleanup" mode to delete the files after benchmarking is complete.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print("Starting storage benchmark...")
			backendCfg.Action = beegfs.BenchStart
			backendCfg.Type = beegfs.WriteBench
			if readBench {
				backendCfg.Type = beegfs.ReadBench
			}
			return storageBenchDispatcher(cmd.Context(), frontendCfg, backendCfg)
		},
	}
	cmd.Flags().AddFlagSet(parentFlags)
	util.I64BytesVar(cmd.Flags(), &backendCfg.FileSize, "size", "s", "1GiB", "The total amount of data to read/write per thread to each benchmark file, specified with an IEC or SI suffix (e.g, 1GiB). Omit the suffix for bytes.")
	util.I64BytesVar(cmd.Flags(), &backendCfg.BlockSize, "block-size", "b", "128k", "I/O request size used by the benchmark with an IEC or SI suffix (e.g, 1MiB). Omit the suffix for bytes.")
	cmd.Flags().Int32VarP(&backendCfg.Threads, "num-tasks", "n", 1, "The number of client streams to simulate per target for the benchmark. Each task will read or write from a separate benchmark file.")
	cmd.Flags().BoolVarP(&backendCfg.ODirect, "odirect", "D", false, "Use direct I/O (O_DIRECT) on the storage nodes.")
	cmd.Flags().BoolVarP(&readBench, "read", "r", false, "By default a write benchmark is performed. Set to perform a read benchmark instead. \n\tNote: Requires first running a write benchmark with at least as many tasks and the same/larger file size as the read benchmark.")
	return cmd
}

func newStatusCmd(frontendCfg *frontendCfg, backendCfg *benchmark.StorageBenchConfig, parentFlags *pflag.FlagSet) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the status and results from a benchmark.",
		Long:  `Print the status and results from the current/last benchmark for the specified storage targets.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.Action = beegfs.BenchStatus
			return storageBenchDispatcher(cmd.Context(), frontendCfg, backendCfg)
		},
	}
	cmd.Flags().AddFlagSet(parentFlags)
	return cmd
}

func newStopCmd(frontendCfg *frontendCfg, backendCfg *benchmark.StorageBenchConfig, parentFlags *pflag.FlagSet) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop a running benchmark.",
		Long:  `Stop a running benchmark on the specified storage targets.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print("Stopping storage benchmark, this may take a few moments...")
			backendCfg.Action = beegfs.BenchStop
			return storageBenchDispatcher(cmd.Context(), frontendCfg, backendCfg)
		},
	}
	cmd.Flags().AddFlagSet(parentFlags)
	return cmd
}

func newCleanupCmd(frontendCfg *frontendCfg, backendCfg *benchmark.StorageBenchConfig, parentFlags *pflag.FlagSet) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup the benchmark files from the storage targets.",
		Long:  `Cleanup (delete) the benchmark files from the specified storage targets.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print("Cleaning up storage benchmark folder on each target...")
			backendCfg.Action = beegfs.BenchCleanup
			return storageBenchDispatcher(cmd.Context(), frontendCfg, backendCfg)
		},
	}
	cmd.Flags().AddFlagSet(parentFlags)
	return cmd
}

func storageBenchDispatcher(ctx context.Context, frontendCfg *frontendCfg, backendCfg *benchmark.StorageBenchConfig) error {
	results, err := benchmark.ExecuteStorageBenchAction(ctx, backendCfg)
	if err != nil {
		return err
	}

	// For all actions if we were not requested to wait for or watch the results return now.
	if !frontendCfg.wait && !frontendCfg.watch {
		if backendCfg.Action == beegfs.BenchStatus {
			// The status action should always print status.
			printStorageBenchResults(results, *frontendCfg)
		} else {
			fmt.Println(" use 'status' to check the results (HINT: Use --wait or --watch to automatically poll for updates).")
		}
		return nil
	} else {
		fmt.Println("  waiting for the request to complete (use Ctrl+C to stop waiting and use 'status' instead to check on the request).")
	}

	// Otherwise update the action to collect the bench status.
	backendCfg.Action = beegfs.BenchStatus
	ticker := time.NewTicker(frontendCfg.refreshInterval)
	defer ticker.Stop()
	for {
		results, err = benchmark.ExecuteStorageBenchAction(ctx, backendCfg)
		if err != nil {
			return err
		}
		if frontendCfg.watch {
			err := refreshScreenAndPrintResults(results, *frontendCfg)
			if err != nil {
				return err
			}
		}
		if !hasActiveBenchmark(results) {
			if frontendCfg.wait {
				printStorageBenchResults(results, *frontendCfg)
			}
			util.TerminalAlert()
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}

// hasActiveBenchmark() returns true if benchmarks are still active, or false if all benchmarks are
// complete or no benchmark results were provided.
func hasActiveBenchmark(results []benchmark.StorageBenchResult) bool {
	for _, result := range results {
		if result.Status == beegfs.BenchRunning ||
			result.Status == beegfs.BenchFinishing ||
			result.Status == beegfs.BenchStopping {
			return true
		}
	}
	return false
}

// hasBenchmarkError returns true if any benchmark had an error, false if all benchmarks have no
// error or no benchmark results were provided.
func hasBenchmarkError(results []benchmark.StorageBenchResult) bool {
	for _, result := range results {
		if result.ErrorCode != beegfs.BenchErr_NoError {
			return true
		}
	}
	return false
}

func refreshScreenAndPrintResults(results []benchmark.StorageBenchResult, frontendCfg frontendCfg) error {
	t := util.TermRefresher{}
	err := t.StartRefresh()
	if err != nil {
		return err
	}
	printStorageBenchResults(results, frontendCfg)
	err = t.FinishRefresh(util.WithTermFooter(fmt.Sprintf("Refreshing every %s until Ctrl+C or the benchmark completes (last refresh: %s).", frontendCfg.refreshInterval, time.Now().Format(time.TimeOnly))))
	if err != nil {
		return err
	}
	return nil
}

func printStorageBenchResults(results []benchmark.StorageBenchResult, frontendCfg frontendCfg) {
	t := table.NewWriter()

	if frontendCfg.unadornedTable {
		t.SetStyle(table.Style{
			Box: table.BoxStyle{
				PaddingRight:  "  ",
				PageSeparator: "\n",
			},
			Format: table.FormatOptions{
				Header: text.FormatUpper,
			},
		})
	}
	t.Style().Title.Align = text.AlignCenter

	printOverallStatus(t, results)
	t.ResetHeaders()
	t.ResetRows()

	if hasBenchmarkError(results) || viper.GetBool(config.DebugKey) {
		printStatusByNode(t, results)
		t.ResetHeaders()
		t.ResetRows()
	}

	printResultsSummary(t, results)
	t.ResetHeaders()
	t.ResetRows()

	if frontendCfg.verbose {
		printResultsByTarget(t, results)
		t.ResetHeaders()
		t.ResetRows()
	}
}

func printOverallStatus(t table.Writer, results []benchmark.StorageBenchResult) {
	statusResults := benchStatusResults{}
	for _, result := range results {
		statusResults.append(result.Status)
	}

	t.SetTitle("Overall Benchmark Status")
	t.AppendHeader(table.Row{
		"Uninitialized",
		"Initialized",
		"Error",
		"Running",
		"Stopping",
		"Stopped",
		"Finishing",
		"Finished",
		"Unknown",
	})
	statusSummary := statusResults.summarize()
	t.AppendRow(table.Row{
		statusSummary.NotInitialized,
		statusSummary.Initialized,
		statusSummary.Err,
		statusSummary.Running,
		statusSummary.Stopping,
		statusSummary.Stopped,
		statusSummary.Finishing,
		statusSummary.Finished,
		statusSummary.Unknown,
	})
	fmt.Println(t.Render())
}

func printStatusByNode(t table.Writer, results []benchmark.StorageBenchResult) {
	t.SetTitle("Benchmark Status by Node")
	t.AppendHeader(table.Row{
		"node ID",
		"node alias",
		"status",
		"action",
		"error code",
	})
	t.SortBy([]table.SortBy{
		{Name: "node ID", Mode: table.Asc},
	})
	for _, result := range results {
		t.AppendRow(table.Row{
			result.Node.LegacyId,
			result.Node.Alias,
			result.Status,
			result.Action,
			result.ErrorCode,
		})
	}
	fmt.Println(t.Render())
}

func printResultsSummary(t table.Writer, results []benchmark.StorageBenchResult) {
	perfResults := benchPerfResults{}
	// The length of the benchType map must be zero so we can use it to determine if there is more
	// than one test type in the results.
	benchType := make(map[beegfs.StorageBenchType]beegfs.StorageBenchType, 0)
	for _, result := range results {
		for target, perfResult := range result.TargetResults {
			perfResults.append(result.Node, target, perfResult)
		}
		benchType[result.Type] = result.Type
	}

	// Refuse to summarize results for mixed test types.
	testType := "unknown"
	if len(benchType) == 1 {
		testType = strings.ToUpper(benchType[0].String())
	} else if len(benchType) > 1 {
		fmt.Println("WARNING: Unable to summarize results - found results for both read and write benchmarks.")
		return
	} else {
		fmt.Println("WARNING: Unable to summarize results - no results found.")
		return
	}

	perfSummary := perfResults.summarize()
	t.SetTitle("%s Test Summary", testType)
	t.AppendHeader(table.Row{"metric", "bandwidth", "target ID", "target alias", "on node"})
	t.AppendRows([]table.Row{
		{"Minimum", normalizeStorageBenchResults(perfSummary.SlowestTargetThroughput), perfSummary.SlowestTarget.LegacyId, perfSummary.SlowestTarget.Alias, perfSummary.SlowestTargetNode.Alias},
		{"Maximum", normalizeStorageBenchResults(perfSummary.FastestTargetThroughput), perfSummary.FastestTarget.LegacyId, perfSummary.FastestTarget.Alias, perfSummary.FastestTargetNode.Alias},
		{"Average", normalizeStorageBenchResults(perfSummary.AverageThroughput), "-", "-"},
		{"Aggregate", normalizeStorageBenchResults(perfSummary.AggregateThroughput), "-", "-"},
	})
	fmt.Println(t.Render())
}

func printResultsByTarget(t table.Writer, results []benchmark.StorageBenchResult) {
	t.SetTitle("Benchmark Results by Target")
	t.AppendHeader(table.Row{"type", "throughput", "target ID", "target alias", "on node"})
	t.SortBy([]table.SortBy{
		{Name: "target ID", Mode: table.Asc},
	})

	for _, result := range results {
		for target, throughput := range result.TargetResults {
			t.AppendRow(table.Row{
				result.Type,
				normalizeStorageBenchResults(int(throughput)),
				target.LegacyId,
				target.Alias,
				result.Node.Alias,
			})
		}
	}
	fmt.Println(t.Render())
}

// normalizeStorageBenchResults() normalizes the raw storage bench throughput results from the
// storage nodes into MiB/s, GiB/s, etc. depending on the results. It works by converting the raw
// results to bytes, then scaling that to the most optimal unit for readability. For example
// 1,073,741,824 bytes/s would become 1GiB/s.
//
// If for some reason the units cannot be normalized OR if the debug flag is set, it returns the
// original unmodified results.
func normalizeStorageBenchResults(throughput int) string {
	parsedThroughput := "<invalid>"
	// IMPORTANT: "kiB"  must match the iecPrefixes in the utils for use with util.ParseIntFromStr().
	raw, err := util.ParseIntFromStr(fmt.Sprintf("%d%s", throughput, "kiB"))
	if err != nil || viper.GetBool(config.RawKey) {
		// Parsing errors should never happen unless a future change introduces a bug. If this
		// happens just return results as they were originally.
		parsedThroughput = fmt.Sprintf("%d%s", throughput, "kiB/s")
	} else {
		parsedThroughput = fmt.Sprintf("%sB/s", strings.TrimRight(unitconv.FormatPrefix(float64(raw), unitconv.IEC, 0), " "))
	}
	return parsedThroughput
}

// benchStatusSummary contains the number of nodes reporting each benchmark status.
type benchStatusSummary struct {
	NotInitialized int
	Initialized    int
	Err            int
	Running        int
	Stopping       int
	Stopped        int
	Finishing      int
	Finished       int
	Unknown        int
}

// benchStatusResults is used to aggregate the results from one or more targets so the statuses can
// be summarized into a benchStatusSummary.
type benchStatusResults struct {
	summary benchStatusSummary
}

// append the provided status to the benchStatusSummary.
func (s *benchStatusResults) append(status beegfs.StorageBenchStatus) {
	switch status {
	case beegfs.BenchUninitialized:
		s.summary.NotInitialized++
	case beegfs.BenchInitialized:
		s.summary.Initialized++
	case beegfs.BenchError:
		s.summary.Err++
	case beegfs.BenchRunning:
		s.summary.Running++
	case beegfs.BenchStopping:
		s.summary.Stopping++
	case beegfs.BenchStopped:
		s.summary.Stopped++
	case beegfs.BenchFinishing:
		s.summary.Finishing++
	case beegfs.BenchFinished:
		s.summary.Finished++
	default:
		s.summary.Unknown++
	}
}

// summarize returns the benchStatusSummary.
func (s *benchStatusResults) summarize() benchStatusSummary {
	return s.summary
}

type benchPerfSummary struct {
	FastestTarget           beegfs.EntityIdSet
	FastestTargetNode       beegfs.EntityIdSet
	FastestTargetThroughput int
	SlowestTarget           beegfs.EntityIdSet
	SlowestTargetNode       beegfs.EntityIdSet
	SlowestTargetThroughput int
	AggregateThroughput     int
	AverageThroughput       int
}

// benchPerfResults is used to aggregate benchmark results from one or more targets so they can be
// summarized into a benchPerfSummary.
type benchPerfResults struct {
	summary            benchPerfSummary
	initializedFastest bool
	initializedSlowest bool
	targetsSeen        int
}

// append recalculates the benchPerfSummary with the results from the provided target.
func (s *benchPerfResults) append(node beegfs.EntityIdSet, target beegfs.EntityIdSet, result int64) {
	s.targetsSeen++
	if !s.initializedFastest || int(result) > s.summary.FastestTargetThroughput {
		s.initializedFastest = true
		s.summary.FastestTarget = target
		s.summary.FastestTargetThroughput = int(result)
		s.summary.FastestTargetNode = node
	}
	if !s.initializedSlowest || int(result) < s.summary.SlowestTargetThroughput {
		s.initializedSlowest = true
		s.summary.SlowestTarget = target
		s.summary.SlowestTargetThroughput = int(result)
		s.summary.SlowestTargetNode = node
	}
	s.summary.AggregateThroughput += int(result)
	s.summary.AverageThroughput = int(s.summary.AggregateThroughput) / s.targetsSeen
}

// summarize returns the benchPerfSummary for all appended targets.
func (s *benchPerfResults) summarize() benchPerfSummary {
	return s.summary
}
