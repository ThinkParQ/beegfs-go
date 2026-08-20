package iotest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"golang.org/x/term"
)

// Tool is implemented by each iotest workload. The framework handles SSH
// fan-out, coordination files, and the start/status/stop lifecycle.
// Tools only need to run work and respect the stop signal.
type Tool interface {
	Name() string
	Run(ctx context.Context, cfg RunConfig) error
}

// FlagProvider is an optional interface tools can implement to register
// their own flags on the 'start' command. RegisterFlags is called once
// during command construction; flags registered here are forwarded to
// remote nodes verbatim via the SSH fan-out mechanism.
type FlagProvider interface {
	RegisterFlags(cmd *cobra.Command)
}

// CompletionHinter is an optional interface tools can implement to print
// post-run guidance after all workloads finish. The framework calls it once
// per unique tool type after a successful start --wait/--watch completes.
type CompletionHinter interface {
	CompletionHint(path string) string
}

// StatusFormatter is an optional interface tools can implement to render
// their ExtraData field in beegfs iotest status output.
type StatusFormatter interface {
	FormatExtraStatus(node string, data json.RawMessage) string
}

// StatusFootnote is an optional interface tools can implement to print a
// one-time explanatory footer after all per-node FormatExtraStatus blocks.
type StatusFootnote interface {
	FormatStatusFootnote() string
}

// Describer is an optional interface tools can implement to provide a one-line
// description shown in 'beegfs iotest start' help text.
type Describer interface {
	Description() string
}

// RunConfig is passed to Tool.Run by the framework.
type RunConfig struct {
	Path           string
	NodeName       string        // this node's identity for cross-node coordination; falls back to os.Hostname() when empty. On multi-node runs this is the --nodes name the operator typed (and is therefore resolvable by the other participating nodes), NOT necessarily os.Hostname()'s value.
	BlockSize      int           // bytes per verifiable block; 0 = the tool's own default
	Threads        int           // worker/thread count; 0 = the tool's own default
	TotalOps       *atomic.Int64 // increment on each completed operation
	StopSignal     *atomic.Bool  // poll this; return nil when true
	SetExtraStatus func(v any)   // write tool-specific data into the status file
}

// workloadStatus is the on-disk format of a coordination status file
// (.iotest-<workload>-<hostname>.json).  It is written periodically while
// a tool runs and once more with the final status on completion.
// ExtraData carries tool-specific JSON that StatusFormatter implementations
// decode for display; the framework treats it as an opaque blob.
type workloadStatus struct {
	Node      string          `json:"node"`
	Workload  string          `json:"workload"`
	Status    string          `json:"status"` // "running" | "done" | "failed"
	StartTime time.Time       `json:"startTime"`
	EndTime   *time.Time      `json:"endTime,omitempty"`
	TotalOps  int64           `json:"totalOps"`
	Error     string          `json:"error,omitempty"`
	ExtraData json.RawMessage `json:"extraData,omitempty"`
}

type frameworkCfg struct {
	path            string
	nodes           string
	nodeName        string // overrides os.Hostname() for status file naming; injected by startMultiNode
	wait            bool
	watch           bool
	refreshInterval time.Duration
	fresh           bool
	blockSize       int // --blocksize; bytes per verifiable block, 0 = each tool's default
	threads         int // --threads; worker count, 0 = each tool's default
}

// NewFrameworkCmds returns the start/status/stop cobra commands for
// the given tools. Add the returned commands to the iotest parent command.
func NewFrameworkCmds(tools ...Tool) []*cobra.Command {
	cfg := &frameworkCfg{}

	toolMap := make(map[string]Tool, len(tools))
	for _, t := range tools {
		toolMap[t.Name()] = t
	}

	addPathFlag := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&cfg.path, "path", "/tmp/beegfs-iotest",
			"directory for coordination and data files (use a shared BeeGFS path for multi-node runs)")
	}
	addWaitWatch := func(cmd *cobra.Command) {
		cmd.Flags().DurationVar(&cfg.refreshInterval, "wait", 0,
			"block and poll for completion at this interval, printing final status when done (e.g. --wait=5s)")
		cmd.Flags().DurationVarP(&cfg.refreshInterval, "watch", "w", 0,
			"continuously refresh and display status at this interval until all workloads complete (e.g. --watch=5s)")
		cmd.MarkFlagsMutuallyExclusive("wait", "watch")
	}

	return []*cobra.Command{
		newFrameworkStartCmd(cfg, toolMap, addPathFlag, addWaitWatch),
		newFrameworkStatusCmd(cfg, toolMap, addPathFlag, addWaitWatch),
		newFrameworkStopCmd(cfg, addPathFlag),
	}
}

// ── Commands ──────────────────────────────────────────────────────────────────

// helpWidth returns the terminal width to wrap help text to. It mirrors the
// clamp used by the help renderer (getTermWidthForHelp in root.go: min 80,
// max 150, fallback 80 when stdout is not a terminal) so that our pre-wrapped
// lines already fit and the renderer's own wrapping pass leaves them unchanged.
func helpWidth() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return 80
	}
	return min(150, max(80, w))
}

// wrapWithHangingIndent word-wraps s so that each line fits within width,
// indenting every line after the first by indent spaces. The first line is
// returned without leading indent so the caller can place it after a label
// (e.g. a padded workload name), producing a hanging-indent layout. It wraps
// two columns short of the available width so the help renderer's own wrapping
// pass (which subtracts one and re-adds indentation) does not re-break lines.
func wrapWithHangingIndent(s string, indent, width int) string {
	avail := max(width-indent-2, 20) // 20 = sane floor for very narrow terminals
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	pad := strings.Repeat(" ", indent)
	var b strings.Builder
	b.WriteString(words[0])
	lineLen := len(words[0])
	for _, w := range words[1:] {
		if lineLen+1+len(w) > avail {
			b.WriteString("\n")
			b.WriteString(pad)
			b.WriteString(w)
			lineLen = len(w)
		} else {
			b.WriteString(" ")
			b.WriteString(w)
			lineLen += 1 + len(w)
		}
	}
	return b.String()
}

func newFrameworkStartCmd(cfg *frameworkCfg, tools map[string]Tool, addPathFlag, addWaitWatch func(*cobra.Command)) *cobra.Command {
	names := make([]string, 0, len(tools))
	for n := range tools {
		names = append(names, n)
	}
	sort.Strings(names)

	var longBuf strings.Builder
	// The blank line after the header matters: the help renderer (see
	// wrapHelpText in root.go) collapses a ":"-terminated header block into a
	// single line, but preserves an indented paragraph verbatim as a code
	// block. Emitting the list as its own indented paragraph keeps our
	// hanging-indent wrapping intact.
	longBuf.WriteString("Start one or more iotest workloads locally or across multiple nodes via SSH.\n\nAvailable workloads:\n\n")
	const descCol = 12 // "  " + %-8s name + "  "
	width := helpWidth()
	for _, n := range names {
		desc := ""
		if d, ok := tools[n].(Describer); ok {
			desc = d.Description()
		}
		fmt.Fprintf(&longBuf, "  %-8s  %s\n", n, wrapWithHangingIndent(desc, descCol, width))
	}

	cmd := &cobra.Command{
		Use:   "start [workload...]",
		Short: "Start one or more iotest workloads",
		Long:  longBuf.String(),
		Example: `  # Run soak on the local node
  beegfs iotest start soak --path /mnt/beegfs/testdir

  # Run soak across two nodes and watch until complete
  beegfs iotest start soak --nodes test-r9-01,test-r9-02 --path /mnt/beegfs/testdir --watch=5s

  # Run soak and inval simultaneously across two nodes
  beegfs iotest start soak inval --nodes test-r9-01,test-r9-02 --path /mnt/beegfs/testdir`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				_ = cmd.Help()
				return nil
			}
			if cmd.Flags().Changed("watch") {
				cfg.watch = true
			} else if cmd.Flags().Changed("wait") {
				cfg.wait = true
			}

			selected := make([]Tool, 0, len(args))
			for _, name := range args {
				t, ok := tools[name]
				if !ok {
					names := make([]string, 0, len(tools))
					for n := range tools {
						names = append(names, n)
					}
					return fmt.Errorf("unknown workload %q (available: %s)", name, strings.Join(names, ", "))
				}
				selected = append(selected, t)
			}

			if cfg.fresh {
				resolvedPath, err := freshCheckPath(cfg.path)
				if err != nil {
					return err
				}
				if err := freshDelete(resolvedPath); err != nil {
					return err
				}
			}

			_ = os.MkdirAll(cfg.path, 0755)
			// Only the invoking node records the invocation. Remote fan-out nodes
			// (launched with --iotest-node) share this BeeGFS path and would
			// otherwise overwrite the file with their per-node args (--iotest-node,
			// -N stripped), hiding the real multi-node command from `status`.
			if cfg.nodeName == "" {
				saveInvocation(cfg.path)
			}

			if cfg.nodes != "" {
				nodes, err := parseNodes(cfg.nodes)
				if err != nil {
					return err
				}
				if err := startMultiNode(cmd.Context(), cfg, selected, nodes); err != nil {
					return err
				}
				if !cfg.wait && !cfg.watch {
					fmt.Printf("beegfs iotest status --path %s  # %d node(s) started\n",
						cfg.path, len(nodes))
					return nil
				}
				if err := waitOrWatch(cmd.Context(), cfg, tools); err != nil {
					return err
				}
				printCompletionHints(cfg.path, selected)
				return nil
			}
			if err := startLocal(cmd.Context(), cfg, selected); err != nil {
				return err
			}
			printCompletionHints(cfg.path, selected)
			return nil
		},
	}
	addPathFlag(cmd)
	addWaitWatch(cmd)
	cmd.Flags().StringVarP(&cfg.nodes, "nodes", "N", "", "comma-separated node names or path to a nodes file")
	cmd.Flags().BoolVar(&cfg.fresh, "fresh", false, "remove iotest data and coordination files from --path before starting")
	cmd.Flags().IntVar(&cfg.blockSize, "blocksize", 0, "bytes per verifiable block for tools that write blocks (soak, bench); 0 = each tool's own default")
	cmd.Flags().IntVar(&cfg.threads, "threads", 0, "worker/thread count for tools that support it (soak, bench); 0 = each tool's own default")
	cmd.Flags().StringVar(&cfg.nodeName, "iotest-node", "", "override the node name used in status files (injected by multi-node SSH invocations)")
	_ = cmd.Flags().MarkHidden("iotest-node")
	for _, t := range tools {
		if fp, ok := t.(FlagProvider); ok {
			fp.RegisterFlags(cmd)
		}
	}
	return cmd
}

func newFrameworkStatusCmd(cfg *frameworkCfg, tools map[string]Tool, addPathFlag, addWaitWatch func(*cobra.Command)) *cobra.Command {
	cmd := &cobra.Command{
		Use:         "status",
		Short:       "Show the status of active or recent iotest workloads",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("watch") {
				cfg.watch = true
			} else if cmd.Flags().Changed("wait") {
				cfg.wait = true
			}

			if cfg.wait || cfg.watch {
				return waitOrWatch(cmd.Context(), cfg, tools)
			}

			statuses, err := readStatuses(cfg.path)
			if err != nil {
				return err
			}
			if len(statuses) == 0 {
				fmt.Printf("No iotest status files found at %s\n", cfg.path)
				return nil
			}
			printStatusTable(cfg.path, statuses, tools)
			return nil
		},
	}
	addPathFlag(cmd)
	addWaitWatch(cmd)
	return cmd
}

func newFrameworkStopCmd(cfg *frameworkCfg, addPathFlag func(*cobra.Command)) *cobra.Command {
	cmd := &cobra.Command{
		Use:         "stop",
		Short:       "Signal all running iotest workloads to stop",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.Create(stopPath(cfg.path))
			if err != nil {
				return fmt.Errorf("write stop signal: %w", err)
			}
			// watchStopSignal polls for this file's existence, not any content
			// written to it -- but if Close fails the create may not be durably
			// visible yet, and running workloads would never see the signal
			// while this command reports success.
			if err := f.Close(); err != nil {
				return fmt.Errorf("write stop signal: %w", err)
			}
			fmt.Println("Stop signal written. Workloads will exit at the next poll interval.")
			return nil
		},
	}
	addPathFlag(cmd)
	_ = cmd.MarkFlagRequired("path")
	return cmd
}

// ── Local execution ───────────────────────────────────────────────────────────

func startLocal(ctx context.Context, cfg *frameworkCfg, tools []Tool) error {
	hostname := cfg.nodeName
	if hostname == "" {
		h, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("get hostname: %w", err)
		}
		hostname = h
	}

	if err := os.MkdirAll(cfg.path, 0755); err != nil {
		return fmt.Errorf("create path %s: %w", cfg.path, err)
	}

	// Refuse to start if the same workload type is already running on this node.
	// Only check workloads being started here — different workload types are
	// expected to run concurrently (each gets its own SSH session and stop-signal
	// goroutine). Only check the local hostname; other nodes' status files are
	// visible on a shared path and must not block this node from starting.
	starting := make(map[string]bool, len(tools))
	for _, t := range tools {
		starting[t.Name()] = true
	}
	if existing, _ := readStatuses(cfg.path); len(existing) > 0 {
		for _, s := range existing {
			if s.Status == "running" && s.Node == hostname && starting[s.Workload] {
				return fmt.Errorf("workload %q is still running on this node at %s; "+
					"wait for it to finish or remove the coordination files with 'rm %s/.iotest-*'",
					s.Workload, cfg.path, cfg.path)
			}
		}
	}
	_ = os.Remove(stopPath(cfg.path)) // clear any stale stop signal from a previous run

	stopSig := &atomic.Bool{}
	stopCtx, cancelStop := context.WithCancel(ctx)
	defer cancelStop()
	go watchStopSignal(stopCtx, cfg.path, stopSig)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string

	for _, tool := range tools {
		tool := tool
		startTime := time.Now()
		spath := statusPath(cfg.path, tool.Name(), hostname)
		ops := &atomic.Int64{}

		var extraStatus atomic.Value

		_ = writeStatus(workloadStatus{
			Node: hostname, Workload: tool.Name(),
			Status: "running", StartTime: startTime,
		}, spath)

		wg.Add(1)
		go func() {
			defer wg.Done()

			setExtra := func(v any) {
				if data, err := json.Marshal(v); err == nil {
					extraStatus.Store(json.RawMessage(data))
				}
			}
			getExtra := func() json.RawMessage {
				raw, _ := extraStatus.Load().(json.RawMessage)
				return raw
			}

			updateCtx, cancelUpdate := context.WithCancel(ctx)
			defer cancelUpdate() // safety net for early returns
			updateDone := make(chan struct{})
			go func() {
				defer close(updateDone)
				t := time.NewTicker(5 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-t.C:
						_ = writeStatus(workloadStatus{
							Node: hostname, Workload: tool.Name(),
							Status: "running", StartTime: startTime,
							TotalOps: ops.Load(), ExtraData: getExtra(),
						}, spath)
					case <-updateCtx.Done():
						return
					}
				}
			}()

			runErr := tool.Run(ctx, RunConfig{
				Path: cfg.path, NodeName: cfg.nodeName, BlockSize: cfg.blockSize, Threads: cfg.threads, TotalOps: ops, StopSignal: stopSig,
				SetExtraStatus: setExtra,
			})

			// Stop the update goroutine and wait for it to exit before writing
			// the final status, so the ticker cannot overwrite "done"/"failed".
			cancelUpdate()
			<-updateDone

			now := time.Now()
			status, errStr := "done", ""
			if runErr != nil {
				status = "failed"
				errStr = runErr.Error()
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%s: %s", tool.Name(), errStr))
				mu.Unlock()
			}
			_ = writeStatus(workloadStatus{
				Node: hostname, Workload: tool.Name(),
				Status: status, StartTime: startTime, EndTime: &now,
				TotalOps: ops.Load(), Error: errStr, ExtraData: getExtra(),
			}, spath)
		}()
	}

	wg.Wait()

	if len(failures) > 0 {
		return fmt.Errorf("workload(s) failed:\n  %s", strings.Join(failures, "\n  "))
	}
	return nil
}

// ── Multi-node execution ──────────────────────────────────────────────────────

// startMultiNode launches one SSH session per (tool, node) pair so each
// workload gets its own log file named .iotest-<workload>-<node>.log.
func startMultiNode(ctx context.Context, cfg *frameworkCfg, tools []Tool, nodes []NodeSpec) error {
	if err := os.MkdirAll(cfg.path, 0755); err != nil {
		return fmt.Errorf("create path %s: %w", cfg.path, err)
	}

	allNames := make([]string, len(tools))
	for i, t := range tools {
		allNames[i] = t.Name()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string

	for _, tool := range tools {
		for _, node := range nodes {
			tool, node := tool, node
			remoteArgs := append(buildRemoteArgsForWorkload(os.Args, allNames, tool.Name()),
				"--iotest-node", node.Name)
			lPath := filepath.Join(cfg.path, fmt.Sprintf(".iotest-%s-%s.log", tool.Name(), node.Name))
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sshStartBackground(ctx, node, remoteArgs, lPath); err != nil {
					mu.Lock()
					failures = append(failures, err.Error())
					mu.Unlock()
				}
			}()
		}
	}

	wg.Wait()

	if len(failures) > 0 {
		return fmt.Errorf("failed to start on node(s):\n  %s", strings.Join(failures, "\n  "))
	}
	return nil
}

// ── Wait / Watch ──────────────────────────────────────────────────────────────

func waitOrWatch(ctx context.Context, cfg *frameworkCfg, tools map[string]Tool) error {
	if cfg.refreshInterval == 0 {
		cfg.refreshInterval = 5 * time.Second
	}
	ticker := time.NewTicker(cfg.refreshInterval)
	defer ticker.Stop()

	for {
		statuses, err := readStatuses(cfg.path)
		if err != nil {
			return err
		}

		if cfg.watch && len(statuses) > 0 {
			r := util.TermRefresher{}
			if err := r.StartRefresh(); err != nil {
				return err
			}
			printStatusTable(cfg.path, statuses, tools)
			_ = r.FinishRefresh(util.WithTermFooter(fmt.Sprintf(
				"Refreshing every %s (last: %s). Ctrl+C to stop watching.",
				cfg.refreshInterval, time.Now().Format(time.TimeOnly),
			)))
		}

		if len(statuses) > 0 && allTerminated(statuses) {
			if cfg.wait {
				printStatusTable(cfg.path, statuses, tools)
			}
			util.TerminalAlert()
			return terminationError(statuses)
		}

		select {
		case <-ctx.Done():
			// Return any failures that already completed rather than hiding them.
			statuses, _ = readStatuses(cfg.path)
			return terminationError(statuses)
		case <-ticker.C:
		}
	}
}

// ── File helpers ──────────────────────────────────────────────────────────────

func statusPath(dir, workload, hostname string) string {
	return filepath.Join(dir, fmt.Sprintf(".iotest-%s-%s.json", workload, hostname))
}

func stopPath(dir string) string {
	return filepath.Join(dir, ".iotest.stop")
}

func invocationPath(dir string) string {
	return filepath.Join(dir, ".iotest-invocation.json")
}

type invocationRecord struct {
	Args      []string  `json:"args"`
	StartTime time.Time `json:"startTime"`
}

func saveInvocation(dir string) {
	rec := invocationRecord{Args: os.Args, StartTime: time.Now()}
	if data, err := json.Marshal(rec); err == nil {
		_ = os.WriteFile(invocationPath(dir), data, 0644)
	}
}

func readInvocation(dir string) *invocationRecord {
	data, err := os.ReadFile(invocationPath(dir))
	if err != nil {
		return nil
	}
	var rec invocationRecord
	if json.Unmarshal(data, &rec) != nil {
		return nil
	}
	return &rec
}

func writeStatus(s workloadStatus, path string) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func readStatuses(dir string) ([]workloadStatus, error) {
	matches, err := filepath.Glob(filepath.Join(dir, ".iotest-*.json"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", dir, err)
	}
	invPath := invocationPath(dir)
	statuses := make([]workloadStatus, 0, len(matches))
	for _, m := range matches {
		if m == invPath {
			continue
		}
		data, err := os.ReadFile(m)
		if err != nil {
			continue
		}
		var s workloadStatus
		if err := json.Unmarshal(data, &s); err != nil {
			continue
		}
		statuses = append(statuses, s)
	}
	return statuses, nil
}

// watchStopSignal polls for the stop file every second and sets sig when found.
// Runs in a goroutine; exits when ctx is cancelled or the stop file appears.
func watchStopSignal(ctx context.Context, dir string, sig *atomic.Bool) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if _, err := os.Stat(stopPath(dir)); err == nil {
				sig.Store(true)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// ── Output ────────────────────────────────────────────────────────────────────

// printStatusTable renders all workload statuses as a table, then appends any
// tool-specific extra lines from StatusFormatter implementors below the table.
// If any workload has failed, it also prints debugging hints pointing to the
// SSH log for each failed node.
func printStatusTable(path string, statuses []workloadStatus, tools map[string]Tool) {
	if inv := readInvocation(path); inv != nil {
		fmt.Printf("[%s] %s\n", inv.StartTime.Format(time.DateTime), strings.Join(inv.Args, " "))
	}

	t := table.NewWriter()
	t.AppendHeader(table.Row{"WORKLOAD", "NODE", "STATUS", "OPS", "ELAPSED", "ERROR"})
	for _, s := range statuses {
		elapsed := "-"
		if !s.StartTime.IsZero() {
			end := time.Now()
			if s.EndTime != nil {
				end = *s.EndTime
			}
			elapsed = end.Sub(s.StartTime).Round(time.Second).String()
		}
		t.AppendRow(table.Row{s.Workload, s.Node, s.Status, s.TotalOps, elapsed, s.Error})
	}
	fmt.Println(t.Render())

	// Print tool-specific extra data for any status that has it.
	for _, s := range statuses {
		if len(s.ExtraData) == 0 {
			continue
		}
		tool, ok := tools[s.Workload]
		if !ok {
			continue
		}
		sf, ok := tool.(StatusFormatter)
		if !ok {
			continue
		}
		if line := sf.FormatExtraStatus(s.Node, s.ExtraData); line != "" {
			fmt.Println(line)
		}
	}

	// Print a one-time footer for any tool that implements StatusFootnote.
	seenFootnote := map[string]bool{}
	for _, s := range statuses {
		if seenFootnote[s.Workload] {
			continue
		}
		tool, ok := tools[s.Workload]
		if !ok {
			continue
		}
		if fn, ok := tool.(StatusFootnote); ok {
			if footer := fn.FormatStatusFootnote(); footer != "" {
				fmt.Println(footer)
			}
		}
		seenFootnote[s.Workload] = true
	}

	// Print debug hints for any failed workloads. Each workload on each node
	// has its own SSH log file named .iotest-<workload>-<node>.log.
	var failed []workloadStatus
	for _, s := range statuses {
		if s.Status == "failed" {
			failed = append(failed, s)
		}
	}
	if len(failed) > 0 {
		fmt.Println("# To investigate failures, check the SSH log for each failed workload:")
		for _, s := range failed {
			logFile := filepath.Join(path, fmt.Sprintf(".iotest-%s-%s.log", s.Workload, s.Node))
			fmt.Printf("cat %s\n", logFile)
		}
	}
}

// printCompletionHints calls CompletionHint once per unique tool type.
func printCompletionHints(path string, tools []Tool) {
	seen := map[string]bool{}
	for _, t := range tools {
		if seen[t.Name()] {
			continue
		}
		seen[t.Name()] = true
		if h, ok := t.(CompletionHinter); ok {
			if hint := h.CompletionHint(path); hint != "" {
				fmt.Println(hint)
			}
		}
	}
}

func allTerminated(statuses []workloadStatus) bool {
	for _, s := range statuses {
		if s.Status == "running" {
			return false
		}
	}
	return true
}

// iotestHumanSize formats n as a human-readable IEC size string (KiB/MiB/GiB).
func iotestHumanSize(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.2f GiB", float64(n)/gib)
	case n >= mib:
		return fmt.Sprintf("%.2f MiB", float64(n)/mib)
	case n >= kib:
		return fmt.Sprintf("%.2f KiB", float64(n)/kib)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// ── Fresh-start helpers ───────────────────────────────────────────────────────

// freshBlocklist contains absolute paths that --fresh refuses to clean.
// These are the standard Linux/Unix directories where deleting iotest-*
// files would be surprising at best and destructive at worst.
var freshBlocklist = map[string]bool{
	"/":      true,
	"/bin":   true,
	"/boot":  true,
	"/dev":   true,
	"/etc":   true,
	"/home":  true,
	"/lib":   true,
	"/lib32": true,
	"/lib64": true,
	"/opt":   true,
	"/proc":  true,
	"/root":  true,
	"/run":   true,
	"/sbin":  true,
	"/srv":   true,
	"/sys":   true,
	"/tmp":   true,
	"/usr":   true,
	"/var":   true,
}

// freshCheckPath returns the resolved, symlink-free absolute form of path,
// or an error if that resolves to a blocklisted directory. Callers must
// pass the returned path to freshDelete rather than the original path --
// checking the lexical path but deleting through the original would let a
// symlinked --path whose target is a blocklisted directory bypass the
// check entirely (the check would see the symlink's own path, not where it
// points, while the OS transparently follows the symlink during the
// delete).
func freshCheckPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("--fresh: resolve path %q: %w", path, err)
	}
	// EvalSymlinks fails if the path doesn't exist yet -- the common case on
	// a first run, before the MkdirAll below creates it. A nonexistent path
	// can't be a symlink to anywhere, so fall back to the unresolved
	// absolute path rather than treating that as an error.
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("--fresh: resolve symlinks in %q: %w", path, err)
		}
		resolved = abs
	}
	if freshBlocklist[resolved] {
		return "", fmt.Errorf("--fresh: refusing to clean %q (system directory); use a dedicated test path", resolved)
	}
	return resolved, nil
}

// freshDelete removes iotest data and coordination files from dir.
// Files matching iotest-* (data files) and .iotest-* (status, log, stop
// signal) are deleted. Non-matching files are left untouched.
func freshDelete(dir string) error {
	var deleted int
	for _, pat := range []string{"iotest-*", ".iotest-*"} {
		matches, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return fmt.Errorf("--fresh: glob %q: %w", pat, err)
		}
		for _, m := range matches {
			if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("--fresh: remove %q: %w", m, err)
			}
			deleted++
		}
	}
	if deleted > 0 {
		fmt.Printf("--fresh: removed %d file(s) from %s\n", deleted, dir)
	}
	return nil
}

// terminationError returns nil if every workload succeeded, or an error listing
// all failed workloads. Workloads still in "running" state are ignored — callers
// that reach this only do so after allTerminated returns true.
func terminationError(statuses []workloadStatus) error {
	var failed []string
	for _, s := range statuses {
		if s.Status == "failed" {
			msg := s.Workload + " on " + s.Node
			if s.Error != "" {
				msg += ": " + s.Error
			}
			failed = append(failed, msg)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("workload(s) failed:\n  %s", strings.Join(failed, "\n  "))
	}
	return nil
}
