package health

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	tgtFrontend "github.com/thinkparq/beegfs-go/ctl/internal/cmd/target"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/health"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/license"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	watchFlag              = "watch"
	ignoreFailedChecksFlag = "ignore-failed-checks"
)

var statusIconMap = map[backend.Status]struct {
	emoji       string
	alternative string
}{
	backend.Unknown:  {"❓", "(UNKNOWN)"},
	backend.Healthy:  {"✅", "(HEALTHY)"},
	backend.Degraded: {"⚠️ ", "(DEGRADED)"},
	backend.Critical: {"🛑", "(CRITICAL)"},
}

// statusIcon renders a status for human output as an emoji, or a text alternative when emojis are
// disabled.
func statusIcon(s backend.Status) string {
	rep, ok := statusIconMap[s]
	if !ok {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return rep.emoji
	}
	return rep.alternative
}

type checkCfg struct {
	noHints                 bool
	printNetworkConnections bool
	printDF                 bool
	watchInterval           time.Duration
	ignoreFailedChecks      bool
	connectionTimeout       time.Duration
	forceConnections        bool
}

func newCheckCmd() *cobra.Command {

	frontendCfg := checkCfg{}

	cmd := &cobra.Command{
		Use:   "check [<mount-path>] ...",
		Short: "Run a series of checks against BeeGFS to verify its health",
		Long: fmt.Sprintf(`Run a series of checks against BeeGFS to verify its health.

If there are multiple filesystems mounted to this client, only one can be checked at a time.
The filesystem that is checked is determined by the --%s parameter.
If this filesystem is mounted multiple times, network connections will be checked for each mount point.
Optionally specify one or more <mount-paths> to limit the connection checks.
		`, config.ManagementAddrKey),
		Annotations: map[string]string{
			"health.SkipAlerts": "",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			outputType := config.OutputType(viper.GetString(config.OutputKey))

			if !cmd.Flags().Changed(watchFlag) {
				return runHealthCheck(cmd.Context(), args, frontendCfg, outputType)
			}

			// Watch mode: a single JSON document cannot be streamed, so require NDJSON to stream.
			if outputType == config.OutputJSON || outputType == config.OutputJSONPretty {
				return fmt.Errorf("--%s is not supported with --%s %s; use --%s ndjson to stream results", watchFlag, config.OutputKey, outputType, config.OutputKey)
			}

			ticker := time.NewTicker(frontendCfg.watchInterval)
			defer ticker.Stop()
			for {
				if outputType == config.OutputNDJSON {
					// NDJSON streams one report object per refresh, no terminal clearing.
					if err := runHealthCheck(cmd.Context(), args, frontendCfg, outputType); err != nil {
						return err
					}
				} else {
					t := util.TermRefresher{}
					if err := t.StartRefresh(); err != nil {
						return err
					}
					if err := runHealthCheck(cmd.Context(), args, frontendCfg, outputType); err != nil {
						t.FinishRefresh()
						return err
					}
					t.FinishRefresh(util.WithTermFooter(fmt.Sprintf("Refreshing every %s until Ctrl+C or a check fails (last refresh: %s).", frontendCfg.watchInterval, time.Now().Format(time.TimeOnly))))
				}
				select {
				case <-cmd.Context().Done():
					return nil
				case <-ticker.C:
					continue
				}
			}
		},
	}
	cmd.Flags().DurationVar(&frontendCfg.connectionTimeout, connectionTimeoutFlag, time.Second*5, "Timeout when attempting to establish connections for the network connection check.")
	cmd.Flags().BoolVar(&frontendCfg.forceConnections, forceConnectionsFlag, true, "By default the network connection check will first attempt to establish storage node connections by running df. Connections may be <none> if this is set to false.")
	cmd.Flags().BoolVar(&frontendCfg.printNetworkConnections, "print-net", false, "By default network connections are only printed whenever an issue is detected. Optionally they can always be printed.")
	cmd.Flags().BoolVar(&frontendCfg.printDF, "print-df", false, "By default available disk capacity and inodes are only printed whenever an issue is detected. Optionally they can always be printed.")
	cmd.Flags().BoolVar(&frontendCfg.noHints, "no-hints", false, "Disable printing additional hints.")
	cmd.Flags().DurationVar(&frontendCfg.watchInterval, watchFlag, 1*time.Second, "Periodically re-run the health check until cancelled with Ctrl+C or a check fails. Set --ignore-failed-checks to continue running event if checks are failing.")
	cmd.Flags().BoolVar(&frontendCfg.ignoreFailedChecks, ignoreFailedChecksFlag, false, "Don't return a non-zero exit code when checks fail.")

	return cmd
}

// runHealthCheck collects the health report and renders it in the requested output format. Structured
// output (JSON/NDJSON) goes to stdout; operational messages always go to stderr.
func runHealthCheck(ctx context.Context, filterByMounts []string, cfg checkCfg, outputType config.OutputType) error {
	report, err := backend.Collect(ctx, backend.CollectConfig{
		ConnectionTimeout: cfg.connectionTimeout,
		ForceConnections:  cfg.forceConnections,
		FilterByMounts:    filterByMounts,
	})
	if err != nil {
		return err
	}

	switch outputType {
	case config.OutputJSON, config.OutputNDJSON:
		emitConnNotices(report, cfg)
		data, err := json.Marshal(report)
		if err != nil {
			return fmt.Errorf("marshaling health report: %w", err)
		}
		fmt.Println(string(data))
	case config.OutputJSONPretty:
		emitConnNotices(report, cfg)
		data, err := json.MarshalIndent(report, "", " ")
		if err != nil {
			return fmt.Errorf("marshaling health report: %w", err)
		}
		fmt.Println(string(data))
	default:
		renderHuman(ctx, report, cfg)
	}

	if report.Status != backend.Healthy && !cfg.ignoreFailedChecks {
		if outputType == config.OutputTable || outputType == "" {
			util.TerminalAlert()
		}
		return util.NewCtlError(fmt.Errorf("one or more checks failed"), util.GeneralError)
	}
	return nil
}

// renderHuman prints the report as the human-readable text report. Structured/report content goes to
// stdout; operational notices about the connection check go to stderr (see emitConnNotices).
func renderHuman(ctx context.Context, report *backend.Report, cfg checkCfg) {
	printHeader(fmt.Sprintf("Running Health Check for %s\n(Filesystem UUID: %s)", report.FS, report.FsUUID), "#")

	for _, section := range report.Sections {
		switch section.Title {
		case backend.SectionGeneralChecks:
			printHeader(">>>>> General Checks <<<<<", "#")
			for _, c := range section.Checks {
				fmt.Printf("\n%s", statusLine(c.Status, c.Name, c.Summary, cfg.noHints))
			}
			fmt.Print("\n\n")

		case backend.SectionBusyNodes:
			printHeader(">>>>> Checking for Busy Nodes <<<<<", "#")
			anyBusy := false
			for _, c := range section.Checks {
				fmt.Printf("\n%s", statusLine(c.Status, c.Name, c.Summary, cfg.noHints))
				if c.Status != backend.Healthy {
					anyBusy = true
					fmt.Print("\n\n")
					if bd, ok := c.Detail.(backend.BusyDetail); ok {
						printBusyNodes(bd.Raw)
					}
				}
			}
			if anyBusy && !cfg.noHints {
				fmt.Print("\n\nHINT: Investigate further with 'beegfs stats server'")
			}
			fmt.Print("\n\n")

		case backend.SectionTargets:
			printHeader(">>>>> Checking Targets <<<<<", "#")
			fmt.Print("\n")
			unhealthy := false
			for _, c := range section.Checks {
				fmt.Printf("%s\n", statusLine(c.Status, c.Name, c.Summary, cfg.noHints))
				if c.Status != backend.Healthy {
					unhealthy = true
				}
			}
			fmt.Print("\n")
			if unhealthy || cfg.printDF {
				if td, ok := section.Detail.(backend.TargetsDetail); ok {
					printDF(ctx, td.Raw, tgtFrontend.PrintConfig{Capacity: true, State: true})
				}
			}
			if !cfg.noHints {
				fmt.Print("HINT: This mode does not check filesystem consistency. To check for filesystem inconsistencies,\n      you can run 'beegfs-fsck --checkfs --readOnly' and consult with ThinkParQ support.\n")
			}
			fmt.Println()

		case backend.SectionConnections:
			printHeader(">>>>> Checking Connections to Server Nodes <<<<<", "#")
			emitConnNotices(report, cfg)
			for _, cc := range connectionClients(report) {
				printClientHeader(cc.Raw, "=")
				fmt.Printf("\n%s\n", statusLine(cc.Status, "Fallbacks", cc.Summary, cfg.noHints))
				if cc.Status != backend.Healthy || cfg.printNetworkConnections {
					fmt.Println()
					printBeeGFSNet(cc.Raw)
				}
				fmt.Println()
			}
		}
	}
}

// statusLine formats a single "<icon> <name> -> <summary>" line. The summary (and its "-> " prefix)
// is omitted when hints are disabled.
func statusLine(status backend.Status, name, summary string, noHints bool) string {
	hint := ""
	if !noHints {
		hint = "-> " + summary
	}
	return fmt.Sprintf("%s %s %s", statusIcon(status), name, hint)
}

// emitConnNotices writes operational notices about the connection check to stderr, regardless of the
// output mode, so they never pollute stdout (essential for JSON output).
func emitConnNotices(report *backend.Report, cfg checkCfg) {
	if report.ConnCheckErr != nil {
		cmdfmt.Printf("Error establishing new connections, further connection checks may be incomplete or skipped: %s (ignoring)\n", report.ConnCheckErr)
		if !cfg.noHints {
			cmdfmt.Printf("HINT: Try increasing the '--%s' flag or setting '--%s=false` to skip establishing new connections.\n\n", connectionTimeoutFlag, forceConnectionsFlag)
		}
	}
	results := connectionClients(report)
	if len(results) == 0 {
		cmdfmt.Printf("No client mounts found, skipping connection checks\n\n")
	} else if len(results) > 1 && !cfg.noHints {
		cmdfmt.Printf("\nHINT: Multiple client mounts detected, connections will be checked from each mount point (specify <mount-path> if this is not what you want).\n\n")
	}
}

// connectionClients returns the per-client connection results kept in the connection section's
// Detail, or nil if there is no such section.
func connectionClients(report *backend.Report) []backend.ClientConn {
	for _, section := range report.Sections {
		if section.Title == backend.SectionConnections {
			if detail, ok := section.Detail.(backend.ConnectionsDetail); ok {
				return detail.Clients
			}
		}
	}
	return nil
}

func printBusyNodes(nodes []stats.NodeStats) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Node.Id.NumId < nodes[j].Node.Id.NumId
	})
	for _, n := range nodes {
		if n.Stats.QueuedRequests > backend.QueuedReqsDegradedThreshold {
			fmt.Printf("* %s [%s] has %d queued requests\n", n.Node.Alias, n.Node.Id, n.Stats.QueuedRequests)
		}
	}
}

// QuickChecks executes a subset of the full health check returning strings of requiredAlerts and
// dismissibleAlerts. If no alerts are present these strings are empty. If DisableAlerts is set then
// dismissibleAlerts will always be empty. It is up to the caller to decide if errs are ignored.
//
// This function notably skips any health checks that require an active client mount, or involve
// non-critical or transient conditions that might be gone before a user runs a health check (for
// example busy node checks). When adding checks be careful not to add checks that add noticeable
// runtime or could potentially cause commands to hang for an extended period.
func QuickChecks(ctx context.Context) (requiredAlerts string, dismissibleAlerts string, err error) {

	var mgmtdPeer peer.Peer
	targets, err := tgtBackend.GetTargets(ctx, grpc.Peer(&mgmtdPeer))
	if err != nil {
		return requiredAlerts, dismissibleAlerts, fmt.Errorf("unable to execute quick checks: %w", err)
	}

	result := license.Check(ctx, targets)
	if result.Err != nil {
		return requiredAlerts, dismissibleAlerts, fmt.Errorf("unable to execute quick checks: %w", result.Err)
	}

	if !result.IsHealthy() {
		if result.InvalidMsg != "" {
			requiredAlerts = fmt.Sprintf(`WARNING: This system does not have a valid license installed.
To avoid disruptions, run 'beegfs license' and follow the required steps.
Reason: %s.
`, result.InvalidMsg)
		}
		if result.ExpirationMsg != "" {
			requiredAlerts += fmt.Sprintf("WARNING: License is nearing expiration (%s). Run 'beegfs license' for more details.\n", result.ExpirationMsg)
		}
		if result.ViolationsMsg != "" {
			requiredAlerts += fmt.Sprintf("WARNING: License violations found (%s). Run 'beegfs license' for more details.\n", result.ViolationsMsg)
		}
	}

	if viper.GetBool(config.DisableAlerts) {
		return requiredAlerts, dismissibleAlerts, nil
	}

	dismissibleWarning := false
	if tlsStatus, _ := backend.CheckTLSCertificates(ctx, mgmtdPeer); tlsStatus != backend.Healthy {
		dismissibleWarning = true
	}
	reachabilityStatus, consistencyStatus, capacityStatus, mappingStatus := backend.CheckTargets(targets)
	if reachabilityStatus != backend.Healthy || consistencyStatus != backend.Healthy || capacityStatus != backend.Healthy || mappingStatus != backend.Healthy {
		dismissibleWarning = true
	}
	if dismissibleWarning {
		dismissibleAlerts = fmt.Sprintf("WARNING: One or more health checks are failing, run 'beegfs health check' for more details. Set the global %s flag to turn off this message.\n", config.DisableAlerts)
	}
	return requiredAlerts, dismissibleAlerts, nil
}
