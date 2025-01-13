package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	tgtFrontend "github.com/thinkparq/beegfs-go/ctl/internal/cmd/target"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

type Status int

func (s *Status) updateStatusIfWorse(newStatus Status) {
	if newStatus > *s {
		*s = newStatus
	}
}

const (
	Unknown  Status = iota
	Healthy  Status = 1
	Degraded Status = 2
	Critical Status = 3
)

const (
	queuedReqsDegradedThreshold = 16
	queuedReqsCriticalThreshold = 512
)

func (s Status) String() string {
	representation, exists := healthCheckStatusMap[s]
	if !exists {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return representation.emoji
	}
	return representation.alternative
}

type healthCheckStatusEmoji struct {
	emoji       string
	alternative string
}

var healthCheckStatusMap = map[Status]healthCheckStatusEmoji{
	0: {"❓", "(UNKNOWN)"},
	1: {"✅", "(HEALTHY)"},
	2: {"\u26A0\ufe0f ", "(DEGRADED)"},
	3: {"🛑", "(CRITICAL)"},
}

const (
	watchFlag              = "watch"
	ignoreFailedChecksFlag = "ignore-failed-checks"
)

type checkCfg struct {
	noHints                 bool
	printNetworkConnections bool
	printDF                 bool
	watchInterval           time.Duration
	ignoreFailedChecks      bool
	connectionTimeout       time.Duration
}

func newCheckCmd() *cobra.Command {

	frontendCfg := checkCfg{}
	procfsCfg := procfs.GetBeeGFSClientsConfig{}

	cmd := &cobra.Command{
		Use:   "check [<mount-path>] ...",
		Short: "Runs a series of checks against BeeGFS to verify its health",
		Long: fmt.Sprintf(`Runs a series of checks against BeeGFS to verify its health.

If there are multiple file systems mounted to this client, only one can be checked at a time. 
The file system that is checked is determined by the --%s parameter.
If this file system is mounted multiple times, network connections will be checked for each mount point. 
Optionally specify one or more <mount-paths> to limit the connection checks.
		`, config.ManagementAddrKey),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed(watchFlag) {
				ticker := time.NewTicker(frontendCfg.watchInterval)
				for {
					t := util.TermRefresher{}
					if err := t.StartRefresh(); err != nil {
						return err
					}
					// Run the health checks:
					if result := runHealthCheckCmd(cmd.Context(), args, frontendCfg, procfsCfg); result != nil {
						t.FinishRefresh()
						return result
					}
					t.FinishRefresh(util.WithTermFooter(fmt.Sprintf("Refreshing every %s until Ctrl+C or a check fails (last refresh: %s).", frontendCfg.watchInterval, time.Now().Format(time.TimeOnly))))
					select {
					case <-cmd.Context().Done():
						return nil
					case <-ticker.C:
						continue
					}
				}
			} else {
				return runHealthCheckCmd(cmd.Context(), args, frontendCfg, procfsCfg)
			}
		},
	}
	cmd.Flags().DurationVar(&frontendCfg.connectionTimeout, connectionTimeoutFlag, time.Second*5, "Timeout when attempting to establish connections for the network connection check.")
	cmd.Flags().BoolVar(&procfsCfg.ForceConnections, forceConnectionsFlag, true, "By default the network connection check will first attempt to establish storage server connections by running df. Connections may be <none> if this is set to false.")
	cmd.Flags().BoolVar(&frontendCfg.printNetworkConnections, "print-net", false, "By default network connections are only printed whenever an issue is detected. Optionally they can always be printed.")
	cmd.Flags().BoolVar(&frontendCfg.printDF, "print-df", false, "By default available disk capacity and inodes are only printed whenever an issue is detected. Optionally they can always be printed.")
	cmd.Flags().BoolVar(&frontendCfg.noHints, "no-hints", false, "Disable printing additional hints.")
	cmd.Flags().DurationVar(&frontendCfg.watchInterval, watchFlag, 1*time.Second, "Periodically re-run the health check until cancelled with Ctrl+C or a check fails. Set --ignore-failed-checks to continue running event if checks are failing.")
	cmd.Flags().BoolVar(&frontendCfg.ignoreFailedChecks, ignoreFailedChecksFlag, false, "Don't return a non-zero exit code when checks fail.")

	return cmd
}

func runHealthCheckCmd(ctx context.Context, filterByMounts []string, frontendCfg checkCfg, backendCfg procfs.GetBeeGFSClientsConfig) error {

	log, _ := config.GetLogger()

	// Should be set to true if any check falls and the command should return with a non-zero exit
	// code after all checks are run and results printed (provided checks aren't ignored).
	failedCheck := false

	hint := func(hint string) string {
		if frontendCfg.noHints {
			return ""
		}
		return hint
	}

	mgmtd, err := config.ManagementClient()
	if err != nil {
		return fmt.Errorf("unable to proceed without a working management node: %w", err)
	}
	printHeader(fmt.Sprintf("Running Health Check for beegfs://%s", mgmtd.GetAddress()), "#")
	printHeader(">>>>> Checking for Busy Nodes <<<<<", "#")
	log.Debug("collecting meta stats")
	metaNodes, err := stats.MultiServerNodes(ctx, beegfs.Meta)
	if err != nil {
		return err
	}
	metaBusy := checkForBusyNodes(metaNodes)
	fmt.Printf("\n%s Busy Metadata Nodes %s", metaBusy, hint(fmt.Sprintf("-> Does the number of queued requests exceed the degraded (%d) or critical (%d) thresholds?", queuedReqsDegradedThreshold, queuedReqsCriticalThreshold)))
	if metaBusy != Healthy {
		failedCheck = true
		fmt.Print("\n\n")
		printBusyNodes(metaNodes)
	}
	log.Debug("collecting storage stats")
	storageNodes, err := stats.MultiServerNodes(ctx, beegfs.Storage)
	if err != nil {
		return err
	}
	storBusy := checkForBusyNodes(storageNodes)
	fmt.Printf("\n%s Busy Storage Nodes %s", storBusy, hint(fmt.Sprintf("-> Does the number of queued requests exceed the degraded (%d) or critical (%d) thresholds?", queuedReqsDegradedThreshold, queuedReqsCriticalThreshold)))
	if storBusy != Healthy {
		failedCheck = true
		fmt.Print("\n\n")
		printBusyNodes(storageNodes)
	}
	if metaBusy != Healthy || storBusy != Healthy {
		fmt.Print(hint("\n\nHINT: Investigate further with 'beegfs stats server'"))
	}
	fmt.Print("\n\n")

	printHeader(">>>>> Checking Targets <<<<<", "#")
	log.Debug("getting target list")
	targets, err := tgtBackend.GetTargets(ctx)
	if err != nil {
		return err
	}
	reachabilityStatus, consistencyStatus, capacityStatus := checkTargets(targets)

	fmt.Printf("\n%s Reachability %s\n", reachabilityStatus, hint("-> Are all targets responding?"))
	fmt.Printf("%s Consistency %s\n", consistencyStatus, hint("-> Are all mirrors synchronized?"))
	fmt.Printf("%s Available Capacity %s\n\n", capacityStatus, hint("-> Are any targets low on free space based on the thresholds defined by the management service?"))

	unhealthyTargets := reachabilityStatus != Healthy || consistencyStatus != Healthy || capacityStatus != Healthy
	if unhealthyTargets || frontendCfg.printDF {
		if unhealthyTargets {
			failedCheck = true
		}
		printDF(ctx, targets, tgtFrontend.PrintConfig{Capacity: true, State: true})
	}

	fmt.Print(hint("HINT: This mode does not check file system consistency. To check for file system inconsistencies,\n      you can run 'beegfs-fsck --checkfs --readOnly' and consult with ThinkParQ support.\n"))

	fmt.Println()

	printHeader(">>>>> Checking Connections to Server Nodes <<<<<", "#")
	log.Debug("getting filtered client list")
	procCtx, procCtxCancel := context.WithTimeout(ctx, frontendCfg.connectionTimeout)
	clients, err := getFilteredClientList(procCtx, false, filterByMounts, backendCfg)
	procCtxCancel()
	if err != nil {
		if !errors.Is(err, procfs.ErrEstablishingConnections) {
			return err
		}
		fmt.Printf("WARNING: Error establishing new connections, further connection checks may be incomplete or skipped: %s (ignoring)\n", err)
		fmt.Print(hint(fmt.Sprintf("HINT: Try increasing the '--%s' flag or setting '--%s=false` to skip establishing new connections.\n\n", connectionTimeoutFlag, forceConnectionsFlag)))
	}

	if len(clients) == 0 {
		fmt.Printf("WARNING: No client mounts found, skipping connection checks\n\n")
	} else if len(clients) > 1 {
		fmt.Print(hint("\nHINT: Multiple client mounts detected, connections will be checked from each mount point (specify <mount-path> if this is not what you want).\n\n"))
	}

	for _, c := range clients {
		printClientHeader(c, "=")
		netStatus := checkForFallbacks(c)
		fmt.Printf("\n%s Fallbacks %s\n", netStatus, hint("-> Are any connections using secondary NICs or protocols (such as Ethernet/TCP when RDMA is preferred)?"))
		if netStatus != Healthy {
			failedCheck = true
			fmt.Println()
			printBeeGFSNet(c)
		} else if frontendCfg.printNetworkConnections {
			fmt.Println()
			printBeeGFSNet(c)
		}
		fmt.Println()
	}

	// The following checks/text should be printed at the bottom. Don't add checks after this point.
	if failedCheck && !frontendCfg.ignoreFailedChecks {
		util.TerminalAlert()
		return util.NewCtlError(fmt.Errorf("one or more checks failed"), 1)
	}
	return nil
}

func checkForFallbacks(client procfs.Client) Status {
	hasFallbacks := func(nodes []procfs.Node) bool {
		for _, node := range nodes {
			for _, peer := range node.Peers {
				if peer.Fallback {
					return true
				}
			}
		}
		return false
	}
	if hasFallbacks(client.MgmtdNodes) || hasFallbacks(client.MetaNodes) || hasFallbacks(client.StorageNodes) {
		return Degraded
	}
	return Healthy
}

// checkTargets() returns the overall reachability, consistency, capacity of all targets and returns
// each status based on the target in the worst condition.
func checkTargets(targets []tgtBackend.GetTargets_Result) (Status, Status, Status) {
	reachability, consistency, capacity := Healthy, Healthy, Healthy
	for _, t := range targets {
		switch t.ReachabilityState {
		case tgtBackend.ReachabilityOnline:
			reachability.updateStatusIfWorse(Healthy)
		case tgtBackend.ReachabilityProbablyOffline:
			reachability.updateStatusIfWorse(Degraded)
		case tgtBackend.ReachabilityOffline:
			reachability.updateStatusIfWorse(Critical)
		}

		switch t.ConsistencyState {
		case tgtBackend.ConsistencyGood:
			consistency.updateStatusIfWorse(Healthy)
		case tgtBackend.ConsistencyNeedsResync:
			consistency.updateStatusIfWorse(Degraded)
		case tgtBackend.ConsistencyBad:
			consistency.updateStatusIfWorse(Critical)
		}

		switch t.CapacityPool {
		case tgtBackend.CapacityNormal:
			capacity.updateStatusIfWorse(Healthy)
		case tgtBackend.CapacityLow:
			capacity.updateStatusIfWorse(Degraded)
		case tgtBackend.CapacityEmergency:
			capacity.updateStatusIfWorse(Critical)
		}
	}
	return reachability, consistency, capacity
}

// checkForBusyNodes() checks if any nodes are busy and returns the overall status based on the node
// in the worst condition.
func checkForBusyNodes(nodes []stats.NodeStats) Status {
	busy := Healthy
	for _, node := range nodes {
		if node.Stats.QueuedRequests > queuedReqsCriticalThreshold {
			busy.updateStatusIfWorse(Critical)
		} else if node.Stats.QueuedRequests > queuedReqsDegradedThreshold {
			busy.updateStatusIfWorse(Degraded)
		}
	}
	return busy
}

func printBusyNodes(nodes []stats.NodeStats) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Node.Id.NumId < nodes[j].Node.Id.NumId
	})
	for _, n := range nodes {
		if n.Stats.QueuedRequests > queuedReqsDegradedThreshold {
			fmt.Printf("* %s [%s] has %d queued requests\n", n.Node.Alias, n.Node.Id, n.Stats.QueuedRequests)
		}
	}
}
