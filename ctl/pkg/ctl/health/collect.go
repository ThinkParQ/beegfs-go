package health

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/strfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/license"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// QueuedReqsDegradedThreshold and QueuedReqsCriticalThreshold are the queued-request counts at which
// a node is considered degraded or critical. They are exported so the frontend and the structured
// output can reference them; they may become configurable later.
const (
	QueuedReqsDegradedThreshold = 16
	QueuedReqsCriticalThreshold = 512
)

// CollectConfig controls how the connection checks are performed. It only carries options that
// affect collection; how much detail is displayed is a rendering concern owned by the frontend.
type CollectConfig struct {
	// ConnectionTimeout bounds establishing new client/server connections for the connection check.
	ConnectionTimeout time.Duration
	// ForceConnections attempts to establish storage server connections (by running df) before the
	// connection check so they are not reported as <none>.
	ForceConnections bool
	// FilterByMounts limits the connection check to specific client mount paths (empty means all
	// mounts of the managed file system).
	FilterByMounts []string
}

// Collect runs all health checks and returns the results as a Report. It performs no output; the
// caller renders the Report as human-readable text or JSON.
//
// When updating this function consider if QuickChecks (in the frontend) needs to be updated as well.
func Collect(ctx context.Context, cfg CollectConfig) (*Report, error) {
	log, _ := config.GetLogger()

	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	fsUUID, err := mgmtd.GetFsUUID(ctx)
	// If the mgmtd is not available other checks cannot proceed and fail in confusing ways.
	if err != nil {
		// TLS errors are vague, offer some hints for common misconfigurations:
		if strings.Contains(err.Error(), "tls: first record does not look like a TLS handshake") {
			return nil, fmt.Errorf("unable to proceed without a working management node: %w\n(hint: TLS does not appear to be enabled on the management, try setting %s)", err, config.TlsDisableKey)
		}
		if strings.Contains(err.Error(), "error reading server preface: unexpected EOF") {
			return nil, fmt.Errorf("unable to proceed without a working management node: %w\n(hint: TLS appears to be enabled on the management but disabled in CTL, try unsetting %s and configure %s if needed)", err, config.TlsDisableKey, config.TlsCertFile)
		}
		if strings.Contains(err.Error(), "failed to verify certificate") {
			return nil, fmt.Errorf("unable to proceed without a working management node: %w\n(hint: TLS appears to be enabled on the management, verify CTL has the correct certificates installed at the path specified by %s or added to the system certificate chain)", err, config.TlsCertFile)
		}
		return nil, fmt.Errorf("unable to proceed without a working management node: %w", err)
	}

	report := &Report{
		FS:     fmt.Sprintf("beegfs://%s", mgmtd.GetAddress()),
		FsUUID: fsUUID,
	}

	// General checks. mgmtPeer is extracted from the GetTargets response to avoid a no-op gRPC just
	// to inspect the TLS configuration.
	var mgmtPeer peer.Peer
	targets, err := tgtBackend.GetTargets(ctx, grpc.Peer(&mgmtPeer))
	if err != nil {
		return nil, err
	}
	general := Section{Title: SectionGeneralChecks}
	tlsStatus, tlsMsg := CheckTLSCertificates(ctx, mgmtPeer)
	general.Checks = append(general.Checks, Check{Name: "Management TLS", Status: tlsStatus, Summary: tlsMsg + "."})
	licenseStatus, licenseSummary := checkLicense(ctx, targets)
	general.Checks = append(general.Checks, Check{Name: "License Status", Status: licenseStatus, Summary: licenseSummary})
	report.Sections = append(report.Sections, general)

	// Busy nodes.
	metaNodes, err := stats.MultiServerNodes(ctx, beegfs.Meta)
	if err != nil {
		return nil, err
	}
	storageNodes, err := stats.MultiServerNodes(ctx, beegfs.Storage)
	if err != nil {
		return nil, err
	}
	busy := Section{Title: SectionBusyNodes}
	metaBusy := checkForBusyNodes(metaNodes)
	busy.Checks = append(busy.Checks, Check{Name: "Busy Metadata Nodes", Status: metaBusy, Summary: busySummary(metaBusy), Detail: newBusyDetail(metaNodes)})
	storageBusy := checkForBusyNodes(storageNodes)
	busy.Checks = append(busy.Checks, Check{Name: "Busy Storage Nodes", Status: storageBusy, Summary: busySummary(storageBusy), Detail: newBusyDetail(storageNodes)})
	report.Sections = append(report.Sections, busy)

	// Targets.
	reachability, consistency, capacity, mapping := CheckTargets(targets)
	targetsSection := Section{Title: SectionTargets, Detail: newTargetsDetail(targets)}
	targetsSection.Checks = append(targetsSection.Checks,
		Check{Name: "Reachability", Status: reachability, Summary: healthyOr(reachability, "All targets are responding.", "Not all targets are responding.")},
		Check{Name: "Consistency", Status: consistency, Summary: healthyOr(consistency, "All mirrors are synchronized.", "Not all mirrors are synchronized.")},
		Check{Name: "Available Capacity", Status: capacity, Summary: healthyOr(capacity, "All targets have sufficient free space based on the thresholds defined by the management service's configuration.", "Not all targets have sufficient free space based on the thresholds defined by the management service's configuration.")},
		Check{Name: "Mapping Status", Status: mapping, Summary: healthyOr(mapping, "All targets are mapped to a storage node.", "Not all targets are mapped to a storage node.")},
	)
	report.Sections = append(report.Sections, targetsSection)

	// Connections to server nodes.
	connections := Section{Title: SectionConnections}
	procCtx, cancel := context.WithTimeout(ctx, cfg.ConnectionTimeout)
	clients, err := procfs.GetBeeGFSClients(procCtx, procfs.GetBeeGFSClientsConfig{
		ForceConnections: cfg.ForceConnections,
		FilterByUUID:     fsUUID,
		FilterByMounts:   cfg.FilterByMounts,
	}, log)
	cancel()
	if err != nil {
		if !errors.Is(err, procfs.ErrEstablishingConnections) {
			return nil, err
		}
		// Non-fatal: proceed with whatever clients were found. The frontend surfaces this to stderr.
		report.ConnCheckErr = err
	}
	clientConns := make([]ClientConn, 0, len(clients))
	fallbackStatus := Healthy
	for _, client := range clients {
		s := checkForFallbacks(client)
		fallbackStatus.updateStatusIfWorse(s)
		clientConns = append(clientConns, ClientConn{
			ID:           client.ID,
			Mount:        client.Mount.Path,
			Status:       s,
			Summary:      healthyOr(s, "All connections are using preferred NICs and protocols.", "Not all connections are using preferred NICs or protocols (such as Ethernet/TCP when RDMA is preferred)."),
			MgmtdNodes:   nodeConns(client.MgmtdNodes),
			MetaNodes:    nodeConns(client.MetaNodes),
			StorageNodes: nodeConns(client.StorageNodes),
			Raw:          client,
		})
	}
	if len(clientConns) > 0 {
		// A single aggregate status across all client mounts; the per-client breakdown is in Detail.
		connections.Checks = append(connections.Checks, Check{
			Name:    "Fallbacks",
			Status:  fallbackStatus,
			Summary: healthyOr(fallbackStatus, "All connections are using preferred NICs and protocols.", "Not all connections are using preferred NICs or protocols (such as Ethernet/TCP when RDMA is preferred)."),
		})
	}
	connections.Detail = ConnectionsDetail{Clients: clientConns}
	report.Sections = append(report.Sections, connections)

	report.Status = report.worst()
	return report, nil
}

// healthyOr returns healthyMsg when s is Healthy and unhealthyMsg otherwise.
func healthyOr(s Status, healthyMsg, unhealthyMsg string) string {
	if s == Healthy {
		return healthyMsg
	}
	return unhealthyMsg
}

func busySummary(s Status) string {
	if s == Healthy {
		return fmt.Sprintf("Number of queued requests does not exceed the degraded (%d) or critical (%d) thresholds.", QueuedReqsDegradedThreshold, QueuedReqsCriticalThreshold)
	}
	return fmt.Sprintf("Number of queued requests exceeds the degraded (%d) or critical (%d) thresholds.", QueuedReqsDegradedThreshold, QueuedReqsCriticalThreshold)
}

// checkLicense maps a license check result to a status and human-readable summary.
func checkLicense(ctx context.Context, targets []tgtBackend.GetTargets_Result) (Status, string) {
	result := license.Check(ctx, targets)
	switch {
	case result.IsHealthy():
		return Healthy, "No issues detected. Run 'beegfs license' for more details."
	case result.Err != nil:
		return Critical, fmt.Sprintf("Error checking status (%s).", result.Err)
	case result.InvalidMsg != "":
		return Critical, "No valid license found. Run 'beegfs license' for more details."
	case result.ViolationsMsg != "":
		return Critical, fmt.Sprintf("Violations found (%s). Run 'beegfs license' for more details.", result.ViolationsMsg)
	case result.ExpirationMsg != "":
		return Degraded, fmt.Sprintf("Nearing expiration (%s). Run 'beegfs license' for more details.", result.ExpirationMsg)
	default:
		// If a check failed but couldn't be mapped to a known message, a new result was probably
		// added and this mapping needs to be updated.
		return Critical, "Unknown (run 'beegfs license' for details)."
	}
}

// checkForFallbacks returns Degraded if any of the client's connections are using a fallback NIC or
// protocol, otherwise Healthy.
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

// CheckTargets returns the reachability, consistency, capacity, and mapping status across all
// targets, each reflecting the target in the worst condition.
func CheckTargets(targets []tgtBackend.GetTargets_Result) (reachability, consistency, capacity, mapping Status) {
	reachability, consistency, capacity, mapping = Healthy, Healthy, Healthy, Healthy
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

		if t.Node == nil {
			mapping = Degraded
		}
	}
	return reachability, consistency, capacity, mapping
}

// checkForBusyNodes returns the overall busy status across nodes, reflecting the node in the worst
// condition.
func checkForBusyNodes(nodes []stats.NodeStats) Status {
	busy := Healthy
	for _, node := range nodes {
		if node.Stats.QueuedRequests > QueuedReqsCriticalThreshold {
			busy.updateStatusIfWorse(Critical)
		} else if node.Stats.QueuedRequests > QueuedReqsDegradedThreshold {
			busy.updateStatusIfWorse(Degraded)
		}
	}
	return busy
}

// CheckTLSCertificates checks the TLS certificates presented by a peer service and returns a health
// status based on the earliest expiring certificate in the chain.
func CheckTLSCertificates(ctx context.Context, peer peer.Peer) (Status, string) {
	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return Degraded, "Not in use"
	}
	return tlsCertExpirationStatus(tlsInfo.State.PeerCertificates, time.Now())
}

// tlsCertExpirationStatus determines the health status based on the earliest expiring certificate.
// It accepts the current time as a parameter to enable deterministic testing.
func tlsCertExpirationStatus(certs []*x509.Certificate, now time.Time) (Status, string) {
	if len(certs) == 0 {
		return Critical, "No certificates were presented by the node"
	}

	earliest := certs[0].NotAfter
	for _, cert := range certs[1:] {
		if cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}

	remaining := earliest.Sub(now)
	expirationMsg := "Certificate " + strfmt.ExpirationString(remaining)
	switch {
	case remaining <= 0:
		return Critical, expirationMsg
	case remaining < 30*24*time.Hour:
		return Critical, expirationMsg
	case remaining < 90*24*time.Hour:
		return Degraded, expirationMsg
	default:
		return Healthy, expirationMsg
	}
}
