package health

import (
	"context"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

func TestCollectConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		degraded    uint32
		critical    uint32
		expectedErr string
	}{
		{
			name:     "defaults are valid",
			degraded: DefaultQueuedReqsDegradedThreshold,
			critical: DefaultQueuedReqsCriticalThreshold,
		},
		{
			name:     "zero degraded threshold is valid",
			degraded: 0,
			critical: 1,
		},
		{
			name:        "zero value is rejected",
			expectedErr: "queued requests degraded threshold (0) must be less than the critical threshold (0)",
		},
		{
			name:        "equal thresholds are rejected",
			degraded:    100,
			critical:    100,
			expectedErr: "queued requests degraded threshold (100) must be less than the critical threshold (100)",
		},
		{
			name:        "degraded above critical is rejected",
			degraded:    600,
			critical:    DefaultQueuedReqsCriticalThreshold,
			expectedErr: "queued requests degraded threshold (600) must be less than the critical threshold (512)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CollectConfig{QueuedReqsDegradedThreshold: tc.degraded, QueuedReqsCriticalThreshold: tc.critical}.Validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.expectedErr, err.Error())
		})
	}
}

func TestCollectRejectsInvalidConfigBeforeContactingServices(t *testing.T) {
	// No management service is configured in unit tests. Collect must fail on the config before it
	// tries to reach one.
	_, err := Collect(context.Background(), CollectConfig{})
	require.Error(t, err)
	assert.Equal(t, "queued requests degraded threshold (0) must be less than the critical threshold (0)", err.Error())
}

func TestCheckForBusyNodes(t *testing.T) {
	nodesWith := func(queuedReqs ...uint32) []stats.NodeStats {
		ns := make([]stats.NodeStats, 0, len(queuedReqs))
		for _, q := range queuedReqs {
			ns = append(ns, stats.NodeStats{Stats: stats.Stats{QueuedRequests: q}})
		}
		return ns
	}
	unreadable := stats.NodeStats{Err: errors.New("connection refused")}

	tests := []struct {
		name           string
		nodes          []stats.NodeStats
		degraded       uint32
		critical       uint32
		expectedStatus Status
	}{
		{
			name:           "no nodes",
			nodes:          nil,
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Healthy,
		},
		{
			name:           "at the degraded threshold is still healthy",
			nodes:          nodesWith(0, DefaultQueuedReqsDegradedThreshold),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Healthy,
		},
		{
			name:           "past the degraded threshold is degraded",
			nodes:          nodesWith(0, DefaultQueuedReqsDegradedThreshold+1),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Degraded,
		},
		{
			name:           "at the critical threshold is degraded",
			nodes:          nodesWith(DefaultQueuedReqsCriticalThreshold),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Degraded,
		},
		{
			name:           "worst node determines the status",
			nodes:          nodesWith(0, DefaultQueuedReqsDegradedThreshold+1, DefaultQueuedReqsCriticalThreshold+1),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Critical,
		},
		{
			name:           "lowered thresholds flag nodes the defaults would not",
			nodes:          nodesWith(4),
			degraded:       2,
			critical:       8,
			expectedStatus: Degraded,
		},
		{
			name:           "raised thresholds ignore nodes the defaults would flag",
			nodes:          nodesWith(DefaultQueuedReqsCriticalThreshold + 1),
			degraded:       DefaultQueuedReqsCriticalThreshold * 2,
			critical:       DefaultQueuedReqsCriticalThreshold * 4,
			expectedStatus: Healthy,
		},
		{
			name:           "zero degraded threshold flags any queued request",
			nodes:          nodesWith(1),
			degraded:       0,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Degraded,
		},
		{
			name:           "unreadable node is critical, not healthy",
			nodes:          append(nodesWith(0), unreadable),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Critical,
		},
		{
			name:           "unreadable node is critical overriding a degraded node",
			nodes:          append(nodesWith(DefaultQueuedReqsDegradedThreshold+1), unreadable),
			degraded:       DefaultQueuedReqsDegradedThreshold,
			critical:       DefaultQueuedReqsCriticalThreshold,
			expectedStatus: Critical,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := checkForBusyNodes(tc.nodes, tc.degraded, tc.critical)
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}

func TestBusySummary(t *testing.T) {
	_, summary := checkForBusyNodes([]stats.NodeStats{{}}, 2, 8)
	assert.Equal(t, "Number of queued requests does not exceed the degraded (2) or critical (8) thresholds.", summary)

	_, summary = checkForBusyNodes([]stats.NodeStats{{Stats: stats.Stats{QueuedRequests: 9}}}, 2, 8)
	assert.Equal(t, "Number of queued requests exceeds the degraded (2) or critical (8) thresholds.", summary)

	_, summary = checkForBusyNodes([]stats.NodeStats{{}, {Err: errors.New("connection refused")}}, 2, 8)
	assert.Equal(t, "Unable to read stats from 1 of 2 nodes. Number of queued requests does not exceed the degraded (2) or critical (8) thresholds.", summary)
}

func TestCheckTargets(t *testing.T) {
	healthy := tgtBackend.GetTargets_Result{
		ReachabilityState: tgtBackend.ReachabilityOnline,
		ConsistencyState:  tgtBackend.ConsistencyGood,
		CapacityPool:      tgtBackend.CapacityNormal,
		Node:              &beegfs.EntityIdSet{},
	}
	with := func(modify func(*tgtBackend.GetTargets_Result)) tgtBackend.GetTargets_Result {
		t := healthy
		modify(&t)
		return t
	}

	tests := []struct {
		name         string
		target       tgtBackend.GetTargets_Result
		reachability Status
		consistency  Status
		capacity     Status
		mapping      Status
	}{
		{
			name:         "healthy target",
			target:       healthy,
			reachability: Healthy, consistency: Healthy, capacity: Healthy, mapping: Healthy,
		},
		{
			name:         "capacity pool not reported yet is degraded",
			target:       with(func(t *tgtBackend.GetTargets_Result) { t.CapacityPool = "" }),
			reachability: Healthy, consistency: Healthy, capacity: Degraded, mapping: Healthy,
		},
		{
			name:         "unknown reachability is degraded",
			target:       with(func(t *tgtBackend.GetTargets_Result) { t.ReachabilityState = "" }),
			reachability: Degraded, consistency: Healthy, capacity: Healthy, mapping: Healthy,
		},
		{
			name:         "unknown consistency is degraded",
			target:       with(func(t *tgtBackend.GetTargets_Result) { t.ConsistencyState = "" }),
			reachability: Healthy, consistency: Degraded, capacity: Healthy, mapping: Healthy,
		},
		{
			name: "known bad states are still critical",
			target: with(func(t *tgtBackend.GetTargets_Result) {
				t.ReachabilityState = tgtBackend.ReachabilityOffline
				t.CapacityPool = tgtBackend.CapacityEmergency
			}),
			reachability: Critical, consistency: Healthy, capacity: Critical, mapping: Healthy,
		},
		{
			name:         "unmapped target",
			target:       with(func(t *tgtBackend.GetTargets_Result) { t.Node = nil }),
			reachability: Healthy, consistency: Healthy, capacity: Healthy, mapping: Degraded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reachability, consistency, capacity, mapping := CheckTargets([]tgtBackend.GetTargets_Result{tc.target})
			assert.Equal(t, tc.reachability, reachability, "reachability")
			assert.Equal(t, tc.consistency, consistency, "consistency")
			assert.Equal(t, tc.capacity, capacity, "capacity")
			assert.Equal(t, tc.mapping, mapping, "mapping")
		})
	}
}

func TestTLSCertExpirationStatus(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		notAfter       time.Time
		expectedStatus Status
		expectedMsg    string
	}{
		{
			name:           "valid certificate with plenty of time",
			notAfter:       now.Add(365 * 24 * time.Hour),
			expectedStatus: Healthy,
			expectedMsg:    "Certificate expires in 365 days",
		},
		{
			name:           "exactly 90 days remaining is healthy",
			notAfter:       now.Add(90 * 24 * time.Hour),
			expectedStatus: Healthy,
			expectedMsg:    "Certificate expires in 90 days",
		},
		{
			name:           "just under 90 days is degraded",
			notAfter:       now.Add(89*24*time.Hour + 23*time.Hour),
			expectedStatus: Degraded,
			expectedMsg:    "Certificate expires in 90 days",
		},
		{
			name:           "45 days remaining is degraded",
			notAfter:       now.Add(45 * 24 * time.Hour),
			expectedStatus: Degraded,
			expectedMsg:    "Certificate expires in 45 days",
		},
		{
			name:           "exactly 30 days remaining is degraded",
			notAfter:       now.Add(30 * 24 * time.Hour),
			expectedStatus: Degraded,
			expectedMsg:    "Certificate expires in 30 days",
		},
		{
			name:           "just under 30 days is critical",
			notAfter:       now.Add(29*24*time.Hour + 23*time.Hour),
			expectedStatus: Critical,
			expectedMsg:    "Certificate expires in 30 days",
		},
		{
			name:           "7 days remaining is critical",
			notAfter:       now.Add(7 * 24 * time.Hour),
			expectedStatus: Critical,
			expectedMsg:    "Certificate expires in 7 days",
		},
		{
			name:           "1 day remaining is critical",
			notAfter:       now.Add(24 * time.Hour),
			expectedStatus: Critical,
			expectedMsg:    "Certificate expires in 1 day",
		},
		{
			name:           "already expired",
			notAfter:       now.Add(-5 * 24 * time.Hour),
			expectedStatus: Critical,
			expectedMsg:    "Certificate expired 5 days ago",
		},
		{
			name:           "expired less than a day ago",
			notAfter:       now.Add(-12 * time.Hour),
			expectedStatus: Critical,
			expectedMsg:    "Certificate expired 12 hours ago",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			certs := []*x509.Certificate{{NotAfter: tc.notAfter}}
			status, msg := tlsCertExpirationStatus(certs, now)
			assert.Equal(t, tc.expectedStatus, status)
			assert.Equal(t, tc.expectedMsg, msg)
		})
	}
}

func TestTLSCertExpirationStatusUsesEarliestExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	certs := []*x509.Certificate{
		{NotAfter: now.Add(365 * 24 * time.Hour)}, // leaf: valid for a year
		{NotAfter: now.Add(20 * 24 * time.Hour)},  // intermediate: expires in 20 days
	}
	status, msg := tlsCertExpirationStatus(certs, now)
	assert.Equal(t, Critical, status)
	assert.Equal(t, "Certificate expires in 20 days", msg)
}

func TestTLSCertExpirationStatusNoCerts(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	status, msg := tlsCertExpirationStatus(nil, now)
	assert.Equal(t, Critical, status)
	assert.Contains(t, msg, "No certificates were presented by the node")
}
