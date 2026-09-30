package health

import (
	"context"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedStatus, checkForBusyNodes(tc.nodes, tc.degraded, tc.critical))
		})
	}
}

func TestBusySummaryReportsConfiguredThresholds(t *testing.T) {
	assert.Equal(t, "Number of queued requests does not exceed the degraded (2) or critical (8) thresholds.", busySummary(Healthy, 2, 8))
	assert.Equal(t, "Number of queued requests exceeds the degraded (2) or critical (8) thresholds.", busySummary(Critical, 2, 8))
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
