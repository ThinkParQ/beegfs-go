package health

import (
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
