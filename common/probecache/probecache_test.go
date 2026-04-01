package probecache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAvailabilityColdState(t *testing.T) {
	a := New(5 * time.Minute)
	assert.True(t, a.ShouldAttempt(), "expected ShouldAttempt to return true on a fresh Availability")
}

func TestAvailabilityAfterMarkUnavailable(t *testing.T) {
	a := New(5 * time.Minute)
	a.MarkUnavailable()
	assert.False(t, a.ShouldAttempt(), "expected ShouldAttempt to return false immediately after MarkUnavailable")
}

func TestAvailability_AfterWindowExpires(t *testing.T) {
	a := New(1 * time.Millisecond)
	a.MarkUnavailable()
	time.Sleep(5 * time.Millisecond)
	assert.True(t, a.ShouldAttempt(), "expected ShouldAttempt to return true after the recheck window expires")
}

func TestAvailabilityRepeatedMarkUnavailableDoesNotExtendWindow(t *testing.T) {
	ttl := 50 * time.Millisecond
	a := New(ttl)

	a.MarkUnavailable()
	firstDeadline := a.unavailableUntil

	// Calling MarkUnavailable again within the window should not change the deadline.
	time.Sleep(5 * time.Millisecond)
	a.MarkUnavailable()
	assert.Equal(t, a.unavailableUntil, firstDeadline, fmt.Sprintf("expected deadline to remain %v, got %v", firstDeadline, a.unavailableUntil))
}

func TestAvailabilityMarkUnavailableAfterWindowResetsDeadline(t *testing.T) {
	a := New(10 * time.Millisecond)
	a.MarkUnavailable()

	// Wait for the window to expire.
	time.Sleep(20 * time.Millisecond)

	assert.True(t, a.ShouldAttempt(), "expected ShouldAttempt to return true after TTL expired")

	// A new MarkUnavailable after expiry should arm a fresh window.
	a.MarkUnavailable()
	assert.False(t, a.ShouldAttempt(), "expected ShouldAttempt to return false after re-arming")
}
