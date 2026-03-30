package dispatch

import (
	"testing"
	"time"
)

// newTestRateLimiter creates a rateLimiter with a controllable clock.
func newTestRateLimiter(window time.Duration, maxEvents int) (*rateLimiter, *time.Time) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl := newRateLimiter(window, maxEvents)
	rl.nowFunc = func() time.Time { return now }
	return rl, &now
}

func TestAllow_FirstEventAlwaysAllowed(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 3)
	if !rl.allow(1) {
		t.Fatal("first event for a new user should be allowed")
	}
}

func TestAllow_RejectsAtLimit(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 3)

	for i := range 3 {
		if !rl.allow(1) {
			t.Fatalf("event %d should be allowed", i+1)
		}
	}
	if rl.allow(1) {
		t.Fatal("4th event should be rejected (limit is 3)")
	}
}

func TestAllow_IndependentUsers(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 2)

	// User 1 hits limit.
	rl.allow(1)
	rl.allow(1)
	if rl.allow(1) {
		t.Fatal("user 1 should be rate limited")
	}

	// User 2 is unaffected.
	if !rl.allow(2) {
		t.Fatal("user 2 should not be affected by user 1's limit")
	}
}

func TestAllow_ResetsAfterFullWindowElapsed(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 3)

	// Exhaust limit.
	for range 3 {
		rl.allow(1)
	}
	if rl.allow(1) {
		t.Fatal("should be rate limited")
	}

	// Advance past two full windows so all history is stale.
	*now = now.Add(11 * time.Minute)

	if !rl.allow(1) {
		t.Fatal("should be allowed after two full windows have elapsed")
	}
}

func TestAllow_WindowRotation(t *testing.T) {
	rl, now := newTestRateLimiter(10*time.Minute, 5)

	// Fill up 4 events in the first window.
	for range 4 {
		rl.allow(1)
	}

	// Advance into the next window (just past boundary).
	*now = now.Add(10*time.Minute + 1*time.Second)

	// After rotation: prevCount=4, currCount=0.
	// weight ≈ 1.0 - (1s / 10m) ≈ 0.998, estimate ≈ 4*0.998 + 0 ≈ 3.99.
	// Should allow since estimate < 5.
	if !rl.allow(1) {
		t.Fatal("should be allowed right after window rotation")
	}
}

func TestAllow_SlidingWeightDecay(t *testing.T) {
	rl, now := newTestRateLimiter(10*time.Minute, 10)

	// Fill exactly 10 events (hit the limit).
	for range 10 {
		rl.allow(1)
	}
	if rl.allow(1) {
		t.Fatal("should be rate limited at 10 events")
	}

	// Advance to just past the window boundary. prevCount=10, currCount=0.
	// weight ≈ 0.998, estimate ≈ 10*0.998 ≈ 9.98 — just below limit, so one event is allowed.
	*now = now.Add(10*time.Minute + 1*time.Second)
	if !rl.allow(1) {
		t.Fatal("should be allowed right after rotation (estimate just under limit)")
	}

	// Now currCount=1. estimate ≈ 10*0.998 + 1 ≈ 10.98 — over limit.
	if rl.allow(1) {
		t.Fatal("should be limited after one event brings estimate over limit")
	}

	// Advance to halfway through the new window. weight=0.5, estimate=10*0.5+1=6.
	*now = now.Add(5*time.Minute - 1*time.Second)
	if !rl.allow(1) {
		t.Fatal("should be allowed once previous window weight has decayed enough")
	}
}

func TestAllow_StaleHistoryCleared(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 3)

	// User triggers events.
	rl.allow(1)
	rl.allow(1)

	// Advance past 2 full windows.
	*now = now.Add(15 * time.Minute)

	// Should behave as if the user is brand new (advance clears all state).
	if !rl.allow(1) {
		t.Fatal("should be allowed after all history is stale")
	}

	uw := rl.users[1]
	if uw.prevCount != 0 {
		t.Fatalf("prevCount should be 0 after stale reset, got %d", uw.prevCount)
	}
	if uw.currCount != 1 {
		t.Fatalf("currCount should be 1 after one allowed event, got %d", uw.currCount)
	}
}

func TestCleanup_RemovesExpiredUsers(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 100)

	rl.allow(1)
	rl.allow(2)
	rl.allow(3)

	// Advance past 2 full windows so all users are stale.
	*now = now.Add(11 * time.Minute)

	rl.cleanup(*now)

	if len(rl.users) != 0 {
		t.Fatalf("expected 0 users after cleanup, got %d", len(rl.users))
	}
}

func TestCleanup_RetainsActiveUsers(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 100)

	rl.allow(1)
	rl.allow(2)

	// Advance so user 1 and 2 are stale.
	*now = now.Add(11 * time.Minute)

	// User 3 is active.
	rl.allow(3)

	rl.cleanup(*now)

	if _, ok := rl.users[1]; ok {
		t.Fatal("user 1 should have been cleaned up")
	}
	if _, ok := rl.users[2]; ok {
		t.Fatal("user 2 should have been cleaned up")
	}
	if _, ok := rl.users[3]; !ok {
		t.Fatal("user 3 should still be present")
	}
}

func TestCleanup_TriggeredPeriodically(t *testing.T) {
	rl, now := newTestRateLimiter(1*time.Minute, cleanupInterval+10)

	// Create a user that will be stale after we advance time.
	rl.allow(99)
	*now = now.Add(3 * time.Minute)

	// Issue cleanupInterval+1 allowed events for user 1 to trigger cleanup. The +1 accounts for
	// the first call creating a new user entry without incrementing the cleanup counter.
	for range cleanupInterval + 1 {
		rl.allow(1)
	}

	if _, ok := rl.users[99]; ok {
		t.Fatal("stale user 99 should have been cleaned up after cleanupInterval calls")
	}
}

func TestAdvance_NoRotationWithinWindow(t *testing.T) {
	rl, now := newTestRateLimiter(10*time.Minute, 5)

	rl.allow(1)
	rl.allow(1)

	*now = now.Add(3 * time.Minute)
	rl.allow(1)

	uw := rl.users[1]
	if uw.prevCount != 0 {
		t.Fatalf("prevCount should be 0 within the same window, got %d", uw.prevCount)
	}
	if uw.currCount != 3 {
		t.Fatalf("currCount should be 3, got %d", uw.currCount)
	}
}

func TestAdvance_SingleRotation(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 100)
	start := *now

	rl.allow(1)
	rl.allow(1)
	rl.allow(1)

	// Advance into second window.
	*now = now.Add(7 * time.Minute)
	rl.allow(1)

	uw := rl.users[1]
	if uw.prevCount != 3 {
		t.Fatalf("prevCount should be 3 after rotation, got %d", uw.prevCount)
	}
	if uw.currCount != 1 {
		t.Fatalf("currCount should be 1 after one event in new window, got %d", uw.currCount)
	}
	expectedStart := start.Add(5 * time.Minute)
	if !uw.windowStart.Equal(expectedStart) {
		t.Fatalf("windowStart should be %v, got %v", expectedStart, uw.windowStart)
	}
}

func TestAdvance_DoubleWindowSkip(t *testing.T) {
	rl, now := newTestRateLimiter(5*time.Minute, 100)

	rl.allow(1)
	rl.allow(1)

	// Advance past two full windows.
	target := now.Add(15 * time.Minute)
	*now = target
	rl.allow(1)

	uw := rl.users[1]
	if uw.prevCount != 0 {
		t.Fatalf("prevCount should be 0 after double-window skip, got %d", uw.prevCount)
	}
	if uw.currCount != 1 {
		t.Fatalf("currCount should be 1, got %d", uw.currCount)
	}
	if !uw.windowStart.Equal(target) {
		t.Fatalf("windowStart should be reset to now (%v), got %v", target, uw.windowStart)
	}
}

func TestResetUser_ClearsRateLimit(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 3)

	// Exhaust limit.
	for range 3 {
		rl.allow(1)
	}
	if rl.allow(1) {
		t.Fatal("should be rate limited")
	}

	rl.resetUser(1)

	if !rl.allow(1) {
		t.Fatal("should be allowed after reset")
	}
}

func TestResetUser_DoesNotAffectOtherUsers(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 2)

	rl.allow(1)
	rl.allow(1)
	rl.allow(2)
	rl.allow(2)

	// Reset only user 1.
	rl.resetUser(1)

	if !rl.allow(1) {
		t.Fatal("user 1 should be allowed after reset")
	}
	if rl.allow(2) {
		t.Fatal("user 2 should still be rate limited")
	}
}

func TestResetUser_NonexistentUserIsNoop(t *testing.T) {
	rl, _ := newTestRateLimiter(5*time.Minute, 3)
	// Should not panic.
	rl.resetUser(999)
}
