package dispatch

import (
	"sync"
	"time"
)

// userWindow holds the counters for a sliding window approximation. By tracking the previous
// window's count alongside the current window's count, we can estimate the rate across the boundary
// without storing individual timestamps. The approximation weights the previous window's count by
// how much of it still overlaps with the sliding window.
type userWindow struct {
	prevCount   int
	currCount   int
	windowStart time.Time
}

// rateLimiter implements a per-user sliding window rate limiter using a counter-based approximation.
// Each user requires only a few integers of state regardless of event volume. It is safe for
// concurrent use.
type rateLimiter struct {
	mu        sync.Mutex
	window    time.Duration
	maxEvents int
	users     map[uint32]*userWindow
	// cleanupCounter tracks calls to allow() to trigger periodic cleanup of idle users.
	cleanupCounter int
	// nowFunc returns the current time. Defaults to time.Now; overridden in tests.
	nowFunc func() time.Time
}

const cleanupInterval = 1000

func newRateLimiter(window time.Duration, maxEvents int) *rateLimiter {
	return &rateLimiter{
		window:    window,
		maxEvents: maxEvents,
		users:     make(map[uint32]*userWindow),
		nowFunc:   time.Now,
	}
}

// allow returns true if the user has not exceeded the rate limit. When allowed, the current
// window's counter is incremented. The effective count is approximated by weighting the previous
// window's count by its remaining overlap with the sliding window.
func (r *rateLimiter) allow(userId uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.nowFunc()

	uw := r.users[userId]
	if uw == nil {
		r.users[userId] = &userWindow{currCount: 1, windowStart: now}
		return true
	}

	r.advance(uw, now)

	// Approximate the sliding window count: the current window's count plus a weighted portion of
	// the previous window's count based on how much of it still overlaps.
	elapsed := now.Sub(uw.windowStart)
	weight := 1.0 - (float64(elapsed) / float64(r.window))
	if weight < 0 {
		weight = 0
	}
	estimate := float64(uw.prevCount)*weight + float64(uw.currCount)

	if estimate >= float64(r.maxEvents) {
		return false
	}

	uw.currCount++

	r.cleanupCounter++
	if r.cleanupCounter >= cleanupInterval {
		r.cleanup(now)
		r.cleanupCounter = 0
	}

	return true
}

// advance rotates windows as needed so that windowStart is within the current window. Must be
// called with mu held.
func (r *rateLimiter) advance(uw *userWindow, now time.Time) {
	elapsed := now.Sub(uw.windowStart)
	if elapsed < r.window {
		return
	}
	if elapsed < 2*r.window {
		// Rotate: current becomes previous.
		uw.prevCount = uw.currCount
		uw.currCount = 0
		uw.windowStart = uw.windowStart.Add(r.window)
	} else {
		// More than two windows have passed; all history is stale.
		uw.prevCount = 0
		uw.currCount = 0
		uw.windowStart = now
	}
}

// resetUser clears the rate limit state for a specific user, allowing them to immediately make
// requests again. This is intended for admin-initiated resets.
func (r *rateLimiter) resetUser(userId uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.users, userId)
}

// cleanup removes users whose windows are fully expired. Must be called with mu held.
func (r *rateLimiter) cleanup(now time.Time) {
	for userId, uw := range r.users {
		if now.Sub(uw.windowStart) >= 2*r.window {
			delete(r.users, userId)
		}
	}
}
