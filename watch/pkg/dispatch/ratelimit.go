package dispatch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
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

// interval is a single entry in the sorted interval table for range-based overrides.
type interval struct {
	start     uint32
	end       uint32
	maxEvents int
}

// overrideLookup provides O(1) lookups for exact user IDs and O(log N) lookups for range-based
// overrides. It is built once at startup and is immutable after construction.
type overrideLookup struct {
	// exact maps individual user IDs to their maxEvents limit.
	exact map[uint32]int
	// intervals is sorted by start for binary search. Intervals must not overlap.
	intervals []interval
}

// buildOverrideLookup parses config overrides and constructs the lookup in a single pass. Each
// element of user-ids is a comma-separated mix of single IDs ("1000") and inclusive ranges
// ("2000-2999"). Single IDs go into the exact map; ranges go into the sorted interval table.
// Returns an error if any user-ids string is malformed or if intervals overlap.
func buildOverrideLookup(overrides []RateLimitOverride) (*overrideLookup, error) {
	ol := &overrideLookup{exact: make(map[uint32]int)}

	for _, o := range overrides {
		found := false
		for _, part := range strings.Split(o.UserIDs, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			found = true

			if startStr, endStr, ok := strings.Cut(part, "-"); ok {
				s, err := strconv.ParseUint(strings.TrimSpace(startStr), 10, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid range start %q in %q: %w", startStr, o.UserIDs, err)
				}
				e, err := strconv.ParseUint(strings.TrimSpace(endStr), 10, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid range end %q in %q: %w", endStr, o.UserIDs, err)
				}
				if s > e {
					return nil, fmt.Errorf("range start %d is greater than end %d in %q", s, e, o.UserIDs)
				}
				ol.intervals = append(ol.intervals, interval{
					start:     uint32(s),
					end:       uint32(e),
					maxEvents: o.MaxEvents,
				})
			} else {
				id, err := strconv.ParseUint(part, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid user ID %q in %q: %w", part, o.UserIDs, err)
				}
				ol.exact[uint32(id)] = o.MaxEvents
			}
		}
		if !found {
			return nil, fmt.Errorf("no user IDs specified in %q", o.UserIDs)
		}
	}

	sort.Slice(ol.intervals, func(i, j int) bool {
		return ol.intervals[i].start < ol.intervals[j].start
	})

	for i := 1; i < len(ol.intervals); i++ {
		if ol.intervals[i].start <= ol.intervals[i-1].end {
			return nil, fmt.Errorf("overlapping rate-limit-override ranges: [%d-%d] and [%d-%d]",
				ol.intervals[i-1].start, ol.intervals[i-1].end,
				ol.intervals[i].start, ol.intervals[i].end)
		}
	}

	return ol, nil
}

// lookup returns the maxEvents override for the given user ID, or -1 if no override matches.
// Exact matches take priority over range matches.
func (ol *overrideLookup) lookup(userId uint32) int {
	if maxEvents, ok := ol.exact[userId]; ok {
		return maxEvents
	}

	// Binary search: find the last interval whose start <= userId.
	n := len(ol.intervals)
	i := sort.Search(n, func(i int) bool {
		return ol.intervals[i].start > userId
	}) - 1

	if i >= 0 && userId <= ol.intervals[i].end {
		return ol.intervals[i].maxEvents
	}

	return -1
}

// rateLimiter implements a per-user sliding window rate limiter using a counter-based approximation.
// Each user requires only a few integers of state regardless of event volume. It is safe for
// concurrent use.
type rateLimiter struct {
	mu        sync.Mutex
	window    time.Duration
	maxEvents int
	overrides *overrideLookup
	users     map[uint32]*userWindow
	// cleanupCounter tracks calls to allow() to trigger periodic cleanup of idle users.
	cleanupCounter int
	// nowFunc returns the current time. Defaults to time.Now; overridden in tests.
	nowFunc func() time.Time
}

const cleanupInterval = 1000

func newRateLimiter(window time.Duration, maxEvents int, overrides *overrideLookup) *rateLimiter {
	return &rateLimiter{
		window:    window,
		maxEvents: maxEvents,
		overrides: overrides,
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

	if estimate >= float64(r.maxEventsForUser(userId)) {
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

// maxEventsForUser returns the effective max events limit for the given user ID. Exact ID matches
// are O(1) via map lookup; range matches are O(log N) via binary search. Falls back to the global
// limit if no override matches. Must be called with mu held.
func (r *rateLimiter) maxEventsForUser(userId uint32) int {
	if r.overrides != nil {
		if maxEvents := r.overrides.lookup(userId); maxEvents >= 0 {
			return maxEvents
		}
	}
	return r.maxEvents
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
