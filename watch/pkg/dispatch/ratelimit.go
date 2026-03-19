package dispatch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/beewatch"
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

// userEventKey is a compound key for O(1) lookups keyed by both user ID and event type.
type userEventKey struct {
	userId    uint32
	eventType beewatch.V2Event_Type
}

// interval is a single entry in the sorted interval table for range-based overrides. Each interval
// carries per-event-type limits and an optional wildcard limit.
type interval struct {
	start       uint32
	end         uint32
	maxByType   map[beewatch.V2Event_Type]int // specific event type limits
	hasWildcard bool                          // whether a wildcard limit is set
	wildcard    int                           // maxEvents for wildcard (only valid if hasWildcard)
}

// overrideLookup provides O(1) lookups for exact user IDs and O(log N) + O(1) lookups for
// range-based overrides. It is built once at startup and is immutable after construction.
type overrideLookup struct {
	// exact maps (userId, eventType) to maxEvents for specific event type overrides. O(1).
	exact map[userEventKey]int
	// exactWild maps userId to maxEvents for wildcard (event-types=["*"]) overrides. O(1).
	exactWild map[uint32]int
	// intervals is sorted by start for binary search. Intervals must not overlap.
	intervals []interval
}

// rangeKey identifies a unique user-id range for merging overrides during construction.
type rangeKey struct {
	start uint32
	end   uint32
}

// parseEventTypes converts config event type strings to typed values. Returns the list of specific
// event types and whether a wildcard ("*") was present. If types is empty, wildcard defaults to
// true for backward compatibility.
func parseEventTypes(types []string) (specific []beewatch.V2Event_Type, wildcard bool, err error) {
	if len(types) == 0 {
		return nil, true, nil
	}
	for _, t := range types {
		if t == "*" {
			wildcard = true
		} else {
			v, ok := beewatch.V2Event_Type_value[t]
			if !ok {
				return nil, false, fmt.Errorf("invalid event type %q", t)
			}
			specific = append(specific, beewatch.V2Event_Type(v))
		}
	}
	return specific, wildcard, nil
}

// buildOverrideLookup parses config overrides and constructs the lookup. Each element of user-ids
// is a comma-separated mix of single IDs ("1000") and inclusive ranges ("2000-2999"). Single IDs
// go into the exact maps; ranges go into the sorted interval table. Multiple overrides with the
// same range but different event types are merged into a single interval entry. Returns an error if
// any user-ids string is malformed, any event type is invalid, or if intervals overlap.
func buildOverrideLookup(overrides []RateLimitOverride) (*overrideLookup, error) {
	ol := &overrideLookup{
		exact:     make(map[userEventKey]int),
		exactWild: make(map[uint32]int),
	}

	// Temporary map to accumulate per-range event type mappings before converting to intervals.
	rangeMappings := make(map[rangeKey]*interval)

	for _, o := range overrides {
		specific, wildcard, err := parseEventTypes(o.EventTypes)
		if err != nil {
			return nil, fmt.Errorf("in override for user-ids %q: %w", o.UserIDs, err)
		}

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

				rk := rangeKey{start: uint32(s), end: uint32(e)}
				iv := rangeMappings[rk]
				if iv == nil {
					iv = &interval{start: rk.start, end: rk.end, maxByType: make(map[beewatch.V2Event_Type]int)}
					rangeMappings[rk] = iv
				}
				for _, et := range specific {
					iv.maxByType[et] = o.MaxEvents
				}
				if wildcard {
					iv.hasWildcard = true
					iv.wildcard = o.MaxEvents
				}
			} else {
				id, err := strconv.ParseUint(part, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid user ID %q in %q: %w", part, o.UserIDs, err)
				}
				uid := uint32(id)
				for _, et := range specific {
					ol.exact[userEventKey{uid, et}] = o.MaxEvents
				}
				if wildcard {
					ol.exactWild[uid] = o.MaxEvents
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("no user IDs specified in %q", o.UserIDs)
		}
	}

	// Convert range mappings to sorted interval list.
	ol.intervals = make([]interval, 0, len(rangeMappings))
	for _, iv := range rangeMappings {
		ol.intervals = append(ol.intervals, *iv)
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

// lookup returns the maxEvents override for the given user ID and event type, or -1 if no override
// matches. Precedence: exact (userId, eventType) > exact wildcard > range specific type > range
// wildcard.
func (ol *overrideLookup) lookup(userId uint32, eventType beewatch.V2Event_Type) int {
	if v, ok := ol.exact[userEventKey{userId, eventType}]; ok {
		return v
	}
	if v, ok := ol.exactWild[userId]; ok {
		return v
	}

	// Binary search: find the last interval whose start <= userId.
	n := len(ol.intervals)
	i := sort.Search(n, func(i int) bool {
		return ol.intervals[i].start > userId
	}) - 1

	if i >= 0 && userId <= ol.intervals[i].end {
		if v, ok := ol.intervals[i].maxByType[eventType]; ok {
			return v
		}
		if ol.intervals[i].hasWildcard {
			return ol.intervals[i].wildcard
		}
	}

	return -1
}

// rateLimiter implements a per-user, per-event-type sliding window rate limiter using a
// counter-based approximation. Each (user, eventType) pair requires only a few integers of state
// regardless of event volume. It is safe for concurrent use.
type rateLimiter struct {
	mu     sync.Mutex
	window time.Duration
	// maxEvents is the default limit applied to event types listed in defaultEventTypes.
	maxEvents int
	// allEventTypes is true when the default limit applies to all event types (rate-limit-event-types
	// is ["*"] or absent). When true, lookups shortcircuit without checking defaultEventTypes.
	allEventTypes bool
	// defaultEventTypes lists the specific event types the default maxEvents limit applies to.
	// Only consulted when allEventTypes is false.
	defaultEventTypes map[beewatch.V2Event_Type]bool
	overrides         *overrideLookup
	// users maps userId -> eventType -> sliding window state.
	users map[uint32]map[beewatch.V2Event_Type]*userWindow
	// cleanupCounter tracks calls to record() to trigger periodic cleanup of idle users.
	cleanupCounter int
	// nowFunc returns the current time. Defaults to time.Now; overridden in tests.
	nowFunc func() time.Time
}

const cleanupInterval = 1000

func newRateLimiter(cfg Config) (*rateLimiter, error) {
	overrides, err := buildOverrideLookup(cfg.RateLimitOverrides)
	if err != nil {
		return nil, fmt.Errorf("invalid rate-limit-override configuration: %w", err)
	}

	specific, wildcard, err := parseEventTypes(cfg.RateLimitEventTypes)
	if err != nil {
		return nil, fmt.Errorf("invalid rate-limit-event-types: %w", err)
	}

	allEventTypes := wildcard
	var defaultEventTypes map[beewatch.V2Event_Type]bool
	if !allEventTypes && len(specific) > 0 {
		defaultEventTypes = make(map[beewatch.V2Event_Type]bool, len(specific))
		for _, et := range specific {
			defaultEventTypes[et] = true
		}
	}

	if cfg.RateLimitWindow == 0 {
		cfg.RateLimitWindow = 5 * time.Minute
	}

	return &rateLimiter{
		window:            cfg.RateLimitWindow,
		maxEvents:         cfg.RateLimitMaxEvents,
		allEventTypes:     allEventTypes,
		defaultEventTypes: defaultEventTypes,
		overrides:         overrides,
		users:             make(map[uint32]map[beewatch.V2Event_Type]*userWindow),
		nowFunc:           time.Now,
	}, nil
}

// check returns true if the user has not exceeded the rate limit for the given event type. It
// initializes the user's window if necessary and advances it, but does NOT increment the event
// count. Call record() after a successful dispatch to charge the event against the rate limit.
func (r *rateLimiter) check(userId uint32, eventType beewatch.V2Event_Type) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.nowFunc()

	byType := r.users[userId]
	if byType == nil {
		byType = make(map[beewatch.V2Event_Type]*userWindow)
		r.users[userId] = byType
	}

	uw := byType[eventType]
	if uw == nil {
		uw = &userWindow{windowStart: now}
		byType[eventType] = uw
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

	return estimate < float64(r.maxEventsFor(userId, eventType))
}

// record increments the event count for the given user and event type. It should be called after
// check() returns true and the dispatch function confirms that the event triggered an action.
func (r *rateLimiter) record(userId uint32, eventType beewatch.V2Event_Type) {
	r.mu.Lock()
	defer r.mu.Unlock()

	byType := r.users[userId]
	if byType == nil {
		byType = make(map[beewatch.V2Event_Type]*userWindow)
		r.users[userId] = byType
	}

	uw := byType[eventType]
	if uw == nil {
		uw = &userWindow{windowStart: r.nowFunc()}
		byType[eventType] = uw
	}

	uw.currCount++

	r.cleanupCounter++
	if r.cleanupCounter >= cleanupInterval {
		r.cleanup(r.nowFunc())
		r.cleanupCounter = 0
	}
}

// maxEventsFor returns the effective max events limit for the given user ID and event type.
// Checks overrides first, then falls back to the default limit if the event type is covered.
// Returns 0 for event types not covered by any override or default. Must be called with mu held.
func (r *rateLimiter) maxEventsFor(userId uint32, eventType beewatch.V2Event_Type) int {
	if r.overrides != nil {
		if v := r.overrides.lookup(userId, eventType); v >= 0 {
			return v
		}
	}
	if r.allEventTypes || r.defaultEventTypes[eventType] {
		return r.maxEvents
	}
	return 0
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

// resetUser clears the rate limit state for a specific user across all event types, allowing them
// to immediately make requests again. This is intended for admin-initiated resets.
func (r *rateLimiter) resetUser(userId uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.users, userId)
}

// cleanup removes users whose windows are fully expired. Must be called with mu held.
func (r *rateLimiter) cleanup(now time.Time) {
	for userId, byType := range r.users {
		for et, uw := range byType {
			if now.Sub(uw.windowStart) >= 2*r.window {
				delete(byType, et)
			}
		}
		if len(byType) == 0 {
			delete(r.users, userId)
		}
	}
}
