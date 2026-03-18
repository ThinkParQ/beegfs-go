// Package probecache provides a mechanism to cache negative feature/capability probe results. It is
// intended to optimize situations where a caller should first try to use a newer ioctl/RPC that
// might not be supported yet by the client/server, then fall back to an older ioctl/RPC if needed.
// By caching the probe result with a recheckAfter TTL, it allows rolling upgrades where eventually
// the client/server supports the newer ioctl/RPC.
package probecache

import (
	"sync"
	"time"
)

// Availability caches a negative probe result for recheckAfter so tight loops skip repeated failing
// operations. After the TTL expires, ShouldAttempt returns true so the caller re-probes, allowing
// recovery if the underlying capability becomes available (e.g. after a client upgrade).
type Availability struct {
	mu               sync.Mutex
	unavailableUntil time.Time
	recheckAfter     time.Duration
}

// New returns an Availability that will re-probe after recheckAfter has elapsed
// since the last MarkUnavailable call.
func New(recheckAfter time.Duration) *Availability {
	return &Availability{recheckAfter: recheckAfter}
}

// ShouldAttempt returns true if the operation should be attempted. This indicates either it has
// never been marked unavailable, or the recheck window has expired.
func (a *Availability) ShouldAttempt() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Now().After(a.unavailableUntil)
}

// MarkUnavailable records that the operation is currently unavailable and arms the recheck timer.
// It is a no-op if already within an unavailability window, which prevents a tight loop of failures
// from continuously pushing out the recheck deadline.
func (a *Availability) MarkUnavailable() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if time.Now().After(a.unavailableUntil) {
		a.unavailableUntil = time.Now().Add(a.recheckAfter)
	}
}
