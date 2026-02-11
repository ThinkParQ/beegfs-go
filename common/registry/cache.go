package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
)

type getClientFunc func() (*RegistryGetter, error)

type CachedComponentRegistry struct {
	getClientFunc getClientFunc
	// Component start timestamp
	startTime time.Time
	// Last time registry update was attempted.
	lastUpdate time.Time
	// Error that was returned the last time registry update was attempted.
	lastErr error
	// Minimum time before the next request forces a registry update.
	ttl time.Duration
	// Mutex governing the registry update.
	mu sync.RWMutex
	// Component registry
	registry *ComponentRegistry
}

type cachedComponentRegistryCfg struct {
	// Minimum time before the next request forces a registry update.
	ttl      time.Duration
	skipInit bool
}

type CachedComponentRegistryOpt func(*cachedComponentRegistryCfg)

// WithRegistryTTL sets the minimum duration between update attempts.
func WithRegistryTTL(d time.Duration) CachedComponentRegistryOpt {
	return func(cfg *cachedComponentRegistryCfg) {
		if d < 0 {
			d = 0
		}
		cfg.ttl = d
	}
}

// WithSkipInit defers resolving the client and fetching capabilities. The registry remains empty
// until a method like RequireFeature(s) or GetBuildInfo triggers the first update.
func WithSkipInit() CachedComponentRegistryOpt {
	return func(cfg *cachedComponentRegistryCfg) {
		cfg.skipInit = true
	}
}

// NewCachedComponentRegistry builds a cached registry and applies options. WithSkipInit defers
// client resolution and capability fetch until first use. clientFunc supplies the lazy client
// resolver.
func NewCachedComponentRegistry(ctx context.Context, clientFunc getClientFunc, opts ...CachedComponentRegistryOpt) (*CachedComponentRegistry, error) {
	cfg := &cachedComponentRegistryCfg{
		ttl:      60 * time.Second,
		skipInit: false,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if clientFunc == nil {
		return nil, ErrRegistryClientUnavailable
	}

	var lastErr error
	var clientRegistry *ComponentRegistry
	var startTime time.Time
	if cfg.skipInit {
		lastErr = ErrRegistryUninitialized
	} else {
		client, err := clientFunc()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrRegistryClientUnavailable, err)
		}
		if clientRegistry, startTime, err = GetComponentRegistry(ctx, *client); err != nil {
			return nil, err
		}
	}

	return &CachedComponentRegistry{
		ttl:           cfg.ttl,
		getClientFunc: clientFunc,
		startTime:     startTime,
		lastUpdate:    time.Now(),
		lastErr:       lastErr,
		mu:            sync.RWMutex{},
		registry:      clientRegistry,
	}, nil
}

// GetBuildInfo retrieves the component's build information.
func (r *CachedComponentRegistry) GetBuildInfo(ctx context.Context) *flex.BuildInfo {
	r.updateIfNeeded(ctx)
	r.mu.RLock()
	reg := r.registry
	r.mu.RUnlock()

	if reg == nil {
		return &flex.BuildInfo{
			BinaryName: "unknown",
			Version:    "unknown",
			Commit:     "unknown",
			BuildTime:  "unknown",
		}
	}
	return reg.GetBuildInfo()
}

func (r *CachedComponentRegistry) RequireFeature(ctx context.Context, feature string, subFeatures ...string) error {
	r.updateIfNeeded(ctx)
	r.mu.RLock()
	reg := r.registry
	lastErr := r.lastErr
	r.mu.RUnlock()

	if lastErr != nil {
		return lastErr
	}
	if reg == nil {
		return ErrRegistryUninitialized
	}
	return reg.RequireFeature(feature, subFeatures...)
}

func (r *CachedComponentRegistry) RequireFeatures(ctx context.Context, required map[string]*flex.Feature) error {
	r.updateIfNeeded(ctx)
	r.mu.RLock()
	reg := r.registry
	lastErr := r.lastErr
	r.mu.RUnlock()

	if lastErr != nil {
		return lastErr
	}
	if reg == nil {
		return ErrRegistryUninitialized
	}
	return reg.RequireFeatures(required)
}

func (r *CachedComponentRegistry) updateIfNeeded(ctx context.Context) {
	r.mu.RLock()
	lastUpdate := r.lastUpdate
	lastErr := r.lastErr
	r.mu.RUnlock()

	if lastErr != nil {
		r.updateRegistry(ctx, lastUpdate)
		return
	}
	if time.Since(lastUpdate) < r.ttl {
		return
	}

	go func() {
		r.updateRegistry(ctx, lastUpdate)
	}()
}

// updateRegistry cached component registry only if there's an error or ttl expired.
func (r *CachedComponentRegistry) updateRegistry(ctx context.Context, lastKnownUpdate time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lastKnownUpdate != r.lastUpdate {
		return
	}
	r.lastUpdate = time.Now()
	r.lastErr = nil

	client, err := r.getClientFunc()
	if err != nil {
		r.lastErr = err
		return
	}

	updatedRegistry, updatedStartTime, err := GetComponentRegistry(ctx, *client)
	if err != nil {
		r.lastErr = err
	} else if r.startTime != updatedStartTime {
		r.registry = updatedRegistry
		r.startTime = updatedStartTime
	}
}
