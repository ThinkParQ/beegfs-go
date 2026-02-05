package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clientFunc func() (*RegistryComponent, error)

type CachedComponentRegistry struct {
	getClientFunc clientFunc
	// Component start of day
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
	ttl           time.Duration
	getClientFunc clientFunc
	skipInit      bool
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

// WithGetClientFunc supplies a function used to resolve the registry client on demand.
func WithGetClientFunc(f clientFunc) CachedComponentRegistryOpt {
	return func(cfg *cachedComponentRegistryCfg) {
		cfg.getClientFunc = f
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
// client resolution and capability fetch until first use. WithGetClientFunc supplies a lazy client
// resolver when no client is provided.
func NewCachedComponentRegistry(ctx context.Context, client *RegistryComponent, opts ...CachedComponentRegistryOpt) (*CachedComponentRegistry, error) {
	cfg := &cachedComponentRegistryCfg{
		ttl:           60 * time.Second,
		getClientFunc: nil,
		skipInit:      false,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	var err error
	if client != nil {
		cfg.getClientFunc = func() (*RegistryComponent, error) {
			return client, nil
		}
	} else if cfg.getClientFunc == nil {
		return nil, fmt.Errorf("either client or WithGetClientFunc needs to be defined")
	}

	var resolvedClient *RegistryComponent
	if client != nil {
		resolvedClient = client
	} else if !cfg.skipInit {
		if resolvedClient, err = cfg.getClientFunc(); err != nil {
			return nil, err
		}
	}

	var lastErr error
	var clientRegistry *ComponentRegistry
	var startTime time.Time
	if cfg.skipInit {
		lastErr = ErrRegistryUninitalized
	} else {
		if resolvedClient == nil {
			return nil, fmt.Errorf("registry client is not available")
		}
		clientRegistry, startTime, err = GetComponentRegistry(ctx, *resolvedClient)
		if err != nil {
			if status.Code(err) == codes.Unimplemented {
				return nil, fmt.Errorf("component registry not implemented")
			}
			return nil, err
		}
	}

	return &CachedComponentRegistry{
		ttl:           cfg.ttl,
		getClientFunc: cfg.getClientFunc,
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
	if reg == nil {
		if lastErr != nil {
			return lastErr
		}
		return ErrRegistryUninitalized
	}
	return reg.RequireFeature(feature, subFeatures...)
}

func (r *CachedComponentRegistry) RequireFeatures(ctx context.Context, required map[string]*flex.Feature) error {
	r.updateIfNeeded(ctx)
	r.mu.RLock()
	reg := r.registry
	lastErr := r.lastErr
	r.mu.RUnlock()

	if reg == nil {
		if lastErr != nil {
			return lastErr
		}
		return ErrRegistryUninitalized
	}
	return reg.RequireFeatures(required)
}

func (r *CachedComponentRegistry) updateIfNeeded(ctx context.Context) {
	r.mu.RLock()
	lastUpdate := r.lastUpdate
	if r.lastErr != nil {
		r.mu.RUnlock()
		r.updateRegistry(ctx, lastUpdate)
		return
	}
	r.mu.RUnlock()

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
		if status.Code(err) == codes.Unimplemented {
			err = fmt.Errorf("component registry not implemented")
		}
		r.lastErr = err
	} else if r.startTime != updatedStartTime {
		r.registry = updatedRegistry
		r.startTime = updatedStartTime
	}
}
