package dispatch

import (
	"context"
	"expvar"
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

var (
	dispatchEventsProcessed   = expvar.NewInt("dispatch_events_processed")
	dispatchEventsRateLimited = expvar.NewInt("dispatch_events_rate_limited")
)

// DispatchFunc is called for each event that passes rate limiting. The caller is responsible for
// all logging and error handling. The event is always acknowledged regardless.
type DispatchFunc func(event *beewatch.Event)

// RateLimitOverride configures a custom max-events limit for specific user IDs or ranges.
type RateLimitOverride struct {
	// UserIDs is a comma-separated string of user IDs and/or inclusive ranges (e.g., "1000,2000-2999").
	UserIDs string `mapstructure:"user-ids"`
	// MaxEvents is the maximum number of events these users can trigger per window.
	MaxEvents int `mapstructure:"max-events"`
}

type Config struct {
	// RateLimitWindow is the rolling time window for per-user rate limiting.
	RateLimitWindow time.Duration `mapstructure:"rate-limit-window"`
	// RateLimitMaxEvents is the maximum number of events a single user can trigger per window.
	RateLimitMaxEvents int `mapstructure:"rate-limit-max-events"`
	// RateLimitOverrides configures per-user or per-range max-events limits that take precedence
	// over RateLimitMaxEvents. Overrides are evaluated in order; the first match wins.
	RateLimitOverrides []RateLimitOverride `mapstructure:"rate-limit-override"`
}

type Manager struct {
	log         *zap.Logger
	dispatchFn  DispatchFunc
	events      <-chan *beewatch.Event
	acks        chan<- subscriber.Ack
	wg          sync.WaitGroup
	ctx         context.Context
	ctxCancel   context.CancelFunc
	rateLimiter *rateLimiter
}

func New(cfg Config, log *zap.Logger, fn DispatchFunc, events <-chan *beewatch.Event, acks chan<- subscriber.Ack) (*Manager, error) {
	overrides, err := buildOverrideLookup(cfg.RateLimitOverrides)
	if err != nil {
		return nil, fmt.Errorf("invalid rate-limit-override configuration: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))
	if cfg.RateLimitWindow == 0 {
		cfg.RateLimitWindow = 5 * time.Minute
	}
	return &Manager{
		log:         log,
		dispatchFn:  fn,
		events:      events,
		acks:        acks,
		ctx:         ctx,
		ctxCancel:   cancel,
		rateLimiter: newRateLimiter(cfg.RateLimitWindow, cfg.RateLimitMaxEvents, overrides),
	}, nil
}

func (m *Manager) Start() {
	m.wg.Go(func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			case event := <-m.events:
				m.acks <- m.dispatch(event)
			}
		}
	})
}

// ResetUserRateLimit clears the rate limit state for the given user ID, allowing them to
// immediately trigger events again. Safe to call from any goroutine (e.g., a gRPC handler).
func (m *Manager) ResetUserRateLimit(userId uint32) {
	m.rateLimiter.resetUser(userId)
}

func (m *Manager) Stop() {
	m.ctxCancel()
	m.wg.Wait()
	m.log.Info("stopped dispatcher")
}

func (m *Manager) dispatch(event *beewatch.Event) subscriber.Ack {
	ack := subscriber.Ack{
		MetaId: event.MetaId,
		SeqId:  event.SeqId,
	}

	e, ok := event.EventData.(*beewatch.Event_V2)
	if !ok {
		m.log.Debug("event version is unsupported (ignoring)", zap.Any("ack", ack))
		return ack
	}

	userId := e.V2.GetMsgUserId()
	if userId > 0 && !m.rateLimiter.allow(userId) {
		m.log.Debug("event rate limited",
			zap.Uint32("userId", userId),
			zap.String("path", e.V2.GetPath()),
			zap.Any("type", e.V2.GetType()))
		dispatchEventsRateLimited.Add(1)
		return ack
	}

	m.dispatchFn(event)

	dispatchEventsProcessed.Add(1)
	return ack
}
