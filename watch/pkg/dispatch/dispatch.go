package dispatch

import (
	"context"
	"expvar"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	dispatchEventsRateLimited = expvar.NewInt("dispatch_events_rate_limited")
	dispatchEventsAccepted    = expvar.NewInt("dispatch_events_accepted")
	dispatchEventsRejected    = expvar.NewInt("dispatch_events_rejected")
)

// DispatchFunc is called for each event that passes rate limiting. It returns true if the event
// triggered an action, which causes the event to count against the user's rate limit. The caller
// is responsible for all logging and error handling. The event is always acknowledged regardless.
type DispatchFunc func(event *beewatch.Event) bool

// RateLimitOverride configures a custom max-events limit for specific user IDs or ranges.
type RateLimitOverride struct {
	// UserIDs is a comma-separated string of user IDs and/or inclusive ranges (e.g., "1000,2000-2999").
	UserIDs string `mapstructure:"user-ids"`
	// MaxEvents is the maximum number of events these users can trigger per window.
	MaxEvents int `mapstructure:"max-events"`
	// EventTypes lists the event types this override applies to (e.g., ["OPEN_BLOCKED"]). If set to
	// ["*"] or not present, the override applies to all event types independently.
	EventTypes []string `mapstructure:"event-types"`
}

type Config struct {
	Enabled bool `mapstructure:"enabled"`
	// RateLimitWindow is the rolling time window for per-user rate limiting. It is set to a sane
	// default below if "0" is specified.
	RateLimitWindow time.Duration `mapstructure:"rate-limit-window"`
	// RateLimitMaxEvents is the maximum number of events a single user can trigger per window. It
	// naturally defaults to 0 and does not override this default below.
	RateLimitMaxEvents int `mapstructure:"rate-limit-max-events"`
	// RateLimitEventTypes lists the event types that RateLimitMaxEvents applies to. If set to ["*"]
	// or not present, the default limit applies to all event types. Event types not in this list
	// (and not covered by an override) are always rate limited with max-events=0.
	RateLimitEventTypes []string `mapstructure:"rate-limit-event-types"`
	// RateLimitOverrides configures per-user or per-range max-events limits that take precedence
	// over RateLimitMaxEvents. Overrides are evaluated in order; the first match wins.
	RateLimitOverrides []RateLimitOverride `mapstructure:"rate-limit-override"`
	CheckpointPath     string              `mapstructure:"checkpoint-path"`
	// CheckpointFrequency is also the same as the subscriber's ackFrequency, since checkpoints are
	// written out at the same cadence. This is set to a sane default below if "0" is specified.
	CheckpointFrequency time.Duration `mapstructure:"checkpoint-frequency"`
	Workers             int           `mapstructure:"workers"`
}

type Manager struct {
	log               *zap.Logger
	defaultDispatchFn DispatchFunc
	dispatchFns       map[beewatch.V2Event_Type]DispatchFunc
	wg                *sync.WaitGroup
	ctx               context.Context
	ctxCancel         context.CancelFunc
	rateLimiter       *rateLimiter
	eventSubscriber   *subscriber.Service
	enabled           bool
	workers           int
	existingServer    *grpc.Server
	server            *subscriber.Server
	serverCfg         *subscriber.Config
	serverErrs        chan<- error
	events            chan *beewatch.Event
	acks              chan subscriber.Ack
}

type DispatchOption func(*Manager)

func WithDefaultDispatchFn(fn DispatchFunc) DispatchOption {
	return func(m *Manager) { m.defaultDispatchFn = fn }
}

func WithDispatchFns(fns map[beewatch.V2Event_Type]DispatchFunc) DispatchOption {
	return func(m *Manager) { m.dispatchFns = fns }
}

// WithExistingGRPCServer attaches the subscriber services to an existing gRPC server. Mutually
// exclusive with the WithNewGRPCServer option.
func WithExistingGRPCServer(attachToServer *grpc.Server) DispatchOption {
	return func(m *Manager) { m.existingServer = attachToServer }
}

// WithNewGRPCServer starts a new gRPC server based on the specified configuration and returns any
// fatal errors on errChan. Mutually exclusive with the WithExistingGRPCServer option. When set the
// gRPC server started and stopped with manager.Start() and manager.Stop().
func WithNewGRPCServer(config subscriber.Config, errChan chan<- error) DispatchOption {
	return func(m *Manager) {
		m.serverCfg = &config
		m.serverErrs = errChan
	}
}

func New(cfg Config, log *zap.Logger, serviceOpts []subscriber.ServiceOption, opts ...DispatchOption) (*Manager, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))
	if !cfg.Enabled {
		log.Warn("automatically dispatching jobs from file system modification events is disabled")
		return &Manager{}, nil
	}

	rl, err := newRateLimiter(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		log:         log,
		ctx:         ctx,
		ctxCancel:   cancel,
		wg:          &sync.WaitGroup{},
		rateLimiter: rl,
		enabled:     true,
		workers:     cfg.Workers,
		events:      make(chan *beewatch.Event, 1024),
		acks:        make(chan subscriber.Ack, 1024),
	}
	for _, opt := range opts {
		opt(m)
	}

	if len(m.dispatchFns) == 0 && m.defaultDispatchFn == nil {
		return nil, fmt.Errorf("no dispatch functions configured (this is likely a bug)")
	}

	diskStore, err := subscriber.NewDiskStore(cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}

	if cfg.CheckpointFrequency == 0 {
		cfg.CheckpointFrequency = time.Second * 1
	}

	if m.serverCfg != nil && m.existingServer != nil {
		return nil, fmt.Errorf("invalid dispatch configuration: cannot both attach to an existing server and start a new one (this is probably a bug)")
	} else if m.serverCfg != nil {
		m.server, err = subscriber.NewServer(log, *m.serverCfg, diskStore, serviceOpts...)
	} else if m.existingServer != nil {
		m.eventSubscriber, err = subscriber.NewService(log, cfg.CheckpointFrequency, m.wg, m.existingServer, diskStore, serviceOpts...)
	} else {
		return nil, fmt.Errorf("invalid dispatch configuration: either an existing gRPC server or configuration for a new server is required (this is probably a bug)")
	}
	if err != nil {
		return nil, err
	}

	log.Info("automatic job dispatch from file system modification events is enabled")
	return m, nil
}

func (m *Manager) Start() {
	if !m.enabled {
		return
	}
	workers := runtime.GOMAXPROCS(0)
	if m.workers > 0 {
		workers = m.workers
	}
	for range workers {
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
	if m.server != nil {
		m.server.ListenAndServe(m.events, m.acks, m.serverErrs)
	} else {
		m.eventSubscriber.Start(m.events, m.acks)
	}
}

// ResetUserRateLimit clears the rate limit state for the given user ID, allowing them to
// immediately trigger events again. Safe to call from any goroutine (e.g., a gRPC handler).
func (m *Manager) ResetUserRateLimit(userId uint32) {
	if !m.enabled {
		return
	}
	m.rateLimiter.resetUser(userId)
}

func (m *Manager) Stop() {
	if !m.enabled {
		return
	}
	if m.server != nil {
		m.server.Stop()
	}
	m.ctxCancel()
	m.wg.Wait()
	close(m.acks)
	m.log.Info("stopped dispatcher")
}

func (m *Manager) dispatch(event *beewatch.Event) subscriber.Ack {
	ack := subscriber.Ack{
		MetaId: event.MetaId,
		SeqId:  event.SeqId,
	}

	e, ok := event.EventData.(*beewatch.Event_V2)
	if !ok {
		m.log.Error("event version is unsupported (ignoring)", zap.Any("ack", ack))
		return ack
	}

	var dispatch DispatchFunc
	if m.dispatchFns != nil {
		if dispatch, ok = m.dispatchFns[e.V2.Type]; !ok {
			if m.defaultDispatchFn == nil {
				// Event was filtered out, just ack it.
				return ack
			}
			dispatch = m.defaultDispatchFn
		}
	} else if m.defaultDispatchFn != nil {
		dispatch = m.defaultDispatchFn
	} else {
		// Should be impossible if the Manager was initialized with New().
		m.log.Warn("no default or event specific dispatch function was defined (this is probably a bug)", zap.Any("ack", ack))
		return ack
	}

	userId := e.V2.GetMsgUserId()
	eventType := e.V2.GetType()
	if !m.rateLimiter.check(userId, eventType) {
		m.log.Info("event rate limited",
			zap.Uint32("userId", userId),
			zap.String("path", e.V2.GetPath()),
			zap.Any("type", eventType))
		dispatchEventsRateLimited.Add(1)
		return ack
	}

	if dispatch(event) {
		m.rateLimiter.record(userId, eventType)
		dispatchEventsAccepted.Add(1)
	} else {
		dispatchEventsRejected.Add(1)
	}
	return ack
}
