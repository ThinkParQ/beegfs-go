package dispatch

import (
	"context"
	"expvar"
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/job"
	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/beewatch"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

var (
	dispatchRestoresTriggered   = expvar.NewInt("beeremote_dispatch_restores_triggered")
	dispatchRestoresRateLimited = expvar.NewInt("beeremote_dispatch_restores_rate_limited")
)

// RateLimitOverride configures a custom max-events limit for specific user IDs or ranges.
type RateLimitOverride struct {
	// UserIDs is a comma-separated string of user IDs and/or inclusive ranges (e.g., "1000,2000-2999").
	UserIDs string `mapstructure:"user-ids"`
	// MaxEvents is the maximum number of restores these users can trigger per window.
	MaxEvents int `mapstructure:"max-events"`
}

type Config struct {
	// RateLimitWindow is the rolling time window for per-user rate limiting.
	RateLimitWindow time.Duration `mapstructure:"rate-limit-window"`
	// RateLimitMaxEvents is the maximum number of restores a single user can trigger per window.
	RateLimitMaxEvents int `mapstructure:"rate-limit-max-events"`
	// RateLimitOverrides configures per-user or per-range max-events limits that take precedence
	// over RateLimitMaxEvents. Overrides are evaluated in order; the first match wins.
	RateLimitOverrides []RateLimitOverride `mapstructure:"rate-limit-override"`
}

type Manager struct {
	log         *zap.Logger
	jobMgr      *job.Manager
	events      <-chan *beewatch.Event
	acks        chan<- subscriber.Ack
	wg          sync.WaitGroup
	ctx         context.Context
	ctxCancel   context.CancelFunc
	rateLimiter *rateLimiter
}

func New(cfg Config, log *zap.Logger, jobMgr *job.Manager, events <-chan *beewatch.Event, acks chan<- subscriber.Ack) (*Manager, error) {
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
		jobMgr:      jobMgr,
		events:      events,
		acks:        acks,
		ctx:         ctx,
		ctxCancel:   cancel,
		rateLimiter: newRateLimiter(cfg.RateLimitWindow, cfg.RateLimitMaxEvents, overrides),
	}, nil
}

func (d *Manager) Start() {
	d.wg.Go(func() {
		for {
			select {
			case <-d.ctx.Done():
				return
			case event := <-d.events:
				d.acks <- d.dispatch(event)
			}
		}
	})
}

// ResetUserRateLimit clears the rate limit state for the given user ID, allowing them to
// immediately trigger restores again. Safe to call from any goroutine (e.g., a gRPC handler).
func (d *Manager) ResetUserRateLimit(userId uint32) {
	d.rateLimiter.resetUser(userId)
}

func (d *Manager) Stop() {
	d.ctxCancel()
	d.wg.Wait()
	d.log.Info("stopped dispatcher")
}

func (d *Manager) dispatch(event *beewatch.Event) subscriber.Ack {

	ack := subscriber.Ack{
		MetaId: event.MetaId,
		SeqId:  event.SeqId,
	}

	e, ok := event.EventData.(*beewatch.Event_V2)
	if !ok {
		d.log.Warn("event version is unsupported (ignoring)", zap.Any("ack", ack))
		return ack
	} else if e.V2.Type != beewatch.V2Event_OPEN_BLOCKED {
		d.log.Debug("filtered out event type (ignoring)", zap.Any("ack", ack), zap.Any("type", e.V2.GetType()))
		return ack
	}

	path := e.V2.GetPath()
	userId := e.V2.GetMsgUserId()

	d.log.Info("detected a blocked open event, checking if the stub file is eligible for automatic restore", zap.String("path", path), zap.Any("ack", ack), zap.Any("type", e.V2.GetType()))
	entry, err := entry.GetEntry(d.ctx, nil, entry.GetEntriesCfg{}, e.V2.Path)
	if err != nil {
		d.log.Warn("skipping automatic restore as the stub file's data state could not be determined", zap.String("path", path), zap.Error(err))
		return ack
	}

	if entry.Entry.FileState.GetDataState() != beegfs.DataStateAutoRestore {
		d.log.Debug("stub file's data state does not allow an automatic restore", zap.String("path", path), zap.Any("dataState", entry.Entry.FileState.GetDataState()))
		return ack
	}

	if !d.rateLimiter.allow(userId) {
		d.log.Debug("skipping automatic restore due to per-user rate limit", zap.String("path", path), zap.Uint32("userId", userId))
		dispatchRestoresRateLimited.Add(1)
		return ack
	}

	// If a restore is currently in progress this will fail for "stub file is malformed". If a
	// restore was already requested but not yet started, then the job submission below is rejected.
	// We could add a dedupe map here, but if we set to short a TTL duplicates will slip through
	// anyway, and too long and we might suppress legitimate retries after a failed restore or
	// scenarios where a file was offloaded/restored/offloaded (weird, but possible). We already
	// have a three layer defense against races between the data state check, GetStubContents
	// failure, and SubmitJobRequest rejecting duplicate requests, no need to add extra complexity.
	id, url, err := d.jobMgr.GetStubContents(path)
	if err != nil {
		d.log.Warn("skipping automatic restore as the stub file's contents could not be retrieved", zap.String("path", path), zap.Error(err))
		return ack
	}

	result, err := d.jobMgr.SubmitJobRequest(&beeremote.JobRequest{
		Path:                path,
		Priority:            1,
		RemoteStorageTarget: id,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_DOWNLOAD,
				RemotePath: url,
				Overwrite:  true,
			},
		},
	})

	if err != nil {
		d.log.Warn("unable to create sync job to restore the specified stub file's contents", zap.String("path", path), zap.Error(err))
		return ack
	}

	dispatchRestoresTriggered.Add(1)
	d.log.Info("stub file restore triggered", zap.Any("result", result))

	return ack
}
