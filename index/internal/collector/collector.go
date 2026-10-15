// Package collector receives events from BeeGFS Watch, batches them in memory and hands each
// batch to markwalk. An event is acknowledged to Watch only once the suspect file of its batch
// is on disk. Watch keeps every unacknowledged event and resends it after a reconnect, so a
// crash costs a replay and nothing else.
//
// Watch never waits for a subscriber: when the collector falls behind, Watch's own buffer fills
// and drops the oldest events. The queues here are small on purpose. When markwalk is slower
// than events arrive, the batcher blocks, the event queue fills, the streams stop reading and
// Watch holds the rest.
package collector

import (
	"context"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

type Config struct {
	// Listen is the address (host:port) Watch connects to.
	Listen string `mapstructure:"listen"`
	// Checkpoint is the file where the subscriber SDK records acknowledged sequence IDs.
	Checkpoint string `mapstructure:"checkpoint"`
	// AckFrequency controls how often acknowledged sequence IDs are sent to Watch and written to
	// the checkpoint.
	AckFrequency time.Duration `mapstructure:"ack-frequency"`
	// BatchEvery closes the open batch at least this often.
	BatchEvery time.Duration `mapstructure:"batch-every"`
	// BatchEvents closes the open batch once it holds this many events.
	BatchEvents int `mapstructure:"batch-events"`
	// Queue is the number of events buffered between the gRPC streams and the batcher.
	Queue int `mapstructure:"queue"`
	// BatchQueue is the number of closed batches that may wait for markwalk before the batcher
	// blocks.
	BatchQueue int  `mapstructure:"batch-queue"`
	TLSDisable bool `mapstructure:"tls-disable"`
}

// Run serves Watch until ctx is done, then stops without draining: whatever was not
// acknowledged is still in Watch and comes back on the next connect. It returns early only
// when the subscriber server fails.
func Run(ctx context.Context, cfg Config, mw markwalk.Config, log *zap.Logger) error {
	last, err := markwalk.LastBatch(mw.Work)
	if err != nil {
		return fmt.Errorf("unable to read work directory: %w", err)
	}
	store, err := subscriber.NewDiskStore(cfg.Checkpoint)
	if err != nil {
		return fmt.Errorf("unable to open checkpoint: %w", err)
	}
	stored, err := store.Retrieve()
	if err != nil {
		return fmt.Errorf("unable to read checkpoint: %w", err)
	}
	server, err := subscriber.NewServer(log, subscriber.Config{
		Address: cfg.Listen, TlsDisable: cfg.TLSDisable, AckFrequency: cfg.AckFrequency,
	}, store)
	if err != nil {
		return fmt.Errorf("unable to create subscriber server: %w", err)
	}
	c := &collector{
		log: log, markwalk: mw, maxEvents: cfg.BatchEvents, last: last, seqs: newSeqTracker(stored),
		events:  make(chan *bw.Event, cfg.Queue),
		batches: make(chan *batch, cfg.BatchQueue),
		acks:    make(chan subscriber.Ack, 1<<16),
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() { c.batcher(loopCtx, cfg.BatchEvery) })
	loops.Go(func() { c.processor(loopCtx) })
	errs := make(chan error, 1)
	server.ListenAndServe(c.events, c.acks, errs)
	log.Info("collector started", zap.String("listen", cfg.Listen), zap.Int("batch-events", cfg.BatchEvents),
		zap.Duration("batch-every", cfg.BatchEvery), zap.Uint64("after_batch", last))

	var serverErr error
	select {
	case serverErr = <-errs:
		log.Error("subscriber server failed", zap.Error(serverErr))
	case <-ctx.Done():
	}
	// Inflow first, then the loops, then the acks. Acknowledgements the SDK had not written to
	// the checkpoint yet are delivered again after a restart.
	server.Stop()
	cancel()
	loops.Wait()
	close(c.acks)
	server.WaitFlushed()
	log.Info("collector stopped")
	return serverErr
}

// batch is a closed group of events waiting for, or in, markwalk. Its number names the suspect
// file's directory.
type batch struct {
	n      uint64
	events []*bw.Event
}

type collector struct {
	log       *zap.Logger
	markwalk  markwalk.Config
	maxEvents int

	events  chan *bw.Event      // from the SDK's stream goroutines to the batcher
	batches chan *batch         // from the batcher to the processor; the batcher closes it
	acks    chan subscriber.Ack // from the processor to the SDK; Run closes it last

	// Owned by the batcher.
	cur  *batch
	last uint64 // number of the newest batch
	seqs *seqTracker
}

// batcher is the only goroutine that touches cur, last and seqs. The hand-off to the processor
// blocks while BatchQueue batches wait, which is what slows the streams, and so Watch, down. On
// ctx done the open batch is dropped: it is unacknowledged, so Watch resends it.
func (c *collector) batcher(ctx context.Context, every time.Duration) {
	defer close(c.batches)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case ev := <-c.events:
			c.add(ev)
			if len(c.cur.events) < c.maxEvents {
				continue
			}
		case <-tick.C:
			if c.cur == nil {
				continue
			}
		case <-ctx.Done():
			return
		}
		select {
		case c.batches <- c.cur:
			c.cur = nil
		case <-ctx.Done():
			return
		}
	}
}

func (c *collector) add(ev *bw.Event) {
	meta, seq := ev.GetMetaId(), ev.GetSeqId()
	if n := c.seqs.see(meta, seq); n > 0 {
		c.log.Warn("missing events: Watch no longer had them", zap.Uint32("meta", meta),
			zap.Uint64("from", seq-n), zap.Uint64("to", seq-1), zap.Uint64("missing", n),
			zap.Uint64("missing_total", c.seqs.missing[meta]))
	}
	if c.cur == nil {
		c.last++
		c.cur = &batch{n: c.last, events: make([]*bw.Event, 0, c.maxEvents)}
	}
	c.cur.events = append(c.cur.events, ev)
}

// processor hands each batch to markwalk and acknowledges its events once the suspect file is
// on disk. A failed batch is retried with a growing delay and never acknowledged, so nothing is
// skipped. On ctx done it returns at once, even mid-batch: Watch resends what is unacknowledged.
func (c *collector) processor(ctx context.Context) {
	retry := backoff.NewExponentialBackOff()
	for b := range c.batches {
		for {
			err := safely(func() error { return markwalk.Process(ctx, c.markwalk, c.log, b.n, b.events) })
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			d := retry.NextBackOff()
			c.log.Error("batch failed, retrying", zap.Uint64("batch", b.n), zap.Error(err), zap.Duration("after", d))
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		}
		retry.Reset()
		// The SDK tracks deliveries one by one, so each event is acknowledged on its own. It
		// reads acks until Run closes the channel, so the send only waits for it to catch up.
		for _, ev := range b.events {
			c.acks <- subscriber.Ack{MetaId: ev.GetMetaId(), SeqId: ev.GetSeqId()}
		}
	}
}

// safely runs fn and turns a panic into an error, so a bug in one batch is retried like any
// other failure instead of taking down the process.
func safely(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	return fn()
}

// seqTracker finds holes in each meta's sequence IDs. Only the batcher uses it.
type seqTracker struct {
	last    map[uint32]uint64 // highest sequence ID seen, per meta
	missing map[uint32]uint64 // events skipped so far, per meta
}

// newSeqTracker starts from the checkpoint, where Watch resumes, so a hole that opened while
// the collector was down is found on the first event after it. A meta that is not in the
// checkpoint, or only as the SDK's "seek to end" placeholder (MaxUint64), starts at its first
// event.
func newSeqTracker(checkpoint map[uint32]uint64) *seqTracker {
	t := &seqTracker{last: make(map[uint32]uint64, len(checkpoint)), missing: map[uint32]uint64{}}
	for meta, seq := range checkpoint {
		if seq != math.MaxUint64 {
			t.last[meta] = seq
		}
	}
	return t
}

// see records seq for meta and returns how many events were skipped just before it. A meta not
// seen before starts at seq. A replay, at or below the highest seen, skips nothing: Watch
// resends what was not acked.
func (t *seqTracker) see(meta uint32, seq uint64) uint64 {
	prev, ok := t.last[meta]
	if !ok || seq > prev {
		t.last[meta] = seq
	}
	if !ok || seq <= prev+1 {
		return 0
	}
	n := seq - prev - 1
	t.missing[meta] += n
	return n
}
