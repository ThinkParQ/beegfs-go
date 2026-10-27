// Package collector receives events from BeeGFS Watch, batches them in memory and hands each
// batch to markwalk, which adds it to its run set. Nothing is acknowledged then. When the updater
// starts a run, markwalk writes the run set to the run's suspect file, and only then does the
// collector acknowledge every event the file covers. Watch keeps every unacknowledged event and
// resends it after a reconnect, so a crash costs a replay and nothing else.
//
// Watch never waits for a subscriber: once its buffer is full, it drops the oldest events. So
// Watch's buffer holds every event since the last run started. When one metadata server's
// unacknowledged events reach Config.HighWaterMark, the collector asks the updater for a run,
// whose start acknowledges them. The queues here are small on purpose. When markwalk is slower
// than events arrive or its work directory is full, the batcher blocks, the event queue fills,
// the streams stop reading and Watch holds the rest.
//
// The collector runs the gRPC server itself, so the IndexUpdater API the beegfs CLI uses shares
// the listener Watch connects to; the subscriber SDK's handler is registered on it.
package collector

import (
	"context"
	"fmt"
	"maps"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
	"github.com/thinkparq/beegfs-go/index/internal/updater"
	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/protobuf/go/beeindex"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Config struct {
	Listen       string        `mapstructure:"listen"`
	Checkpoint   string        `mapstructure:"checkpoint"` // the subscriber SDK's acknowledged sequence IDs
	AckFrequency time.Duration `mapstructure:"ack-frequency"`
	BatchEvery   time.Duration `mapstructure:"batch-every"`
	BatchEvents  int           `mapstructure:"batch-events"`
	BatchQueue   int           `mapstructure:"batch-queue"`
	// HighWaterMark is in unacknowledged events per metadata server. Watch's event-buffer-size is
	// also per metadata server, so the mark should stay well below it.
	HighWaterMark uint64 `mapstructure:"high-water-mark"`
	TLSCertFile   string `mapstructure:"tls-cert-file"`
	TLSKeyFile    string `mapstructure:"tls-key-file"`
	TLSDisable    bool   `mapstructure:"tls-disable"`
}

type Collector struct {
	beeindex.UnimplementedIndexUpdaterServer
	log     *zap.Logger
	cfg     Config
	marker  *markwalk.Marker
	updater *updater.Updater
	server  *grpc.Server
	svc     *subscriber.Service

	events  chan *bw.Event       // from the SDK's stream handlers to the batcher
	batches chan *markwalk.Batch // from the batcher to the processor; the batcher closes it
	acks    chan subscriber.Ack  // from Acknowledge to the SDK; Run closes it last

	// ackMu orders Acknowledge, which the updater calls, against Run closing acks.
	ackMu      sync.Mutex
	acksClosed bool

	// Owned by the batcher.
	cur  *markwalk.Batch
	last uint64 // number of the newest batch, for the log only
	seqs *seqTracker

	// What the loops are doing, for GetStatus. The batcher writes all but acked.
	state    atomic.Int32  // a beeindex.CollectorStatus_State
	open     atomic.Uint64 // events in the open batch
	received atomic.Uint64 // events received since the service started
	closed   atomic.Uint64 // batches closed since the service started
	acked    atomic.Uint64 // batches whose events were acknowledged since the service started
}

// New opens the checkpoint and builds the gRPC server with the subscriber and IndexUpdater
// services on it. Nothing listens until Run.
func New(cfg Config, marker *markwalk.Marker, up *updater.Updater, log *zap.Logger) (*Collector, error) {
	store, err := subscriber.NewDiskStore(cfg.Checkpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to open checkpoint: %w", err)
	}
	stored, err := store.Retrieve()
	if err != nil {
		return nil, fmt.Errorf("unable to read checkpoint: %w", err)
	}
	c := &Collector{
		log: log, cfg: cfg, marker: marker, updater: up,
		seqs:    newSeqTracker(stored, up.LastSeq()),
		events:  make(chan *bw.Event, cfg.BatchEvents),
		batches: make(chan *markwalk.Batch, cfg.BatchQueue),
		acks:    make(chan subscriber.Ack, 1<<16),
	}
	up.SetAcknowledge(c.Acknowledge)
	// WaitForHandlers makes Stop return only once every stream handler has, so no handler
	// outlives the loops it sends to.
	opts := []grpc.ServerOption{grpc.WaitForHandlers(true)}
	if cfg.TLSDisable {
		log.Warn("serving Watch and the CLI without TLS because it was disabled")
	} else {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load the TLS certificate and key: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	c.server = grpc.NewServer(opts...)
	if c.svc, err = subscriber.NewService(log, cfg.AckFrequency, c.server, store); err != nil {
		return nil, fmt.Errorf("unable to create subscriber service: %w", err)
	}
	beeindex.RegisterIndexUpdaterServer(c.server, c)
	return c, nil
}

// Run serves Watch and the CLI until ctx is done, then stops without draining: whatever was not
// acknowledged is still in Watch and comes back on the next connect. It returns early only when
// the gRPC server fails.
func (c *Collector) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.cfg.Listen)
	if err != nil {
		return fmt.Errorf("unable to listen on %s: %w", c.cfg.Listen, err)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() { c.batcher(loopCtx) })
	loops.Go(func() { c.processor(loopCtx) })
	c.svc.Start(c.events, c.acks)
	served := make(chan error, 1)
	go func() { served <- c.server.Serve(lis) }()
	c.log.Info("collector started", zap.String("listen", c.cfg.Listen), zap.Uint64("after_batch", c.last))

	var serveErr error
	select {
	case serveErr = <-served:
		c.log.Error("gRPC server failed", zap.Error(serveErr))
	case <-ctx.Done():
	}
	// Inflow first, then the loops, then the acks. Acknowledgements the SDK had not written to
	// the checkpoint yet are delivered again after a restart.
	c.server.Stop()
	cancel()
	loops.Wait()
	c.ackMu.Lock()
	close(c.acks)
	c.acksClosed = true
	c.ackMu.Unlock()
	c.svc.WaitFlushed()
	c.log.Info("collector stopped")
	return serveErr
}

// batcher is the only goroutine that touches cur and last. Acknowledge updates seqs too, under
// the tracker's own lock. The hand-off to the processor blocks while BatchQueue batches wait,
// which is what slows the streams, and so Watch, down. On ctx done the open batch is dropped: it
// is unacknowledged, so Watch resends it.
func (c *Collector) batcher(ctx context.Context) {
	defer close(c.batches)
	tick := time.NewTicker(c.cfg.BatchEvery)
	defer tick.Stop()
	for {
		var closedBy string
		select {
		case ev := <-c.events:
			c.add(ev)
			if c.cur.Events < c.cfg.BatchEvents {
				continue
			}
			closedBy = "size"
		case <-tick.C:
			if c.cur == nil {
				continue
			}
			closedBy = "timer"
		case <-ctx.Done():
			return
		}
		c.log.Info("batch closed", zap.Uint64("batch", c.cur.N), zap.Int("events", c.cur.Events), zap.String("closed_by", closedBy))
		c.closed.Add(1)
		select {
		case c.batches <- c.cur:
		default:
			// Every slot is taken: markwalk is behind. The state says so for as long as the wait lasts.
			c.state.Store(int32(beeindex.CollectorStatus_BLOCKED))
			select {
			case c.batches <- c.cur:
			case <-ctx.Done():
				return
			}
		}
		c.cur = nil
		c.open.Store(0)
		c.state.Store(int32(beeindex.CollectorStatus_IDLE))
	}
}

// add records ev in the open batch, opening one when needed. The batch keeps what markwalk needs,
// not the event. The checks per event:
//   - A hole in the meta's sequence IDs means Watch no longer had those events. They are
//     counted, and a full repair is requested, because nothing else says which directories the
//     lost events changed.
//   - When the meta's unacknowledged events reach the high-water mark, the updater is asked for
//     a run, once per crossing.
func (c *Collector) add(ev *bw.Event) {
	meta, seq := ev.GetMetaId(), ev.GetSeqId()
	// Check first: building the fields for every event is wasted work at the default level.
	if entry := c.log.Check(zap.DebugLevel, "event"); entry != nil {
		v := ev.GetV2()
		entry.Write(zap.Uint32("meta", meta), zap.Uint64("seq", seq), zap.String("type", v.GetType().String()),
			zap.String("path", v.GetPath()), zap.String("target", v.GetTargetPath()), zap.Uint64("num_links", v.GetNumLinks()))
	}
	if n := c.seqs.see(meta, seq); n > 0 {
		c.updater.Repair()
		c.log.Warn("missing events: Watch no longer had them; a full repair is due", zap.Uint32("meta", meta),
			zap.Uint64("from", seq-n), zap.Uint64("to", seq-1))
	}
	if c.seqs.reached(meta, c.cfg.HighWaterMark) {
		c.updater.Due()
		c.log.Warn("high-water mark reached: an update is due to acknowledge the events Watch holds",
			zap.Uint32("meta", meta), zap.Uint64("seq", seq), zap.Uint64("high_water_mark", c.cfg.HighWaterMark))
	}
	if c.cur == nil {
		c.last++
		c.cur = markwalk.NewBatch(c.last)
		c.state.Store(int32(beeindex.CollectorStatus_GATHERING))
	}
	c.marker.Add(c.cur, ev)
	c.open.Store(uint64(c.cur.Events))
	c.received.Add(1)
}

// processor hands each batch to markwalk, which adds it to the run set. A failed batch,
// including one held back by a full work directory, is retried with a growing delay and never
// added, so nothing is skipped. On ctx done it returns at once, even mid-batch: Watch resends
// what is unacknowledged.
func (c *Collector) processor(ctx context.Context) {
	retry := backoff.NewExponentialBackOff()
	for b := range c.batches {
		var failing time.Time
		for {
			err := c.marker.Process(ctx, b)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			if failing.IsZero() {
				failing = time.Now()
			}
			d := retry.NextBackOff()
			c.log.Error("batch failed, retrying", zap.Uint64("batch", b.N), zap.Error(err),
				zap.Duration("failing_for", time.Since(failing)), zap.Duration("after", d))
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		}
		if !failing.IsZero() {
			c.log.Info("batch recovered", zap.Uint64("batch", b.N), zap.Duration("after", time.Since(failing)))
		}
		retry.Reset()
	}
}

// Acknowledge runs when the updater has a run's suspect file on disk. It acknowledges every
// event the file covers, one ack per event, because the subscriber SDK advances what it reports
// to Watch only past events acknowledged one by one. Each meta's acks go in delivery order, so
// the SDK finds each at the front of its list. After Run has closed the acks channel the acks
// are dropped, and Watch resends those events after the restart.
func (c *Collector) Acknowledge(d markwalk.Dumped) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	if c.acksClosed {
		c.log.Info("not acknowledging: the collector stopped; Watch resends these events", zap.Any("highest_seq", d.Seqs))
		return
	}
	// The SDK reads acks until Run closes the channel, so the send only waits for it to catch up.
	for meta, ranges := range d.Delivered {
		for _, r := range ranges {
			// Stop at Last, not past it, so a range ending at the largest ID cannot wrap around.
			for seq := r.First; ; seq++ {
				c.acks <- subscriber.Ack{MetaId: meta, SeqId: seq}
				if seq == r.Last {
					break
				}
			}
		}
	}
	c.seqs.acknowledged(d.Seqs)
	c.acked.Add(uint64(d.Batches))
	c.log.Info("events acknowledged", zap.Int("batches", d.Batches), zap.Any("highest_seq", d.Seqs))
}

func (c *Collector) RunUpdate(ctx context.Context, req *beeindex.RunUpdateRequest) (*beeindex.RunUpdateResponse, error) {
	var since time.Time
	if req.GetSince() != nil {
		since = req.GetSince().AsTime()
	}
	run, err := c.updater.Request(req.GetMode(), since, req.GetNoBeegfsMetadata())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	summary, err := run.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return &beeindex.RunUpdateResponse{Summary: summary}, nil
}

func (c *Collector) SetSchedule(_ context.Context, req *beeindex.SetScheduleRequest) (*beeindex.SetScheduleResponse, error) {
	schedule, err := c.updater.SetSchedule(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &beeindex.SetScheduleResponse{Schedule: schedule}, nil
}

// GetStatus reports the three stages. Each queue length is a snapshot taken without a lock, so
// the numbers of one reply are from slightly different moments.
func (c *Collector) GetStatus(context.Context, *beeindex.GetStatusRequest) (*beeindex.GetStatusResponse, error) {
	return &beeindex.GetStatusResponse{
		Collector: &beeindex.CollectorStatus{
			State:               beeindex.CollectorStatus_State(c.state.Load()),
			Events:              queue(len(c.events), cap(c.events)),
			OpenBatch:           &beeindex.Queue{Size: c.open.Load(), Capacity: uint64(c.cfg.BatchEvents)},
			Batches:             queue(len(c.batches), cap(c.batches)),
			Acks:                queue(len(c.acks), cap(c.acks)),
			Metas:               c.seqs.progress(),
			EventsReceived:      c.received.Load(),
			BatchesClosed:       c.closed.Load(),
			BatchesAcknowledged: c.acked.Load(),
		},
		Markwalk: c.marker.Status(),
		Update:   c.updater.Status(),
	}, nil
}

func queue(size, capacity int) *beeindex.Queue {
	return &beeindex.Queue{Size: uint64(size), Capacity: uint64(capacity)}
}

// seqTracker finds holes in each meta's sequence IDs and tells when a meta's unacknowledged
// events reach the high-water mark. The batcher and Acknowledge write it; GetStatus reads the
// counts.
type seqTracker struct {
	mu      sync.Mutex
	last    map[uint32]uint64 // highest sequence ID seen, per meta
	acked   map[uint32]uint64 // highest sequence ID acknowledged, per meta; MaxUint64 stands for -1
	skipped map[uint32]uint64 // events skipped so far, per meta
	due     map[uint32]bool   // the mark was reached and nothing was acknowledged since, per meta
}

// newSeqTracker starts each meta from the higher of two values, so a hole that opened while the
// collector was down is found on the first event after it:
//   - checkpoint is what the subscriber SDK last flushed, where Watch resumes. The SDK flushes
//     on a timer, so after a crash it can lag behind what Watch has already seen acknowledged.
//   - processed is the highest sequence ID in a suspect file on disk, saved before each
//     acknowledgement. Watch never resumes above it, because events are acknowledged only after
//     the file that covers them is written.
//
// Without processed, a crash would make Watch resume above the checkpoint, and the events in
// between would look lost. A meta in neither, or in the checkpoint only as the SDK's "seek to
// end" placeholder (MaxUint64), starts at its first event. The acknowledged sequence IDs start
// at the same values: everything up to them is on disk.
func newSeqTracker(checkpoint, processed map[uint32]uint64) *seqTracker {
	t := &seqTracker{last: maps.Clone(processed), skipped: map[uint32]uint64{}, due: map[uint32]bool{}}
	if t.last == nil {
		t.last = map[uint32]uint64{}
	}
	for meta, seq := range checkpoint {
		if seq != math.MaxUint64 {
			t.last[meta] = max(t.last[meta], seq)
		}
	}
	t.acked = maps.Clone(t.last)
	return t
}

// see records seq for meta and returns how many events were skipped just before it. A meta not
// seen before starts at seq. A replay, at or below the highest seen, skips nothing: Watch
// resends what was not acked.
func (t *seqTracker) see(meta uint32, seq uint64) (skipped uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.last[meta]
	if !ok || seq > prev {
		t.last[meta] = seq
	}
	if !ok {
		// Nothing before the first event is ours to acknowledge, so the mark counts from here.
		// A new file system numbers its events from 0, so this can wrap to MaxUint64, which
		// stands for -1. The unsigned subtraction in reached still counts right.
		t.acked[meta] = seq - 1
	}
	if !ok || seq <= prev+1 {
		return 0
	}
	n := seq - prev - 1
	t.skipped[meta] += n
	return n
}

// reached reports whether meta's unacknowledged events, the gap between the highest sequence ID
// seen and the highest acknowledged, have reached mark. It returns true once per crossing: an
// acknowledgement of meta re-arms it. A mark of zero never fires.
func (t *seqTracker) reached(meta uint32, mark uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if mark == 0 || t.due[meta] || t.last[meta]-t.acked[meta] < mark {
		return false
	}
	t.due[meta] = true
	return true
}

// acknowledged records the highest sequence ID per meta whose events were acknowledged. The
// comparison adds one to both sides: it compares the next sequence ID to acknowledge, so a
// start of MaxUint64, which stands for -1, counts as 0 and any acknowledgement moves it.
func (t *seqTracker) acknowledged(seqs map[uint32]uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for meta, seq := range seqs {
		if seq+1 > t.acked[meta]+1 {
			t.acked[meta] = seq
		}
		delete(t.due, meta)
	}
}

// progress reports every meta seen, in meta order.
func (t *seqTracker) progress() []*beeindex.MetaProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*beeindex.MetaProgress, 0, len(t.last))
	for _, meta := range slices.Sorted(maps.Keys(t.last)) {
		out = append(out, &beeindex.MetaProgress{Meta: meta, ReceivedSeq: t.last[meta],
			AcknowledgedSeq: t.acked[meta], MissingEvents: t.skipped[meta]})
	}
	return out
}
