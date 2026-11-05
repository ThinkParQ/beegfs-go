// Package collector receives events from BeeGFS Watch, gathers them into batches and looks each
// batch up in the index when it closes. It keeps the inodes the lookups found, each once, and the
// sequence IDs of their events. Nothing is acknowledged then. When the updater starts a run, its
// hand-over takes the inodes and writes the run's suspect file, and only then does the collector
// acknowledge every event the file covers. Watch keeps every unacknowledged event and resends it
// after a reconnect, so a crash costs a replay and nothing else.
//
// A batch holds the changed paths, so its memory follows the path length. An inode takes a few
// dozen bytes, whatever the path length. So the open batch costs most of the memory, and
// Config.BatchEvents bounds it.
//
// Watch never waits for a subscriber: once its buffer is full, it drops the oldest events. So
// Watch's buffer holds every event since the last run started. Two limits ask the updater for a
// run:
//   - Config.HighWaterMark, when one metadata server's unacknowledged events reach it. It
//     protects Watch's buffer.
//   - Config.MaxInodes, when the lookups found that many inodes. At this bound the collector also
//     stops reading until a hand-over takes them, so the streams stop and Watch holds the rest.
//
// Lookups go on while gufi_incremental_update runs. Nothing orders a lookup against the run, so a
// lookup may read index directories the run is rewriting or moving.
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
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
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

// eventQueue is the capacity of the channel from the SDK's stream handlers to collect. An event
// waits there while collect adds the one before it or looks up a batch. A backlog stays in
// Watch's buffer, not here, so the channel is small.
const eventQueue = 1024

type Config struct {
	Listen       string        `mapstructure:"listen"`
	Checkpoint   string        `mapstructure:"checkpoint"` // the subscriber SDK's acknowledged sequence IDs
	AckFrequency time.Duration `mapstructure:"ack-frequency"`
	// BatchEvents and BatchEvery close a batch: at that many events, or that long after its first
	// event, whichever comes first.
	BatchEvents int           `mapstructure:"batch-events"`
	BatchEvery  time.Duration `mapstructure:"batch-every"`
	// MaxInodes bounds the inodes kept between two hand-overs. A lookup adds its whole batch, so
	// the inodes can pass the bound by one batch's worth.
	MaxInodes int `mapstructure:"max-inodes"`
	// HighWaterMark is in unacknowledged events per metadata server. Watch's event-buffer-size is
	// also per metadata server, so the mark should stay well below it.
	HighWaterMark uint64 `mapstructure:"high-water-mark"`
	Query         string `mapstructure:"query"`  // the gufi_query binary
	Sqlite        string `mapstructure:"sqlite"` // the gufi_sqlite3 binary
	TrackOpens    bool   `mapstructure:"track-opens"`
	TLSCertFile   string `mapstructure:"tls-cert-file"`
	TLSKeyFile    string `mapstructure:"tls-key-file"`
	TLSDisable    bool   `mapstructure:"tls-disable"`
}

type Collector struct {
	beeindex.UnimplementedIndexUpdaterServer
	log     *zap.Logger
	cfg     Config
	lookup  *lookup
	updater *updater.Updater
	server  *grpc.Server
	svc     *subscriber.Service

	events chan *bw.Event      // from the SDK's stream handlers to collect
	acks   chan subscriber.Ack // from Acknowledge to the SDK; Run closes it last

	// ackMu orders Acknowledge, which the updater calls, against Run closing acks.
	ackMu      sync.Mutex
	acksClosed bool

	// Owned by collect.
	open *batch // the batch being gathered, or a closed one waiting for its retry; nil when none
	last uint64 // number of the newest batch, for the log and the status

	// The inodes looked up since the last hand-over. collect adds to them. The updater's
	// hand-over swaps them through Take and Restore.
	setMu sync.Mutex
	set   updater.Handover
	// boundDue says the inodes reached Config.MaxInodes and the updater was asked for a run. Take
	// and Restore clear it, so each set asks once, and a set put back asks again.
	boundDue bool
	// swapped wakes collect after Take or Restore, so it checks the bound again.
	swapped chan struct{}

	seqs *seqTracker

	// What the collector is doing, for GetStatus.
	state    atomic.Int32  // a beeindex.CollectorStatus_State
	opened   atomic.Uint64 // events in the open batch
	received atomic.Uint64 // events received since the service started
	closed   atomic.Uint64 // batches looked up since the service started
	acked    atomic.Uint64 // batches whose events were acknowledged since the service started
}

// New checks the lookup's binaries and the index, opens the checkpoint, and builds the gRPC
// server with the subscriber and IndexUpdater services on it. Nothing listens until Run. Its
// inputs besides the configuration:
//   - index is the index root with symlinks resolved, because the lookup checks paths with lstat;
//   - work is the updater's work directory, where each lookup keeps its scratch files;
//   - threads is the thread count for gufi_query.
func New(ctx context.Context, cfg Config, index, work string, threads int, up *updater.Updater, log *zap.Logger) (*Collector, error) {
	lk := &lookup{index: index, gufiQuery: cfg.Query, gufiSqlite: cfg.Sqlite, scratch: filepath.Join(work, "lookup"),
		threads: threads, log: log}
	if err := lk.check(ctx); err != nil {
		return nil, fmt.Errorf("unable to check the index: %w", err)
	}
	store, err := subscriber.NewDiskStore(cfg.Checkpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to open checkpoint: %w", err)
	}
	stored, err := store.Retrieve()
	if err != nil {
		return nil, fmt.Errorf("unable to read checkpoint: %w", err)
	}
	c := &Collector{
		log: log, cfg: cfg, lookup: lk, updater: up,
		seqs:    newSeqTracker(stored, up.LastSeq()),
		events:  make(chan *bw.Event, eventQueue),
		acks:    make(chan subscriber.Ack, 1<<16),
		set:     newHandover(),
		swapped: make(chan struct{}, 1),
	}
	up.SetCollector(c)
	// WaitForHandlers makes Stop return only once every stream handler has, so no handler
	// outlives the loop it sends to.
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

func newHandover() updater.Handover {
	return updater.Handover{Inodes: map[uint64]struct{}{}, Delivered: map[uint32][]updater.SeqRange{}}
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
	var loop sync.WaitGroup
	loop.Go(func() { c.collect(loopCtx) })
	c.svc.Start(c.events, c.acks)
	served := make(chan error, 1)
	go func() { served <- c.server.Serve(lis) }()
	c.log.Info("collector started", zap.String("listen", c.cfg.Listen), zap.Int("batch_events", c.cfg.BatchEvents),
		zap.Duration("batch_every", c.cfg.BatchEvery), zap.Int("max_inodes", c.cfg.MaxInodes))

	var serveErr error
	select {
	case serveErr = <-served:
		c.log.Error("gRPC server failed", zap.Error(serveErr))
	case <-ctx.Done():
	}
	// Inflow first, then collect, then the acks. Acknowledgements the SDK had not written to the
	// checkpoint yet are delivered again after a restart.
	c.server.Stop()
	cancel()
	loop.Wait()
	c.ackMu.Lock()
	close(c.acks)
	c.acksClosed = true
	c.ackMu.Unlock()
	c.svc.WaitFlushed()
	c.log.Info("collector stopped")
	return serveErr
}

// collect is the only goroutine that reads events and looks up batches. A batch closes at
// Config.BatchEvents events or Config.BatchEvery after its first event, and is looked up at once.
// collect reads no event in three cases, so the SDK's stream handlers block on the full event
// channel and Watch keeps the events in its buffer:
//   - while a lookup runs;
//   - while a lookup fails: the batch is kept and retried after a growing delay;
//   - while the inodes are at Config.MaxInodes, until a hand-over takes them.
//
// On ctx done the batch and the inodes are dropped: they are unacknowledged, so Watch resends
// them.
func (c *Collector) collect(ctx context.Context) {
	retry := backoff.NewExponentialBackOff()
	var deadline, retryAt <-chan time.Time
	full := false
	for {
		events := c.events
		if full || retryAt != nil {
			events = nil // a nil channel is never ready
		}
		select {
		case ev := <-events:
			if c.open == nil {
				c.last++
				c.open = newBatch(c.last)
				deadline = time.After(c.cfg.BatchEvery)
			}
			began := time.Now()
			c.add(ev)
			c.open.adding += time.Since(began)
			if c.open.events < c.cfg.BatchEvents {
				c.report(full, false)
				continue
			}
		case <-deadline:
		case <-retryAt:
		case <-c.swapped:
			full = c.atBound()
			c.report(full, retryAt != nil)
			continue
		case <-ctx.Done():
			return
		}
		deadline, retryAt = nil, nil
		if c.open.gathered == 0 { // a retry keeps the time of the first close
			c.open.gathered = time.Since(c.open.opened)
		}
		atBound, err := c.lookUp(ctx)
		switch {
		case err == nil:
			full = atBound
			retry.Reset()
		case ctx.Err() != nil:
			return
		default:
			d := retry.NextBackOff()
			retryAt = time.After(d)
			c.log.Error("lookup failed; the batch is kept and retried, and no event is read from Watch meanwhile",
				zap.Uint64("batch", c.open.n), zap.Error(err), zap.Duration("retry_after", d))
		}
		c.report(full, retryAt != nil)
	}
}

// add records ev in the open batch. The batch keeps what the lookup needs, not the event. The
// checks per event:
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
	c.open.add(ev, c.cfg.TrackOpens)
	c.received.Add(1)
}

// lookUp looks the open batch up and adds its inodes and sequence IDs to the set. It reports
// whether the set is at Config.MaxInodes, see atBound. A batch adds all its inodes, so the set can
// pass the bound by one batch's worth. A failed lookup keeps the batch, so its events stay
// unacknowledged until a retry succeeds.
//
// It logs one line per batch with the time of each step the batch went through: gathering it,
// adding its events, the lookup, and adding the inodes to the set. A retried batch reports its
// last lookup only.
func (c *Collector) lookUp(ctx context.Context) (full bool, err error) {
	b := c.open
	began := time.Now()
	inos, fields, err := c.lookup.convert(ctx, b)
	if err != nil {
		return false, err
	}
	looked := time.Since(began)
	began = time.Now()
	c.setMu.Lock()
	for _, ino := range inos {
		c.set.Inodes[ino] = struct{}{}
	}
	appendRanges(c.set.Delivered, b.delivered)
	c.set.Events += b.events
	c.set.Batches++
	kept := len(c.set.Inodes)
	c.setMu.Unlock()
	inserted := time.Since(began)
	c.open = nil
	c.closed.Add(1)
	c.log.Info("lookup done", append(fields, zap.Int("inodes_kept", kept), zap.Duration("gather_took", b.gathered),
		zap.Duration("add_took", b.adding), zap.Duration("lookup_took", looked), zap.Duration("insert_took", inserted))...)
	return c.atBound(), nil
}

// atBound reports whether the set is at Config.MaxInodes. The first time a set gets there, it asks
// the updater for a run. A set put back by Restore asks again, because the run that failed to
// take it may have been the only one queued.
func (c *Collector) atBound() bool {
	c.setMu.Lock()
	n := len(c.set.Inodes)
	full := n >= c.cfg.MaxInodes
	ask := full && !c.boundDue
	if ask {
		c.boundDue = true
	}
	c.setMu.Unlock()
	if ask {
		c.updater.Due()
		c.log.Warn("inode bound reached: an update is due, and no event is read from Watch until it takes the inodes",
			zap.Int("inodes", n), zap.Int("max_inodes", c.cfg.MaxInodes))
	}
	return full
}

// appendRanges adds the ranges of later after those already in to, per meta, so each meta's
// sequence IDs stay in delivery order.
func appendRanges(to, later map[uint32][]updater.SeqRange) {
	for meta, ranges := range later {
		for _, r := range ranges {
			to[meta] = extend(to[meta], r)
		}
	}
}

// report sets the state GetStatus shows. The collector is blocked while it reads nothing for a
// reason other than a lookup in progress: at the bound, or while a lookup fails.
func (c *Collector) report(full, failing bool) {
	state := beeindex.CollectorStatus_GATHERING
	switch {
	case full || failing:
		state = beeindex.CollectorStatus_BLOCKED
	case c.open == nil:
		state = beeindex.CollectorStatus_IDLE
	}
	c.state.Store(int32(state))
	var opened uint64
	if c.open != nil {
		opened = uint64(c.open.events)
	}
	c.opened.Store(opened)
}

// Take runs at the hand-over, on the updater's goroutine. It returns the inodes looked up so far
// and starts an empty set. A batch whose lookup is still running goes into the new set. A collect
// stopped at the bound then reads again.
func (c *Collector) Take() updater.Handover {
	c.setMu.Lock()
	h := c.set
	c.set, c.boundDue = newHandover(), false
	c.setMu.Unlock()
	c.wake()
	return h
}

// Restore runs when a hand-over failed. It puts h back in front of what was looked up since, so
// the next hand-over covers both, and each meta's sequence IDs stay in delivery order. The set
// can then be past the bound, and collect stops reading until a hand-over succeeds.
func (c *Collector) Restore(h updater.Handover) {
	c.setMu.Lock()
	maps.Copy(h.Inodes, c.set.Inodes)
	appendRanges(h.Delivered, c.set.Delivered)
	h.Events += c.set.Events
	h.Batches += c.set.Batches
	c.set, c.boundDue = h, false
	c.setMu.Unlock()
	c.wake()
}

// wake tells collect that Take or Restore changed the set, so it checks the bound again.
func (c *Collector) wake() {
	select {
	case c.swapped <- struct{}{}:
	default: // collect has a wake-up pending already
	}
}

// Acknowledge runs when the updater has a run's suspect file on disk. It acknowledges every
// event the file covers, one ack per event, because the subscriber SDK advances what it reports
// to Watch only past events acknowledged one by one. Each meta's acks go in delivery order, so
// the SDK finds each at the front of its list. After Run has closed the acks channel the acks
// are dropped, and Watch resends those events after the restart.
func (c *Collector) Acknowledge(h updater.Handover) {
	seqs := h.Seqs()
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	if c.acksClosed {
		c.log.Info("not acknowledging: the collector stopped; Watch resends these events", zap.Any("highest_seq", seqs))
		return
	}
	// The SDK reads acks until Run closes the channel, so the send only waits for it to catch up.
	for meta, ranges := range h.Delivered {
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
	c.seqs.acknowledged(seqs)
	c.acked.Add(uint64(h.Batches))
	c.log.Info("events acknowledged", zap.Int("batches", h.Batches), zap.Int("events", h.Events), zap.Any("highest_seq", seqs))
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

// GetStatus reports the three stages. Each number is read on its own, so the numbers of one reply
// are from slightly different moments. The collector fills the batch fields of the status:
//   - OpenBatch is the open batch's events against Config.BatchEvents.
//   - Batches is the inodes kept for the next hand-over against Config.MaxInodes.
//   - BatchesClosed counts the batches looked up, BatchesAcknowledged those whose events were
//     acknowledged.
//
// The Markwalk field reports the lookup.
func (c *Collector) GetStatus(context.Context, *beeindex.GetStatusRequest) (*beeindex.GetStatusResponse, error) {
	c.setMu.Lock()
	inodes := len(c.set.Inodes)
	c.setMu.Unlock()
	return &beeindex.GetStatusResponse{
		Collector: &beeindex.CollectorStatus{
			State:               beeindex.CollectorStatus_State(c.state.Load()),
			Events:              queue(len(c.events), cap(c.events)),
			OpenBatch:           queue(int(c.opened.Load()), c.cfg.BatchEvents),
			Batches:             queue(inodes, c.cfg.MaxInodes),
			Acks:                queue(len(c.acks), cap(c.acks)),
			Metas:               c.seqs.progress(),
			EventsReceived:      c.received.Load(),
			BatchesClosed:       c.closed.Load(),
			BatchesAcknowledged: c.acked.Load(),
		},
		Markwalk: c.lookup.status(),
		Update:   c.updater.Status(),
	}, nil
}

func queue(size, capacity int) *beeindex.Queue {
	return &beeindex.Queue{Size: uint64(size), Capacity: uint64(capacity)}
}

// seqTracker finds holes in each meta's sequence IDs and tells when a meta's unacknowledged
// events reach the high-water mark. collect and Acknowledge write it; GetStatus reads the
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
