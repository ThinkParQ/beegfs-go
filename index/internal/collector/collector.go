// Package collector receives events from one or more BeeGFS Watch instances and
// writes them to spool files for markwalk to pick up.
//
// Events are appended, exactly as Watch sent them, to seg-<N>.binpb.partial:
// protobuf binary, each record prefixed with its length (protodelim). The spool
// package reads them. When a file holds enough events, or has been open long
// enough, it is handed to a sealer, which fsyncs it, gives it its final
// seg-<N>.binpb name, fsyncs the directory, and only then acks its events back
// to Watch. A sealed file is therefore complete and on disk, and is markwalk's
// to take.
//
// N counts up from 1, zero-padded, and continues from the highest sealed file
// after a restart, or from the persisted counter when that is higher, so name
// order is number order is arrival order even after old files are removed.
//
// Nothing here can slow down the file system: the metadata service and Watch
// never wait for us. What falling behind costs is Watch's buffer, which drops
// the oldest events once it is full. So the receive path never waits on disk,
// and the sealer only gets in its way when fsync is several files behind.
//
// Watch replays anything we never ack, so a crash costs a replay and nothing
// else. What Watch no longer holds cannot come back: its buffer overflowed, or
// the metadata service discarded events while Watch was away. Sequence IDs are
// per meta and count up by one, so the receive loop logs every jump as
// "missing events", from the first event after a restart on (it starts from the
// checkpoint). Any .partial left behind is incomplete and is deleted at
// startup. A disk error stops the collector instead of guessing: nothing
// unsealed is acked, and the replay after a restart fills the hole.
package collector

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/renameio/v2"
	"github.com/thinkparq/beegfs-go/index/internal/spool"
	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Config holds the collector's configuration.
type Config struct {
	// Spool is the directory segment files are written to, set from the shared spool option.
	Spool string `mapstructure:"-"`
	// Listen is the local network address (host:port) Watch connects to.
	Listen string `mapstructure:"listen"`
	// Checkpoint is the file where the subscriber SDK records acknowledged sequence IDs.
	Checkpoint string `mapstructure:"checkpoint"`
	// Counter is the file recording the highest sealed segment number, so numbering continues
	// after segments are removed. Empty keeps no counter.
	Counter string `mapstructure:"counter"`
	// AckFrequency controls how often acknowledged sequence IDs are sent to Watch and written to
	// the checkpoint.
	AckFrequency time.Duration `mapstructure:"ack-frequency"`
	// RollEvery seals the open segment at least this often.
	RollEvery time.Duration `mapstructure:"roll-every"`
	// RollEvents seals the open segment once it holds this many events.
	RollEvents int `mapstructure:"roll-events"`
	// Queue is the number of events buffered between the gRPC streams and the segment writer.
	Queue int `mapstructure:"queue"`
	// SealQueue is the number of written segments that may wait for fsync before receiving blocks.
	SealQueue int `mapstructure:"seal-queue"`
	// TLSDisable disables TLS for the subscriber gRPC server. Not recommended for production use.
	TLSDisable bool `mapstructure:"tls-disable"`
}

// Run serves Watch until ctx is done, then stops cleanly: nothing new is
// accepted, every file is sealed, and the checkpoint records what was acked.
// After each seal it does a non-blocking send on sealed, when that is not nil.
// A disk error returns at once: nothing unsealed was acked, so Watch replays
// it after a restart.
func Run(ctx context.Context, cfg Config, log *zap.Logger, sealed chan<- struct{}) error {
	c := &collector{
		log: log, dir: cfg.Spool, counter: cfg.Counter, maxEvents: cfg.RollEvents, sealed: sealed,
		events: make(chan *bw.Event, cfg.Queue),
		acks:   make(chan subscriber.Ack, 1<<16),
		sealq:  make(chan *file, cfg.SealQueue),
		w:      bufio.NewWriterSize(nil, 1<<20),
	}
	if err := c.prepare(); err != nil {
		return fmt.Errorf("unable to prepare spool: %w", err)
	}
	store, err := subscriber.NewDiskStore(cfg.Checkpoint)
	if err != nil {
		return fmt.Errorf("unable to open checkpoint: %w", err)
	}
	stored, err := store.Retrieve()
	if err != nil {
		return fmt.Errorf("unable to read checkpoint: %w", err)
	}
	c.seqs = newSeqTracker(stored)
	server, err := subscriber.NewServer(log, subscriber.Config{
		Address: cfg.Listen, TlsDisable: cfg.TLSDisable, AckFrequency: cfg.AckFrequency,
	}, store)
	if err != nil {
		return fmt.Errorf("unable to create subscriber server: %w", err)
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	high := map[uint32]uint64{}
	written, sealedAll := make(chan error, 1), make(chan error, 1)
	go func() { written <- c.run(loopCtx, cfg.RollEvery) }()
	go func() { sealedAll <- c.sealer(high) }()
	errs := make(chan error, 1)
	go server.ListenAndServe(c.events, c.acks, errs)
	log.Info("collector started", zap.String("listen", cfg.Listen), zap.String("spool", cfg.Spool),
		zap.Int("roll-events", cfg.RollEvents), zap.Duration("roll-every", cfg.RollEvery))

	var serverErr error
	select {
	case serverErr = <-errs:
		log.Error("subscriber server failed", zap.Error(serverErr))
	case err := <-written:
		return describe("write failed", err)
	case err := <-sealedAll:
		return describe("seal failed", err)
	case <-ctx.Done():
	}

	// Stop the source first so nothing new arrives, let the loop drain, then
	// let the sealer finish every file before acks close for the final flush.
	server.Stop()
	cancel()
	if err := <-written; err != nil {
		return describe("write failed", err)
	}
	if err := <-sealedAll; err != nil {
		return describe("seal failed", err)
	}
	// The SDK records watermarks in the checkpoint from its per-stream
	// goroutines, which Stop has already ended, so the last files' acks would
	// never reach it and would replay after a restart. Everything received has
	// now been sealed, so each meta's highest acked ID is safe to record. Only
	// raise an entry, or replace the SDK's "seek to end" placeholder (MaxUint64)
	// for a meta it had not checkpointed yet.
	if stored, err := store.Retrieve(); err == nil {
		for meta, seq := range high {
			if old, ok := stored[meta]; !ok || old == math.MaxUint64 || seq > old {
				store.Store(meta, seq)
			}
		}
	}
	close(c.acks)
	server.WaitFlushed()
	log.Info("collector stopped")
	return serverErr
}

// describe names the two disk errors an operator can act on.
func describe(what string, err error) error {
	switch {
	case errors.Is(err, syscall.ENOSPC):
		return fmt.Errorf("%s: spool disk is full; events stay in Watch until it has room: %w", what, err)
	case errors.Is(err, fs.ErrExist):
		return fmt.Errorf("%s: a sealed file already has this name; refusing to replace it: %w", what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// seqTracker finds holes in each meta's sequence IDs. Only the receive loop
// uses it.
type seqTracker struct {
	last    map[uint32]uint64 // highest sequence ID seen, per meta
	missing map[uint32]uint64 // events skipped so far, per meta
}

// newSeqTracker starts from the checkpoint, where Watch resumes, so a hole that
// opened while the collector was down is found on the first event after it.
// The checkpoint trails the spool by up to AckEvery, so the first hole after a
// crash can include a few events that were sealed just before it.
//
// A meta that is not in the checkpoint, or only as the SDK's "seek to end"
// placeholder (MaxUint64), starts at its first event.
func newSeqTracker(checkpoint map[uint32]uint64) *seqTracker {
	t := &seqTracker{last: make(map[uint32]uint64, len(checkpoint)), missing: map[uint32]uint64{}}
	for meta, seq := range checkpoint {
		if seq != math.MaxUint64 {
			t.last[meta] = seq
		}
	}
	return t
}

// see records seq for meta and returns how many events were skipped just
// before it. A replay (seq at or below the highest seen) skips nothing.
func (t *seqTracker) see(meta uint32, seq uint64) uint64 {
	n := spool.SeqGap(t.last, meta, seq)
	if n > 0 {
		t.missing[meta] += n
	}
	return n
}

// file is one spool file and the acks owed for what is in it.
//
// acks holds one entry per event (16 B), because the SDK takes acks one event at
// a time and keeps its own 16 B per unacked event as well. Up to SealQueue + 2
// files are in flight (the open one, the queue, the one being sealed), so
// RollEvents bounds that memory, and also how much of Watch's buffer sits
// unacked. Once the SDK can ack a whole prefix (DESIGN.md D8, AckUpTo), this
// becomes each meta's highest SeqId per file.
type file struct {
	f    *os.File
	n    uint64
	name string
	acks []subscriber.Ack
}

// counter is what the Counter file holds: the highest segment number sealed.
type counter struct {
	Segment uint64 `json:"segment"`
}

type collector struct {
	log       *zap.Logger
	dir       string
	counter   string
	maxEvents int
	sealed    chan<- struct{} // nudged after each seal; nil when nobody listens

	events chan *bw.Event
	acks   chan subscriber.Ack
	sealq  chan *file // written, waiting for fsync; the sealer is its only reader

	// Owned by the receive loop.
	cur  *file
	w    *bufio.Writer // reset onto each new file
	last uint64        // number of the newest file, sealed or open
	seqs *seqTracker
	enc  []byte                      // the last event's encoding; reused, it grows to the largest event seen
	size [binary.MaxVarintLen64]byte // its length prefix; here, not on the stack, which Write would make escape
}

// prepare creates the spool directory, removes any file left mid-write, and
// continues numbering after the highest sealed file or the persisted counter.
func (c *collector) prepare() error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	old, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range old {
		if n, ok := spool.SegmentNumber(e.Name()); ok {
			c.last = max(c.last, n)
			continue
		}
		if !strings.HasSuffix(e.Name(), spool.PartialSuffix) {
			continue
		}
		// Its events were never acked, so Watch replays them into a new file.
		// Its number is free again.
		if err := os.Remove(filepath.Join(c.dir, e.Name())); err != nil {
			return err
		}
	}
	if c.counter == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(c.counter), 0o755); err != nil {
		return err
	}
	data, err := os.ReadFile(c.counter)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var saved counter
	if err := json.Unmarshal(data, &saved); err != nil {
		return fmt.Errorf("%s: %w", c.counter, err)
	}
	c.last = max(c.last, saved.Segment)
	return nil
}

func (c *collector) add(ev *bw.Event) error {
	if n := c.seqs.see(ev.GetMetaId(), ev.GetSeqId()); n > 0 {
		meta, seq := ev.GetMetaId(), ev.GetSeqId()
		c.log.Warn("missing events: Watch no longer had them",
			zap.Uint32("meta", meta), zap.Uint64("from", seq-n), zap.Uint64("to", seq-1),
			zap.Uint64("missing", n), zap.Uint64("missing_total", c.seqs.missing[meta]))
	}
	if c.cur == nil {
		// O_EXCL here and the sealer's link, which refuses to replace a file,
		// make a reused number an error rather than a lost file.
		c.last++
		name := spool.SegmentName(c.last)
		f, err := os.OpenFile(filepath.Join(c.dir, name+spool.PartialSuffix), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		c.cur = &file{f: f, n: c.last, name: name, acks: make([]subscriber.Ack, 0, min(c.maxEvents, 1<<16))}
		c.w.Reset(f)
	}
	// Every event has to be acked exactly once or its meta's watermark stops
	// moving, so the ack waits in the file's list until the file is safe.
	c.cur.acks = append(c.cur.acks, subscriber.Ack{MetaId: ev.GetMetaId(), SeqId: ev.GetSeqId()})
	// The whole event, v1 or v2, as Watch sent it. Reading it is downstream's job.
	if err := c.writeRecord(ev); err != nil {
		return err
	}
	if len(c.cur.acks) >= c.maxEvents {
		return c.rotate()
	}
	return nil
}

// writeRecord writes ev as one protodelim record: its length as a uvarint, then
// its bytes. It is what protodelim.MarshalTo writes, without that function's
// two allocations per call (a fresh slice for the message and one for the
// length): this runs once per event, at hundreds of thousands a second, so it
// allocates nothing once enc has grown to the largest event.
func (c *collector) writeRecord(ev *bw.Event) error {
	var err error
	if c.enc, err = (proto.MarshalOptions{}).MarshalAppend(c.enc[:0], ev); err != nil {
		return err
	}
	if _, err := c.w.Write(protowire.AppendVarint(c.size[:0], uint64(len(c.enc)))); err != nil {
		return err
	}
	_, err = c.w.Write(c.enc)
	return err
}

// rotate pushes the buffered tail into the file and hands the file to the
// sealer. It blocks only when the sealer is a whole queue of files behind.
func (c *collector) rotate() error {
	if c.cur == nil {
		return nil
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	c.sealq <- c.cur
	c.cur = nil
	return nil
}

// seal makes one file durable and then acks it: fsync the data, link it to its
// final name, fsync the directory so the name survives a power cut, record the
// number, and only then ack. Any other order can ack an event that is not on
// disk. It records the per-meta highest sequence ID it acked in high, and then
// nudges whoever waits for sealed files, without ever waiting for them.
func (c *collector) seal(fl *file, dir *os.File, high map[uint32]uint64) error {
	if err := fl.f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", fl.name, err)
	}
	if err := fl.f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", fl.name, err)
	}
	partial := filepath.Join(c.dir, fl.name+spool.PartialSuffix)
	// Link, not rename: rename silently replaces an existing file, and every
	// event in a sealed file has already been acked.
	if err := os.Link(partial, filepath.Join(c.dir, fl.name)); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := os.Remove(partial); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync spool directory: %w", err)
	}
	if c.counter != "" {
		data, _ := json.Marshal(counter{fl.n})
		if err := renameio.WriteFile(c.counter, data, 0o644, renameio.WithTempDir(filepath.Dir(c.counter))); err != nil {
			return fmt.Errorf("record segment counter: %w", err)
		}
	}
	// One send per event until the SDK has AckUpTo (DESIGN.md D8); see file.
	for _, a := range fl.acks {
		c.acks <- a
		high[a.MetaId] = max(high[a.MetaId], a.SeqId)
	}
	c.log.Info("sealed", zap.String("file", fl.name), zap.Int("events", len(fl.acks)))
	if c.sealed != nil {
		select {
		case c.sealed <- struct{}{}:
		default: // one pending nudge covers any number of seals
		}
	}
	return nil
}

// sealer seals files in the order they were written until sealq is closed.
// A failure is returned at once and nothing after it is acked.
func (c *collector) sealer(high map[uint32]uint64) error {
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	for fl := range c.sealq {
		if err := c.seal(fl, dir, high); err != nil {
			return err
		}
	}
	return nil
}

// run is the only reader of events, so the open file needs no lock. Events
// from every Watch arrive already tagged with a meta ID and are written in the
// order they arrive.
func (c *collector) run(ctx context.Context, every time.Duration) error {
	defer close(c.sealq)
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case ev := <-c.events:
			if err := c.add(ev); err != nil {
				return err
			}
		case <-tick.C:
			if err := c.rotate(); err != nil {
				return err
			}
		case <-ctx.Done():
			for {
				select {
				case ev := <-c.events:
					if err := c.add(ev); err != nil {
						return err
					}
				default:
					return c.rotate()
				}
			}
		}
	}
}
