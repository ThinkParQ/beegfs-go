package markwalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/google/renameio/v2"
	"github.com/thinkparq/beegfs-go/index/internal/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

// state is how far markwalk got.
type state struct {
	// Segment is the last segment whose suspect file is written; the next batch starts after it.
	Segment uint64 `json:"segment"`
	// Seqs is each meta's highest sequence ID seen, so holes are found across batches and restarts.
	Seqs map[uint32]uint64 `json:"seqs"`
}

func readState(path string) (state, error) {
	st := state{Seqs: map[uint32]uint64{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, fmt.Errorf("read state: %w", err)
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("%s: %w", path, err)
	}
	if st.Seqs == nil {
		st.Seqs = map[uint32]uint64{}
	}
	return st, nil
}

// writeFileDurable replaces path with data so that after a crash it holds either the old or
// the new content. renameio writes, fsyncs and renames a temp file; the directory is then
// fsynced too, so the rename itself survives a crash before the state file moves past it.
func writeFileDurable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := renameio.WriteFile(path, data, 0o644, renameio.WithTempDir(dir)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// reader turns segments into suspect files.
type reader struct {
	Config
	log *zap.Logger
}

// run makes batches until ctx is done. Behind, when a batch filled up to MaxSegments, it goes
// straight on to the next. Caught up, it waits for a nudge on sealed, or Poll when sealed is
// nil, and then lets Every pass since the last batch: steady load makes one batch per Every,
// and idle costs nothing. A failed batch is retried after a growing delay, and nothing is
// skipped. With Once it makes batches of what is there now and returns the first error.
func (r *reader) run(ctx context.Context, sealed <-chan struct{}) error {
	st, err := readState(r.State)
	if err != nil {
		return err
	}
	r.log.Info("markwalk started", zap.String("spool", r.Spool), zap.Uint64("after_segment", st.Segment),
		zap.String("state", r.State))
	retry := backoff.NewExponentialBackOff()
	var last time.Time // when the last batch was made
	for {
		segs, err := spool.Sealed(r.Spool, st.Segment)
		full := err == nil && len(segs) > r.MaxSegments
		if full {
			segs = segs[:r.MaxSegments]
		}
		if err == nil && len(segs) > 0 {
			err = safely(func() error { return r.process(ctx, &st, segs) })
		}
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil // stopping mid-batch; the batch repeats after a restart
			}
			if r.Once {
				return err
			}
			d := retry.NextBackOff()
			r.log.Error("batch failed, retrying", zap.Error(err), zap.Duration("after", d))
			if !sleep(ctx, d) {
				return nil
			}
			continue
		case len(segs) > 0:
			retry.Reset()
			last = time.Now()
			if r.Retain {
				r.retain(st.Segment)
			}
			if full || r.Once {
				continue // behind: straight on to the next batch
			}
		case r.Once:
			return nil
		}
		// Caught up: wait for a seal, then let Every pass since the last batch.
		if sealed == nil {
			if !sleep(ctx, r.Poll) {
				return nil
			}
		} else {
			select {
			case <-sealed:
			case <-ctx.Done():
				return nil
			}
		}
		if !sleep(ctx, time.Until(last.Add(r.Every))) {
			return nil
		}
	}
}

// sleep waits d, or until ctx is done, which it reports as false.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// safely runs fn and turns a panic into an error, so a bug in one batch is retried like any
// other failure instead of taking down the collector sharing the process.
func safely(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	return fn()
}

// retain removes the sealed segments up to committed, whose suspect files are written. The
// newest sealed segment stays, so the collector's numbering continues from it after a restart.
func (r *reader) retain(committed uint64) {
	segs, err := spool.Sealed(r.Spool, 0)
	if err != nil {
		r.log.Warn("could not list segments for retention", zap.Error(err))
		return
	}
	for _, s := range segs[:max(len(segs)-1, 0)] {
		if s.Number > committed {
			break
		}
		if err := os.Remove(s.Path); err != nil {
			r.log.Warn("could not remove segment", zap.String("segment", s.Path), zap.Error(err))
		}
	}
}

// process writes the suspect file for segs, then moves st past them. A crash in between
// repeats the batch, which writes the same file again.
func (r *reader) process(ctx context.Context, st *state, segs []spool.Segment) error {
	first, last := segs[0].Number, segs[len(segs)-1].Number
	seqs := maps.Clone(st.Seqs)
	dirs := map[string]struct{}{}
	links := map[string]struct{}{} // entry IDs of multi-name files, see siblingDirs
	var events, v1, missing uint64

	for _, s := range segs {
		err := readSegment(s.Path, func(ev *bw.Event) {
			events++
			if n := spool.SeqGap(seqs, ev.GetMetaId(), ev.GetSeqId()); n > 0 {
				missing += n
				r.log.Warn("missing events: the spool has a hole, those directories may stay stale",
					zap.Uint32("meta", ev.GetMetaId()), zap.Uint64("from", ev.GetSeqId()-n),
					zap.Uint64("to", ev.GetSeqId()-1), zap.Uint64("missing", n), zap.Uint64("segment", s.Number))
			}
			v2 := ev.GetV2()
			if v2 == nil {
				v1++ // BeeGFS 7 format, not used
				return
			}
			ds, id := dirsFor(v2, r.Mount, r.TrackOpens)
			for _, d := range ds {
				dirs[d] = struct{}{}
			}
			if id != "" {
				links[id] = struct{}{}
			}
		})
		if err != nil {
			return err // a damaged segment stops markwalk rather than skip what it held
		}
	}

	marked := append(slices.Collect(maps.Keys(dirs)),
		siblingDirs(ctx, r.log, slices.Collect(maps.Keys(links)), r.Mount, r.Index, r.Query)...)
	if err := ctx.Err(); err != nil {
		return err // a lookup cut short must not reach the suspect file
	}
	out := filepath.Join(r.Work, fmt.Sprintf("batch-%010d", last), "suspects")
	marks, stats, err := writeSuspects(marked, r.Mount, r.Index, out, r.StatCache)
	if err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}

	st.Segment, st.Seqs = last, seqs
	data, err := json.Marshal(st)
	if err == nil {
		err = writeFileDurable(r.State, append(data, '\n'))
	}
	if err != nil {
		return fmt.Errorf("write %s: %w", r.State, err)
	}

	if marks == 0 {
		out = "none: nothing left to mark"
	}
	r.log.Info("batch done", zap.Uint64("first_segment", first), zap.Uint64("last_segment", last),
		zap.Int("segments", len(segs)), zap.Uint64("events", events), zap.Int("directories", len(dirs)),
		zap.Int("multi_name_inodes", len(links)), zap.Int("marks", marks), zap.Int("stats", stats),
		zap.Uint64("v1_events_skipped", v1), zap.Uint64("missing_events", missing), zap.String("suspects", out))
	return nil
}

// readSegment calls fn for every event in a segment. fn must not keep the event, which is reused.
func readSegment(path string, fn func(*bw.Event)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sr := spool.NewReader(f)
	var ev bw.Event
	for {
		err := sr.Next(&ev)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		fn(&ev)
	}
}
