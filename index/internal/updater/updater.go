// Package updater applies suspect files to the index with gufi_incremental_update, and repairs
// a gap in the event stream with a full rebuild. One run goes at a time: every trigger only
// queues the next run, so a slow run never overlaps the next, and a request made during a run is
// served by the run that follows it.
//
// A normal run applies the suspect files of every batch completed so far and deletes those
// batches on success. A repair run passes the tool a time instead of a file: the tool stats every
// entry and rebuilds each directory with anything changed at or after that time. A repair limited
// by time touches no batch, so it fits between two normal runs. The repair that follows a gap
// passes time 0, so every directory is rebuilt. That covers every batch completed before it
// started, so a full repair replaces the normal run queued before it and deletes those batches
// when it succeeds. A normal run requested while it runs waits and runs after it.
//
// The tool walks the whole mount and diffs it against the whole index by inode. It deletes every
// index directory whose inode it does not find in the tree, so it runs only over a mounted BeeGFS,
// see tool. While it runs it parks index directories beside the index, see parkingLot.
package updater

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/google/renameio/v2"
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
	"github.com/thinkparq/protobuf/go/beeindex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MinEvery is the shortest interval a schedule may set. A run with anything to apply walks the
// whole mount, so a shorter interval only queues runs back to back.
const MinEvery = time.Minute

// parkingPrefix starts the parking lot's name. The dot makes the beegfs CLI skip the lot when it
// looks for the index tree beside it.
const parkingPrefix = ".index-updater-parking-"

type Config struct {
	Mount       string `mapstructure:"mount"`
	Binary      string `mapstructure:"binary"`
	Treesummary string `mapstructure:"treesummary"` // empty skips it
	// Plugin is <entrypoint>:<path>. NoBeegfsMetadata leaves it out of every run, to match an
	// index built with beegfs index create -B.
	Plugin           string `mapstructure:"plugin"`
	NoBeegfsMetadata bool   `mapstructure:"no-beegfs-metadata"`
	Threads          int    `mapstructure:"threads"`
	// The tool's own options, passed through when set. The suspect method, file and time are not
	// among them, because they are how a normal run differs from a repair.
	Compress      bool   `mapstructure:"compress"`
	MaxSubtrees   int    `mapstructure:"max-subtrees"`
	KeepArtifacts string `mapstructure:"keep-artifacts"`
	// Every and PendingDirs are the schedule at startup. SetSchedule changes it until a restart.
	Every       time.Duration `mapstructure:"every"`
	PendingDirs uint64        `mapstructure:"pending-dirs"`
	State       string        `mapstructure:"state"`
}

// ValidEvery accepts 0, which disables the timer, or at least MinEvery.
func ValidEvery(d time.Duration) error {
	if d != 0 && d < MinEvery {
		return fmt.Errorf("the update interval must be 0, which disables it, or at least %s; got %s", MinEvery, d)
	}
	return nil
}

// state is what survives a restart.
type state struct {
	RepairPending bool `json:"repair_pending"`
	// LastSeq is the highest sequence ID per metadata node whose batch is on disk. The collector
	// starts from it after a restart, so events processed before a crash, but acknowledged
	// after the last checkpoint, are not counted as lost.
	LastSeq map[uint32]uint64 `json:"last_seq"`
}

// totals counts the runs since the service started, for Status.
type totals struct {
	runs, repairs, failed uint64
	batches, suspectDirs  uint64 // consumed, and named by their suspect files, by the runs that succeeded
	time                  time.Duration
}

// hold is the failure backoff of one kind of run. Automatic runs of that kind wait until
// notBefore. A requested run does not wait, and a failure of one kind never holds the other back.
type hold struct {
	retry     *backoff.ExponentialBackOff
	notBefore time.Time
}

func newHold() hold {
	h := hold{retry: backoff.NewExponentialBackOff()}
	h.retry.MaxInterval = 10 * time.Minute
	return h
}

type Updater struct {
	cfg     Config
	index   string
	work    string
	parking string
	mounted func() error // reports why cfg.Mount is not a mounted BeeGFS, or nil
	log     *zap.Logger

	mu             sync.Mutex
	st             state
	every          time.Duration
	threshold      uint64 // pending directories that queue a normal run
	completed      uint64 // highest batch whose suspect file is on disk
	pendingBatches uint64 // see recount
	pendingDirs    uint64
	running        *Run
	nextNormal     *Run
	nextRepair     *Run
	repairGap      bool // Watch lost events while the running full repair walked the tree
	last           *beeindex.RunSummary
	lastNormal     time.Time // start of the last normal run, for the interval
	totals         totals
	normalHold     hold
	repairHold     hold
	wake           chan struct{}
}

// New loads the state, cleans the parking lot and counts the batches already in the work
// directory. Every batch on disk at startup is complete: the processor acknowledges a batch only
// after writing it, and Watch resends a batch that a crash left without its suspect file, under a
// new number. mounted is called before every run of the tool, see tool.
//
// A parking lot that is not empty means a run was killed while it had index directories parked.
// Cleaning the lot deletes them from the index. New then queues a full repair, which finds them
// in the tree again. It does so after a killed normal run too, because the lot does not say which
// kind of run left it, and a full repair covers both.
func New(cfg Config, index, work string, mounted func() error, log *zap.Logger) (*Updater, error) {
	cfg.Mount = filepath.Clean(cfg.Mount)
	index = filepath.Clean(index)
	if err := checkConfig(cfg, index); err != nil {
		return nil, err
	}
	u := &Updater{
		cfg: cfg, index: index, work: work, mounted: mounted, log: log,
		every: cfg.Every, threshold: cfg.PendingDirs,
		normalHold: newHold(), repairHold: newHold(),
		wake: make(chan struct{}, 1),
	}
	if err := u.load(); err != nil {
		return nil, fmt.Errorf("state file %s: %w", cfg.State, err)
	}
	var err error
	var left int
	if u.parking, left, err = parkingLot(index, log); err != nil {
		return nil, err
	}
	if left > 0 && !u.st.RepairPending {
		u.st.RepairPending = true
		if err := u.write(); err != nil {
			return nil, fmt.Errorf("state file %s: %w", cfg.State, err)
		}
	}
	nums, err := u.batches(math.MaxUint64)
	if err != nil {
		return nil, err
	}
	if len(nums) > 0 {
		u.completed = nums[len(nums)-1]
	}
	if err := u.recount(); err != nil {
		return nil, err
	}
	return u, nil
}

// checkConfig refuses at startup a setting that would fail every run.
func checkConfig(cfg Config, index string) error {
	// The tool finds the index directory of a tree directory under dirname(index), by the tree
	// path from basename(mount) down. With different names it would write beside the index.
	if filepath.Base(index) != filepath.Base(cfg.Mount) {
		return fmt.Errorf("the index %s and the mount %s must have the same name, because %s finds index directories by the mount's name",
			index, cfg.Mount, filepath.Base(cfg.Binary))
	}
	for _, bin := range []string{cfg.Binary, cfg.Treesummary} {
		if bin == "" {
			continue
		}
		if _, err := exec.LookPath(bin); err != nil {
			return err
		}
	}
	if cfg.Plugin != "" && !cfg.NoBeegfsMetadata {
		_, lib, ok := strings.Cut(cfg.Plugin, ":")
		if !ok {
			return fmt.Errorf("update.plugin %q is not <entrypoint>:<path>", cfg.Plugin)
		}
		if _, err := os.Stat(lib); err != nil {
			return fmt.Errorf("update.plugin: %w", err)
		}
	}
	if cfg.KeepArtifacts != "" {
		info, err := os.Stat(cfg.KeepArtifacts)
		if err != nil {
			return fmt.Errorf("update.keep-artifacts: %w", err)
		}
		if !info.IsDir() {
			return fmt.Errorf("update.keep-artifacts: %s is not a directory", cfg.KeepArtifacts)
		}
	}
	return nil
}

// load reads the state file and writes it back, so a state directory the service cannot write
// stops it at startup instead of losing every later save.
func (u *Updater) load() error {
	data, err := os.ReadFile(u.cfg.State)
	switch {
	case err == nil:
		err = json.Unmarshal(data, &u.st)
	case errors.Is(err, os.ErrNotExist):
		err = os.MkdirAll(filepath.Dir(u.cfg.State), 0o755)
	}
	if err != nil {
		return err
	}
	if u.st.LastSeq == nil {
		u.st.LastSeq = map[uint32]uint64{}
	}
	return u.write()
}

// parkingLot returns where the tool parks index directories during a run, and how many entries a
// killed run left there. It deletes those entries, because the tool cannot park a directory whose
// name is taken. They are parked index directories and the update databases the tool builds for
// them. A killed run is never continued: New queues a full repair instead. The lot:
//   - sits beside the index, not in it, because the tool walks the index;
//   - is on the index's file system, because the tool moves directories with rename(2);
//   - is named for the index, so two updaters whose indexes share a parent never clean each
//     other's lot.
func parkingLot(index string, log *zap.Logger) (lot string, left int, err error) {
	parent := filepath.Dir(index)
	same, err := sameFileSystem(index, parent)
	if err != nil {
		return "", 0, err
	}
	if !same {
		return "", 0, fmt.Errorf("the index %s is a mount point, so the parking lot beside it in %s would be on another file system", index, parent)
	}
	lot = filepath.Join(parent, parkingPrefix+filepath.Base(index))
	if entries, err := os.ReadDir(lot); err == nil && len(entries) > 0 {
		left = len(entries)
		log.Warn("deleting what an interrupted update left in the parking lot; a full repair indexes those directories again",
			zap.String("parking_lot", lot), zap.Int("entries", left))
	}
	if err := os.RemoveAll(lot); err != nil {
		return "", 0, fmt.Errorf("cleaning the parking lot: %w", err)
	}
	return lot, left, nil
}

func sameFileSystem(a, b string) (bool, error) {
	ia, err := os.Stat(a)
	if err != nil {
		return false, err
	}
	ib, err := os.Stat(b)
	if err != nil {
		return false, err
	}
	return ia.Sys().(*syscall.Stat_t).Dev == ib.Sys().(*syscall.Stat_t).Dev, nil
}

// recount sets the pending counts from the batch directories on disk, at startup and after every
// normal run. Counting from disk, not by deltas, keeps the counts true when a delete fails half way
// or an operator deletes batches by hand. Only batches up to u.completed count: the processor
// writes a batch before it calls Completed, which counts it then. The caller holds the lock, so
// no batch completes during the count.
func (u *Updater) recount() error {
	nums, err := u.batches(u.completed)
	if err != nil {
		return err
	}
	var dirs uint64
	for _, n := range nums {
		lines, err := suspectLines(markwalk.BatchDir(u.work, n))
		if err != nil {
			return err
		}
		dirs += lines
	}
	u.pendingBatches, u.pendingDirs = uint64(len(nums)), dirs
	return nil
}

// suspectLines returns 0 for a batch with nothing to mark, which has no suspect file.
func suspectLines(batch string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(batch, "suspects"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(strings.Count(string(data), "\n")), nil
}

// LastBatch is the newest batch on disk at startup, so the collector numbers on from it.
func (u *Updater) LastBatch() uint64 {
	return u.completed
}

func (u *Updater) LastSeq() map[uint32]uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return maps.Clone(u.st.LastSeq)
}

// Completed records that batch n is on disk, and the highest sequence ID per metadata node in it.
func (u *Updater) Completed(n uint64, b markwalk.Batch, seqs map[uint32]uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.completed = n
	if b.OnDisk {
		u.pendingBatches++
		u.pendingDirs += uint64(b.Suspects)
	}
	for meta, seq := range seqs {
		u.st.LastSeq[meta] = max(u.st.LastSeq[meta], seq)
	}
	u.save()
	u.signal()
}

// Repair records that Watch lost events, so a full rebuild is due. A full repair that is running
// when the loss is found may already have walked past the directories the lost events changed,
// so its success does not clear the repair this call asks for.
func (u *Updater) Repair() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if r := u.running; r != nil && r.Mode == beeindex.RunUpdateRequest_REPAIR && r.Since.IsZero() {
		u.repairGap = true
	}
	if !u.st.RepairPending {
		u.st.RepairPending = true
		u.save()
	}
	u.signal()
}

// Request queues a requested run and returns the run that will serve it: the one already queued
// when there is one. Two repair requests that share a run get the wider of their two scopes, and
// a zero since is the widest. noPlugin applies to the shared run too.
func (u *Updater) Request(mode beeindex.RunUpdateRequest_Mode, since time.Time, noPlugin bool) (*Run, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch mode {
	case beeindex.RunUpdateRequest_NORMAL:
		if u.nextNormal == nil {
			u.nextNormal = newRun(mode, time.Time{}, true)
		}
		u.nextNormal.explicit = true
		u.nextNormal.NoPlugin = u.nextNormal.NoPlugin || noPlugin
		u.signal()
		return u.nextNormal, nil
	case beeindex.RunUpdateRequest_REPAIR:
		if !since.IsZero() && (since.Unix() < 0 || since.After(time.Now())) {
			return nil, fmt.Errorf("a repair cannot start from %s: the time must lie between 1970 and now", since.Format(time.RFC3339))
		}
		if u.nextRepair == nil {
			u.nextRepair = newRun(mode, since, true)
		} else if since.IsZero() || (!u.nextRepair.Since.IsZero() && since.Before(u.nextRepair.Since)) {
			u.nextRepair.Since = since
		}
		u.nextRepair.explicit = true
		u.nextRepair.NoPlugin = u.nextRepair.NoPlugin || noPlugin
		u.signal()
		return u.nextRepair, nil
	}
	return nil, fmt.Errorf("unknown run mode %d", mode)
}

// SetSchedule changes the fields the request sets, until the service restarts.
func (u *Updater) SetSchedule(req *beeindex.SetScheduleRequest) (*beeindex.Schedule, error) {
	if req.Every != nil {
		if err := ValidEvery(req.GetEvery().AsDuration()); err != nil {
			return nil, err
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if req.Every != nil {
		u.every = req.GetEvery().AsDuration()
	}
	if req.PendingDirs != nil {
		u.threshold = req.GetPendingDirs()
	}
	u.signal()
	return u.schedule(), nil
}

// Status reports the backlog, the schedule, the run in progress or the next one due, the last
// run and the totals, for the IndexUpdater API.
func (u *Updater) Status() *beeindex.UpdateStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := &beeindex.UpdateStatus{
		PendingBatches: u.pendingBatches, PendingDirs: u.pendingDirs, Schedule: u.schedule(),
		LastRun: u.last, RepairPending: u.st.RepairPending,
		Runs: u.totals.runs, Repairs: u.totals.repairs, FailedRuns: u.totals.failed,
		BatchesApplied: u.totals.batches, SuspectDirsApplied: u.totals.suspectDirs,
		RunTime: durationpb.New(u.totals.time),
	}
	if r := u.running; r != nil {
		s.Running = &beeindex.RunSummary{Mode: r.Mode, Started: timestamppb.New(r.started)}
		if !r.Since.IsZero() {
			s.Running.Since = timestamppb.New(r.Since)
		}
	} else if at := u.wakeAt(time.Now()); !at.IsZero() {
		s.NextRun = timestamppb.New(at)
	}
	return s
}

// count adds a finished run to the totals. The caller holds the lock.
func (u *Updater) count(s *beeindex.RunSummary) {
	u.totals.runs++
	if s.Mode == beeindex.RunUpdateRequest_REPAIR {
		u.totals.repairs++
	}
	if s.Error != "" {
		u.totals.failed++
	} else {
		u.totals.batches += s.Batches
		u.totals.suspectDirs += s.SuspectDirs
	}
	u.totals.time += s.Finished.AsTime().Sub(s.Started.AsTime())
}

// The caller holds the lock.
func (u *Updater) schedule() *beeindex.Schedule {
	return &beeindex.Schedule{Every: durationpb.New(u.every), PendingDirs: u.threshold}
}

// save logs a failed write and keeps the state in memory, so the next change tries again. The
// caller holds the lock.
func (u *Updater) save() {
	if err := u.write(); err != nil {
		u.log.Error("unable to save updater state", zap.String("file", u.cfg.State), zap.Error(err))
	}
}

// write syncs the directory too, so the rename survives a power loss.
func (u *Updater) write() error {
	data, err := json.MarshalIndent(u.st, "", "  ")
	if err != nil {
		return err
	}
	if err := renameio.WriteFile(u.cfg.State, data, 0o644); err != nil {
		return err
	}
	return syncDir(filepath.Dir(u.cfg.State))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (u *Updater) signal() {
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

// Run executes runs until ctx is done. Each pass runs every run that may start, then sleeps until
// a run may start or something wakes the loop. The sleep is computed after the runs, from the
// times they left behind. So one trigger starts one run, and a run held back by a failure backoff
// makes the loop sleep, not spin.
func (u *Updater) Run(ctx context.Context) error {
	for {
		for {
			if ctx.Err() != nil {
				return nil
			}
			r := u.next(time.Now())
			if r == nil {
				break
			}
			u.execute(ctx, r)
		}
		u.mu.Lock()
		at := u.wakeAt(time.Now())
		u.mu.Unlock()
		var timer *time.Timer
		var due <-chan time.Time
		if !at.IsZero() {
			timer = time.NewTimer(time.Until(at))
			due = timer.C
		}
		select {
		case <-ctx.Done():
		case <-u.wake:
		case <-due:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (u *Updater) next(now time.Time) *Run {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.queue(now)
	return u.take(now)
}

// queue adds the automatic runs that are due at now. take decides whether one starts. The caller
// holds the lock. The rules:
//   - A full repair is queued while one is pending.
//   - A normal run is queued when the interval since the last one started has passed.
//   - A normal run is queued when the pending directories reach the threshold.
func (u *Updater) queue(now time.Time) {
	if u.st.RepairPending && u.nextRepair == nil {
		u.nextRepair = newRun(beeindex.RunUpdateRequest_REPAIR, time.Time{}, false)
	}
	if u.nextNormal == nil {
		interval := u.every > 0 && !now.Before(u.lastNormal.Add(u.every))
		threshold := u.threshold > 0 && u.pendingDirs >= u.threshold
		if interval || threshold {
			u.nextNormal = newRun(beeindex.RunUpdateRequest_NORMAL, time.Time{}, false)
		}
	}
}

// take pops the run that may start at now, a repair first because the events it covers are older
// than any batch. While a repair is pending, a requested repair limited by time is widened to a
// full one, because only a full rebuild covers the events Watch lost. A full repair takes over
// the queued normal run: the rebuild covers every batch on disk, so that run would only walk the
// mount again. Its requester gets the repair's summary. The caller holds the lock.
func (u *Updater) take(now time.Time) *Run {
	if r := u.nextRepair; r != nil && (r.explicit || !now.Before(u.repairHold.notBefore)) {
		u.nextRepair = nil
		if u.st.RepairPending {
			r.Since = time.Time{}
		}
		if r.Since.IsZero() && u.nextNormal != nil {
			r.replaced = u.nextNormal
			u.nextNormal = nil
		}
		u.repairGap = false
		r.started = now
		u.running = r
		return r
	}
	if r := u.nextNormal; r != nil && (r.explicit || !now.Before(u.normalHold.notBefore)) {
		u.nextNormal = nil
		u.lastNormal = now
		r.started = now
		u.running = r
		return r
	}
	return nil
}

// wakeAt returns when a backoff that holds a queued run ends, or when the interval ends. Zero
// means only a wake-up can make a run due. take has just found that no queued run may start, so
// each queued run waits for its backoff. The caller holds the lock.
func (u *Updater) wakeAt(now time.Time) time.Time {
	var at time.Time
	earliest := func(t time.Time) {
		if t.Before(now) {
			t = now
		}
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	if u.nextRepair != nil {
		earliest(u.repairHold.notBefore)
	}
	if u.nextNormal != nil {
		earliest(u.normalHold.notBefore)
	} else if u.every > 0 {
		earliest(u.lastNormal.Add(u.every))
	}
	return at
}

// execute runs r, records its summary and releases everyone waiting on it.
func (u *Updater) execute(ctx context.Context, r *Run) {
	summary := &beeindex.RunSummary{Mode: r.Mode, Started: timestamppb.New(r.started)}
	switch r.Mode {
	case beeindex.RunUpdateRequest_NORMAL:
		u.normal(ctx, r, summary)
	case beeindex.RunUpdateRequest_REPAIR:
		u.repair(ctx, r, summary)
	}
	summary.Finished = timestamppb.Now()
	stopped := ctx.Err() != nil && summary.Error != ""
	if stopped {
		summary.Error = "stopped because the service is shutting down: " + summary.Error
	}

	u.mu.Lock()
	u.running = nil
	u.last = summary
	u.count(summary)
	h := &u.normalHold
	if r.Mode == beeindex.RunUpdateRequest_REPAIR {
		h = &u.repairHold
	}
	if summary.Error == "" {
		h.retry.Reset()
		h.notBefore = time.Time{}
	} else {
		h.notBefore = time.Now().Add(h.retry.NextBackOff())
	}
	retryAt := h.notBefore
	u.mu.Unlock()
	r.summary = summary
	close(r.done)
	if r.replaced != nil {
		r.replaced.summary = summary
		close(r.replaced.done)
	}

	fields := []zap.Field{zap.String("mode", r.Mode.String()), zap.Uint64("batches", summary.Batches),
		zap.Uint64("suspect_dirs", summary.SuspectDirs), zap.Uint64("error_lines", summary.ErrorLines),
		zap.Int32("exit_code", summary.ExitCode), zap.Duration("took", summary.Finished.AsTime().Sub(summary.Started.AsTime()))}
	if summary.Since != nil {
		fields = append(fields, zap.Time("since", summary.Since.AsTime()))
	}
	switch {
	case stopped:
		u.log.Info("update stopped: "+summary.Error, fields...)
	case summary.Error != "":
		u.log.Error("update failed: "+summary.Error, append(fields, zap.Time("automatic_retry_after", retryAt))...)
	case r.Mode == beeindex.RunUpdateRequest_NORMAL && summary.Batches == 0:
		u.log.Debug("update done: nothing to apply", fields...) // an interval run on a quiet system
	default:
		u.log.Info("update done", fields...)
	}
}

// normal applies the suspect files of every batch completed when the run started, then deletes
// those batches. A run with no directory to rebuild skips the tool: its full walk would find
// nothing a later run does not find too.
func (u *Updater) normal(ctx context.Context, r *Run, summary *beeindex.RunSummary) {
	defer u.recountLogged()
	nums, merged, err := u.pending(summary)
	if err != nil {
		summary.Error = err.Error()
		return
	}
	if len(nums) == 0 {
		return
	}
	if summary.SuspectDirs > 0 {
		file := filepath.Join(u.work, "update-suspects")
		if err := os.WriteFile(file, merged, 0o644); err != nil {
			summary.Error = err.Error()
			return
		}
		defer os.Remove(file)
		if !u.tool(ctx, r, summary, "--suspect-method", "1", "--suspect-file", file) {
			return
		}
	}
	if err := u.consume(nums); err != nil {
		summary.Error = err.Error()
		return
	}
	if summary.SuspectDirs > 0 {
		u.treesummary(ctx, summary)
	}
}

// repair rebuilds directories judged from file times: every directory when r.Since is zero,
// otherwise those with an entry changed at or after r.Since. A full rebuild that succeeds clears
// the pending repair, unless Watch lost events again while it ran, and deletes the batches
// completed before it started: their changes were on the file system when the tool walked it.
// A batch completed while it ran stays for the next normal run.
func (u *Updater) repair(ctx context.Context, r *Run, summary *beeindex.RunSummary) {
	var since int64
	var nums []uint64
	if r.Since.IsZero() {
		defer u.recountLogged()
		var err error
		if nums, _, err = u.pending(summary); err != nil {
			summary.Error = err.Error()
			return
		}
	} else {
		summary.Since = timestamppb.New(r.Since)
		since = r.Since.Unix()
	}
	if !u.tool(ctx, r, summary, "--suspect-method", "3", "--suspect-time", strconv.FormatInt(since, 10)) {
		return
	}
	if r.Since.IsZero() {
		u.mu.Lock()
		if u.st.RepairPending != u.repairGap {
			u.st.RepairPending = u.repairGap
			u.save()
		}
		u.mu.Unlock()
		if err := u.consume(nums); err != nil {
			summary.Error = err.Error()
			return
		}
	}
	u.treesummary(ctx, summary)
}

// pending lists the batches completed when the run started and merges their suspect files, and
// fills the summary's batch and directory counts from them.
func (u *Updater) pending(summary *beeindex.RunSummary) (nums []uint64, merged []byte, err error) {
	u.mu.Lock()
	upTo := u.completed
	u.mu.Unlock()
	if nums, err = u.batches(upTo); err != nil {
		return nil, nil, err
	}
	for _, n := range nums {
		data, err := os.ReadFile(filepath.Join(markwalk.BatchDir(u.work, n), "suspects"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		merged = append(merged, data...)
	}
	summary.Batches = uint64(len(nums))
	summary.SuspectDirs = uint64(strings.Count(string(merged), "\n"))
	return nums, merged, nil
}

// consume deletes batches whose changes the index now holds. One that cannot be deleted is
// applied again by the next normal run, which costs a walk and nothing else.
func (u *Updater) consume(nums []uint64) error {
	for _, n := range nums {
		if err := os.RemoveAll(markwalk.BatchDir(u.work, n)); err != nil {
			return fmt.Errorf("remove applied batch: %w", err)
		}
	}
	return nil
}

// recountLogged sets the pending counts from the disk after a run, see recount.
func (u *Updater) recountLogged() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.recount(); err != nil {
		u.log.Error("unable to count the pending batches", zap.Error(err))
	}
}

// tool runs gufi_incremental_update and reports whether it exited zero. It first checks the
// mount, because over an empty mount point the tool finds none of the index's directories in the
// tree, and deletes every one of them.
func (u *Updater) tool(ctx context.Context, r *Run, summary *beeindex.RunSummary, suspect ...string) bool {
	if err := u.mounted(); err != nil {
		summary.Error = fmt.Sprintf("not running %s: %v", filepath.Base(u.cfg.Binary), err)
		return false
	}
	args := []string{"-n", strconv.Itoa(u.cfg.Threads)}
	if u.cfg.Plugin != "" && !u.cfg.NoBeegfsMetadata && !r.NoPlugin {
		args = append(args, "--plugin", u.cfg.Plugin)
	}
	if u.cfg.Compress {
		args = append(args, "--compress")
	}
	if u.cfg.MaxSubtrees > 0 {
		args = append(args, "--max-subtrees", strconv.Itoa(u.cfg.MaxSubtrees))
	}
	if u.cfg.KeepArtifacts != "" {
		args = append(args, "--keep-artifacts", u.cfg.KeepArtifacts)
	}
	args = append(slices.Concat(args, suspect), u.index, u.cfg.Mount, u.parking)
	u.log.Info("running", zap.String("binary", u.cfg.Binary), zap.Strings("args", args))
	code, errLines, err := u.run(ctx, u.cfg.Binary, args)
	summary.ExitCode, summary.ErrorLines = int32(code), errLines
	if err != nil {
		summary.Error = fmt.Sprintf("%s: %v", filepath.Base(u.cfg.Binary), err)
		return false
	}
	return true
}

// treesummary fails the run but keeps its batches deleted, because the index itself is updated.
func (u *Updater) treesummary(ctx context.Context, summary *beeindex.RunSummary) {
	if u.cfg.Treesummary == "" {
		return
	}
	if _, _, err := u.run(ctx, u.cfg.Treesummary, []string{"-n", strconv.Itoa(u.cfg.Threads), u.index}); err != nil {
		summary.Error = fmt.Sprintf("tree summary: %v", err)
	}
}

// batches lists the batch directories numbered upTo or below, in order.
func (u *Updater) batches(upTo uint64) ([]uint64, error) {
	entries, err := os.ReadDir(u.work)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for _, e := range entries {
		digits, ok := strings.CutPrefix(e.Name(), "batch-")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(digits, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: not a batch directory", filepath.Join(u.work, e.Name()))
		}
		if n <= upTo {
			nums = append(nums, n)
		}
	}
	slices.Sort(nums)
	return nums, nil
}

// maxLine is the longest output line run logs whole. Output past it on one line is still read
// and discarded, so the tool never blocks on a full pipe.
const maxLine = 1 << 20

// run returns the exit code and the number of output lines starting with "Error:". Those lines
// are logged as warnings, because the tool exits zero after a failed apply step and only its
// output says so. The output is always read to the end, because Wait returns only after the child
// exits, and a child blocked on a full pipe never would.
func (u *Updater) run(ctx context.Context, bin string, args []string) (code int, errLines uint64, err error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return 0, 0, err
	}
	name := filepath.Base(bin)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64<<10), maxLine)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Error:") {
			errLines++
			u.log.Warn(name + ": " + line)
		} else {
			u.log.Debug(name + ": " + line)
		}
	}
	if err := scanner.Err(); err != nil {
		u.log.Warn(name+": the rest of the output is not logged", zap.Error(err))
	}
	if _, err := io.Copy(io.Discard, stdout); err != nil {
		u.log.Warn(name+": reading the output failed", zap.Error(err))
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), errLines, errors.New(exit.ProcessState.String())
	}
	return 0, errLines, err
}

// Run is one queued or running update.
type Run struct {
	Mode     beeindex.RunUpdateRequest_Mode
	Since    time.Time // for a repair; zero rebuilds every directory
	NoPlugin bool
	explicit bool      // requested over the API, so a failure backoff does not hold it
	started  time.Time // set by take
	replaced *Run      // for a full repair: the queued normal run it took over, released with its summary
	done     chan struct{}
	summary  *beeindex.RunSummary // set before done is closed
}

func newRun(mode beeindex.RunUpdateRequest_Mode, since time.Time, explicit bool) *Run {
	return &Run{Mode: mode, Since: since, explicit: explicit, done: make(chan struct{})}
}

func (r *Run) Wait(ctx context.Context) (*beeindex.RunSummary, error) {
	select {
	case <-r.done:
		return r.summary, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
