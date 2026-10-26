package posixbench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// Runner executes a posixbench write (and optionally read) workload.
type Runner struct {
	// Environment is copied verbatim into the results file and used nowhere
	// else. The library never populates it -- see the Environment type. Set it
	// after NewRunner and before Run.
	Environment Environment

	cfg Config
}

// NewRunner creates a Runner from cfg. cfg must pass Validate and have a
// non-zero Seed before this call.
func NewRunner(cfg Config) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Seed == 0 {
		return nil, fmt.Errorf("posixbench.NewRunner: seed is zero; call cfg.EnsureSeed() first")
	}
	return &Runner{cfg: cfg}, nil
}

// Run executes the write phase and, if doRead is true, a sequential read
// phase for bandwidth measurement. The manifest is written before IO begins
// so Verify can always find the run parameters.
//
// If one or more workers encounter a fatal IO error, Run returns a joined
// error covering every failing worker (not just the first) alongside a
// partial Result (bytes written/read up to that point are still reported).
//
// ctx cancels the run. Workers check it once per block, so a cancelled run
// stops during IO rather than at the next phase boundary -- a region is a
// whole per-worker extent, which for the 1 GiB/worker default would mean no
// effective cancellation at all. A cancelled write phase skips the read phase
// entirely.
//
// When a run is cancelled and nothing else went wrong, Run returns ctx.Err()
// alongside the partial Result -- once, rather than as N joined copies from
// every in-flight worker: cancellation is not a worker failure, and only the
// caller knows whether a stop counts as an error.
//
// When a run is cancelled AND a worker hit a real IO error, Run returns the
// worker errors and NOT ctx.Err(). That ranking is deliberate and callers
// depend on it: the usual way to recognise a stop is errors.Is(err,
// context.Canceled), and if a cancel were joined in alongside a genuine ENOSPC
// that test would still match, so the failure would be reported as a clean
// stop. The cost is that such a run no longer announces it was also cut short;
// the partial Result still carries how far each worker got. See phaseErr.
//
// Note the partial Result's throughput figures are meaningless for a cancelled
// run -- the elapsed time covers a full phase, the byte counts do not. Callers
// should report that it was stopped rather than compute a rate from them.
func (r *Runner) Run(ctx context.Context, doRead bool) (Result, error) {
	if err := os.MkdirAll(r.cfg.Path, 0755); err != nil {
		return Result{}, fmt.Errorf("posixbench.Run: mkdir %s: %w", r.cfg.Path, err)
	}
	m := configToManifest(r.cfg)
	m.StartedAt = time.Now().UTC()
	if err := writeManifest(r.cfg.Path, m); err != nil {
		return Result{}, fmt.Errorf("posixbench.Run: %w", err)
	}
	// Rewrite the manifest with FinishedAt on the way out and drop the results
	// file beside it. Only the success paths call this; every error return above
	// and below deliberately leaves both absent, so an unstamped manifest means
	// the run did not finish.
	//
	// A reader wanting that fact should key on FinishedAt and not on the results
	// file. The two are separate writes and cannot be made atomic, so a results
	// write that fails leaves FinishedAt stamped with no results file beside it.
	finish := func(res Result) (Result, error) {
		done := time.Now().UTC()
		m.FinishedAt = &done
		if err := writeManifest(r.cfg.Path, m); err != nil {
			return res, fmt.Errorf("posixbench.Run: recording completion: %w", err)
		}
		env := r.Environment
		if env.ToolVersion == "" {
			// A caller that set nothing gets honest "unknown"s rather than a
			// record whose blank fields read as "there is no BeeGFS here".
			env = DefaultEnvironment()
		}
		if err := writeRunRecord(r.cfg.Path, newRunRecord(m, env, res, doRead)); err != nil {
			return res, fmt.Errorf("posixbench.Run: %w", err)
		}
		return res, nil
	}

	// For n-to-1, the shared file must exist at full size before workers
	// begin writing to arbitrary offsets within it.
	if r.cfg.Layout == LayoutNto1 {
		sharedPath := filepath.Join(r.cfg.Path, pbenchFilePrefix(r.cfg)+"-shared.dat")
		totalSize := int64(r.cfg.Threads) * r.cfg.FileSize
		if err := preallocate(sharedPath, totalSize); err != nil {
			return Result{}, fmt.Errorf("posixbench.Run: preallocate: %w", err)
		}
	}

	assignments := workerRegions(r.cfg)
	workers := make([]WorkerResult, r.cfg.Threads)

	// Write phase. Each worker fills its OWN histogram and they are merged once
	// the phase joins, so the per-operation timing costs no synchronisation.
	writeLat := make([]Latency, r.cfg.Threads)
	var wg sync.WaitGroup
	writeStart := time.Now()
	for i := 0; i < r.cfg.Threads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workers[id].WorkerID = id
			t := time.Now()
			workers[id].BytesWritten, workers[id].GenerateElapsed, workers[id].WriteErr =
				writeRegions(ctx, r.cfg, assignments[id], &writeLat[id])
			workers[id].WriteElapsed = time.Since(t)
		}(i)
	}
	wg.Wait()

	res := Result{Workers: workers, WriteElapsed: time.Since(writeStart)}
	for i := range writeLat {
		res.WriteLatency.Merge(&writeLat[i])
	}
	for i := range workers {
		res.TotalWritten += workers[i].BytesWritten
		if io := workers[i].WriteIOElapsed(); io > res.WriteIOElapsed {
			res.WriteIOElapsed = io
		}
		if workers[i].GenerateElapsed > res.GenerateElapsed {
			res.GenerateElapsed = workers[i].GenerateElapsed
		}
	}
	// Also stops a cancelled write phase from running the read phase below.
	if err := phaseErr(workers, ctx.Err(), "worker", func(w WorkerResult) error { return w.WriteErr }); err != nil {
		return res, err
	}

	if !doRead {
		return finish(res)
	}

	// Read phase (bandwidth measurement only; no verification).
	readLat := make([]Latency, r.cfg.Threads)
	readStart := time.Now()
	for i := 0; i < r.cfg.Threads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			t := time.Now()
			workers[id].BytesRead, workers[id].ReadErr = readRegions(ctx, r.cfg, assignments[id], &readLat[id])
			workers[id].ReadElapsed = time.Since(t)
		}(i)
	}
	wg.Wait()

	res.ReadElapsed = time.Since(readStart)
	for i := range readLat {
		res.ReadLatency.Merge(&readLat[i])
	}
	for i := range workers {
		res.TotalRead += workers[i].BytesRead
	}
	if err := phaseErr(workers, ctx.Err(), "read worker", func(w WorkerResult) error { return w.ReadErr }); err != nil {
		return res, err
	}

	return finish(res)
}

// phaseErr resolves a finished phase's outcome from its workers' errors and the
// context's state. It is the single place the two are ranked, shared by both
// phases so they cannot drift.
//
// A cancel is not a worker failure: every in-flight worker returns ctx.Err(),
// and N joined copies of one stop bury anything real, so those are dropped and
// the cancel is reported once. A genuine IO error is the opposite case, and it
// OUTRANKS the cancel. Both halves of that ranking are load-bearing:
//
//   - Collecting real errors at all, and doing it BEFORE any early
//     `return res, ctx.Err()`. Behind such a return a worker's ENOSPC
//     disappears the moment the operator presses Ctrl+C: Run yields only
//     context.Canceled and the error is never printed anywhere.
//   - Not returning the cancel alongside them. errors.Join would keep
//     errors.Is(err, context.Canceled) true, and the CLI branches on exactly
//     that to print "STOPPED ... (partial run; no results reported)" and exit
//     0 -- which would report a clean stop over a failed run.
func phaseErr(workers []WorkerResult, ctxErr error, label string, get func(WorkerResult) error) error {
	var errs []error
	for i := range workers {
		if err := get(workers[i]); err != nil && !isCancel(err) {
			errs = append(errs, fmt.Errorf("%s %d: %w", label, i, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("posixbench.Run: %w", errors.Join(errs...))
	}
	return ctxErr
}

// isCancel reports whether err is the context stopping the run rather than the
// filesystem failing it.
func isCancel(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// preallocate creates (or truncates) the file at path to exactly size bytes.
// On Linux this produces a sparse file; actual disk blocks are allocated on
// first write.
func preallocate(path string, size int64) (err error) {
	// O_NOFOLLOW: path may be a predictable default; refuse to follow a
	// symlink planted there rather than truncate whatever it points to.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC|unix.O_NOFOLLOW, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	return f.Truncate(size)
}

// ioBuffer allocates one worker's transfer buffer.
//
// The single allocation site on purpose: an IOType that needs an aligned
// buffer (O_DIRECT) or no buffer at all (mmap) changes it here rather than in
// every loop that happens to need one.
func ioBuffer(cfg Config) []byte {
	return make([]byte, cfg.BlockSize)
}

// writeRegions iterates over all regions assigned to one worker and returns
// the total bytes written and the cumulative time spent generating pattern
// data (see writeRegion) -- excluded from the caller's throughput figures so
// they measure the filesystem, not the pattern generator.
func writeRegions(ctx context.Context, cfg Config, regions []region, lat *Latency) (int64, time.Duration, error) {
	buf := ioBuffer(cfg)
	var total int64
	var genElapsed time.Duration
	for _, reg := range regions {
		n, g, err := writeRegion(ctx, cfg, reg, buf, lat)
		total += n
		genElapsed += g
		if err != nil {
			return total, genElapsed, err
		}
	}
	return total, genElapsed, nil
}

// writeRegion writes reg's blocks and returns the bytes written plus the
// portion of that time spent regenerating buf's pattern rather than in
// WriteAt -- see genElapsed's use in Run, which subtracts it from the wall
// clock so the reported write rate reflects the storage, not the generator.
// Without the split, a run's throughput ceiling silently became whichever is
// slower of the storage and the pattern generator for the chosen -kind, so the
// reported rate could reflect the generator rather than the storage, and the
// ceiling moved every time an operator changed -kind.
//
// The region is fsync'd before returning, still inside the timed window the
// caller measures: without it, WriteAt returning success only means the
// bytes reached the page cache, not stable storage, so the timer stopped
// with dirty pages outstanding and the reported rate could be a multiple of
// what the storage could durably sustain -- confirmed on NVMe, where a
// separately-timed fsync took longer than the "completed" write it followed.
func writeRegion(ctx context.Context, cfg Config, reg region, buf []byte, lat *Latency) (written int64, genElapsed time.Duration, err error) {
	var flags int
	if reg.exclusive {
		// O_NOFOLLOW: reg.path is a predictable per-worker filename under a
		// caller-supplied (often shared) directory; refuse to follow a
		// symlink planted there rather than truncate whatever it points to.
		flags = os.O_CREATE | os.O_RDWR | os.O_TRUNC | unix.O_NOFOLLOW
	} else {
		// Same reasoning applies to the shared (n-to-1) file: reg.path is
		// still a predictable, caller-supplied path.
		flags = os.O_RDWR | unix.O_NOFOLLOW
	}
	f, err := os.OpenFile(reg.path, flags, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", reg.path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	blocks := reg.length / int64(cfg.BlockSize)
	for _, b := range blockOrder(cfg, reg.fileIndex, blocks) {
		// Per block, not per region: a region is a whole per-worker extent, so
		// a region-level check would never fire mid-run.
		select {
		case <-ctx.Done():
			return written, genElapsed, ctx.Err()
		default:
		}
		seed := blockSeed(cfg.Seed, reg.fileIndex, b)
		off := reg.startOff + b*int64(cfg.BlockSize)
		genStart := time.Now()
		if err := block.GenerateBody(cfg.Kind, seed, buf); err != nil {
			return written, genElapsed, fmt.Errorf("generate block %d in %s: %w", b, reg.path, err)
		}
		genElapsed += time.Since(genStart)
		ioStart := time.Now()
		if _, err := f.WriteAt(buf, off); err != nil {
			return written, genElapsed, fmt.Errorf("write block %d in %s: %w", b, reg.path, err)
		}
		lat.Observe(time.Since(ioStart))
		written += int64(cfg.BlockSize)
	}
	// Also what makes the read phase's cache eviction possible:
	// invalidate_mapping_pages skips DIRTY pages, so dropCache would silently
	// do nothing if this sync moved or became conditional.
	if err := f.Sync(); err != nil {
		return written, genElapsed, fmt.Errorf("fsync %s: %w", reg.path, err)
	}
	return written, genElapsed, nil
}

// readRegions reads all blocks in the assigned regions for bandwidth
// measurement. Data is not verified here; use Verifier for that.
func readRegions(ctx context.Context, cfg Config, regions []region, lat *Latency) (int64, error) {
	buf := ioBuffer(cfg)
	var total int64
	for _, reg := range regions {
		f, err := os.Open(reg.path)
		if err != nil {
			return total, fmt.Errorf("open %s for read: %w", reg.path, err)
		}
		// Evict this file's cached pages so the read phase measures storage
		// rather than the write phase's own recently synced pages. Linux-only;
		// see dropcache_linux.go and its no-op sibling.
		dropCache(f)
		blocks := reg.length / int64(cfg.BlockSize)
		for _, b := range blockOrder(cfg, reg.fileIndex, blocks) {
			select {
			case <-ctx.Done():
				f.Close()
				return total, ctx.Err()
			default:
			}
			off := reg.startOff + b*int64(cfg.BlockSize)
			ioStart := time.Now()
			if _, err := f.ReadAt(buf, off); err != nil {
				f.Close()
				return total, fmt.Errorf("read block %d in %s: %w", b, reg.path, err)
			}
			lat.Observe(time.Since(ioStart))
			total += int64(cfg.BlockSize)
		}
		f.Close()
	}
	return total, nil
}
