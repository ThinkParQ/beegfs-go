//go:build linux

// iotest-soak -- multi-threaded soak test for verifyio.
//
// Thread 0 operates exclusively on its own .noOverlap file. The remaining
// N-1 threads share a pool of (N-1)/2 .shared files, using OFD locking to
// coordinate concurrent writes and verifications.
//
// Each worker repeatedly picks a random (file, op, offset) triple and
// executes it until the duration expires or an anomaly is detected.
//
//	iotest-soak -path /tmp/soak -threads 8 -duration 5m
//	iotest-soak -path /tmp/soak -threads 16 -duration 1h -blocksize 4096 -file-blocks 256
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/trace"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
	"go.uber.org/zap"
)

func main() {
	var (
		path       = flag.String("path", "/tmp/iotest-soak", "directory for soak data files")
		threads    = flag.Int("threads", 4, "number of concurrent worker goroutines (min 1)")
		duration   = flag.Duration("duration", 30*time.Second, "how long to run (0 = until anomaly)")
		blockSize  = flag.Int("blocksize", 1024, "total bytes per block on disk (use a power of two: 512, 1024, 4096, ...)")
		fileBlocks = flag.Int("file-blocks", 64, "number of blocks per file")
		iotrace    = flag.Int("iotrace", 0, "IO trace level (0=off, 1=error, 2=warn, 3=info, 4/5=debug)")
		iologfile  = flag.String("iologfile", "", "IO trace destination file (default stderr)")
	)
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintln(w, "Usage: iotest-soak [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Multi-threaded soak test that continuously writes and verifies blocks with OFD locking.")
		fmt.Fprintln(w, "Thread 0 uses an exclusive .noOverlap file; remaining threads share a pool of .shared files.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-soak -path /tmp/soak -threads 8 -duration 5m")
		fmt.Fprintln(w, "  iotest-soak -path /tmp/soak -threads 16 -duration 1h -blocksize 4096 -file-blocks 256")
	}
	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(2)
	}
	flag.Parse()

	if *threads <= 0 {
		die("threads must be > 0")
	}
	if *fileBlocks <= 0 {
		die("file-blocks must be > 0")
	}
	if _, err := block.BodyLen(*blockSize); err != nil {
		die("invalid -blocksize: %v", err)
	}

	tl, err := trace.NewTraceLoggers(trace.TraceConfig{
		Level:   int8(*iotrace),
		LogFile: *iologfile,
	})
	if err != nil {
		die("trace: %v", err)
	}
	defer tl.Sync()

	noOverlapPaths, sharedPaths, err := resolveFiles(*path, *threads)
	if err != nil {
		die("%v", err)
	}
	printPlan(*threads, noOverlapPaths, sharedPaths, *duration)

	// baseCtx is cancelled with a cause on the first anomaly.
	// ctx adds an optional deadline on top.
	baseCtx, cancelCause := context.WithCancelCause(context.Background())
	defer cancelCause(nil)
	ctx := baseCtx
	if *duration > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(baseCtx, *duration)
		defer stop()
	}

	ops := []Op{
		writeBufferedOp(),
		verifyRangeOp(*blockSize),
	}
	nBlocks := int64(*fileBlocks)

	var (
		wg       sync.WaitGroup
		failures atomic.Int64
		totalOps atomic.Int64
	)
	start := time.Now()

	go progressLoop(ctx, start, &totalOps)

	plan := runPlan{ops: ops, nBlocks: nBlocks, blockSize: *blockSize}

	// Thread 0: exclusive noOverlap worker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		wc := workerCtx{ctx: ctx, cancelCause: cancelCause, workerID: 0, totalOps: &totalOps, log: tl.IO}
		if err := spawnWorker(wc, noOverlapPaths, plan); err != nil {
			failures.Add(1)
		}
	}()

	// Threads 1..N-1: shared-pool workers.
	for i := 1; i < *threads; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			wc := workerCtx{ctx: ctx, cancelCause: cancelCause, workerID: workerID, totalOps: &totalOps, log: tl.IO}
			if err := spawnWorker(wc, sharedPaths, plan); err != nil {
				failures.Add(1)
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start).Round(time.Millisecond)
	fmt.Printf("elapsed=%s ops=%d\n", elapsed, totalOps.Load())

	cause := context.Cause(baseCtx)
	if failures.Load() > 0 || (cause != nil && cause != context.DeadlineExceeded) {
		if cause != nil && cause != context.DeadlineExceeded {
			fmt.Fprintf(os.Stderr, "cause: %v\n", cause)
		}
		fmt.Println("FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS")
}

// workerCtx bundles the per-worker execution environment: everything a
// worker needs to report progress and signal a fatal anomaly to its
// siblings. Identical across every worker spawned from main except workerID.
type workerCtx struct {
	ctx         context.Context
	cancelCause context.CancelCauseFunc
	workerID    int
	totalOps    *atomic.Int64
	log         *zap.Logger
}

// runPlan bundles the run configuration every worker operates under: which
// ops to sample from and the block layout. Identical across every worker
// spawned from main.
type runPlan struct {
	ops       []Op
	nBlocks   int64
	blockSize int
}

// spawnWorker opens handles for the given file pool, creates an opRunner,
// and runs until ctx is done or an anomaly is found.
func spawnWorker(wc workerCtx, paths []string, plan runPlan) error {
	handles, err := openHandles(paths, wc.workerID, plan.blockSize, wc.log)
	if err != nil {
		err = fmt.Errorf("worker %d: open handles: %w", wc.workerID, err)
		fmt.Fprintln(os.Stderr, err)
		wc.cancelCause(err)
		return err
	}
	defer closeHandles(handles)

	r := newOpRunner(handles, plan.ops, int64(wc.workerID), plan.nBlocks, plan.blockSize)
	return runWorker(wc, r)
}

// runWorker loops until ctx is done or an anomaly is returned by runOnce.
func runWorker(wc workerCtx, r *opRunner) error {
	for {
		select {
		case <-wc.ctx.Done():
			return nil
		default:
		}
		if err := r.runOnce(wc.ctx); err != nil {
			err = fmt.Errorf("worker %d: %w", wc.workerID, err)
			fmt.Fprintln(os.Stderr, err)
			wc.cancelCause(err)
			return err
		}
		wc.totalOps.Add(1)
	}
}

// progressLoop prints an ops count line every 5 seconds until ctx is done.
func progressLoop(ctx context.Context, start time.Time, totalOps *atomic.Int64) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			elapsed := time.Since(start).Round(time.Second)
			fmt.Printf("[%s] ops=%d\n", elapsed, totalOps.Load())
		case <-ctx.Done():
			return
		}
	}
}

// resolveFiles returns the noOverlap file path and the shared file pool.
// Both are created inside dir. The number of shared files is (threads-1)/2,
// clamped to a minimum of 1 when there are shared workers.
func resolveFiles(dir string, threads int) (noOverlapPaths, sharedPaths []string, err error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	noOverlapPaths = []string{filepath.Join(dir, "iotest-soak-0.noOverlap")}

	nShared := threads - 1
	if nShared <= 0 {
		return noOverlapPaths, nil, nil
	}
	nFiles := nShared / 2
	if nFiles == 0 {
		nFiles = 1
	}
	sharedPaths = make([]string, nFiles)
	for i := range sharedPaths {
		sharedPaths[i] = filepath.Join(dir, fmt.Sprintf("iotest-soak-%d.shared", i))
	}
	return noOverlapPaths, sharedPaths, nil
}

// openHandles opens a fileHandle for each path in paths on behalf of workerID.
// Each handle gets its own file description so OFD locks between workers work correctly.
func openHandles(paths []string, workerID, blockSize int, log *zap.Logger) ([]*fileHandle, error) {
	handles := make([]*fileHandle, 0, len(paths))
	for _, p := range paths {
		h, err := openHandle(p, workerID, blockSize, log)
		if err != nil {
			closeHandles(handles)
			return nil, err
		}
		handles = append(handles, h)
	}
	return handles, nil
}

func openHandle(path string, workerID, blockSize int, log *zap.Logger) (*fileHandle, error) {
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	store, err := xattrstore.OpenStore(path)
	if err != nil {
		closeErr := f.Close()
		return nil, fmt.Errorf("OpenStore %s: %w", path, errors.Join(err, closeErr))
	}
	w, err := xattrstore.NewWriter(f, store, workerID, block.KindDecimal, blockSize, log)
	if err != nil {
		closeErr := f.Close()
		return nil, fmt.Errorf("NewWriter %s: %w", path, errors.Join(err, closeErr))
	}
	return &fileHandle{path: path, file: f, store: store, writer: w}, nil
}

func closeHandles(handles []*fileHandle) {
	for _, h := range handles {
		if err := h.close(); err != nil {
			fmt.Fprintf(os.Stderr, "close %s: %v\n", h.path, err)
		}
	}
}

func printPlan(threads int, noOverlapPaths, sharedPaths []string, duration time.Duration) {
	fmt.Printf("Starting soak: %d thread(s)", threads)
	if duration > 0 {
		fmt.Printf(" for %s", duration)
	}
	fmt.Println()
	fmt.Printf("  noOverlap: %s\n", noOverlapPaths[0])
	for _, p := range sharedPaths {
		fmt.Printf("  shared:    %s\n", p)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
