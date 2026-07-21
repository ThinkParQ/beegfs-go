//go:build linux

package iotest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
	"go.uber.org/zap"
)

const (
	soakDefaultThreads    = 4
	soakDefaultDuration   = 5 * time.Minute
	soakDefaultBlockSize  = 1024
	soakDefaultFileBlocks = 64
)

// SoakTool implements Tool. It runs a multi-threaded soak test that
// continuously writes and verifies blocks with OFD locking.
type SoakTool struct {
	logLevel    int
	logFile     string
	lockTimeout time.Duration
	duration    time.Duration
}

func (s *SoakTool) Name() string { return "soak" }
func (s *SoakTool) Description() string {
	return "continuous write-verify loop; detects corruption, torn writes, and stale cache"
}

func (s *SoakTool) RegisterFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(&s.logLevel, "iolog-level", 0, "IO log level (0=off, 1=error, 2=warn, 3=info, 4/5=debug)")
	cmd.Flags().StringVar(&s.logFile, "iolog-file", "", "IO log destination file (default stderr)")
	cmd.Flags().DurationVar(&s.lockTimeout, "lock-timeout", 30*time.Second,
		"give up on a stuck POSIX lock/xattr syscall after this long and fail (0 = wait indefinitely)")
	cmd.Flags().DurationVar(&s.duration, "duration", soakDefaultDuration,
		"how long to run (0 = until stopped or an anomaly)")
}

func (s *SoakTool) CompletionHint(path string) string {
	files, _ := filepath.Glob(filepath.Join(path, "iotest-soak-*"))
	if len(files) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("Soak data files:\n")
	const maxShow = 5
	for i, f := range files {
		if i >= maxShow {
			fmt.Fprintf(&sb, "  ... and %d more\n", len(files)-maxShow)
			break
		}
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		fmt.Fprintf(&sb, "  %8s  %s\n", iotestHumanSize(info.Size()), filepath.Base(f))
	}
	sb.WriteString("\nTo verify:\n")
	for i, f := range files {
		if i >= maxShow {
			break
		}
		fmt.Fprintf(&sb, "  beegfs iotest verify --path %s\n", f)
	}
	return sb.String()
}

func (s *SoakTool) Run(ctx context.Context, cfg RunConfig) error {
	ioLog, cleanup, err := newIOTraceLogger(s.logLevel, s.logFile)
	if err != nil {
		return err
	}
	defer cleanup()
	var localNode string
	if hostname, err := os.Hostname(); err == nil {
		localNode = hostname
		ioLog = ioLog.With(zap.String("node", hostname))
	}

	// --threads (RunConfig.Threads) overrides the worker count; 0 = soak default.
	threads := cfg.Threads
	if threads <= 0 {
		threads = soakDefaultThreads
	}
	dur := s.duration // --duration; 0 = run until stopped or an anomaly

	if err := soakCheckGlobalFileLocks(ctx, cfg.Path, ioLog); err != nil {
		return err
	}

	noOverlapPaths, sharedPaths, err := soakResolveFiles(cfg.Path, threads)
	if err != nil {
		return err
	}
	soakPrintPlan(threads, noOverlapPaths, sharedPaths, dur)

	// Open one Store per file before spawning workers. Workers operating on the
	// same file share a Store so the intra-process mutex inside each Store
	// serialises all goroutines on this node (POSIX F_SETLK only provides
	// inter-process exclusivity, not inter-goroutine).
	noOverlapStores, err := soakOpenStores(noOverlapPaths, s.lockTimeout)
	if err != nil {
		return err
	}
	sharedStores, err := soakOpenStores(sharedPaths, s.lockTimeout)
	if err != nil {
		return err
	}

	baseCtx, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)
	// dur == 0 means run until the stop signal or an anomaly cancels baseCtx.
	var runCtx context.Context
	var stop context.CancelFunc
	if dur > 0 {
		runCtx, stop = context.WithTimeout(baseCtx, dur)
	} else {
		runCtx, stop = context.WithCancel(baseCtx)
	}
	defer stop()

	// --blocksize (RunConfig.BlockSize) overrides the per-block size; 0 means
	// use soak's default.
	bs := cfg.BlockSize
	if bs <= 0 {
		bs = soakDefaultBlockSize
	}
	ops := []soakOp{
		soakWriteBufferedOp(),
		soakVerifyRangeOp(bs, localNode),
	}
	nBlocks := int64(soakDefaultFileBlocks)

	var (
		wg       sync.WaitGroup
		failures atomic.Int64
	)
	start := time.Now()

	go soakProgressLoop(runCtx, start, cfg.TotalOps)
	go soakStopWatchLogger(runCtx, cfg.StopSignal, ioLog)

	plan := soakPlan{ops: ops, nBlocks: nBlocks, blockSize: bs}

	wg.Add(1)
	go func() {
		defer wg.Done()
		wc := soakWorkerCtx{ctx: runCtx, cancelCause: cancelCause, workerID: 0, totalOps: cfg.TotalOps, stopSig: cfg.StopSignal, log: ioLog}
		if err := soakSpawnWorker(wc, noOverlapPaths, noOverlapStores, plan); err != nil {
			failures.Add(1)
		}
	}()

	for i := 1; i < threads; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			wc := soakWorkerCtx{ctx: runCtx, cancelCause: cancelCause, workerID: workerID, totalOps: cfg.TotalOps, stopSig: cfg.StopSignal, log: ioLog}
			if err := soakSpawnWorker(wc, sharedPaths, sharedStores, plan); err != nil {
				failures.Add(1)
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start).Round(time.Millisecond)
	fmt.Printf("elapsed=%s ops=%d\n", elapsed, cfg.TotalOps.Load())

	if cfg.StopSignal != nil && cfg.StopSignal.Load() {
		fmt.Println("STOPPED (stop signal received)")
		return nil
	}
	cause := context.Cause(baseCtx)
	workerFailure := cause != nil && cause != context.DeadlineExceeded && cause != context.Canceled
	if failures.Load() > 0 || workerFailure {
		// The failing worker already printed the full anomaly inline (see
		// soakRunWorker); don't echo the same multi-line cause again here.
		return fmt.Errorf("FAIL")
	}
	fmt.Println("PASS")
	return nil
}

// ── Op types ──────────────────────────────────────────────────────────────────

// soakOp is a single atomic operation applied to one block within a soak file.
// Implementations must be safe to call concurrently on different handles.
type soakOp func(ctx context.Context, h *soakFileHandle, offset int64) error

// soakFileHandle bundles the open file, its xattr store, and a Writer for a
// single soak data file. Workers hold one handle per file they operate on.
type soakFileHandle struct {
	path   string
	file   *fileops.File
	store  *xattrstore.Store
	writer *xattrstore.Writer
}

func (h *soakFileHandle) close() error { return h.file.Close() }

func soakWriteBufferedOp() soakOp {
	return func(_ context.Context, h *soakFileHandle, offset int64) error {
		err := h.writer.WriteBlock(offset, fileops.IOTypeBuffered, true)
		if errors.Is(err, xattrstore.ErrLockBusy) || errors.Is(err, xattrstore.ErrSetChanged) {
			return nil
		}
		return err
	}
}

func soakVerifyRangeOp(blockSize int, localNode string) soakOp {
	return func(_ context.Context, h *soakFileHandle, offset int64) error {
		lease, err := h.store.TryAcquireShared(h.file.LockFd(), offset, int64(blockSize))
		if err != nil {
			if errors.Is(err, xattrstore.ErrLockBusy) {
				return nil
			}
			return err
		}
		defer func() { _ = lease.Release() }()
		// LockNone: the lease above already covers [offset, offset+blockSize).
		// VerifyFile's default per-span locking would re-acquire a shared lock
		// on the same Store, which self-deadlocks in LockModePOSIX (the
		// in-process mutex is not reentrant).
		return verifier.VerifyFile(h.store, h.file, verifier.Options{
			Range:    &verifier.ByteRange{Offset: offset, Length: int64(blockSize)},
			LockMode: verifier.LockNone,
		}, func(span verifier.Span) error {
			if !soakSpanIsAnomaly(span) {
				return nil
			}
			var ino uint64
			var st unix.Stat_t
			if unix.Fstat(int(h.file.LockFd().Fd()), &st) == nil {
				ino = st.Ino
			}
			msg := fmt.Sprintf("anomaly at [%d,%d): file=%s inode=%d coverage=%s verdict=%s xattrMatch=%v",
				span.Offset, span.Offset+span.Length, h.path, ino, span.Coverage, span.Verdict, span.XattrMatch)
			if span.Header != nil {
				// TimeNs is stamped by the writer at the time of the write. If
				// the clocks on both nodes are NTP-synchronised this gives the
				// true age of the written block at the moment the anomaly is
				// detected. A large write_age for BODY_CORRUPT indicates stale
				// data in the local page cache — the writer flushed long ago but
				// this node has not yet seen the updated bytes from the storage
				// servers.
				writeAge := time.Since(time.Unix(0, span.Header.TimeNs)).Round(time.Millisecond)
				writerNode := block.NodeNameString(span.Header.NodeName)
				fsynced := span.Header.Tag&block.TagFsynced != 0
				msg += fmt.Sprintf(" writer=%s reader=%s write_age=%s fsynced=%v seed=0x%016x cycle=%d",
					writerNode, localNode, writeAge, fsynced, span.Header.Seed, span.Header.Cycle)
				if span.Diag != nil {
					stripeState := fmt.Sprintf("OK(%d/%d)",
						span.Diag.StripesTotal-span.Diag.StripesFailed, span.Diag.StripesTotal)
					if span.Diag.StripesFailed > 0 {
						stripeState = fmt.Sprintf("FAILED(%d/%d)",
							span.Diag.StripesFailed, span.Diag.StripesTotal)
					}
					msg += fmt.Sprintf(" bodyCRC(hdr)=0x%08x bodyCRC(read)=0x%08x stripeCRC=%s",
						span.Header.BodyCRC, span.Diag.ReadBodyCRC, stripeState)
				}
			}
			// The verdict codes are terse; add a plain-English line so the log is
			// self-explanatory (e.g. that BODY_CORRUPT is valid-but-wrong-version
			// data, not necessarily corruption). Only meaningful for CoverageOne,
			// where Verdict is populated.
			if span.Coverage == verifier.CoverageOne {
				msg += "\n  meaning: " + span.Verdict.Explanation()
			}
			return fmt.Errorf("%s", msg)
		})
	}
}

func soakSpanIsAnomaly(s verifier.Span) bool {
	switch s.Coverage {
	case verifier.CoverageMany:
		return true
	case verifier.CoverageOne:
		return s.Verdict != block.VerdictOK
	default:
		return false
	}
}

// ── Op runner ─────────────────────────────────────────────────────────────────

type soakOpRunner struct {
	handles   []*soakFileHandle
	ops       []soakOp
	rng       *rand.Rand
	nBlocks   int64
	blockSize int
}

func newSoakOpRunner(handles []*soakFileHandle, ops []soakOp, seed int64, nBlocks int64, blockSize int) *soakOpRunner {
	return &soakOpRunner{
		handles:   handles,
		ops:       ops,
		rng:       rand.New(rand.NewSource(seed)),
		nBlocks:   nBlocks,
		blockSize: blockSize,
	}
}

func (r *soakOpRunner) runOnce(ctx context.Context) error {
	h := r.handles[r.rng.Intn(len(r.handles))]
	op := r.ops[r.rng.Intn(len(r.ops))]
	offset := r.rng.Int63n(r.nBlocks) * int64(r.blockSize)
	return op(ctx, h, offset)
}

// ── Workers ───────────────────────────────────────────────────────────────────

// soakWorkerCtx bundles the per-worker execution environment: everything a
// worker needs to report progress, signal a fatal anomaly to its siblings,
// and know when to stop. It's identical across every worker started by one
// Run call except workerID.
type soakWorkerCtx struct {
	ctx         context.Context
	cancelCause context.CancelCauseFunc
	workerID    int
	totalOps    *atomic.Int64
	stopSig     *atomic.Bool
	log         *zap.Logger
}

// soakPlan bundles the run configuration every worker operates under: which
// ops to sample from and the block layout. Identical across every worker
// started by one Run call.
type soakPlan struct {
	ops       []soakOp
	nBlocks   int64
	blockSize int
}

func soakSpawnWorker(wc soakWorkerCtx, paths []string, stores map[string]*xattrstore.Store, plan soakPlan) error {
	handles, err := soakOpenHandles(paths, stores, wc.workerID, plan.blockSize, wc.log)
	if err != nil {
		err = fmt.Errorf("worker %d: open handles: %w", wc.workerID, err)
		fmt.Fprintln(os.Stderr, err)
		wc.cancelCause(err)
		return err
	}
	defer soakCloseHandles(handles)

	r := newSoakOpRunner(handles, plan.ops, int64(wc.workerID), plan.nBlocks, plan.blockSize)
	return soakRunWorker(wc, r)
}

func soakRunWorker(wc soakWorkerCtx, r *soakOpRunner) error {
	for {
		select {
		case <-wc.ctx.Done():
			return nil
		default:
		}
		if wc.stopSig != nil && wc.stopSig.Load() {
			return nil
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

func soakStopWatchLogger(ctx context.Context, stopSig *atomic.Bool, log *zap.Logger) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if stopSig != nil && stopSig.Load() {
				log.Info("stop signal detected; waiting for workers to finish")
				return
			}
			log.Info("running; no stop signal detected")
		case <-ctx.Done():
			return
		}
	}
}

func soakProgressLoop(ctx context.Context, start time.Time, totalOps *atomic.Int64) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			fmt.Printf("[%s] ops=%d\n", time.Since(start).Round(time.Second), totalOps.Load())
		case <-ctx.Done():
			return
		}
	}
}

// soakCheckGlobalFileLocks verifies the BeeGFS client mount backing path has
// tuneUseGlobalFileLocks enabled. soakOpenStores relies on
// xattrstore.OpenStoreMultiNode's cross-node POSIX locking (LockModePOSIX),
// which silently degrades to local-only locking if the client does not
// propagate flock/fcntl locks across nodes, breaking soak's correctness
// guarantees when multiple nodes share the same files.
func soakCheckGlobalFileLocks(ctx context.Context, path string, log *zap.Logger) error {
	fs, err := filesystem.NewFromPath(path)
	if err != nil {
		log.Warn("unable to determine BeeGFS mount point for path (skipping cross-node lock check)", zap.String("path", path), zap.Error(err))
		return nil
	}
	mountPath := fs.GetMountPath()

	clients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{FilterByMounts: []string{mountPath}}, &logger.Logger{Logger: log})
	if err != nil {
		log.Warn("unable to verify BeeGFS client configuration: unable to fetch configuration from /proc/fs/beegfs (ignoring)", zap.Error(err))
		return nil
	}
	if len(clients) != 1 {
		log.Warn("unable to verify BeeGFS client configuration: expected exactly one entry in /proc/fs/beegfs for this mount point (ignoring)", zap.String("mountPoint", mountPath))
		return nil
	}

	val, ok := clients[0].Config["tuneUseGlobalFileLocks"]
	if !ok {
		log.Warn("the BeeGFS client version does not appear to support 'tuneUseGlobalFileLocks': cannot verify cross-node locking is enabled (ignoring)")
		return nil
	}
	if val != "1" {
		return fmt.Errorf("soak relies on cross-node POSIX file locking, but the BeeGFS mount %s has 'tuneUseGlobalFileLocks = %s': "+
			"set 'tuneUseGlobalFileLocks = true' in beegfs-client.conf on every node running soak against this mount", mountPath, val)
	}
	return nil
}

// ── File management ───────────────────────────────────────────────────────────

func soakResolveFiles(dir string, threads int) (noOverlapPaths, sharedPaths []string, err error) {
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

// soakOpenStores ensures each path exists and opens one Store per path.
// Callers pass the resulting map to soakOpenHandles so all workers that share
// a file use the same Store instance and its intra-process mutex.
func soakOpenStores(paths []string, lockTimeout time.Duration) (map[string]*xattrstore.Store, error) {
	m := make(map[string]*xattrstore.Store, len(paths))
	for _, p := range paths {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("create %s: %w", p, err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close %s: %w", p, err)
		}
		store, err := xattrstore.OpenStoreMultiNode(p, lockTimeout)
		if err != nil {
			return nil, fmt.Errorf("OpenStore %s: %w", p, err)
		}
		m[p] = store
	}
	return m, nil
}

func soakOpenHandles(paths []string, stores map[string]*xattrstore.Store, workerID, blockSize int, log *zap.Logger) ([]*soakFileHandle, error) {
	handles := make([]*soakFileHandle, 0, len(paths))
	for _, p := range paths {
		h, err := soakOpenHandle(p, stores[p], workerID, blockSize, log)
		if err != nil {
			soakCloseHandles(handles)
			return nil, err
		}
		handles = append(handles, h)
	}
	return handles, nil
}

func soakOpenHandle(path string, store *xattrstore.Store, workerID, blockSize int, log *zap.Logger) (*soakFileHandle, error) {
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	w, err := xattrstore.NewWriter(f, store, workerID, block.KindDecimal, blockSize, log)
	if err != nil {
		closeErr := f.Close()
		return nil, fmt.Errorf("NewWriter %s: %w", path, errors.Join(err, closeErr))
	}
	return &soakFileHandle{path: path, file: f, store: store, writer: w}, nil
}

func soakCloseHandles(handles []*soakFileHandle) {
	for _, h := range handles {
		if err := h.close(); err != nil {
			fmt.Fprintf(os.Stderr, "close %s: %v\n", h.path, err)
		}
	}
}

func soakPrintPlan(threads int, noOverlapPaths, sharedPaths []string, duration time.Duration) {
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
