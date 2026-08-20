package iotest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/verifyio/bench/posixbench"
	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// benchExtraStatus is written into workloadStatus.ExtraData after a bench run
// completes. It captures the full per-node throughput snapshot so that
// "beegfs iotest status" can display bandwidth numbers alongside the summary
// table without re-reading any data files.
type benchExtraStatus struct {
	WriteMBps      float64 `json:"writeMBps"`
	WriteMiBps     float64 `json:"writeMiBps"`
	WriteIOPS      float64 `json:"writeIOPS"`
	MinMBps        float64 `json:"minMBps"`
	MaxMBps        float64 `json:"maxMBps"`
	StdDevMBps     float64 `json:"stdDevMBps"`
	ReadMBps       float64 `json:"readMBps"`
	ReadMiBps      float64 `json:"readMiBps"`
	ReadIOPS       float64 `json:"readIOPS"`
	ReadMinMBps    float64 `json:"readMinMBps"`
	ReadMaxMBps    float64 `json:"readMaxMBps"`
	ReadStdDevMBps float64 `json:"readStdDevMBps"`
}

// FormatExtraStatus implements StatusFormatter. It decodes benchExtraStatus and
// returns one or two lines (write + optional read) formatted for status output.
func (b *BenchTool) FormatExtraStatus(node string, data json.RawMessage) string {
	var s benchExtraStatus
	if err := json.Unmarshal(data, &s); err != nil {
		return ""
	}
	write := fmt.Sprintf("  %-20s  write: %8.2f MB/s  %8.2f MiB/s  %8.0f IOPS  [min=%.2f max=%.2f stddev=%.2f]",
		node, s.WriteMBps, s.WriteMiBps, s.WriteIOPS, s.MinMBps, s.MaxMBps, s.StdDevMBps)
	if s.ReadMBps == 0 {
		return write
	}
	read := fmt.Sprintf("  %-20s  read:  %8.2f MB/s  %8.2f MiB/s  %8.0f IOPS  [min=%.2f max=%.2f stddev=%.2f]",
		"", s.ReadMBps, s.ReadMiBps, s.ReadIOPS, s.ReadMinMBps, s.ReadMaxMBps, s.ReadStdDevMBps)
	return write + "\n" + read
}

const (
	benchDefaultThreads   = 4
	benchDefaultBlockSize = 128 * 1024       // 128 KiB
	benchDefaultFileSize  = 10 * 1024 * 1024 // 10 MiB per worker
)

// BenchTool implements Tool. It runs a write-once POSIX IO benchmark and
// reports bandwidth. A posixbench.json manifest is written for later
// verification with 'beegfs iotest bench verify'.
type BenchTool struct {
	noXattr bool
}

func (b *BenchTool) Name() string { return "bench" }
func (b *BenchTool) Description() string {
	return "write-once POSIX IO benchmark with manifest-based verification"
}

func (b *BenchTool) RegisterFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&b.noXattr, "no-xattr", false,
		"write per-stripe ECC into data files instead of xattrs; blocksize is encoded in filenames; verification is ECC-only")
}

func (b *BenchTool) Run(ctx context.Context, cfg RunConfig) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get hostname: %w", err)
	}

	bc := posixbench.DefaultConfig()
	bc.Path = cfg.Path
	bc.Hostname = hostname
	bc.Threads = benchDefaultThreads
	if cfg.Threads > 0 {
		bc.Threads = cfg.Threads
	}
	// --blocksize (RunConfig.BlockSize) overrides the transfer/block size; 0
	// means use bench's default. posixbench validates it divides FileSize.
	bc.BlockSize = benchDefaultBlockSize
	if cfg.BlockSize > 0 {
		bc.BlockSize = cfg.BlockSize
	}
	bc.FileSize = benchDefaultFileSize
	bc.NoXattr = b.noXattr
	bc.EnsureSeed()
	if err := bc.Validate(); err != nil {
		return err
	}

	totalData := int64(bc.Threads) * bc.FileSize
	fmt.Printf("posixbench run\n")
	fmt.Printf("  path=%s  threads=%d  layout=%s  kind=%s  seed=%d\n",
		bc.Path, bc.Threads, bc.Layout, bc.Kind, bc.Seed)
	fmt.Printf("  blockSize=%s  fileSize=%s/worker  totalData=%s\n",
		iotestHumanSize(int64(bc.BlockSize)), iotestHumanSize(bc.FileSize), iotestHumanSize(totalData))
	fmt.Println()

	r, err := posixbench.NewRunner(bc)
	if err != nil {
		return fmt.Errorf("bench: %w", err)
	}

	start := time.Now()
	res, err := r.Run(true)
	if err != nil {
		return fmt.Errorf("bench: %w", err)
	}
	elapsed := time.Since(start)

	writeIOPS := float64(res.TotalWritten) / float64(bc.BlockSize) / res.WriteElapsed.Seconds()
	writeMiBps := float64(res.TotalWritten) / (1024 * 1024) / res.WriteElapsed.Seconds()
	readIOPS := float64(res.TotalRead) / float64(bc.BlockSize) / res.ReadElapsed.Seconds()
	readMiBps := float64(res.TotalRead) / (1024 * 1024) / res.ReadElapsed.Seconds()
	ws := res.WriteStats()
	rs := res.ReadStats()

	fmt.Printf("%-10s  %10s  %10s  %10s  %10s  %10s  %10s  %12s\n",
		"access", "bw(MB/s)", "bw(MiB/s)", "IOPS", "min(MB/s)", "max(MB/s)", "stddev", "elapsed")
	fmt.Printf("%-10s  %10s  %10s  %10s  %10s  %10s  %10s  %12s\n",
		"------", "--------", "---------", "----", "---------", "---------", "------", "-------")
	fmt.Printf("%-10s  %10.2f  %10.2f  %10.0f  %10.2f  %10.2f  %10.2f  %12s\n",
		"write", res.WriteMBps(), writeMiBps, writeIOPS,
		ws.MinMBps, ws.MaxMBps, ws.StdDevMBps,
		res.WriteElapsed.Round(time.Millisecond))
	fmt.Printf("%-10s  %10.2f  %10.2f  %10.0f  %10.2f  %10.2f  %10.2f  %12s\n",
		"read", res.ReadMBps(), readMiBps, readIOPS,
		rs.MinMBps, rs.MaxMBps, rs.StdDevMBps,
		res.ReadElapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("Elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Manifest: %s\n", posixbench.ManifestPath(bc.Path, bc.Hostname))

	cfg.TotalOps.Add(res.TotalWritten/int64(bc.BlockSize) + res.TotalRead/int64(bc.BlockSize))

	if cfg.SetExtraStatus != nil {
		cfg.SetExtraStatus(benchExtraStatus{
			WriteMBps:      res.WriteMBps(),
			WriteMiBps:     writeMiBps,
			WriteIOPS:      writeIOPS,
			MinMBps:        ws.MinMBps,
			MaxMBps:        ws.MaxMBps,
			StdDevMBps:     ws.StdDevMBps,
			ReadMBps:       res.ReadMBps(),
			ReadMiBps:      readMiBps,
			ReadIOPS:       readIOPS,
			ReadMinMBps:    rs.MinMBps,
			ReadMaxMBps:    rs.MaxMBps,
			ReadStdDevMBps: rs.StdDevMBps,
		})
	}
	return nil
}

func newBenchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Write-once POSIX IO benchmark with manifest-based verification",
		Long: `Write-once POSIX IO benchmark with manifest-based verification.

  run     write data files (+ optional read phase) and report bandwidth
  verify  check data files against the manifest written by run`,
	}
	cmd.AddCommand(newBenchRunCmd(), newBenchVerifyCmd())
	return cmd
}

func newBenchRunCmd() *cobra.Command {
	cfg := posixbench.DefaultConfig()
	var (
		kindStr string
		doRead  bool
		noXattr bool
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Write data files and report bandwidth",
		Long: `Write data files to a directory and report write (and optionally read) bandwidth.
A posixbench.json manifest is written alongside the data for later verification.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if helpIfNoFlags(cmd) {
				return nil
			}
			if cfg.Path == "" {
				return fmt.Errorf("required flag \"path\" not set")
			}
			k, err := block.KindFromString(kindStr)
			if err != nil {
				return fmt.Errorf("run: %w", err)
			}
			cfg.Kind = k
			cfg.NoXattr = noXattr
			cfg.EnsureSeed()
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("run: %w", err)
			}

			r, err := posixbench.NewRunner(cfg)
			if err != nil {
				return fmt.Errorf("run: %w", err)
			}

			totalData := int64(cfg.Threads) * int64(cfg.FilesPerWorker) * cfg.FileSize
			if cfg.Layout == posixbench.LayoutNto1 {
				totalData = int64(cfg.Threads) * cfg.FileSize
			}

			fmt.Printf("posixbench run\n")
			fmt.Printf("  path=%s threads=%d layout=%s kind=%s seed=%d\n",
				cfg.Path, cfg.Threads, cfg.Layout, cfg.Kind, cfg.Seed)
			fmt.Printf("  blockSize=%s fileSize=%s totalData=%s\n",
				iotestHumanSize(int64(cfg.BlockSize)), iotestHumanSize(cfg.FileSize), iotestHumanSize(totalData))
			fmt.Println()

			start := time.Now()
			res, err := r.Run(doRead)
			if err != nil {
				return fmt.Errorf("run: %w", err)
			}
			elapsed := time.Since(start)

			fmt.Printf("Write:  %7.1f MB/s  %s in %s\n",
				res.WriteMBps(),
				iotestHumanSize(res.TotalWritten),
				res.WriteElapsed.Round(time.Millisecond))
			if doRead {
				fmt.Printf("Read:   %7.1f MB/s  %s in %s\n",
					res.ReadMBps(),
					iotestHumanSize(res.TotalRead),
					res.ReadElapsed.Round(time.Millisecond))
			}
			fmt.Printf("Elapsed: %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("Manifest: %s/posixbench.json\n", cfg.Path)
			return nil
		},
	}
	cmd.Flags().StringVar(&cfg.Path, "path", "", "target directory (required)")
	cmd.Flags().IntVar(&cfg.Threads, "threads", cfg.Threads, "worker goroutines")
	cmd.Flags().IntVar(&cfg.BlockSize, "block-size", cfg.BlockSize, "IO transfer size in bytes")
	cmd.Flags().Int64Var(&cfg.FileSize, "file-size", cfg.FileSize, "per-worker data size in bytes")
	cmd.Flags().IntVar(&cfg.FilesPerWorker, "files-per-worker", cfg.FilesPerWorker, "files per worker (1-to-1 layout only)")
	cmd.Flags().StringVar((*string)(&cfg.Layout), "layout", string(cfg.Layout), "file distribution: 1-to-1 or n-to-1")
	cmd.Flags().StringVar(&kindStr, "kind", "decimal", "data pattern: decimal|prng|zeros|ones|countup|repeat")
	cmd.Flags().Uint64Var(&cfg.Seed, "seed", 0, "pattern seed (0 = random, recorded in manifest)")
	cmd.Flags().BoolVar(&doRead, "read", false, "run a sequential read phase after writing")
	cmd.Flags().BoolVar(&noXattr, "no-xattr", false,
		"write per-stripe ECC into data files instead of xattrs; blocksize is encoded in filenames; verification is ECC-only")
	return cmd
}

func newBenchVerifyCmd() *cobra.Command {
	var (
		path     string
		hostname string
		verbose  bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check data files against the posixbench.json manifest written by run",
		Long: `Verify data files against the posixbench.json manifest written by 'bench run'.

When verifying files written by 'beegfs iotest start bench' on a shared path,
pass --hostname to select the per-node manifest (posixbench-{hostname}.json).`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if helpIfNoFlags(cmd) {
				return nil
			}
			if path == "" {
				return fmt.Errorf("required flag \"path\" not set")
			}
			cfg, err := posixbench.ReadManifestHost(path, hostname)
			if err != nil {
				return fmt.Errorf("verify: %w", err)
			}

			v, err := posixbench.NewVerifier(cfg)
			if err != nil {
				return fmt.Errorf("verify: %w", err)
			}

			fmt.Printf("posixbench verify\n")
			fmt.Printf("  path=%s seed=%d kind=%s blockSize=%s\n",
				path, cfg.Seed, cfg.Kind, iotestHumanSize(int64(cfg.BlockSize)))
			fmt.Println()

			start := time.Now()
			n, err := v.Verify(func(a posixbench.Anomaly) error {
				if verbose {
					fmt.Printf("  MISMATCH  file=%s fileIndex=%d blockIndex=%d offset=%d\n",
						a.File, a.FileIndex, a.BlockIndex, a.Offset)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("verify: %w", err)
			}

			fmt.Printf("Elapsed: %s\n", time.Since(start).Round(time.Millisecond))
			if n == 0 {
				fmt.Println("PASS")
				return nil
			}
			return fmt.Errorf("FAIL: %d block(s) mismatched", n)
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "directory containing the manifest (required)")
	cmd.Flags().StringVar(&hostname, "hostname", "", "verify files for this hostname (reads posixbench-{hostname}.json)")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "print all mismatched blocks")
	return cmd
}
