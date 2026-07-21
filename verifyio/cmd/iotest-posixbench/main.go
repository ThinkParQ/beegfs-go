// iotest-posixbench -- write-once POSIX IO benchmark with optional verification.
//
// Subcommands:
//
//	run     write data files (+ optional read phase) and report bandwidth
//	verify  check data files against the manifest written by run
//
// Example:
//
//	iotest-posixbench run -path /mnt/beegfs/testdir -threads 8 -file-size 4294967296
//	iotest-posixbench verify -path /mnt/beegfs/testdir
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/bench/posixbench"
	"github.com/thinkparq/beegfs-go/verifyio/block"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	w := os.Stderr
	fmt.Fprintln(w, "Usage: iotest-posixbench <run|verify> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Write-once POSIX IO benchmark with manifest-based post-run verification.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  run     write data files and optionally read them back")
	fmt.Fprintln(w, "  verify  check data files against the run manifest")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintln(w, "  iotest-posixbench run -path /mnt/testdir -threads 8")
	fmt.Fprintln(w, "  iotest-posixbench run -path /mnt/testdir -threads 8 -file-size 4294967296 -read")
	fmt.Fprintln(w, "  iotest-posixbench verify -path /mnt/testdir")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run 'iotest-posixbench <subcommand>' with no flags for subcommand help.")
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var (
		path    = fs.String("path", "", "target directory (required)")
		threads = fs.Int("threads", 4, "worker goroutines")
		bs      = fs.Int("block-size", posixbench.DefaultBlockSize, "IO transfer size in bytes")
		fsz     = fs.Int64("file-size", 1*1024*1024*1024, "per-worker data size in bytes (bytes, not a size suffix)")
		fpw     = fs.Int("files-per-worker", 1, "files per worker (1-to-1 layout only)")
		layout  = fs.String("layout", "1-to-1", "file distribution: 1-to-1 or n-to-1")
		kind    = fs.String("kind", "decimal", "data pattern: decimal|prng|zeros|ones|countup|repeat")
		seed    = fs.Uint64("seed", 0, "pattern seed (0 = random, recorded in manifest)")
		doRead  = fs.Bool("read", false, "run a sequential read phase after writing")
		noXattr = fs.Bool("no-xattr", false,
			"write per-stripe ECC into data files instead of xattrs; blocksize is encoded in filenames; verification is ECC-only")
	)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: iotest-posixbench run -path <dir> [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Write data files to a directory and report write (and optionally read) bandwidth.")
		fmt.Fprintln(w, "A posixbench.json manifest is written alongside the data for later verification.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-posixbench run -path /mnt/testdir -threads 8")
		fmt.Fprintln(w, "  iotest-posixbench run -path /mnt/testdir -threads 8 -file-size 4294967296 -read")
		fmt.Fprintln(w, "  iotest-posixbench run -path /mnt/testdir -threads 4 -layout n-to-1 -kind prng")
	}
	_ = fs.Parse(args)

	if *path == "" {
		fs.Usage()
		os.Exit(2)
	}
	k, err := block.KindFromString(*kind)
	if err != nil {
		die("run: %v", err)
	}

	cfg := posixbench.Config{
		Path:           *path,
		Threads:        *threads,
		BlockSize:      *bs,
		FileSize:       *fsz,
		FilesPerWorker: *fpw,
		Layout:         posixbench.Layout(*layout),
		Kind:           k,
		Seed:           *seed,
		NoXattr:        *noXattr,
	}
	cfg.EnsureSeed()
	if err := cfg.Validate(); err != nil {
		die("run: %v", err)
	}

	r, err := posixbench.NewRunner(cfg)
	if err != nil {
		die("run: %v", err)
	}

	totalData := int64(cfg.Threads) * int64(cfg.FilesPerWorker) * cfg.FileSize
	if cfg.Layout == posixbench.LayoutNto1 {
		totalData = int64(cfg.Threads) * cfg.FileSize
	}

	fmt.Printf("posixbench run\n")
	fmt.Printf("  path=%s threads=%d layout=%s kind=%s seed=%d\n",
		cfg.Path, cfg.Threads, cfg.Layout, cfg.Kind, cfg.Seed)
	fmt.Printf("  blockSize=%s fileSize=%s totalData=%s\n",
		humanBytes(int64(cfg.BlockSize)), humanBytes(cfg.FileSize), humanBytes(totalData))
	fmt.Println()

	start := time.Now()
	res, err := r.Run(*doRead)
	if err != nil {
		die("run: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("Write:  %7.1f MB/s  %s in %s\n",
		res.WriteMBps(),
		humanBytes(res.TotalWritten),
		res.WriteElapsed.Round(time.Millisecond))
	if *doRead {
		fmt.Printf("Read:   %7.1f MB/s  %s in %s\n",
			res.ReadMBps(),
			humanBytes(res.TotalRead),
			res.ReadElapsed.Round(time.Millisecond))
	}
	fmt.Printf("Elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Manifest: %s/posixbench.json\n", cfg.Path)
}

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var (
		path     = fs.String("path", "", "directory containing posixbench.json (required)")
		verbose  = fs.Bool("verbose", false, "print all anomalies (default: summary only)")
		hostname = fs.String("hostname", "", "verify files for this hostname (reads posixbench-{hostname}.json)")
	)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: iotest-posixbench verify -path <dir> [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Verify data files against the posixbench.json manifest written by 'run'.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "When verifying files written by 'beegfs iotest start bench' on a shared path,")
		fmt.Fprintln(w, "pass -hostname to select the per-node manifest (posixbench-{hostname}.json).")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-posixbench verify -path /mnt/testdir")
		fmt.Fprintln(w, "  iotest-posixbench verify -path /mnt/testdir -verbose")
	}
	_ = fs.Parse(args)

	if *path == "" {
		fs.Usage()
		os.Exit(2)
	}

	cfg, err := posixbench.ReadManifestHost(*path, *hostname)
	if err != nil {
		die("verify: %v", err)
	}

	v, err := posixbench.NewVerifier(cfg)
	if err != nil {
		die("verify: %v", err)
	}

	fmt.Printf("posixbench verify\n")
	fmt.Printf("  path=%s seed=%d kind=%s blockSize=%s\n",
		*path, cfg.Seed, cfg.Kind, humanBytes(int64(cfg.BlockSize)))
	fmt.Println()

	start := time.Now()
	n, err := v.Verify(func(a posixbench.Anomaly) error {
		if *verbose {
			fmt.Printf("  MISMATCH  file=%s fileIndex=%d blockIndex=%d offset=%d\n",
				a.File, a.FileIndex, a.BlockIndex, a.Offset)
		}
		return nil
	})
	if err != nil {
		die("verify: %v", err)
	}

	fmt.Printf("Elapsed: %s\n", time.Since(start).Round(time.Millisecond))
	if n == 0 {
		fmt.Println("PASS")
		return
	}
	fmt.Printf("FAIL  %d block(s) mismatched\n", n)
	os.Exit(1)
}

func humanBytes(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.2f GiB", float64(n)/gib)
	case n >= mib:
		return fmt.Sprintf("%.2f MiB", float64(n)/mib)
	case n >= kib:
		return fmt.Sprintf("%.2f KiB", float64(n)/kib)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
