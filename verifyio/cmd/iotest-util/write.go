package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
	"go.uber.org/zap"
)

func runWrite(path string, blockSize, blocks int, args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	kind := fs.String("kind", "decimal", "body pattern: decimal | prng | repeat | countup | zeros | ones")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: iotest-util [common flags] write [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Write blocks to the file using xattrstore.Writer. The file is")
		fmt.Fprintln(w, "truncated to blocks*blocksize on each run.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)

	if blockSize <= 0 {
		die("blocksize must be > 0 (got %d)", blockSize)
	}
	if blocks <= 0 {
		die("blocks must be > 0")
	}

	bodyKind, err := block.KindFromString(*kind)
	if err != nil {
		die("%v", err)
	}

	// Close is checked explicitly at the end rather than deferred: die()
	// calls os.Exit, which skips deferred functions entirely, so a plain
	// `defer f.Close()` would only ever run on successful completion anyway
	// -- and a silent Close failure there is exactly the class of bug this
	// tool exists to catch.
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		die("open %s: %v", path, err)
	}

	store, err := xattrstore.OpenStore(path)
	if err != nil {
		die("OpenStore: %v", err)
	}

	w, err := xattrstore.NewWriter(f, store, 0, bodyKind, blockSize, zap.NewNop())
	if err != nil {
		die("NewWriter: %v", err)
	}

	fmt.Printf("path=%s  blocksize=%d  blocks=%d  kind=%s\n", path, blockSize, blocks, bodyKind)

	start := time.Now()
	for i := 0; i < blocks; i++ {
		offset := int64(i) * int64(blockSize)
		if err := w.WriteBlock(offset, fileops.IOTypeBuffered, false); err != nil {
			die("WriteBlock i=%d offset=%d: %v", i, offset, err)
		}
	}
	if err := f.Sync(); err != nil {
		die("fsync: %v", err)
	}
	if err := f.Close(); err != nil {
		die("close %s: %v", path, err)
	}

	elapsed := time.Since(start)
	total := int64(blocks) * int64(blockSize)
	mibps := 0.0
	if elapsed.Seconds() > 0 {
		mibps = float64(total) / elapsed.Seconds() / (1024 * 1024)
	}
	fmt.Printf("wrote %d blocks (%d bytes) in %s (%.1f MiB/s)\n",
		blocks, total, elapsed.Round(time.Millisecond), mibps)
}
