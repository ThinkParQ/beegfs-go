package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/climain"
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
		climain.Fail(climain.FailUsage, "blocksize must be > 0 (got %d)", blockSize)
	}
	if blocks <= 0 {
		climain.Fail(climain.FailUsage, "blocks must be > 0")
	}
	// Before the open below, not at NewWriter: a size like 65536 has no valid
	// body length, and failing after the open leaves a 0-byte file that
	// CheckSafeToDestroy then refuses on every later run of this path.
	if _, err := block.BodyLen(blockSize); err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}

	bodyKind, err := block.KindFromString(*kind)
	if err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}

	// Close is checked explicitly at the end rather than deferred: climain.Fail
	// calls os.Exit, which skips deferred functions entirely, so a plain
	// `defer f.Close()` would only ever run on successful completion anyway
	// -- and a silent Close failure there is exactly the class of bug this
	// tool exists to catch.
	// Refuse a path that does not look like a verifyio test file. Writer.Truncate
	// below clears it in both halves, and this normally runs as root on a test
	// box, so `-path /etc/passwd` is one character away from destroying a real
	// file -- and nothing downstream can tell that apart from an intended run.
	// Checked before the open so a refused path is never even created.
	if err := xattrstore.CheckSafeToDestroy(path); err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}

	// Deliberately NOT O_TRUNC: it resets the data and leaves every xattr
	// record behind, so a re-run at a smaller size inherits records claiming
	// data that no longer exists and the file fails verification.
	// Writer.Truncate(0) below resets both halves.
	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "open %s: %v", path, err)
	}

	store, err := xattrstore.OpenStore(path, xattrstore.DefaultLockTimeout)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "OpenStore: %v", err)
	}

	// LockNone: this tool is single-threaded and owns the path it was given.
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{
		File: f, Store: store, WorkerID: 0, Kind: bodyKind, BlockSize: blockSize,
		Locking: xattrstore.LockNone, Log: zap.NewNop(),
	})
	if err != nil {
		climain.Fail(climain.FailEnvironment, "NewWriter: %v", err)
	}

	// Clean in both halves: length zero AND no records. LockNone is safe
	// here because this tool is single-threaded and owns the path.
	if err := w.Truncate(0); err != nil {
		climain.Fail(climain.FailEnvironment, "reset %s: %v", path, err)
	}

	fmt.Printf("path=%s  blocksize=%d  blocks=%d  kind=%s\n", path, blockSize, blocks, bodyKind)

	start := time.Now()
	for i := 0; i < blocks; i++ {
		offset := int64(i) * int64(blockSize)
		if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
			climain.Fail(climain.FailEnvironment, "WriteBlock i=%d offset=%d: %v", i, offset, err)
		}
	}
	if err := f.Sync(); err != nil {
		climain.Fail(climain.FailEnvironment, "fsync: %v", err)
	}
	if err := f.Close(); err != nil {
		climain.Fail(climain.FailEnvironment, "close %s: %v", path, err)
	}
	if err := store.Close(); err != nil {
		climain.Fail(climain.FailEnvironment, "close store %s: %v", path, err)
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
