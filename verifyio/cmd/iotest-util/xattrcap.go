package main

import (
	"flag"
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/internal/climain"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func runXattrCapacity(path string, blockSize, blocks int, args []string) {
	fs := flag.NewFlagSet("xattr-capacity", flag.ExitOnError)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: iotest-util [common flags] xattr-capacity")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Write one xattr per block offset until setxattr fails, reporting the")
		fmt.Fprintln(w, "per-inode xattr capacity of the underlying filesystem. The file at")
		fmt.Fprintln(w, "-path is removed and recreated on each run to start from a clean")
		fmt.Fprintln(w, "xattr namespace, and removed again when the run ends. Increase")
		fmt.Fprintln(w, "-blocks until you see a failure to find the actual limit.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "NOTE: this reports the setxattr limit, which is not always the one")
		fmt.Fprintln(w, "that binds. listxattr cannot retrieve a name list larger than 64 KiB")
		fmt.Fprintln(w, "(XATTR_LIST_MAX), and verifyio needs listxattr for every read AND")
		fmt.Fprintln(w, "write, so the usable ceiling can be far lower than the number below.")
	}
	_ = fs.Parse(args)

	if blockSize <= 0 {
		climain.Fail(climain.FailUsage, "blocksize must be > 0 (got %d)", blockSize)
	}
	if blocks <= 0 {
		climain.Fail(climain.FailUsage, "blocks must be > 0")
	}

	// Refuse to destroy a path that is not plausibly ours. This subcommand's
	// -path is required and has no default, so nothing stops a slip like
	// `iotest-util -path /etc/passwd xattr-capacity` -- which, run as root on a
	// test box (the normal case), would delete and replace that file with a
	// sparse zero file. Removal is unavoidable here: O_TRUNC clears data but
	// leaves existing xattrs, which would let overwrite-style puts succeed past
	// the real per-inode limit and silently produce a wrong answer.
	//
	// The same slip is possible in every verifyio tool that clears its -path, so
	// the guard is shared rather than local to this one.
	if err := xattrstore.CheckSafeToDestroy(path); err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}
	// Remove before creating so the new inode starts with no xattrs.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		climain.Fail(climain.FailEnvironment, "remove %s: %v", path, err)
	}
	// O_NOFOLLOW: path may be a predictable default; a symlink could be
	// replanted here between the Remove above and this Create.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0644)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "create %s: %v", path, err)
	}
	if err := f.Truncate(int64(blocks) * int64(blockSize)); err != nil {
		_ = f.Close() // best-effort cleanup; the truncate error below is what matters
		climain.Fail(climain.FailEnvironment, "truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		climain.Fail(climain.FailEnvironment, "close %s: %v", path, err)
	}

	store, err := xattrstore.OpenStore(path, xattrstore.DefaultLockTimeout)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "OpenStore: %v", err)
	}
	defer store.Close()

	// Remove the artifact on the way out, covering BOTH exits -- the interesting
	// one is the Put failure below, because the documented workflow is to raise
	// -blocks until it fails, so cleaning up only on success would leave the file
	// behind in the common case.
	//
	// The measurement is the printed count; the file has no post-run value. And
	// leaving it behind has a concrete cost beyond clutter: this tool
	// deliberately drives the xattr name list past XATTR_LIST_MAX (64 KiB), after
	// which listxattr cannot retrieve it at all -- so CheckSafeToDestroy, which
	// lists xattrs to confirm the file is a verifyio artifact, fails with E2BIG
	// and the NEXT run refuses to start on its own leftovers.
	//
	// Registered after store.Close's defer so it runs first (LIFO). Unlinking a
	// file that still has an open fd is fine on Linux; the fd closes a moment
	// later. Best-effort by design: the capacity number is already on stdout by
	// the time this runs, so failing the command over cleanup would throw away a
	// good result. Note climain.Fail calls os.Exit, so a Fail path skips this --
	// only reachable via the MarshalHeader check below, which cannot fail here
	// (hdrBuf is exactly block.HeaderSize, the sole cause of that error).
	defer func() {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: could not remove %s: %v\n", path, err)
		}
	}()

	fmt.Printf("path=%s  blocksize=%d  blocks=%d\n", path, blockSize, blocks)

	hdrBuf := make([]byte, block.HeaderSize)
	for i := 0; i < blocks; i++ {
		offset := int64(i) * int64(blockSize)
		hdr := block.Header{
			Version: block.HeaderVersion,
			Kind:    block.KindDecimal,
			Offset:  uint64(offset),
		}
		if err := block.MarshalHeader(hdrBuf, &hdr); err != nil {
			climain.Fail(climain.FailEnvironment, "MarshalHeader i=%d: %v", i, err)
		}
		if err := store.Put(offset, int64(blockSize), hdrBuf); err != nil {
			fmt.Printf("failed at block %d (offset %d): %v\n", i, offset, err)
			fmt.Printf("xattrs written before failure: %d\n", i)
			return
		}
	}
	fmt.Printf("wrote all %d xattrs without error — increase -blocks to find the limit\n", blocks)
}
