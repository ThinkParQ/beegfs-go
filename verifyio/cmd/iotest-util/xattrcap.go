package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/block"
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
		fmt.Fprintln(w, "xattr namespace. Increase -blocks until you see a failure to find")
		fmt.Fprintln(w, "the actual limit.")
	}
	_ = fs.Parse(args)

	if blockSize <= 0 {
		die("blocksize must be > 0 (got %d)", blockSize)
	}
	if blocks <= 0 {
		die("blocks must be > 0")
	}

	// Remove before creating so the new inode starts with no xattrs.
	// O_TRUNC would clear data but leave existing xattrs, which would
	// make overwrite-style puts succeed past the real limit.
	_ = os.Remove(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		die("create %s: %v", path, err)
	}
	if err := f.Truncate(int64(blocks) * int64(blockSize)); err != nil {
		_ = f.Close() // best-effort cleanup; the truncate error below is what matters
		die("truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		die("close %s: %v", path, err)
	}

	store, err := xattrstore.OpenStore(path)
	if err != nil {
		die("OpenStore: %v", err)
	}

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
			die("MarshalHeader i=%d: %v", i, err)
		}
		if err := store.Put(offset, int64(blockSize), hdrBuf); err != nil {
			fmt.Printf("failed at block %d (offset %d): %v\n", i, offset, err)
			fmt.Printf("xattrs written before failure: %d\n", i)
			return
		}
	}
	fmt.Printf("wrote all %d xattrs without error — increase -blocks to find the limit\n", blocks)
}
