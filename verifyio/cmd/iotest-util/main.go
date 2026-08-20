// iotest-util -- developer utility for iotest experiments.
//
// Common file parameters (path, blocksize, blocks) are given as top-level
// flags; the operation to perform is the first positional argument.
//
//	iotest-util -path /mnt/beegfs/test.dat -blocksize 1024 -blocks 64 write
//	iotest-util -path /mnt/beegfs/test.dat -blocksize 1024 -blocks 200 xattr-capacity
//
// Run iotest-util <operation> -help for per-operation flags.
package main

import (
	"flag"
	"fmt"
	"os"
)

const topUsage = `iotest-util — developer utility for iotest experiments

Usage: iotest-util -path <p> -blocksize <n> -blocks <n> <operation> [op-flags]

Common flags:
  -path string       path to the test file (required)
  -blocksize int     total bytes per block on disk (default 1024; use a power of two)
  -blocks int        number of blocks (default 64)

Operations:
  write              write blocks to the file using xattrstore.Writer
  xattr-capacity     write xattrs at successive offsets until failure

Run iotest-util <operation> -help for operation-specific flags.
`

func main() {
	path := flag.String("path", "", "path to the test file (required)")
	blockSize := flag.Int("blocksize", 1024, "body bytes per block")
	blocks := flag.Int("blocks", 64, "number of blocks")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, topUsage)
	}
	flag.Parse()

	if *path == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	op := flag.Arg(0)
	opArgs := flag.Args()[1:]

	switch op {
	case "write":
		runWrite(*path, *blockSize, *blocks, opArgs)
	case "xattr-capacity":
		runXattrCapacity(*path, *blockSize, *blocks, opArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown operation %q\n\n", op)
		flag.Usage()
		os.Exit(2)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
