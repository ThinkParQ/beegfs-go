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
  -blocksize int     total bytes per block on disk (default 1024; a multiple of 516
                     always works -- note 65536 does not, see block.BodyLen)
  -blocks int        number of blocks (default 64)

Operations:
  write              write blocks to the file using xattrstore.Writer
  xattr-capacity     write xattrs at successive offsets until failure

Run iotest-util <operation> -help for operation-specific flags.
`

func main() {
	path := flag.String("path", "", "path to the test file (required)")
	blockSize := flag.Int("blocksize", 1024, "total bytes per block on disk")
	blocks := flag.Int("blocks", 64, "number of blocks")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, topUsage)
	}
	flag.Parse()

	// Deliberately not climain.ExitIfNoArgs: this check is broader, also
	// catching a -path given with no operation. Exit 2 for a usage error matches
	// every other iotest-* tool.
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	op := flag.Arg(0)
	opArgs := flag.Args()[1:]

	// -path is required to run an operation, but not to ask one for its usage:
	// the operation's own FlagSet prints that and exits 0, which is what the
	// package doc above promises. Requiring -path first made the documented
	// `iotest-util <operation> -help` print the top-level usage at exit 2.
	if *path == "" && !isHelpRequest(opArgs) {
		flag.Usage()
		os.Exit(2)
	}

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

// isHelpRequest reports whether args ask for an operation's usage rather than a
// run. The spellings are flag's own, which is what the operation FlagSets parse.
func isHelpRequest(args []string) bool {
	for _, a := range args {
		switch a {
		case "-h", "--h", "-help", "--help":
			return true
		}
	}
	return false
}
