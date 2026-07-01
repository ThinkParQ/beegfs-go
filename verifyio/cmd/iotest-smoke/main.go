// iotest-smoke -- write N blocks to a file, storing each block's header
// in an xattr, then read everything back and verify.
//
// Single-threaded, deterministic. Useful as:
//
//   - End-to-end smoke check ("does the lib still work?")
//   - Copy-paste starter for new consumers of the library
//   - Sandbox for surfacing API friction
//
// The data file is truncated and rewritten on every run.
//
//	iotest-smoke -path /tmp/foo.dat -blocks 1000
//	iotest-smoke -path /tmp/foo.dat -blocks 100 -blocksize 8192 -kind prng
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/trace"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// allVerdicts is the canonical iteration order for tallying. Listed
// explicitly rather than `for v := VerdictOK; v <= ...` so adding a new
// Verdict requires touching this list (loud rather than silent).
var allVerdicts = []block.Verdict{
	block.VerdictOK,
	block.VerdictBodyCorrupt,
	block.VerdictBodyCRCMismatch,
	block.VerdictHeadBadCRC,
	block.VerdictHeadBadMagic,
	block.VerdictHeadBadFormat,
	block.VerdictTruncated,
}

func main() {
	var (
		path      = flag.String("path", "/tmp/iotest-smoke.dat", "data file path")
		records   = flag.Int("blocks", 1000, "number of blocks to write")
		blocksize = flag.Int("blocksize", 4096, "total bytes per block on disk (use a power of two)")
		kind      = flag.String("kind", "decimal", "body pattern: decimal | prng | repeat | countup | zeros | ones")
		iotrace   = flag.Int("iotrace", 0, "IO trace level (0=off, 1=error, 2=warn, 3=info, 4/5=debug)")
		iologfile = flag.String("iologfile", "", "IO trace destination file (default stderr)")
	)
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintln(w, "Usage: iotest-smoke [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Write N blocks to a file, storing each block's header in an xattr, then read back and verify.")
		fmt.Fprintln(w, "The file is truncated on every run. Single-threaded and deterministic.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-smoke -path /tmp/smoke.dat -blocks 1000")
		fmt.Fprintln(w, "  iotest-smoke -path /tmp/smoke.dat -blocks 500 -blocksize 8192 -kind prng")
	}
	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(2)
	}
	flag.Parse()

	if *records <= 0 {
		die("records must be > 0")
	}
	bodyKind, err := block.KindFromString(*kind)
	if err != nil {
		die("%v", err)
	}
	if _, err := block.BodyLen(*blocksize); err != nil {
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

	// Truncate-open so each run starts from a known-clean file. Close is
	// checked explicitly at the end rather than deferred: die() below calls
	// os.Exit, which skips deferred functions entirely, so a plain
	// `defer f.Close()` would only ever run on the PASS path anyway -- and a
	// silent Close failure there is exactly the class of bug this tool
	// exists to catch.
	f, err := fileops.Open(*path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		die("open %s: %v", *path, err)
	}

	store, err := xattrstore.OpenStore(*path)
	if err != nil {
		die("OpenStore: %v", err)
	}

	w, err := xattrstore.NewWriter(f, store, 0, bodyKind, *blocksize, tl.IO)
	if err != nil {
		die("NewWriter: %v", err)
	}

	// Write phase ----------------------------------------------------------
	fmt.Printf("Writing %d blocks (blocksize=%d kind=%s) to %s\n",
		*records, *blocksize, bodyKind, *path)

	writeStart := time.Now()
	for i := 0; i < *records; i++ {
		offset := int64(i) * int64(*blocksize)
		if err := w.WriteBlock(offset, fileops.IOTypeBuffered, false); err != nil {
			die("WriteBlock at i=%d: %v", i, err)
		}
	}
	if err := f.Sync(); err != nil {
		die("fsync: %v", err)
	}
	fmt.Printf("Wrote %d blocks (%d bytes) in %s\n",
		*records, int64(*records)*int64(*blocksize), time.Since(writeStart).Round(time.Millisecond))

	// Verify phase ---------------------------------------------------------
	fmt.Println("Verifying ...")
	buf := make([]byte, *blocksize)
	counts := make(map[block.Verdict]int, len(allVerdicts))
	scratch := make([]byte, *blocksize)

	verifyStart := time.Now()
	for i := 0; i < *records; i++ {
		offset := int64(i) * int64(*blocksize)
		if _, err := f.ReadAt(buf, offset); err != nil {
			die("ReadAt at i=%d offset=%d: %v", i, offset, err)
		}
		hdrBytes, err := store.Get(offset, int64(*blocksize))
		if err != nil {
			die("store.Get at i=%d offset=%d: %v", i, offset, err)
		}
		h, err := block.UnmarshalHeader(hdrBytes)
		if err != nil {
			die("UnmarshalHeader at i=%d: %v", i, err)
		}
		v, _ := block.VerifyBlock(buf, &h, scratch)
		counts[v]++
	}
	verifyElapsed := time.Since(verifyStart).Round(time.Millisecond)

	// Report ---------------------------------------------------------------
	fmt.Println("Results:")
	for _, v := range allVerdicts {
		if c, ok := counts[v]; ok && c > 0 {
			fmt.Printf("  %-18s %d\n", v.String()+":", c)
		}
	}
	fmt.Printf("Verify took %s\n", verifyElapsed)

	if err := f.Close(); err != nil {
		die("close %s: %v", *path, err)
	}

	if counts[block.VerdictOK] == *records {
		fmt.Println("PASS")
		return
	}
	fmt.Println("FAIL")
	os.Exit(1)
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
