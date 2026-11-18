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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/climain"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// Exit codes. This tool's exit code carries a VERDICT, so 1 is reserved for
// FAIL ("the data read back wrong") and every tool failure must report as
// something else -- which is why the failure paths below go through
// climain.Fail, whose Failure values deliberately exclude 1.
//
// 3 and 4 are deliberately unused. They are not free numbers -- both carry a
// meaning in the shared vocabulary that internal/climain's package doc records.
// A tool that writes the data it verifies in the same run has no incomplete and
// no no-data case, so leaving the hole is correct; filling it with a third
// meaning is not.
const (
	exitPass = 0 // every record read back OK
	exitFail = 1 // at least one record did not: see the verdict and absence counts
	// 2 (usage) and 5 (environment) come from climain.Fail, which is where the
	// reserved-1 guarantee lives. Do not spell them here as well.
)

func main() {
	var (
		path      = flag.String("path", "/tmp/iotest-smoke.dat", "data file path")
		records   = flag.Int("blocks", 1000, "number of blocks to write")
		blocksize = flag.Int("blocksize", 4096, "total bytes per block on disk (a multiple of 516 always works; 65536 does not)")
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
		fmt.Fprintln(w, "-kind affects what a later verify can detect, not just this tool's own read-back:")
		fmt.Fprintln(w, "decimal (the default) and countup miss any given stale read about 1 time in 512 /")
		fmt.Fprintln(w, "256; prng detects every one. zeros and ones ignore the seed entirely and can never")
		fmt.Fprintln(w, "detect a stale read. See block.Kind's doc comment for the full detail.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-smoke -path /tmp/smoke.dat -blocks 1000")
		fmt.Fprintln(w, "  iotest-smoke -path /tmp/smoke.dat -blocks 500 -blocksize 8192 -kind prng")
	}
	climain.ExitIfNoArgs()
	flag.Parse()

	if *records <= 0 {
		climain.Fail(climain.FailUsage, "records must be > 0")
	}
	bodyKind, err := block.KindFromString(*kind)
	if err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}
	if _, err := block.BodyLen(*blocksize); err != nil {
		climain.Fail(climain.FailUsage, "invalid -blocksize: %v", err)
	}

	tl, err := climain.NewTraceLoggers(*iotrace, *iologfile)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "trace: %v", err)
	}
	// Close, not Sync: Sync flushes but leaves the log file open, which is the
	// leak the closer field exists to prevent. Nothing is buffered either way --
	// lumberjack has no Sync, so zap writes each line straight through -- so this
	// is about the handle, not about losing trace output.
	defer tl.Close()

	// Truncate-open so each run starts from a known-clean file. Close is
	// checked explicitly at the end rather than deferred: climain.Fail below
	// calls os.Exit, which skips deferred functions entirely, so a plain
	// `defer f.Close()` would only ever run on the PASS path anyway -- and a
	// silent Close failure there is exactly the class of bug this tool
	// exists to catch.
	// Refuse a path that does not look like a verifyio test file. Writer.Truncate
	// below clears it in both halves, and this normally runs as root on a test
	// box, so `-path /etc/passwd` is one character away from destroying a real
	// file -- and nothing downstream can tell that apart from an intended run.
	// Checked before the open so a refused path is never even created.
	// FailUsage, not FailEnvironment: every arm an operator actually reaches
	// here means "the path you named is not one of ours" -- a real file, or a
	// directory. Nothing is broken and retyping the command fixes it, so 5
	// ("I could not do my job") would misreport it. CheckSafeToDestroy does
	// also have two environment-shaped arms (a failed Lstat, a failed
	// listxattr), which are not separable today because its errors are plain
	// strings; typing them is a later change, and iotest-util -- its other
	// caller -- now classifies the same refusal the same way.
	if err := xattrstore.CheckSafeToDestroy(*path); err != nil {
		climain.Fail(climain.FailUsage, "%v", err)
	}

	// Deliberately NOT O_TRUNC: that would reset the data and leaves every xattr
	// record behind, so a re-run at a smaller size inherits records claiming
	// data that no longer exists and the file fails verification.
	// Writer.Truncate(0) below resets both halves.
	f, err := fileops.Open(*path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "open %s: %v", *path, err)
	}

	store, err := xattrstore.OpenStore(*path, xattrstore.DefaultLockTimeout)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "OpenStore: %v", err)
	}

	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: bodyKind, BlockSize: *blocksize, Locking: xattrstore.LockNone, Log: tl.IO})
	if err != nil {
		climain.Fail(climain.FailEnvironment, "NewWriter: %v", err)
	}

	// Clean in both halves: length zero AND no records. LockNone is safe
	// here because this tool is single-threaded and owns the path.
	if err := w.Truncate(0); err != nil {
		climain.Fail(climain.FailEnvironment, "reset %s: %v", *path, err)
	}

	// Write phase ----------------------------------------------------------
	fmt.Printf("Writing %d blocks (blocksize=%d kind=%s) to %s\n",
		*records, *blocksize, bodyKind, *path)

	writeStart := time.Now()
	for i := 0; i < *records; i++ {
		offset := int64(i) * int64(*blocksize)
		if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
			climain.Fail(climain.FailEnvironment, "WriteBlock at i=%d: %v", i, err)
		}
	}
	if err := f.Sync(); err != nil {
		climain.Fail(climain.FailEnvironment, "fsync: %v", err)
	}
	fmt.Printf("Wrote %d blocks (%d bytes) in %s\n",
		*records, int64(*records)*int64(*blocksize), time.Since(writeStart).Round(time.Millisecond))

	// Verify phase ---------------------------------------------------------
	fmt.Println("Verifying ...")
	buf := make([]byte, *blocksize)
	counts := make(map[block.Verdict]int, len(block.AllVerdicts()))
	scratch := make([]byte, *blocksize)

	// Absence counts, deliberately NOT block.Verdict values. Every Verdict
	// describes a record that EXISTS and disagrees with itself; these two are
	// about a record that is not there at all, which is a different question
	// and belongs to the store rather than the block. iotest-verify already
	// draws the line in the same place -- its gaps count sits beside its
	// verdict counts rather than inside them -- so this follows the tool whose
	// job this is.
	//
	// They must be PRINTED, not merely counted: the report loop below ranges
	// block.AllVerdicts(), so anything outside that list is invisible, and a
	// FAIL whose cause is never named is the fail-quiet outcome this tool
	// exists to catch.
	var (
		missingRecords int // the header xattr is gone: store.Get says ErrNotFound
		shortReads     int // the file ends early: ReadAt says io.EOF
	)

	verifyStart := time.Now()
	for i := 0; i < *records; i++ {
		offset := int64(i) * int64(*blocksize)
		// All three of these can fail for either of two reasons, and the
		// split is the whole point: io.EOF and ErrNotFound are the DATA
		// failing, which is a finding this tool exists to report, while any
		// other errno is the tool failing. Mapping either wholesale gets one
		// case badly wrong -- "your data is gone" reported as "I could not
		// check", or the reverse.
		if _, err := f.ReadAt(buf, offset); err != nil {
			// A short read on a file this process just wrote and fsynced is
			// data loss, not a broken environment.
			if errors.Is(err, io.EOF) {
				shortReads++
				continue
			}
			climain.Fail(climain.FailEnvironment, "ReadAt at i=%d offset=%d: %v", i, offset, err)
		}
		hdrBytes, err := store.Get(offset, int64(*blocksize))
		if err != nil {
			// The record we wrote is missing. WriteBlock returns its error on
			// the block-skipped path rather than skipping silently, and that
			// error is checked above, so there is no unwritten-block
			// explanation for this: the header really is gone.
			if errors.Is(err, xattrstore.ErrNotFound) {
				missingRecords++
				continue
			}
			climain.Fail(climain.FailEnvironment, "store.Get at i=%d offset=%d: %v", i, offset, err)
		}
		h, err := block.UnmarshalHeader(hdrBytes)
		if err != nil {
			// No environment arm: every UnmarshalHeader error is a statement
			// about the header bytes. Mapped through block rather than spelled
			// out here so this tool, the verifier and iotest-dump cannot drift
			// -- the comment below records that these copies drifted twice.
			counts[block.VerdictForHeaderError(err)]++
			continue
		}
		// Same self-consistency gate the verifier and iotest-dump apply, so all
		// three agree about the same record. Both fields are fixed by
		// construction here (this tool wrote the block it is reading back), so
		// this can only fire on genuine corruption -- which is the point: it is
		// the third VerifyBlock site, and leaving it out is how the copies
		// drifted apart the first two times.
		if sc := block.RecordSelfCheck(&h, offset, int64(*blocksize)); sc != block.VerdictOK {
			counts[sc]++
			continue
		}
		// VerifyBlock's only error is an unrecognized header Kind, which it
		// reports as VerdictHeadBadFormat -- printed below. The error adds
		// only the offending kind value, so dropping it loses no verdict.
		v, _ := block.VerifyBlock(buf, &h, scratch)
		counts[v]++
	}
	verifyElapsed := time.Since(verifyStart).Round(time.Millisecond)

	// Report ---------------------------------------------------------------
	fmt.Println("Results:")
	for _, v := range block.AllVerdicts() {
		if c, ok := counts[v]; ok && c > 0 {
			fmt.Printf("  %-18s %d\n", v.String()+":", c)
		}
	}
	// Same shape as the verdict lines above, so one report reads as one table.
	if missingRecords > 0 {
		fmt.Printf("  %-18s %d\n", "RECORD_MISSING:", missingRecords)
	}
	if shortReads > 0 {
		fmt.Printf("  %-18s %d\n", "SHORT_READ:", shortReads)
	}
	fmt.Printf("Verify took %s\n", verifyElapsed)

	if err := f.Close(); err != nil {
		climain.Fail(climain.FailEnvironment, "close %s: %v", *path, err)
	}
	if err := store.Close(); err != nil {
		climain.Fail(climain.FailEnvironment, "close store %s: %v", *path, err)
	}

	// Sufficient on its own: a record counted under any other verdict, or
	// skipped as missing or short, leaves this equality false. The absence
	// counters need no clause of their own -- adding one would suggest the
	// equality alone were not enough, which is how a second, weaker check
	// becomes the one a later reader trusts.
	if counts[block.VerdictOK] == *records {
		fmt.Println("PASS")
		os.Exit(exitPass)
	}
	fmt.Println("FAIL")
	os.Exit(exitFail)
}
