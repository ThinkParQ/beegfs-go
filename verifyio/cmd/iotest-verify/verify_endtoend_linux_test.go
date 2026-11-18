// This is a unit test.
//
// Coverage: iotest-verify's main() end to end, against real files and a real
// different-PID lock holder. The unit tests in main_test.go reach countSpan,
// classify and sweepVerdict; nothing there runs main, so the wiring between
// them -- and checkXattrPresence, which main calls before the sweep -- was
// pinned by nothing. Deleting the checkXattrPresence call left every other
// test in this package green.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

const testBlockSize = 1024

// malformedNames are two xattr names in verifyio's namespace that do not parse
// as a record extent -- one an invalid range, one not of the form
// <offset>-<length>, so both ForEachEntryStrict rejection paths are exercised.
// Two rather than one is load-bearing: every count that renders len(entries)
// agrees with a hardcoded 1 when there is only one entry.
var malformedNames = []string{"user.verifyio.100--50", "user.verifyio.notanoffset"}

// buildVerify builds the tool once for the calling test and returns its path.
func buildVerify(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "iotest-verify")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// writeDataFile writes blocks records at consecutive offsets, through the same
// library the real writers use, and returns the path.
func writeDataFile(t *testing.T, dir string, blocks int) string {
	t.Helper()
	path := filepath.Join(dir, "data.dat")
	f, err := fileops.Open(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	store, err := xattrstore.OpenStore(path, xattrstore.DefaultLockTimeout)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer store.Close()

	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{
		File:      f,
		Store:     store,
		Kind:      block.KindDecimal,
		BlockSize: testBlockSize,
		Locking:   xattrstore.LockExclusive,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range blocks {
		if err := w.WriteBlock(int64(i)*testBlockSize, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}
	return path
}

// syncBuf is a bytes.Buffer safe for the two goroutines os/exec uses to drain
// a child's stdout and stderr. Both streams write into one syncBuf so the
// combined text keeps the order the operator saw, which is what makes a
// failure dump worth printing.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runVerify runs the tool and returns its stdout alone, its two streams
// interleaved, and the exit code, on a deadline: a regression in the locking
// paths is a hang, not a wrong value.
//
// The streams are captured SEPARATELY, and lastLine must be run over stdout
// alone. "The last line of stdout is always the summary" (main.go's package
// doc, exitWithLine's, and README.md) is a claim about stdout specifically,
// and CombinedOutput made it unfalsifiable: the tool writes its stderr detail
// BEFORE the summary on every path, so redirecting the summary to stderr left
// the last line of the combined text unchanged and passed this whole file --
// while a wrapper doing `| tail -1` silently got a span detail line instead,
// which a naive parser mis-parses rather than rejects.
//
// combined is what the failure dumps print and what the stderr needles match,
// so a diagnosis still reads in the order it was produced.
func runVerify(t *testing.T, bin, path string) (stdout, combined string, code int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var outBuf, bothBuf syncBuf
	cmd := exec.CommandContext(ctx, bin, "-path", path)
	cmd.Stdout = io.MultiWriter(&outBuf, &bothBuf)
	cmd.Stderr = &bothBuf
	err := cmd.Run()

	if ctx.Err() != nil {
		t.Fatalf("iotest-verify did not exit within the deadline: %s", bothBuf.String())
	}
	var ee *exec.ExitError
	if err != nil {
		if !asExitError(err, &ee) {
			t.Fatalf("run iotest-verify: %v\n%s", err, bothBuf.String())
		}
		code = ee.ExitCode()
	}
	return outBuf.String(), bothBuf.String(), code
}

func asExitError(err error, target **exec.ExitError) bool {
	ee, ok := err.(*exec.ExitError)
	if ok {
		*target = ee
	}
	return ok
}

// lastLine returns the final non-empty line, which is the tool's contract with
// a wrapping script.
func lastLine(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return lines[len(lines)-1]
}

// TestVerifyVerdictEndToEnd pins the verdict, the exit code and the summary
// line the real binary produces, for the three cases a wrapping script has to
// tell apart. Everything here goes through main().
func TestVerifyVerdictEndToEnd(t *testing.T) {
	bin := buildVerify(t)

	t.Run("clean file passes", func(t *testing.T) {
		path := writeDataFile(t, t.TempDir(), 3)
		stdout, out, code := runVerify(t, bin, path)
		if code != exitPass {
			t.Errorf("exit = %d, want %d (PASS)\n%s", code, exitPass, out)
		}
		want := fmt.Sprintf("verdict=PASS records=3 gaps=0 contended=0 anomalies=0 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
	})

	t.Run("a corrupt block is a FAIL that names the damage", func(t *testing.T) {
		// The one input this tool exists for, and no test in the package
		// produced it: every other subtest reaches its verdict through a
		// missing file, missing xattrs, a malformed record NAME or a held
		// lock. None of those reaches the span-detail rendering in main's
		// callback, so `if *verbose || !ok` reduced to `if *verbose` deleted
		// every fact an operator acts on while the FAIL verdict and the
		// summary line both survived -- go test and the exercise script green,
		// because the script greps only for FAIL.
		//
		// The verdict alone is not the deliverable. "Something in this file is
		// corrupt", with no offset, no failing verdict and no writer
		// attribution, does not tell anyone which write went wrong.
		dir := t.TempDir()
		path := writeDataFile(t, dir, 4)

		// The body of the second record, well past its header. KindDecimal
		// bodies hold only ['0'-'9', ' '], so 0xFF is unconditionally a
		// mismatch rather than a value that might collide with what the seed
		// regenerates.
		const corruptAt = testBlockSize + 76
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("open for corruption: %v", err)
		}
		if _, err := f.WriteAt([]byte{0xFF}, corruptAt); err != nil {
			f.Close()
			t.Fatalf("corrupt byte at %d: %v", corruptAt, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		stdout, out, code := runVerify(t, bin, path)
		if code != exitFail {
			t.Errorf("exit = %d, want %d (FAIL)\n%s", code, exitFail, out)
		}
		// All four records are still READ and counted; exactly one of them is
		// bad. A rendering that dropped the anomaly would show anomalies=0
		// here, and one that counted the whole file bad would show 4.
		want := fmt.Sprintf("verdict=FAIL records=4 gaps=0 contended=0 anomalies=1 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
		// WHICH range failed and HOW -- the two things the summary line cannot
		// carry. Matched without the field's padding so a column-width change
		// is not a test failure.
		if !strings.Contains(out, fmt.Sprintf("offset=%d", int64(testBlockSize))) {
			t.Errorf("output does not name the offset of the corrupt block:\n%s", out)
		}
		if !strings.Contains(out, block.VerdictBodyCRCMismatch.String()) {
			t.Errorf("output does not name the failing verdict %s:\n%s",
				block.VerdictBodyCRCMismatch, out)
		}
		// The attribution line. kind, worker, cycle and node are how an
		// operator gets from "this block is bad" to the writer that wrote it,
		// and it renders from a separate branch than the span line above.
		for _, field := range []string{"kind=", "worker=", "cycle=", "node="} {
			if !strings.Contains(out, field) {
				t.Errorf("output does not attribute the corrupt block to its writer (no %s):\n%s",
					field, out)
			}
		}
	})

	t.Run("no records is ERROR, not FAIL", func(t *testing.T) {
		// Deleting main's checkXattrPresence call is caught here and nowhere
		// else: without it the sweep runs, sees one all-zero gap and no record,
		// and reports NO_DATA -- a plausible-looking verdict that loses the
		// mount-option diagnosis this case exists to give.
		path := filepath.Join(t.TempDir(), "no-records.dat")
		if err := os.WriteFile(path, []byte("not written by an iotest tool"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		stdout, out, code := runVerify(t, bin, path)
		if code != exitError {
			t.Errorf("exit = %d, want %d (ERROR)\n%s", code, exitError, out)
		}
		if !strings.Contains(out, "no iotest xattr records found") {
			t.Errorf("output does not diagnose the missing records:\n%s", out)
		}
		if !strings.Contains(out, "Filesystem does not support user xattrs") {
			t.Errorf("output does not offer the mount-option cause:\n%s", out)
		}
		if !strings.HasPrefix(lastLine(stdout), "verdict=ERROR ") {
			t.Errorf("last line = %q, want a verdict=ERROR summary\n%s", lastLine(stdout), out)
		}
	})

	t.Run("malformed record name is a FAIL with the counts intact", func(t *testing.T) {
		// A malformed record NAME describes no byte range, so it can never
		// become a span -- VerifyFile reports it after the sweep instead. It is
		// a finding about the data, not a tool failure, and the sweep that ran
		// alongside it still has results worth printing. Dying on it reported
		// neither the verdict nor the counts.
		dir := t.TempDir()
		path := writeDataFile(t, dir, 2)
		// TWO malformed names, not one. With a single entry, main's
		// "c.anomalies += len(finding.Entries)" and a plain "c.anomalies++"
		// are indistinguishable, and the summary line a wrapping script parses
		// would silently understate anomalies= on any real multi-entry file.
		for _, name := range malformedNames {
			if err := unix.Setxattr(path, name, []byte{0}, 0); err != nil {
				t.Skipf("cannot set a malformed xattr here: %v", err)
			}
		}
		stdout, out, code := runVerify(t, bin, path)
		if code != exitFail {
			t.Errorf("exit = %d, want %d (FAIL)\n%s", code, exitFail, out)
		}
		// The NAMES, not the generic header line: the header already contains
		// the phrase "malformed iotest xattr record", so asserting that alone
		// passes with the per-entry loop deleted and the operator left without
		// the one thing the message exists to give -- which records are corrupt.
		for _, name := range malformedNames {
			if !strings.Contains(out, strconv.Quote(name)) {
				t.Errorf("output does not name the malformed record %s:\n%s", name, out)
			}
		}
		want := fmt.Sprintf("verdict=FAIL records=2 gaps=0 contended=0 anomalies=2 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
		// The counts are the half a Die threw away: the sweep completed and
		// verified both records, and that has to survive into the report.
		if !strings.Contains(out, "2 record(s) verified") {
			t.Errorf("the completed sweep's counts were lost:\n%s", out)
		}
	})

	t.Run("a file with only malformed records is FAIL, not a mount-option diagnosis", func(t *testing.T) {
		// checkXattrPresence has two arms for "no parseable records on a
		// non-empty file" and they give opposite diagnoses: corrupt metadata in
		// verifyio's own namespace (FAIL), or a filesystem without user xattr
		// support (ERROR). Only len(malformed) separates them, and that term
		// decides whether the operator goes looking at their data or at
		// beegfs-client.conf.
		//
		// A file carrying NOTHING but a malformed record is the only input that
		// reaches the first arm. The subtest above writes valid records first,
		// so n == 2 there and VerifyFile reports the malformed name afterwards
		// instead -- which is why deleting this arm leaves that test, and every
		// other test in this package, green.
		path := filepath.Join(t.TempDir(), "malformed-only.dat")
		// Non-empty, or checkXattrPresence returns before it looks at all.
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		// Two, for the same reason as the subtest above: with one entry,
		// counts{anomalies: len(malformed)} and counts{anomalies: 1} agree.
		for _, name := range malformedNames {
			if err := unix.Setxattr(path, name, []byte{0}, 0); err != nil {
				t.Skipf("cannot set a malformed xattr here: %v", err)
			}
		}
		stdout, out, code := runVerify(t, bin, path)
		if code != exitFail {
			t.Errorf("exit = %d, want %d (FAIL)\n%s", code, exitFail, out)
		}
		for _, name := range malformedNames {
			if !strings.Contains(out, strconv.Quote(name)) {
				t.Errorf("output does not name the malformed record %s:\n%s", name, out)
			}
		}
		// Both needles matter: with the arm gone the tool falls through and
		// prints the mount-option block, sending the operator after a config
		// problem over what is actually corruption.
		if strings.Contains(out, "Filesystem does not support user xattrs") {
			t.Errorf("corrupt metadata misdiagnosed as missing xattr support:\n%s", out)
		}
		// Same verdict as the other malformed path, which is what the arm's own
		// comment promises, and the count has to survive into the summary.
		want := fmt.Sprintf("verdict=FAIL records=0 gaps=0 contended=0 anomalies=2 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
	})

	t.Run("an unreadable target is ERROR, not FAIL", func(t *testing.T) {
		// ERROR exists to keep "I could not check this file" out of FAIL, which
		// means "this file is corrupt". A sweep over many files after a run
		// branches on the exit code, so conflating the two reports a vanished
		// file or a mount without user xattrs as data corruption.
		//
		// This is the only test that reaches dieError, which is where every
		// real I/O failure lands -- open, OpenStore, Size, listxattr and a
		// sweep that aborted. The no-records case above exits through a
		// different call and left this one covered by nothing.
		path := filepath.Join(t.TempDir(), "does-not-exist.dat")
		stdout, out, code := runVerify(t, bin, path)
		if code != exitError {
			t.Errorf("exit = %d, want %d (ERROR)\n%s", code, exitError, out)
		}
		if strings.Contains(out, "verdict=FAIL") {
			t.Errorf("an unreadable target reported as corrupt data:\n%s", out)
		}
		want := fmt.Sprintf("verdict=ERROR records=0 gaps=0 contended=0 anomalies=0 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
	})

	t.Run("fully contended is INCOMPLETE, not PASS", func(t *testing.T) {
		dir := t.TempDir()
		path := writeDataFile(t, dir, 3)
		release := holdWholeFileLock(t, path)
		defer release()

		stdout, out, code := runVerify(t, bin, path)
		if code != exitIncomplete {
			t.Errorf("exit = %d, want %d (INCOMPLETE)\n%s", code, exitIncomplete, out)
		}
		// records=0 is the assertion that matters: a sweep that verified
		// nothing must not report coverage, whatever else it printed.
		want := fmt.Sprintf("verdict=INCOMPLETE records=0 gaps=0 contended=3 anomalies=0 path=%s", path)
		if got := lastLine(stdout); got != want {
			t.Errorf("last line = %q\nwant      = %q\n%s", got, want, out)
		}
		if strings.Contains(out, "PASS") {
			t.Errorf("a sweep that verified nothing printed PASS:\n%s", out)
		}
	})
}

// holdWholeFileLock starts a helper PROCESS holding an exclusive whole-file
// F_WRLCK and returns a function that releases it.
//
// A different process is the point: fcntl locks are per (node, pid), so a lock
// taken in this process would be silently upgraded rather than contended, and
// the test would pass against a tool that took no lock at all.
func holdWholeFileLock(t *testing.T, path string) func() {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperHoldWholeFileLock")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "HELPER_LOCK_PATH="+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	// Wait for the helper to confirm it holds the lock; starting the sweep
	// before that would race and make the test pass for the wrong reason.
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "LOCKED" {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("helper did not take the lock: line=%q err=%v", line, err)
	}
	return func() {
		stdin.Close()
		cmd.Wait()
	}
}

// TestHelperHoldWholeFileLock is not a test. It is the helper process
// holdWholeFileLock re-execs, following the idiom in
// xattrstore/lock_crossprocess_linux_test.go.
func TestHelperHoldWholeFileLock(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	f, err := os.OpenFile(os.Getenv("HELPER_LOCK_PATH"), os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	// Start=0, Len=0 is the whole file, including anything appended to it.
	if err := unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
		Type:   int16(unix.F_WRLCK),
		Whence: int16(unix.SEEK_SET),
		Start:  0,
		Len:    0,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "lock:", err)
		os.Exit(1)
	}
	fmt.Println("LOCKED")
	// Block until the parent closes our stdin. Exiting drops the F_SETLK.
	bufio.NewReader(os.Stdin).ReadByte()
	os.Exit(0)
}
