//go:build linux

// This is a unit test.
//
// Coverage: the POSIX F_SETLK half of the lock mechanism, exercised against a
// real second OS process rather than a second goroutine. Every test in
// lock_linux_test.go shares one Store between goroutines, so the in-process
// rangeLockTable decides every outcome before F_SETLK is ever consulted --
// deleting every unix.FcntlFlock call in lock_linux.go leaves that whole
// suite green. These tests hold the lock in a re-exec'd child process
// instead, so the conflict (or lack of one) can only come from the real
// kernel lock: exclusive-vs-exclusive, exclusive-vs-shared, shared-vs-
// exclusive and shared-vs-shared conflict/coexistence, plus a disjoint range
// staying unaffected.
//
// Also the byte range that lock is taken over, which conflict/coexistence
// alone does not pin: that TryAcquireExclusive's union extent -- not the
// requested range -- is what F_SETLK receives, that neither acquire path
// widens to end-of-file, and that Release unlocks its own range and not a
// sibling lease's. Each of those needs either a record straddling the request
// or a holder ABOVE the range under test, and the cases above have neither --
// they run on an empty file with the child always placed below.
//
// Pass -path to point these at a file on a real mount (e.g. a live BeeGFS
// mount) instead of a fresh file under t.TempDir():
//
//	go test ./verifyio/xattrstore/... -run TestCrossProcess -path=/mnt/beegfs/locktest -v
//
// That still only proves same-node cross-process contention -- the child is
// re-exec'd on the same machine as the test binary. Cross-node contention
// needs two separate invocations from two hosts (see work/ofd-lock-probe/
// for the shape that took), which this file does not attempt.
package xattrstore

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/internal/testxattr"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
	"golang.org/x/sys/unix"
)

var lockTestPath = flag.String("path", "",
	"if set, run the cross-process locking tests (TestCrossProcess*) against this file -- "+
		"e.g. a path on a live BeeGFS mount -- instead of a fresh file under t.TempDir(). "+
		"Created if missing; not removed afterward.")

// crossProcessLockTarget returns the file the cross-process locking tests should lock:
// -path if set, otherwise a fresh file under t.TempDir(). -path is what makes it possible
// to re-run these same tests against a real mount later.
func crossProcessLockTarget(t *testing.T) string {
	t.Helper()
	path := *lockTestPath
	if path == "" {
		path = filepath.Join(t.TempDir(), "lock-target")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("create target %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close target %s: %v", path, err)
	}
	if n := existingRecordCount(t, path); n > 0 {
		t.Fatalf("-path target %s already holds %d %s* xattr(s). These tests assume a file with no "+
			"records: a pre-existing one widens the union extent TryAcquireExclusive computes, which "+
			"produces false failures that name no cause. Point -path at an unused file.",
			path, n, XAttrPrefix)
	}
	return path
}

// existingRecordCount reports how many verifyio records the target already
// carries. Only -path can produce a non-empty one; a t.TempDir() target is fresh
// every time. Measured before this check existed: pointing -path at a file
// holding a single [0,16384) record failed 3 of 9 tests against correct code,
// with nothing in the output hinting why.
func existingRecordCount(t *testing.T, path string) int {
	t.Helper()
	names, err := xattr.List(path)
	if err != nil {
		// Not fatal: a filesystem without xattr support fails later and more
		// clearly, and RequireSupport already gates the one test that needs them.
		t.Logf("listing xattrs on %s to check for stale records: %v", path, err)
		return 0
	}
	n := 0
	for _, name := range names {
		if strings.HasPrefix(name, XAttrPrefix) {
			n++
		}
	}
	return n
}

// heldLock is a lock held by a re-exec'd TestHelperHoldLock child process.
type heldLock struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	released bool
}

// release signals the child to unlock and exit, and waits for it. Safe to call
// at most meaningfully once; a second call (e.g. from the t.Cleanup fallback
// after an explicit release) is a no-op.
func (h *heldLock) release(t *testing.T) {
	t.Helper()
	if h.released {
		return
	}
	h.released = true
	if err := h.stdin.Close(); err != nil {
		t.Errorf("close helper stdin: %v", err)
	}
	if err := h.cmd.Wait(); err != nil {
		t.Errorf("helper process exit: %v", err)
	}
}

// holdLockInChildProcess re-execs this test binary as TestHelperHoldLock, which
// takes a real F_SETLK on [offset, offset+length) of path and blocks until its
// stdin is closed. Registers a t.Cleanup fallback so a failing test still
// releases the child rather than leaking a process (and the lock it holds).
func holdLockInChildProcess(t *testing.T, path string, offset, length int64, exclusive bool) *heldLock {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldLock$")
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"HELPER_LOCK_PATH="+path,
		"HELPER_LOCK_OFFSET="+strconv.FormatInt(offset, 10),
		"HELPER_LOCK_LENGTH="+strconv.FormatInt(length, 10),
		"HELPER_LOCK_EXCLUSIVE="+boolEnv(exclusive),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	h := &heldLock{cmd: cmd, stdin: stdin}
	t.Cleanup(func() { h.release(t) })

	type scanResult struct {
		line string
		ok   bool
	}
	lineCh := make(chan scanResult, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		ok := sc.Scan()
		lineCh <- scanResult{line: sc.Text(), ok: ok}
	}()
	// reap stops the child and waits for it BEFORE its stderr is read. Both
	// failure paths below need that: os/exec's copier goroutine writes into
	// stderr until Wait returns, so reading the buffer while the child is still
	// alive is a data race -- on exactly the path where the child's message is
	// the only diagnosis available, and where -race would bury it under a race
	// report. Marking released keeps the t.Cleanup fallback from calling Wait a
	// second time.
	reap := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		h.released = true
	}
	select {
	case r := <-lineCh:
		if !r.ok || r.line != "LOCKED" {
			reap()
			t.Fatalf("helper process did not report LOCKED (line=%q, ok=%v); stderr: %s",
				r.line, r.ok, stderr.String())
		}
	case <-time.After(5 * time.Second):
		reap()
		t.Fatalf("helper process did not report LOCKED within 5s; stderr: %s", stderr.String())
	}
	return h
}

// childCannotLock re-execs the helper and requires that it FAIL to take the
// lock: the inverse of holdLockInChildProcess, for asserting that a range this
// process holds really is held at the kernel level rather than only in the
// in-process range table. Leaving Stdin unset points the child at /dev/null, so
// its post-LOCKED read hits EOF immediately and it exits either way -- no pipe,
// no goroutine, nothing to reap.
func childCannotLock(t *testing.T, path string, offset, length int64, exclusive bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldLock$")
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"HELPER_LOCK_PATH="+path,
		"HELPER_LOCK_OFFSET="+strconv.FormatInt(offset, 10),
		"HELPER_LOCK_LENGTH="+strconv.FormatInt(length, 10),
		"HELPER_LOCK_EXCLUSIVE="+boolEnv(exclusive),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child took a lock on [%d,%d) that this process holds; output: %s",
			offset, offset+length, out)
	}
	// Distinguish "the kernel refused the lock" from "the child never got that
	// far" -- an open failure would otherwise pass this assertion for free.
	if !bytes.Contains(out, []byte("lock:")) {
		t.Fatalf("child failed before reaching F_SETLK, so this proves nothing; output: %s", out)
	}
}

func boolEnv(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// TestHelperHoldLock is not a real test: it is the re-exec target
// holdLockInChildProcess launches. Under a normal `go test` run
// GO_WANT_HELPER_PROCESS is unset, so it returns immediately and reports as a
// trivial pass, matching the standard library's own os/exec_test.go idiom.
func TestHelperHoldLock(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	path := os.Getenv("HELPER_LOCK_PATH")
	offset, err := strconv.ParseInt(os.Getenv("HELPER_LOCK_OFFSET"), 10, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse offset:", err)
		os.Exit(1)
	}
	length, err := strconv.ParseInt(os.Getenv("HELPER_LOCK_LENGTH"), 10, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse length:", err)
		os.Exit(1)
	}
	exclusive := os.Getenv("HELPER_LOCK_EXCLUSIVE") == "1"

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	lockType := int16(unix.F_RDLCK)
	if exclusive {
		lockType = int16(unix.F_WRLCK)
	}
	if err := unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
		Type:   lockType,
		Whence: int16(unix.SEEK_SET),
		Start:  offset,
		Len:    length,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "lock:", err)
		os.Exit(1)
	}
	fmt.Println("LOCKED")
	// Block until the parent closes our stdin (its signal to release and exit).
	// Exiting drops the F_SETLK -- no explicit unlock needed.
	bufio.NewReader(os.Stdin).ReadByte()
	os.Exit(0)
}

// TestCrossProcessExclusiveVsExclusive pins the case that motivates this whole
// file: a real different-PID F_WRLCK holder must make TryAcquireExclusive
// return ErrLockBusy, and releasing it must make a retry succeed. Deleting
// every unix.FcntlFlock call in lock_linux.go must fail this test.
func TestCrossProcessExclusiveVsExclusive(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	child := holdLockInChildProcess(t, target, 0, 4096, true)

	if _, err := store.TryAcquireExclusive(f, 0, 4096); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("TryAcquireExclusive while child holds F_WRLCK: err=%v, want ErrLockBusy", err)
	}

	child.release(t)

	lease, err := store.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive after child released: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestCrossProcessExclusiveVsShared pins that a real cross-process F_WRLCK
// holder blocks a shared acquire too, not only another exclusive one.
func TestCrossProcessExclusiveVsShared(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	holdLockInChildProcess(t, target, 0, 4096, true)

	if _, err := store.TryAcquireShared(f, 0, 4096); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("TryAcquireShared while child holds F_WRLCK: err=%v, want ErrLockBusy", err)
	}
}

// TestCrossProcessSharedVsExclusive pins that a real cross-process F_RDLCK
// holder blocks an exclusive acquire.
func TestCrossProcessSharedVsExclusive(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	holdLockInChildProcess(t, target, 0, 4096, false)

	if _, err := store.TryAcquireExclusive(f, 0, 4096); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("TryAcquireExclusive while child holds F_RDLCK: err=%v, want ErrLockBusy", err)
	}
}

// TestCrossProcessSharedVsShared pins the coexistence half of
// rangeLockTable's doc comment: "overlapping shared locks from other
// processes and other nodes are unaffected and still coexist." Every
// existing shared-lock test is single-process, where the in-process table
// (not F_SETLK) is what actually decides the outcome -- this is the first
// test where a real cross-process F_RDLCK holder is present and a shared
// acquire is still expected to succeed rather than conflict.
func TestCrossProcessSharedVsShared(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	holdLockInChildProcess(t, target, 0, 4096, false)

	lease, err := store.TryAcquireShared(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireShared while child holds F_RDLCK: err=%v, want a granted shared lease", err)
	}
	if err := lease.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestCrossProcessDisjointRangeUnaffected checks the real F_SETLK call is
// scoped by byte range, not by file: a disjoint range must be acquirable
// while a cross-process exclusive holder sits on a different range of the
// same file.
func TestCrossProcessDisjointRangeUnaffected(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	holdLockInChildProcess(t, target, 0, 4096, true)

	lease, err := store.TryAcquireExclusive(f, 8192, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive on disjoint range while child holds [0,4096): %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestCrossProcessUnionExtentReachesTheKernel pins the property that makes
// TryAcquireExclusive more than a passthrough to F_SETLK: the lock covers the
// union of the requested range and every record straddling it, and that WIDENED
// range is what the kernel is asked for.
//
// Nothing else pins it. Every other test in this file runs on a file with no
// records, so unionExtent is the identity function in all of them and the union
// never differs from the request. TestTryAcquireExclusiveUnionExtent (in
// lock_linux_test.go) checks the union arithmetic but asserts lease.Range(),
// which reads Lease struct fields rather than what the syscall received --
// so passing the requested range to Flock_t while leaving the range table and
// the Lease correct fails no test at all, and a lease then reports holding
// bytes the kernel is not defending.
func TestCrossProcessUnionExtentReachesTheKernel(t *testing.T) {
	target := crossProcessLockTarget(t)
	testxattr.RequireSupport(t, target)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	// One record spanning [0,8192). Requesting [4096,8192) widens the lock down
	// to 0, so the child's hold on [0,4096) -- which overlaps the union but NOT
	// the requested range -- has to be what refuses us.
	//
	// Removed afterward because this is the only test here that writes a record,
	// and under -path every test in this file shares one file that is never
	// deleted. A record left behind would silently widen the union -- and so the
	// lock -- for every later test and every later run against that mount.
	putEntries(t, store, [2]int64{0, 8192})
	t.Cleanup(func() {
		if err := store.Remove(0, 8192); err != nil {
			t.Errorf("Remove([0,8192)): %v", err)
		}
	})
	holdLockInChildProcess(t, target, 0, 4096, true)

	if _, err := store.TryAcquireExclusive(f, 4096, 4096); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("TryAcquireExclusive([4096,8192)) with a record at [0,8192) and a child holding "+
			"[0,4096): err=%v, want ErrLockBusy -- the union extent did not reach F_SETLK", err)
	}
}

// TestCrossProcessExclusiveHoldLeavesRangeAboveItFree is the mirror of
// TestCrossProcessDisjointRangeUnaffected, and the asymmetry is the point.
// There the child sits BELOW the range we take, so a Flock_t Len of 0 -- POSIX
// for "to end of file" -- grows away from the child and conflicts with nothing;
// that test passes either way. Here the parent takes the lease first and the
// child asks for a range above it, so locking to EOF instead of lockLen refuses
// the child. holdLockInChildProcess already fails the test when the child
// cannot lock, so no extra assertion is needed.
func TestCrossProcessExclusiveHoldLeavesRangeAboveItFree(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	lease, err := store.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive([0,4096)): %v", err)
	}
	holdLockInChildProcess(t, target, 8192, 4096, true)
	if err := lease.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestCrossProcessSharedHoldLeavesRangeAboveItFree is the shared-path twin of
// the test above: TryAcquireShared builds its own Flock_t, so an over-wide Len
// there is a separate defect with its own separate blind spot.
func TestCrossProcessSharedHoldLeavesRangeAboveItFree(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	lease, err := store.TryAcquireShared(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireShared([0,4096)): %v", err)
	}
	holdLockInChildProcess(t, target, 8192, 4096, true)
	if err := lease.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestCrossProcessReleaseDoesNotUnlockASiblingLease pins that Release unlocks
// only its own range. POSIX record locks are per-process, so an over-wide
// F_UNLCK does not fail -- it silently destroys the kernel state for every
// other lease this process holds in the range it covers, while the range table
// still lists them and their Release still reports success. Nothing in the
// package's single-process tests can see that: they consult the table, which
// stays correct throughout.
func TestCrossProcessReleaseDoesNotUnlockASiblingLease(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	low, err := store.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive([0,4096)): %v", err)
	}
	high, err := store.TryAcquireExclusive(f, 8192, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive([8192,12288)): %v", err)
	}

	// Releasing the low lease must leave the high one's kernel lock intact.
	if err := low.Release(); err != nil {
		t.Fatalf("Release([0,4096)): %v", err)
	}
	childCannotLock(t, target, 8192, 4096, true)

	if err := high.Release(); err != nil {
		t.Errorf("Release([8192,12288)): %v", err)
	}
}

// TestCrossProcessReleaseUnlocksItsOwnRange is the twin of the test above, in
// the direction that one cannot see. Release does check the F_UNLCK return
// code, but F_UNLCK over a range the process does not hold is a no-op that
// SUCCEEDS -- so unlocking the wrong range is indistinguishable from unlocking
// the right one by return value alone, and every same-process
// release-then-retry assertion passes either way, since POSIX record locks
// never conflict within one process. A child asking for the released range is
// the only observer. Without this, shifting Release's Flock_t Start by a block,
// or dropping the FcntlFlock call outright, fails nothing.
func TestCrossProcessReleaseUnlocksItsOwnRange(t *testing.T) {
	target := crossProcessLockTarget(t)
	store, err := OpenStore(target, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f := openTarget(t, store)

	lease, err := store.TryAcquireExclusive(f, 0, 4096)
	if err != nil {
		t.Fatalf("TryAcquireExclusive([0,4096)): %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release([0,4096)): %v", err)
	}
	// holdLockInChildProcess fails the test when the child cannot lock, which is
	// the assertion: [0,4096) must be free at the kernel level now.
	holdLockInChildProcess(t, target, 0, 4096, true)
}
