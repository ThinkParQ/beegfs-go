//go:build linux

// This is a unit test.
//
// Coverage: VerifyFile reports CoverageContended when a span's shared lock
// can't be acquired (held exclusively elsewhere); and the underlying
// trySharedLock primitive refuses a second OVERLAPPING shared lock on the same
// Store while granting a disjoint one, and blocks a conflicting exclusive
// request.
package verifier_test

import (
	"errors"
	"os"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func TestVerifyFileContended(t *testing.T) {
	// Hold an exclusive lock on one record while running VerifyFile.
	// The verifier must report that span as CoverageContended rather than
	// failing or blocking.
	const (
		blockSize = 1024
		numBlocks = 2
	)
	path, store := testEnv(t)
	writeRecords(t, path, store, blockSize, numBlocks)

	blockDataSize := int64(blockSize)

	// A separate fd holds the exclusive lock on record 0.
	lockFd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open lock fd: %v", err)
	}
	defer lockFd.Close()

	lease, err := store.TryAcquireExclusive(lockFd, 0, blockDataSize)
	if err != nil {
		t.Fatalf("TryAcquireExclusive: %v", err)
	}
	defer lease.Release()

	// Verify on a different fd — it should see CoverageContended for
	// the locked record, CoverageOne+OK for the unlocked one.
	fv, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open for verify: %v", err)
	}
	defer fv.Close()

	spans := collectSpans(t, store, fv, verifier.Options{})

	if len(spans) != numBlocks {
		t.Fatalf("got %d spans, want %d", len(spans), numBlocks)
	}
	if spans[0].Coverage != verifier.CoverageContended {
		t.Errorf("span 0 (locked): coverage=%v, want CoverageContended", spans[0].Coverage)
	}
	if spans[1].Coverage != verifier.CoverageOne {
		t.Errorf("span 1 (unlocked): coverage=%v, want CoverageOne", spans[1].Coverage)
	}
	if spans[1].Verdict != block.VerdictOK {
		t.Errorf("span 1: verdict=%v, want VerdictOK", spans[1].Verdict)
	}
}

// TestTryAcquireSharedOverlappingReadersRefused pins the one capability the
// single-mechanism design deliberately gives up: two goroutines in one process
// can no longer hold OVERLAPPING shared locks on the same Store. The range
// table conflicts on any overlap, shared-against-shared included, so the second
// acquire gets ErrLockBusy -- and the verifier reports that span
// CoverageContended.
//
// This asserted the opposite until 2026-08-17, and passed only because
// OpenStore then handed out OFD locks. OFD's conflict-between-file-descriptions
// guarantee does not hold on BeeGFS (it keys record locks by (node, pid) and
// ignores the open file description), so the mode was removed rather than
// relied on. Disjoint ranges are unaffected, and so are overlapping shared
// locks taken by other processes or other nodes -- that is what shared locks
// are for, and F_RDLCK still provides it. See rangeLockTable for why
// refcounting the shared holders is not a small fix.
func TestTryAcquireSharedOverlappingReadersRefused(t *testing.T) {
	path, _ := testEnv(t)
	if err := os.WriteFile(path, make([]byte, 8192), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := xattrstore.OpenStore(path, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}

	f1, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open f1: %v", err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open f2: %v", err)
	}
	defer f2.Close()

	l1, err := store.TryAcquireShared(f1, 0, 1024)
	if err != nil {
		t.Fatalf("first shared lock: %v", err)
	}

	// A second fd does not help: the two acquires share a Store, so the range
	// table sees them both, and they share a pid, so F_SETLK would not have
	// separated them either.
	if _, err := store.TryAcquireShared(f2, 512, 1024); !errors.Is(err, xattrstore.ErrLockBusy) {
		t.Errorf("overlapping second shared lock: err=%v, want ErrLockBusy", err)
	}

	// A disjoint range is still granted while the first lease is held.
	disjoint, err := store.TryAcquireShared(f2, 4096, 1024)
	if err != nil {
		t.Fatalf("disjoint shared lock: %v", err)
	}
	if err := disjoint.Release(); err != nil {
		t.Fatalf("release disjoint: %v", err)
	}

	// Releasing the first lease frees the table entry, so the overlapping
	// range becomes acquirable again.
	if err := l1.Release(); err != nil {
		t.Fatalf("release l1: %v", err)
	}
	l2, err := store.TryAcquireShared(f2, 512, 1024)
	if err != nil {
		t.Fatalf("shared lock after release: %v", err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("release l2: %v", err)
	}
}

// TestTryAcquireSharedBlocksExclusive verifies that a shared lock blocks a
// subsequent exclusive acquire on an overlapping range.
func TestTryAcquireSharedBlocksExclusive(t *testing.T) {
	path, _ := testEnv(t)
	if err := os.WriteFile(path, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := xattrstore.OpenStore(path, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}

	f1, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open f1: %v", err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open f2: %v", err)
	}
	defer f2.Close()

	shared, err := store.TryAcquireShared(f1, 0, 1024)
	if err != nil {
		t.Fatalf("shared lock: %v", err)
	}
	defer shared.Release()

	// Exclusive acquire on overlapping range must fail with ErrLockBusy.
	if _, err := store.TryAcquireExclusive(f2, 0, 1024); err != xattrstore.ErrLockBusy {
		t.Errorf("exclusive while shared held: err=%v, want ErrLockBusy", err)
	}
}
