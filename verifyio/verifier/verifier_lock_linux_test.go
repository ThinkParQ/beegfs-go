//go:build linux

// This is a unit test.
//
// Coverage: VerifyFile reports CoverageContended when a span's shared lock
// can't be acquired (held exclusively elsewhere); and the underlying
// trySharedLock primitive allows multiple concurrent shared readers but
// blocks a conflicting exclusive request.
package verifier_test

import (
	"os"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func TestVerifyFileContended(t *testing.T) {
	// Hold an exclusive OFD lock on one record while running VerifyFile.
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

// TestTryAcquireSharedMultipleReaders verifies that two shared locks can
// coexist on overlapping ranges (shared locks do not conflict with each other).
func TestTryAcquireSharedMultipleReaders(t *testing.T) {
	path, _ := testEnv(t)
	if err := os.WriteFile(path, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := xattrstore.OpenStore(path)
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
	defer l1.Release()

	// Second shared lock on overlapping range must succeed.
	l2, err := store.TryAcquireShared(f2, 512, 1024)
	if err != nil {
		t.Fatalf("second shared lock (should succeed): %v", err)
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
	store, err := xattrstore.OpenStore(path)
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
