// This is a unit test.
//
// Coverage: WriteBlock's ErrLockViolation read-back -- that a foreign header
// landing in the interval the check is supposed to cover is actually detected.
//
// Before the fix the check could not fail. `written` aliased hdrBuf, the
// TagFsynced re-put was unconditional, and the comparison ran after it, so a
// clobber anywhere in the locked interval was overwritten by our own header and
// then read back as a match. Deleting the check, or forcing its comparison to
// "equal", both left the suite green.
//
// Linux only: the read-back is on the LockExclusive path, and lockRegion is a
// stub returning "not supported" elsewhere (see writer_lock_other.go).
//go:build linux

package xattrstore

import (
	"errors"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
)

// TestWriteBlockDetectsClobberUnderLock injects a competing xattr write in the
// window between the data write and the read-back -- exactly what a failed
// distributed lock looks like from this process's point of view.
func TestWriteBlockDetectsClobberUnderLock(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{
		File: f, Store: s, WorkerID: 1, Kind: block.KindDecimal,
		BlockSize: 4096, Locking: LockExclusive,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// A foreign header for the same extent: a different writer's claim on our
	// range, which is what the check exists to catch.
	hookAfterDataWrite = func(hw *Writer, offset, dataSize int64) error {
		return hw.store.Put(offset, dataSize, makeHeader(t, offset, 0xDEAD))
	}
	t.Cleanup(func() { hookAfterDataWrite = nil })

	if err := w.WriteBlock(0, fileops.IOTypeBuffered); !errors.Is(err, ErrLockViolation) {
		t.Fatalf("WriteBlock err = %v, want ErrLockViolation", err)
	}
}

// TestWriteBlockReadBackAcceptsItsOwnWrite is the other half: the check must not
// false-positive on the ordinary single-writer case, where the only thing that
// touched the xattr is this call.
func TestWriteBlockReadBackAcceptsItsOwnWrite(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{
		File: f, Store: s, WorkerID: 1, Kind: block.KindDecimal,
		BlockSize: 4096, Locking: LockExclusive,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for i := range 3 {
		if err := w.WriteBlock(int64(i)*4096, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", i, err)
		}
	}

	// And the TagFsynced re-put still lands after the check, so the stored header
	// ends up stamped -- the property that ordering change must not have cost.
	hdrBytes, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	if hdr.Tag&block.TagFsynced == 0 {
		t.Error("TagFsynced is not set on the stored header; the re-put after the read-back was lost")
	}
}

// TestWriteBlockClobberIsInvisibleWithoutLocking documents the boundary. Under
// LockNone there is no read-back and no re-put, so the foreign header simply
// survives and WriteBlock reports success. That is correct: LockNone declares
// the caller owns exclusion, and the check would have nothing to conclude from.
func TestWriteBlockClobberIsInvisibleWithoutLocking(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	w, err := NewWriter(WriterConfig{
		File: f, Store: s, WorkerID: 1, Kind: block.KindDecimal,
		BlockSize: 4096, Locking: LockNone,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	hookAfterDataWrite = func(hw *Writer, offset, dataSize int64) error {
		return hw.store.Put(offset, dataSize, makeHeader(t, offset, 0xDEAD))
	}
	t.Cleanup(func() { hookAfterDataWrite = nil })

	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock under LockNone: %v", err)
	}
	hdrBytes, err := s.Get(0, 4096)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	hdr, err := block.UnmarshalHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalHeader: %v", err)
	}
	if hdr.Seed != 0xDEAD {
		t.Errorf("stored seed = %#x, want the foreign 0xDEAD -- LockNone is not supposed to re-put", hdr.Seed)
	}
}
