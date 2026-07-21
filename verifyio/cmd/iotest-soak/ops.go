//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// Op is a single verifiable operation. h is the chosen file handle and
// offset is a block-aligned position within the file. A non-nil return
// value is an anomaly that aborts the soak run.
type Op func(ctx context.Context, h *fileHandle, offset int64) error

// fileHandle bundles the per-file state one worker needs to run ops.
// Not safe for concurrent use; each goroutine owns its own set.
type fileHandle struct {
	path   string
	file   *fileops.File
	store  *xattrstore.Store
	writer *xattrstore.Writer
}

func (h *fileHandle) close() error {
	return h.file.Close()
}

// writeBufferedOp returns an Op that writes one block with IOTypeBuffered.
// ErrLockBusy and ErrSetChanged are non-fatal skips (return nil).
func writeBufferedOp() Op {
	return func(_ context.Context, h *fileHandle, offset int64) error {
		err := h.writer.WriteBlock(offset, fileops.IOTypeBuffered, true)
		if errors.Is(err, xattrstore.ErrLockBusy) || errors.Is(err, xattrstore.ErrSetChanged) {
			return nil
		}
		return err
	}
}

// verifyRangeOp returns an Op that verifies the range [offset, offset+blockSize).
// Any anomalous span causes a non-nil return.
func verifyRangeOp(blockSize int) Op {
	return func(_ context.Context, h *fileHandle, offset int64) error {
		return verifier.VerifyFile(h.store, h.file, verifier.Options{
			Range: &verifier.ByteRange{Offset: offset, Length: int64(blockSize)},
		}, func(span verifier.Span) error {
			if spanIsAnomaly(span) {
				return fmt.Errorf("anomaly in %s at [%d,%d): coverage=%s verdict=%s",
					h.path, span.Offset, span.Offset+span.Length,
					span.Coverage, span.Verdict)
			}
			return nil
		})
	}
}

func spanIsAnomaly(s verifier.Span) bool {
	switch s.Coverage {
	case verifier.CoverageMany:
		return true
	case verifier.CoverageOne:
		// The verifier re-reads each header under the shared lock before
		// checking the body, so a concurrent reseed is verified against its
		// fresh header rather than a stale snapshot. Verdict is therefore the
		// meaningful corruption signal here.
		return s.Verdict != block.VerdictOK
	default:
		// CoverageNone: a write can complete entirely after the xattr snapshot
		// but before the data read, leaving apparent data with no xattr claim.
		// CoverageContended: lock was busy; body not read.
		// Neither is a real anomaly under concurrent writes.
		return false
	}
}
