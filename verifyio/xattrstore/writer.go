package xattrstore

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"go.uber.org/zap"
)

// Writer prepares verifiable blocks and writes them to a data file,
// keeping the xattr store in sync. A single WriteBlock call handles
// block construction, optional range locking, xattr update, and the
// IO operation.
//
// Writer is not safe for concurrent use. The intended pattern is one
// Writer per worker goroutine, each with its own fileops.File.
type Writer struct {
	file      *fileops.File
	store     *Store
	nodeName  [16]byte
	workerID  int32
	kind      block.Kind
	blockSize int // total on-disk bytes per block
	bodyLen   int // body bytes; derived from blockSize via block.BodyLen
	cycle     atomic.Uint64
	buf       []byte
	log       *zap.Logger
}

// NewWriter creates a Writer. The hostname is detected automatically.
// blockSize is the total on-disk size per block; bodyLen is derived via
// block.BodyLen. Not all values are valid — use a power of two (512, 1024,
// 4096, ...). log is used for per-operation IO traces; pass nil to disable.
func NewWriter(file *fileops.File, store *Store, workerID int, kind block.Kind, blockSize int, log *zap.Logger) (*Writer, error) {
	if file == nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: file is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: store is nil")
	}
	bodyLen, err := block.BodyLen(blockSize)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: hostname: %w", err)
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Writer{
		file:      file,
		store:     store,
		nodeName:  block.NodeNameFromString(hostname),
		workerID:  int32(workerID),
		kind:      kind,
		blockSize: blockSize,
		bodyLen:   bodyLen,
		buf:       make([]byte, blockSize),
		log:       log,
	}, nil
}

// WriteBlock writes exactly blockSize bytes at offset using the given IOType.
// Tag in the header is set to uint32(ioType) so dump tools can see which IO
// method produced each block.
//
// If takeLock is true, an OFD range lock is acquired before writing
// and released after. ErrLockBusy and ErrSetChanged are returned as
// non-fatal signals — the caller should skip or move to a different
// region rather than retrying immediately.
//
// The xattr is written before the data (intent-before-data ordering)
// so a verifier can distinguish a clean write from a crashed one.
func (w *Writer) WriteBlock(offset int64, ioType fileops.IOType, takeLock bool) error {
	dataSize := int64(w.blockSize)
	unlock, err := w.maybeLock(offset, dataSize, takeLock)
	if err != nil {
		w.log.Warn("block skipped",
			zap.Int32("worker", w.workerID),
			zap.Int64("offset", offset),
			zap.String("ioType", ioType.String()),
			zap.Error(err),
		)
		return err
	}
	defer func() { _ = unlock() }()

	cycle := w.cycle.Add(1) - 1
	seed := uint64(w.workerID)<<32 | cycle

	h := block.Header{
		NodeName: w.nodeName,
		WorkerID: w.workerID,
		TimeNs:   time.Now().UnixNano(),
		Cycle:    cycle,
		Offset:   uint64(offset),
		Tag:      uint32(ioType),
	}
	if err := block.MakeBlock(w.buf, &h, w.kind, seed, w.bodyLen); err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: MakeBlock: %w", err)
	}

	// Marshal the header separately so it can be stored in the xattr.
	var hdrBuf [block.HeaderSize]byte
	if err := block.MarshalHeader(hdrBuf[:], &h); err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: MarshalHeader: %w", err)
	}
	written := hdrBuf[:]

	// Intent-before-data: write the xattr header before the data block, and do
	// NOT reorder these. If the data Write below fails, the xattr intent is left
	// on disk with no matching data — this is deliberate. A verifier can flag and
	// recover from an xattr without data; the inverse (data with no xattr claim)
	// would be unrecoverable, so we never allow it.
	if err := w.store.Put(offset, dataSize, written); err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: store.Put: %w", err)
	}
	if err := w.file.Write(ioType, w.buf, offset); err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: file.Write: %w", err)
	}

	// BeeGFS does not provide cross-node page-cache coherence: a buffered
	// write stays in this node's page cache until the OS writes it back
	// asynchronously. Without an explicit flush, a reader on another node
	// can acquire the lock after we release it and still see stale data from
	// the storage servers, while the updated xattr is already visible (xattrs
	// go through the metadata path, not the data-path cache). Syncing while
	// the lock is still held ensures data reaches the storage servers before
	// any remote reader can acquire the lock and observe an inconsistency.
	if takeLock {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: sync: %w", err)
		}
		// Data has reached the storage servers. Stamp TagFsynced in the xattr
		// so a verifier can distinguish a coherence failure from a missing flush.
		// Re-marshal and re-put while the lock is still held so readers always
		// see a consistent (fsynced=true ↔ data on storage) state.
		h.Tag |= block.TagFsynced
		if err := block.MarshalHeader(hdrBuf[:], &h); err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: re-marshal after sync: %w", err)
		}
		if err := w.store.Put(offset, dataSize, written); err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: xattr update after sync: %w", err)
		}
	}

	// While the locks are still held, read back the xattr and verify it
	// matches what we wrote. A mismatch means another writer modified this
	// range while we held the lock — the filesystem's distributed locking
	// did not provide the expected exclusivity.
	if takeLock {
		readBack, err := w.store.Get(offset, dataSize)
		if err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: read-back: %w", err)
		}
		if !bytes.Equal(readBack, written) {
			formatHeader := func(label string, raw []byte) string {
				h, err := block.UnmarshalHeader(raw)
				if err != nil {
					return fmt.Sprintf("  %s: <unparseable: %v>", label, err)
				}
				return fmt.Sprintf("  %s: %s", label, h)
			}
			fmt.Fprintf(os.Stderr, "lock violation at offset=%d dataSize=%d:\n%s\n%s\n",
				offset, dataSize,
				formatHeader("wrote   ", written),
				formatHeader("read back", readBack),
			)
			return ErrLockViolation
		}
	}

	w.log.Debug("wrote block",
		zap.Int32("worker", w.workerID),
		zap.Int64("offset", offset),
		zap.Uint64("cycle", cycle),
		zap.String("ioType", ioType.String()),
		zap.Int("size", w.blockSize),
	)
	return nil
}

// maybeLock acquires a range lock when takeLock is true. Always
// returns an unlock function that must be called; when takeLock is
// false the function is a no-op.
func (w *Writer) maybeLock(offset, length int64, takeLock bool) (func() error, error) {
	if !takeLock {
		return func() error { return nil }, nil
	}
	return w.lockRegion(offset, length)
}
