package xattrstore

import (
	"bytes"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"go.uber.org/zap"
)

// Writer prepares verifiable blocks and writes them to a data file,
// keeping the xattr store in sync. A single WriteBlock call handles
// block construction, range locking (per the Writer's LockPolicy), xattr
// update, and the IO operation.
//
// Writer is not safe for concurrent use. The intended pattern is one
// Writer per worker goroutine, each with its own fileops.File.
//
// # Coherence precondition
//
// Shredding depends on non-overlapping record extents: the bytes a removed
// extent frees are only unclaimed if nothing else claims them. The precondition
// holds by construction in the normal workflow -- a run resets with Truncate(0),
// writes, and a separate sweep judges.
//
// It is not assumed, though, because the caller cannot discharge it. Extents come
// from xattr NAMES, and a corrupt name is exactly what a misbehaving filesystem
// produces, which is what this tool is deployed to find. A verifier sweep is a
// point-in-time judgment, so even a caller that verifies before every write holds
// no standing guarantee against a bad name appearing afterwards.
//
// So the Writer checks it, on the one path where it matters: the zero-fill
// refuses with ErrExtentOverlap rather than zeroing bytes a surviving record
// still claims. The check is an extent query against the record index, not a read
// of the data, so it does not disturb the IO pattern being measured. See
// zeroShredded and checkGapsUnclaimed.
type Writer struct {
	file      *fileops.File
	store     *Store
	nodeName  [16]byte
	workerID  int32
	runSeed   uint64
	kind      block.Kind
	blockSize int // total on-disk bytes per block
	bodyLen   int // body bytes; derived from blockSize via block.BodyLen
	locking   LockPolicy
	cycle     atomic.Uint64
	buf       []byte
	log       *zap.Logger
}

// LockPolicy selects whether a Writer takes an exclusive range lock around
// every operation that mutates the file or the store.
//
// Chosen once, for the life of the Writer, because whether other writers exist
// is a property of the deployment rather than of any single write. A per-call
// argument would let one Writer lock some operations and not others, and a
// writer that locks most of the time provides no guarantee at all -- a reader
// need only land in one unlocked window. That state is unrepresentable here.
type LockPolicy int

const (
	// LockPolicyUnset is the zero value and is rejected by NewWriter.
	//
	// Deliberate: WriterConfig is a struct, so an omitted field would otherwise
	// default to one of the real policies and silently pick a side of exactly
	// the decision this type exists to make explicit.
	LockPolicyUnset LockPolicy = iota

	// LockExclusive takes an exclusive range lock around each mutating
	// operation, covering the union of the target extent and every record it
	// overlaps. Required when more than one process or goroutine may write the
	// same file, and required for verifier.VerifyFile's guarantees to hold
	// against a live writer.
	//
	// Costs an fcntl pair per operation. ErrLockBusy and ErrSetChanged become
	// reachable and are non-fatal: skip the region or move on, rather than
	// retrying immediately.
	LockExclusive

	// LockNone never locks. Correct when this Writer is the only thing writing
	// the file, and also the right choice when the caller provides its own
	// exclusion -- the library has no way to tell those apart and does not try.
	//
	// A concurrent reader (verifier.VerifyFile, iotest-verify) can observe a
	// partially applied write under this policy. In particular WriteBlock
	// removes the records it is replacing before it writes and zeroes, so a
	// sweep can catch bytes that are momentarily non-zero and unclaimed and
	// report them as an anomaly. That is a true statement about what was on
	// disk, not a bug in the verifier.
	LockNone
)

// WriterConfig is the input to NewWriter.
//
// File, Store and Locking are required; the rest have workable defaults.
type WriterConfig struct {
	File  *fileops.File
	Store *Store

	// WorkerID is recorded in every block header, so a multi-writer run can
	// attribute a block to the writer that produced it.
	WorkerID int

	// RunSeed identifies this RUN, and is mixed into every block's body seed so
	// two runs over the same file write different bytes -- which is what lets a
	// verifier tell a stale read of the previous generation from a clean one.
	// Zero draws a random value.
	//
	// Only as far as the Kind carries the seed, and that bound is the Kind's, not
	// this field's: KindZeros and KindOnes ignore the seed entirely, so two runs
	// are always byte-identical and no run seed rescues them, while KindCountUp
	// and KindDecimal reduce it to one byte or to 0-511 and so collide at 1/256
	// and 1/512. Only KindPRNG and KindRepeat consume all 64 bits. See block.Kind.
	//
	// Pass the SAME RunSeed to every Writer in one run; WorkerID is what separates
	// them. A pinned RunSeed makes a run reproducible, and is the only case where
	// WorkerID uniqueness matters -- two Writers sharing both values produce
	// identical bodies at equal cycles.
	RunSeed uint64

	// Kind selects how block bodies are generated. The zero value is not a
	// valid block.Kind; see block.Kind for what each one can detect.
	Kind block.Kind

	// BlockSize is the total on-disk size per block, from which the body length
	// is derived. Not all values are valid: a multiple of 516 (StripeSize+4)
	// always works, and so do powers of two EXCEPT 65536 -- see block.BodyLen
	// for why 64 KiB is the one natural size that fails.
	BlockSize int

	// Locking must be set explicitly. See LockPolicy.
	Locking LockPolicy

	// Log receives per-operation IO traces. nil disables them.
	Log *zap.Logger
}

// NewWriter creates a Writer from cfg. The hostname is detected automatically.
func NewWriter(cfg WriterConfig) (*Writer, error) {
	if cfg.File == nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: File is nil")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: Store is nil")
	}
	if cfg.Locking != LockExclusive && cfg.Locking != LockNone {
		return nil, fmt.Errorf("xattrstore.NewWriter: Locking must be set to LockExclusive or LockNone")
	}
	bodyLen, err := block.BodyLen(cfg.BlockSize)
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: %w", err)
	}
	// File and Store are two independent descriptors, and nothing else ever
	// compares them: w.file carries the data (WriteAt, Truncate, fstat) and
	// w.store the metadata. Mismatched, Truncate aims one file's metadata at
	// another file's data and the two halves diverge with every operation looking
	// locally fine.
	//
	// This catches caller error, not an attack -- a racing rename between the two
	// opens defeats it just as it defeats any check made after the fact, so it is
	// not a security boundary. It is worth having because the failure is otherwise
	// silent and its symptom (records describing data that was never there) is
	// indistinguishable from the corruption this tool reports.
	// Both sides by fstat on the held descriptor, not by path: comparing paths
	// would answer a different question, and both types already expose what is
	// needed -- LockFd for File, and Store's own field, since this is its package.
	fileInfo, err := cfg.File.LockFd().Stat()
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: stat File: %w", err)
	}
	storeInfo, err := cfg.Store.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: stat Store: %w", err)
	}
	if !os.SameFile(fileInfo, storeInfo) {
		return nil, fmt.Errorf("xattrstore.NewWriter: File (%s) and Store (%s) are different files",
			cfg.File.LockFd().Name(), cfg.Store.Target())
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("xattrstore.NewWriter: hostname: %w", err)
	}
	log := cfg.Log
	if log == nil {
		log = zap.NewNop()
	}
	// Loop rather than draw once: zero stays unambiguously "unset", so a drawn
	// seed can never be mistaken for an unconfigured one.
	runSeed := cfg.RunSeed
	for runSeed == 0 {
		runSeed = mrand.Uint64()
	}
	return &Writer{
		file:      cfg.File,
		store:     cfg.Store,
		nodeName:  block.NodeNameFromString(hostname),
		workerID:  int32(cfg.WorkerID),
		runSeed:   runSeed,
		kind:      cfg.Kind,
		blockSize: cfg.BlockSize,
		bodyLen:   bodyLen,
		locking:   cfg.Locking,
		buf:       make([]byte, cfg.BlockSize),
		log:       log,
	}, nil
}

// RunSeed returns the seed identifying this Writer's run, whether it was
// supplied or drawn. Tools should print it: it is what makes a run repeatable.
func (w *Writer) RunSeed() uint64 { return w.runSeed }

// WriteBlock writes exactly blockSize bytes at offset using the given IOType.
// Tag in the header is set to uint32(ioType) so dump tools can see which IO
// method produced each block.
//
// Under LockExclusive an exclusive range lock is acquired before writing and
// released after, and ErrLockBusy and ErrSetChanged become reachable as
// non-fatal signals — the caller should skip or move to a different region
// rather than retrying immediately. Under LockNone neither can occur.
//
// The xattr is written before the data (intent-before-data ordering)
// so a verifier can distinguish a clean write from a crashed one.
func (w *Writer) WriteBlock(offset int64, ioType fileops.IOType) (err error) {
	dataSize := int64(w.blockSize)
	unlock, err := w.takeLockIfNeeded(offset, dataSize)
	if err != nil {
		w.log.Warn("block skipped",
			zap.Int32("worker", w.workerID),
			zap.Int64("offset", offset),
			zap.String("ioType", ioType.String()),
			zap.Error(err),
		)
		return err
	}
	// The release error must reach the caller, whichever shape it takes. An
	// ambiguous failure (ErrLockTimeout) leaks the range guard, leaving this
	// offset permanently unacquirable; a definite one frees the guard but may
	// leave the kernel lock held with nothing recording it. Both surface later
	// as ErrLockBusy, which callers treat as benign -- so dropping either turns
	// a dead range into apparent healthy throughput. See ReleaseErrorOrCause,
	// which stopped ranking the two after a definite failure was found being
	// discarded in favour of an in-flight body error.
	defer func() {
		if rerr := unlock(); rerr != nil {
			w.log.Error("lock release failed",
				zap.Int32("worker", w.workerID),
				zap.Int64("offset", offset),
				zap.Error(rerr),
			)
			err = ReleaseErrorOrCause(rerr, err)
		}
	}()

	cycle := w.cycle.Add(1) - 1
	// Four coordinates: the run, the worker within it, where the block goes, and
	// when it was written. Offset is among them because cycle is per-Writer and
	// starts at 0, so a run that recreates a worker repeats (runSeed, workerID,
	// cycle) -- but at a different offset. It also gives the better invariant for
	// a verification tool: expected content depends on WHERE a block is, not on
	// when it was written.
	//
	// Consequently a body written for one offset cannot validate at another, so
	// the seed now catches a misdirected write on its own rather than leaving it
	// to Header.Offset (VerdictOffsetMismatch).
	seed := block.DeriveSeed(w.runSeed, uint64(w.workerID), uint64(offset), cycle)

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
	//
	// ReplaceRange, not Put: Put is keyed by (offset, length), so it only ever
	// replaces a record of the identical extent. Writing at a different block
	// size leaves the old record in place beside the new one, and the verifier
	// reports the doubly-claimed bytes as CoverageMany -- which reads as the
	// filesystem having let two writers claim the same range, the most alarming
	// thing this tool can say, for what is actually just a changed --blocksize.
	// Enforcing replace-and-shred here is what makes that state unreachable
	// rather than merely documented.
	removed, err := w.store.ReplaceRange(offset, dataSize, written)
	if err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: store.ReplaceRange: %w", err)
	}

	// A COPY of what was just stored, because written aliases hdrBuf and the
	// TagFsynced re-marshal below writes through it.
	//
	// Not strictly required at the current ordering -- the read-back runs before
	// that re-marshal, so written still holds these bytes when it does. It is here
	// so the check does not silently depend on that: moving the re-marshal earlier
	// would otherwise turn the comparison into our-own-bytes-vs-themselves again,
	// which is precisely how it came to be unfailable.
	var claimed [block.HeaderSize]byte
	copy(claimed[:], written)

	if err := w.file.Write(ioType, w.buf, offset); err != nil {
		return fmt.Errorf("xattrstore.Writer.WriteBlock: file.Write: %w", err)
	}
	if hookAfterDataWrite != nil {
		if err := hookAfterDataWrite(w, offset, dataSize); err != nil {
			return err
		}
	}
	// Shredding a wider record frees the bytes outside this block, and they
	// still hold whatever was written there before. Left alone they read as
	// stale data in a span that claims to be unwritten, so zero them -- the
	// data-side half of replace-and-shred that ReplaceRange deliberately
	// leaves to its caller. Done after the data write so a crash in between
	// leaves uncovered bytes (flaggable, recoverable) rather than a record
	// with no data.
	if len(removed) > 0 {
		if _, err := w.zeroShredded(removed, offset, dataSize); err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: %w", err)
		}
	}

	// BeeGFS does not provide cross-node page-cache coherence: a buffered
	// write stays in this node's page cache until the OS writes it back
	// asynchronously. Without an explicit flush, a reader on another node
	// can acquire the lock after we release it and still see stale data from
	// the storage servers, while the updated xattr is already visible (xattrs
	// go through the metadata path, not the data-path cache). Syncing while
	// the lock is still held ensures data reaches the storage servers before
	// any remote reader can acquire the lock and observe an inconsistency.
	if w.locking == LockExclusive {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: sync: %w", err)
		}

		// Read back BEFORE the TagFsynced re-put, and compare against what
		// ReplaceRange stored. The interval this covers is everything that takes
		// time under the lock: the data write, the zero-fill, and the whole-file
		// fsync. A competing write landing anywhere in there is what "the
		// filesystem's distributed locking did not hold" looks like.
		//
		// The ORDER is the fix, and must not be relaxed back. When the comparison
		// sat after the re-put it could not fail: the re-put is unconditional, so
		// it overwrote any foreign header first and the check then read our own
		// bytes back and agreed with itself. Its live window was re-put -> Get,
		// which contains no work. Restoring that order fails
		// TestWriteBlockDetectsClobberUnderLock.
		//
		// Put #2 -> return stays unchecked, deliberately. Adding a second
		// read-back would close a window with no work in it at the price of
		// another getxattr on every locked write -- distorting the IO pattern this
		// tool exists to measure, the same objection that rules out verifying
		// before every write.
		readBack, err := w.store.Get(offset, dataSize)
		if err != nil {
			return fmt.Errorf("xattrstore.Writer.WriteBlock: read-back: %w", err)
		}
		if !bytes.Equal(readBack, claimed[:]) {
			formatHeader := func(label string, raw []byte) string {
				h, err := block.UnmarshalHeader(raw)
				if err != nil {
					return fmt.Sprintf("  %s: <unparseable: %v>", label, err)
				}
				return fmt.Sprintf("  %s: %s", label, h)
			}
			fmt.Fprintf(os.Stderr, "lock violation at offset=%d dataSize=%d:\n%s\n%s\n",
				offset, dataSize,
				formatHeader("wrote   ", claimed[:]),
				formatHeader("read back", readBack),
			)
			w.log.Error("lock violation: xattr overwritten while the lock was held",
				zap.Int32("worker", w.workerID),
				zap.Int64("offset", offset),
				zap.Int64("dataSize", dataSize),
			)
			return ErrLockViolation
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

	w.log.Debug("wrote block",
		zap.Int32("worker", w.workerID),
		zap.Int64("offset", offset),
		zap.Uint64("cycle", cycle),
		zap.String("ioType", ioType.String()),
		zap.Int("size", w.blockSize),
	)
	return nil
}

// scanTruncateBound returns the first offset beyond everything a truncation
// could touch: the file's current length, or the furthest extent any record
// claims if a record reaches past EOF.
//
// Named for the cost: it scans every record on the file, so a file near the
// per-inode xattr ceiling makes this thousands of getxattr calls per Truncate.
//
// The scan is not serialised against concurrent record creation, so a record
// appearing beyond this bound after the scan would fall outside the lock. That
// is accepted: truncation is a setup/teardown operation, and a caller creating
// records concurrently with one has already lost the guarantee this bound
// exists to provide.
func (w *Writer) scanTruncateBound() (int64, error) {
	end, err := w.file.Size()
	if err != nil {
		return 0, err
	}
	// Overlapping, for two reasons that both still hold. It has to agree with the
	// set RemoveCovering will actually remove and with the union extent
	// TryAcquireExclusive computes -- all three key off the name's extent, so a
	// bound derived any other way could exclude a record that then gets removed
	// inside the lock. And it costs no per-entry getxattr, which ForEachEntry
	// would, for a header this function discards.
	//
	// Not because ForEachEntry skips wrong-sized values: it no longer does, and
	// the two iterators now agree on them (see Store.ForEachEntry). Only the cost
	// and the shared selection key separate them.
	all, err := w.store.Overlapping(0, math.MaxInt64)
	if err != nil {
		return 0, fmt.Errorf("scanning records: %w", err)
	}
	for _, e := range all {
		if x := e.Offset + e.Length; x > end {
			end = x
		}
	}
	return end, nil
}

// hookAfterDataWrite is a test-only injection point. When non-nil it runs inside
// WriteBlock immediately after the data write, i.e. inside the interval the
// read-back is supposed to cover, letting a test clobber the xattr from under a
// held lock. Nothing else can reach that interval: a real violation needs a
// second node whose lock manager failed, which no unit test can arrange.
//
// A non-nil error is returned to WriteBlock's caller unchanged, so a hook can
// also stand in for a failure at this point.
var hookAfterDataWrite func(w *Writer, offset, dataSize int64) error

// shredGap is one clamped byte range the zero-fill intends to write. Named so
// the check pass and the write pass share one definition of what a gap is.
type shredGap struct{ off, len int64 }

// zeroShredded writes zeros over the parts of each removed record's extent
// that fall outside [keepOffset, keepOffset+keepLen) -- the bytes shredding
// freed, which no record claims any more but which still physically hold the
// old block's contents. It returns the number of bytes it wrote, which is what
// tells a caller whether a flush is owed.
//
// Writes are clamped to the file's current length: zeroing past EOF would
// extend the file with bytes nothing claims, turning one inconsistency into a
// larger one.
//
// Every gap is checked against the surviving records BEFORE anything is written,
// and the whole call is refused with ErrExtentOverlap if any survivor still
// claims a byte in one. "These bytes are unclaimed" is the assumption the
// zero-fill rests on, and the Writer cannot establish it (see the coherence
// precondition on Writer) -- so it is checked here instead of assumed. The
// checking pass has to complete before the writing pass: refusing after some
// gaps have been zeroed would leave the damage this guard exists to prevent.
func (w *Writer) zeroShredded(removed []Entry, keepOffset, keepLen int64) (int64, error) {
	size, err := w.file.Size()
	if err != nil {
		return 0, err
	}
	keepEnd := keepOffset + keepLen

	// Each removed extent contributes at most two gaps: the part before the
	// block we just wrote, and the part after it. Collected once and reused by
	// both passes, so the two cannot disagree about what is being zeroed.
	var gaps []shredGap
	for _, e := range removed {
		for _, g := range [2][2]int64{
			{e.Offset, min(e.Offset+e.Length, keepOffset)},
			{max(e.Offset, keepEnd), e.Offset + e.Length},
		} {
			// Size through ReadableLen rather than clamping by hand. ReadableLen
			// is where that clamp is defined, and its doc requires every caller
			// sizing a buffer from a record extent to route through it -- a second
			// copy of the arithmetic here is exactly what drifts from the first.
			if n := ReadableLen(g[0], g[1]-g[0], size); n > 0 {
				gaps = append(gaps, shredGap{g[0], n})
			}
		}
	}
	if len(gaps) == 0 {
		return 0, nil
	}

	if err := w.checkGapsUnclaimed(removed, gaps); err != nil {
		return 0, err
	}

	// One zero buffer for the whole call, grown at most to zeroWriteChunk and
	// never written into, so every gap below reuses it.
	var buf []byte
	var written int64
	for _, g := range gaps {
		if want := min(g.len, int64(zeroWriteChunk)); int64(len(buf)) < want {
			buf = make([]byte, want)
		}
		for off, remaining := g.off, g.len; remaining > 0; {
			chunk := min(int64(len(buf)), remaining)
			if err := w.file.Write(fileops.IOTypeBuffered, buf[:chunk], off); err != nil {
				return written, fmt.Errorf("zero shredded [%d, %d): %w", off, off+chunk, err)
			}
			off += chunk
			remaining -= chunk
			written += chunk
		}
	}
	return written, nil
}

// checkGapsUnclaimed refuses the zero-fill if any record that survived the shred
// still claims a byte in one of the gaps about to be zeroed.
//
// One Overlapping call over the union of the removed extents, which is
// listxattr plus name parsing and no per-entry getxattr -- the same query
// ReplaceRange, scanTruncateBound and TryAcquireExclusive already make on this
// path. Zero false positives is a property rather than a measurement: if extents
// do not overlap then removed and surviving extents are disjoint, and every gap
// is a subset of a removed extent.
//
// The just-written record is a survivor here (intent-before-data puts the xattr
// down first) and is exactly the keep range, which every gap excludes by
// construction, so it can never trip this.
func (w *Writer) checkGapsUnclaimed(removed []Entry, gaps []shredGap) error {
	lo, hi := removed[0].Offset, removed[0].Offset+removed[0].Length
	for _, e := range removed[1:] {
		lo = min(lo, e.Offset)
		hi = max(hi, e.Offset+e.Length)
	}
	survivors, err := w.store.Overlapping(lo, hi-lo)
	if err != nil {
		return fmt.Errorf("checking shredded gaps: %w", err)
	}
	for _, g := range gaps {
		for _, s := range survivors {
			if s.Offset >= g.off+g.len || s.Offset+s.Length <= g.off {
				continue
			}
			// The only surviving evidence: ReplaceRange has already removed the
			// record whose extent was untrustworthy, so the store looks coherent
			// to any later sweep.
			w.log.Error("refusing to zero shredded bytes a surviving record still claims",
				zap.Int32("worker", w.workerID),
				zap.Int64("gapOffset", g.off),
				zap.Int64("gapLength", g.len),
				zap.String("claimant", s.Name),
				zap.Int64("claimantOffset", s.Offset),
				zap.Int64("claimantLength", s.Length),
			)
			return fmt.Errorf("%w: [%d,%d) is still claimed by %s [%d,%d)",
				ErrExtentOverlap, g.off, g.off+g.len,
				s.Name, s.Offset, s.Offset+s.Length)
		}
	}
	return nil
}

// zeroWriteChunk bounds the zero-fill buffer. ReadableLen clamps a gap to the
// FILE, which is right for a read and wrong for an allocation: on a multi-GiB
// target a single make([]byte, n) was a multi-GiB allocation and an uncatchable
// runtime OOM -- killing the soak on the very corruption it exists to report.
// Reachable two ways: a corrupt xattr name claiming an oversized extent
// (validRange checks sign and overflow, not the file), and the routine case of
// a large-blocksize record shredded by a smaller one.
const zeroWriteChunk = 4 << 20 // 4 MiB

// Truncate changes the data file's length to size and drops every record that
// claimed any byte at or beyond it, so the two halves stay consistent.
//
// Preferred caller behaviour for size > 0 is to VERIFY the region being
// discarded first. This is the last moment the evidence exists: whatever a sweep
// would have said about those bytes is unrecoverable afterwards, and reading data
// you are about to delete costs nothing you were keeping.
//
// Not the only operation here that destroys data irreversibly -- WriteBlock's
// shred-zeroing does too, over the bytes a wider record claimed. What makes a
// partial truncate the sharper case is scale and intent: a shred is bounded by
// the one record being replaced, while size is caller-chosen and discards
// everything above it in a single call.
//
// Deliberately a preference and not a requirement. Enforcing it would push every
// tool onto the same read-before-write shape, and a variety of IO patterns is
// the point -- so long as SOME tools verify before truncating, the coverage is
// there. size == 0 needs none of this: it zeroes nothing, because the file is
// already empty by the time the shred-zeroing would run.
//
// A record straddling size is removed entirely: records are never split, and a
// block whose bytes are only partly present is not verifiable as a block.
// size == 0 therefore purges every record, which is what a caller wanting
// O_TRUNC semantics should use -- opening with O_TRUNC discards the data while
// leaving every record behind, and the verifier then (correctly) reports each
// one as claiming data that no longer exists.
//
// Records are removed BEFORE the data is truncated, deliberately, and this
// ordering must not be reversed. A crash in between then leaves bytes no record
// claims -- flaggable and recoverable, the same trade ReplaceRange makes. The
// reverse order would leave records describing already-freed data, which is
// exactly the anomaly this method exists to prevent.
//
// Growing is refused: a size past the current EOF returns
// ErrTruncateGrowUnsupported under both lock policies. See that error for why the
// direction was withdrawn rather than fixed.
//
// Under LockExclusive a range lock is held over [size, end), the region a shrink
// moves through, where end is the bound scanTruncateBound computes -- plus
// whatever unionExtent widens it to. Two limits, both deliberate. A truncate to
// exactly that bound changes nothing and takes no lock, because there is no byte
// to lock. And both the bound and the grow ban come from reads taken BEFORE the
// lock, so a length change landing after them falls outside the guarantee: a
// concurrent shrink can even defeat the ban outright, since size is then past a
// stale EOF. This is setup/teardown, and a caller mutating the file concurrently
// with a truncate has already lost what the lock provides. See scanTruncateBound.
//
// Without the lock -- LockNone -- removing a record out from under a writer that
// holds a lease on it makes that writer's post-write read-back find its xattr
// missing. That surfaces as a wrapped xattr.ErrNotFound from the read-back, NOT
// as ErrLockViolation: the read-back compares bytes and never runs, because the
// Get fails first. Measured, not derived. So the damage is a confusing error on a
// file where nothing was wrong, rather than this package's most alarming one --
// which is the better of the two outcomes, and not what this comment claimed
// before.
//
// Under LockExclusive, do not call this while already holding a lease on the
// same Store. The in-process range table is not reentrant, so the acquire
// returns ErrLockBusy. Setup and teardown on a file no other writer has touched
// yet are the usual reason to want that, and the answer is a LockNone Writer
// for those phases rather than a per-call override -- a Writer that sometimes
// locks provides no guarantee, which is why the policy is fixed at
// construction.
func (w *Writer) Truncate(size int64) (err error) {
	if size < 0 {
		return fmt.Errorf("xattrstore.Writer.Truncate: size must be >= 0 (got %d)", size)
	}
	// Refuse a grow before anything else, under both policies -- the hazard is in
	// the bytes, not the locking. See ErrTruncateGrowUnsupported for what taking
	// it back would require.
	eof, sizeErr := w.file.Size()
	if sizeErr != nil {
		return fmt.Errorf("xattrstore.Writer.Truncate: %w", sizeErr)
	}
	if size > eof {
		return fmt.Errorf("xattrstore.Writer.Truncate: %w (size %d, current length %d)",
			ErrTruncateGrowUnsupported, size, eof)
	}

	// unlock and the release defer both sit at FUNCTION scope deliberately. Inside
	// the conditional-locking block a `:=` shadows the named return err, so the
	// defer would combine the release error into a dead variable and Truncate
	// would report success.
	unlock := func() error { return nil }
	defer func() {
		if rerr := unlock(); rerr != nil {
			w.log.Error("lock release failed after truncate",
				zap.Int64("size", size), zap.Error(rerr))
			err = ReleaseErrorOrCause(rerr, err)
		}
	}()

	if w.locking == LockExclusive {
		// The bound is the current EOF or the furthest extent any record claims,
		// whichever is greater: a record claiming bytes past EOF is precisely the
		// incoherent state this method repairs, so it has to be covered too.
		//
		// The lock does NOT need to be widened downward by hand.
		// TryAcquireExclusive does that itself via unionExtent, taking in any
		// record that straddles size -- which matters because such a record is
		// removed whole and its surviving prefix below size is rewritten, both
		// outside a naive [size, EOF) range.
		end, endErr := w.scanTruncateBound()
		if endErr != nil {
			return fmt.Errorf("xattrstore.Writer.Truncate: %w", endErr)
		}
		// size <= eof <= end holds here, since a grow is refused above, so this is
		// always [size, end). The min/max is kept rather than simplified back to a
		// bare `end > size` gate: that gate is exactly the shape that let a grow
		// through unlocked, and lifting the grow ban should not have to rediscover
		// it.
		lo, hi := min(size, end), max(size, end)
		if hi > lo {
			var lockErr error
			unlock, lockErr = w.lockRegion(lo, hi-lo)
			if lockErr != nil {
				// lockRegion returns a no-op unlock alongside its error, so the
				// deferred release stays harmless on this path.
				return fmt.Errorf("xattrstore.Writer.Truncate: lock [%d,%d): %w", lo, hi, lockErr)
			}
		}
		// hi == lo means size == eof and no record ends above it, so there is
		// nothing to remove and no byte to lock -- ValidRange rejects a zero-length
		// range anyway.
		//
		// Both facts come from pre-lock reads, so this branch is only as good as
		// they are: an out-of-band shrink after them puts size past the real EOF and
		// file.Truncate grows the file here with no lock held at all. That is the
		// case the grow ban was meant to remove and does not, and the reason this
		// branch is the one to look at first if it has to be reopened.
	}

	removed, err := w.store.RemoveCovering(size)
	if err != nil {
		return fmt.Errorf("xattrstore.Writer.Truncate: %w", err)
	}
	if err := w.file.Truncate(size); err != nil {
		return fmt.Errorf("xattrstore.Writer.Truncate: %w", err)
	}
	// A record straddling size is removed whole, so the part of it BELOW size
	// survives the truncate while no longer being claimed by anything -- still
	// holding the old block's bytes, which reads as stale data in a span that
	// says it was never written. Zero it, for the same reason WriteBlock zeroes
	// what shredding frees. Only a non-block-aligned size can reach this: an
	// aligned truncate removes records that lie entirely above it.
	//
	// The keep-range is empty at size, so every surviving byte of a removed
	// extent counts as freed; the part above size is clipped away by the
	// file's new length.
	if len(removed) > 0 {
		written, zerr := w.zeroShredded(removed, size, 0)
		if zerr != nil {
			return fmt.Errorf("xattrstore.Writer.Truncate: %w", zerr)
		}
		// Same reason WriteBlock syncs before releasing: BeeGFS gives no
		// cross-node page-cache coherence, so these zeros can sit in this node's
		// cache while the record removals -- which travel the metadata path --
		// are already visible elsewhere. A reader taking the lock after us then
		// sees the old block's bytes in a span nothing claims, and reports data
		// the tool itself left behind.
		//
		// Gated on bytes actually written, not on len(removed): Truncate(0) is
		// the reset both tools call on every invocation, and it routinely removes
		// records while writing nothing. Gating on len(removed) would add a
		// whole-file fsync to every reset of a multi-GiB target for no benefit.
		if written > 0 && w.locking == LockExclusive {
			if err := w.file.Sync(); err != nil {
				return fmt.Errorf("xattrstore.Writer.Truncate: sync: %w", err)
			}
		}
	}
	w.log.Debug("truncated", zap.Int32("worker", w.workerID), zap.Int64("size", size))
	return nil
}

// takeLockIfNeeded takes an exclusive range lock over [offset, offset+length)
// when w.locking says one is needed -- i.e. anything other than LockNone, which
// declares that either this Writer is the sole writer or the caller provides
// its own exclusion.
//
// ALWAYS returns a callable unlock function, including on the error path and
// under LockNone, so callers can defer unlock() with no nil check.
//
// Locking is the default branch deliberately: a policy value this function does
// not recognise takes the lock rather than silently skipping it.
func (w *Writer) takeLockIfNeeded(offset, length int64) (func() error, error) {
	if w.locking == LockNone {
		return func() error { return nil }, nil
	}
	return w.lockRegion(offset, length)
}
