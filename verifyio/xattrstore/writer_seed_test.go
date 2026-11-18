// This is a unit test.
//
// Coverage: the per-run seed identity that makes a Writer's bodies distinguish
// one run from another. One test per collision class the derivation closes --
// different run, duplicate WorkerID under a drawn seed, and a fresh Writer whose
// cycle counter restarts -- plus the reproducibility property a pinned RunSeed
// is for, and NewWriter's non-zero draw.
//
// Every coordinate WriteBlock composes has one test that fails when it is
// removed, which is what the first three did not give on their own: they all run
// with a DRAWN seed or a fresh Writer, so deleting either uint64(w.workerID) or
// cycle from the derivation left the whole tree green. The two pinned-seed tests
// at the end of this file close that.
//
// These matter because the record header carrying Seed lives only in the xattr:
// the on-disk block is body plus CRC stripes with nothing embedded, so detecting
// a stale read of a superseded generation rests entirely on the stored seed
// differing between generations. Nothing else compares them.
package xattrstore

import (
	"bytes"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
)

// writeBlockAt writes one block with a fresh Writer built from cfg and returns
// the bytes that landed on disk, mimicking a tool invocation: construct a
// Writer, write, read back.
func writeBlockAt(t *testing.T, s *Store, cfg WriterConfig, offset int64) []byte {
	t.Helper()
	f := openWriterTarget(t, s)
	cfg.File = f
	cfg.Store = s
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock at %d: %v", offset, err)
	}
	buf := make([]byte, cfg.BlockSize)
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt %d: %v", offset, err)
	}
	return buf
}

// seedTestConfig is the shared shape: KindPRNG because it is the kind whose doc
// recommends it for detecting a stale read, and the one that consumed the raw
// composed seed before DeriveSeed existed.
func seedTestConfig(runSeed uint64, workerID int) WriterConfig {
	return WriterConfig{
		WorkerID:  workerID,
		RunSeed:   runSeed,
		Kind:      block.KindPRNG,
		BlockSize: 4096,
		Locking:   LockNone,
	}
}

// TestWriteBlockDistinguishesRuns is collision class 1 and the reported defect:
// two runs over the same file at the same offset used to write byte-identical
// data, because the seed was (workerID<<32 | cycle) and every tool passes
// WorkerID 0 with a cycle counter restarting at 0. A stale read of the previous
// generation therefore regenerated a matching body and verified clean.
//
// This test fails if the derivation reverts to anything that omits the run.
func TestWriteBlockDistinguishesRuns(t *testing.T) {
	s := makeTempStore(t)

	first := writeBlockAt(t, s, seedTestConfig(1, 0), 0)
	second := writeBlockAt(t, s, seedTestConfig(2, 0), 0)

	if bytes.Equal(first, second) {
		t.Error("two runs wrote byte-identical blocks; a stale read of the previous generation would verify clean")
	}
}

// TestWriteBlockDistinguishesDuplicateWorkerIDs is collision class 4. Two
// Writers that share a WorkerID -- a caller bug, but a silent one -- used to
// produce identical bodies at equal cycles. A drawn RunSeed separates them
// without the caller having to get IDs right.
//
// Note the deliberate limit: with RunSeed PINNED, duplicate IDs still collide,
// and WriterConfig.RunSeed documents that. Pinning is what buys reproducibility,
// so it cannot also paper over duplicate IDs.
func TestWriteBlockDistinguishesDuplicateWorkerIDs(t *testing.T) {
	s := makeTempStore(t)

	// RunSeed 0 on both: each Writer draws its own.
	first := writeBlockAt(t, s, seedTestConfig(0, 3), 0)
	second := writeBlockAt(t, s, seedTestConfig(0, 3), 0)

	if bytes.Equal(first, second) {
		t.Error("two Writers with a duplicate WorkerID wrote identical blocks under drawn run seeds")
	}
}

// TestWriteBlockDistinguishesRestartedCycles is collision class 5, and the
// reason offset is one of the coordinates. cycle is per-Writer and starts at 0,
// so a run that recreates a worker mid-flight repeats (runSeed, workerID, cycle)
// exactly -- pinned seed and all. Only the offset differs, so only the offset
// can separate them.
func TestWriteBlockDistinguishesRestartedCycles(t *testing.T) {
	s := makeTempStore(t)
	cfg := seedTestConfig(0x5eed, 0)

	// Two fresh Writers, so both write at cycle 0.
	first := writeBlockAt(t, s, cfg, 0)
	second := writeBlockAt(t, s, cfg, 4096)

	if bytes.Equal(first, second) {
		t.Error("a restarted cycle at a different offset wrote an identical block")
	}
}

// TestWriteBlockPinnedRunSeedIsReproducible pins the property a pinned RunSeed
// exists for: the same seed and the same write sequence reproduce identical
// bytes. Without a test, a future change to the coordinates would silently take
// reproducibility away while every distinctness test above stayed green.
func TestWriteBlockPinnedRunSeedIsReproducible(t *testing.T) {
	cfg := seedTestConfig(0xfeedface, 2)

	run := func() [][]byte {
		s := makeTempStore(t)
		f := openWriterTarget(t, s)
		c := cfg
		c.File, c.Store = f, s
		w, err := NewWriter(c)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		var out [][]byte
		for i := range 3 {
			offset := int64(i) * int64(cfg.BlockSize)
			if err := w.WriteBlock(offset, fileops.IOTypeBuffered); err != nil {
				t.Fatalf("WriteBlock at %d: %v", offset, err)
			}
			buf := make([]byte, cfg.BlockSize)
			if _, err := f.ReadAt(buf, offset); err != nil {
				t.Fatalf("ReadAt %d: %v", offset, err)
			}
			out = append(out, buf)
		}
		return out
	}

	// Different files, so only the pinned seed can make the bytes agree. The
	// headers differ (TimeNs), so compare bodies rather than whole blocks.
	a, b := run(), run()
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Errorf("block %d differs between two runs at the same pinned RunSeed", i)
		}
	}
}

func TestNewWriterDrawsNonZeroRunSeed(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)
	base := seedTestConfig(0, 0)
	base.File, base.Store = f, s

	w, err := NewWriter(base)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if w.RunSeed() == 0 {
		t.Error("RunSeed() is 0 after a draw; zero must stay unambiguously \"unset\"")
	}

	// A supplied seed is reported back unchanged, so a tool printing RunSeed()
	// prints something a later run can be pinned to.
	pinned := base
	pinned.RunSeed = 0xabcdef
	w2, err := NewWriter(pinned)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if got := w2.RunSeed(); got != 0xabcdef {
		t.Errorf("RunSeed() = %#x, want %#x", got, 0xabcdef)
	}
}

// TestWriteBlockDistinguishesWorkersUnderAPinnedRunSeed is collision class 4's
// other half, and the one the drawn-seed test above cannot reach.
//
// WriterConfig.RunSeed documents the multi-writer shape as one seed shared by
// every Writer, with WorkerID separating them -- so under that configuration the
// worker coordinate is the ONLY thing keeping two workers at one offset apart.
// Nothing pinned it: TestWriteBlockDistinguishesDuplicateWorkerIDs separates its
// Writers with DRAWN seeds, which already differ per Writer, so it stays green
// with uint64(w.workerID) deleted from the composition.
func TestWriteBlockDistinguishesWorkersUnderAPinnedRunSeed(t *testing.T) {
	s := makeTempStore(t)

	// Pinned, and the same for both: the documented multi-writer configuration.
	const runSeed = uint64(0x9e3779b97f4a7c15)
	first := writeBlockAt(t, s, seedTestConfig(runSeed, 1), 0)
	second := writeBlockAt(t, s, seedTestConfig(runSeed, 2), 0)

	if bytes.Equal(first, second) {
		t.Error("two workers wrote byte-identical blocks at one offset under a shared pinned RunSeed; " +
			"a crossed write between them would verify clean")
	}
}

// TestWriteBlockDistinguishesCyclesWithinOneWriter is the in-run form of the
// defect TestWriteBlockDistinguishesRuns covers across runs.
//
// One Writer rewriting an offset advances only cycle: runSeed, workerID and
// offset are all fixed, so cycle is the sole separator between the two
// generations. Left unpinned, a stale read of the first generation regenerates
// a body matching the second's stored seed and verifies clean.
//
// Written against one Writer deliberately. The tests above build a fresh Writer
// per call, which restarts the cycle counter at 0 and therefore never advances
// the coordinate under test.
func TestWriteBlockDistinguishesCyclesWithinOneWriter(t *testing.T) {
	s := makeTempStore(t)
	f := openWriterTarget(t, s)

	cfg := seedTestConfig(0x5eed5eed, 0)
	cfg.File, cfg.Store = f, s
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	readBlock := func(cycle int) []byte {
		t.Helper()
		buf := make([]byte, cfg.BlockSize)
		if _, err := f.ReadAt(buf, 0); err != nil {
			t.Fatalf("ReadAt after cycle %d: %v", cycle, err)
		}
		return buf
	}

	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock cycle 0: %v", err)
	}
	first := readBlock(0)

	if err := w.WriteBlock(0, fileops.IOTypeBuffered); err != nil {
		t.Fatalf("WriteBlock cycle 1: %v", err)
	}
	second := readBlock(1)

	if bytes.Equal(first, second) {
		t.Error("one Writer rewriting the same offset wrote byte-identical blocks at cycles 0 and 1; " +
			"a stale read of the first would verify clean against the second's header")
	}
}
