//go:build linux

package main

import (
	"context"
	"math/rand"
)

// opRunner executes random operations against a pool of file handles.
// It is not safe for concurrent use; create one per goroutine.
type opRunner struct {
	handles   []*fileHandle
	ops       []Op
	rng       *rand.Rand
	nBlocks   int64 // number of addressable blocks per file
	blockSize int
}

func newOpRunner(handles []*fileHandle, ops []Op, seed int64, nBlocks int64, blockSize int) *opRunner {
	return &opRunner{
		handles:   handles,
		ops:       ops,
		rng:       rand.New(rand.NewSource(seed)),
		nBlocks:   nBlocks,
		blockSize: blockSize,
	}
}

// runOnce picks a random (file, op, offset) triple and executes the op.
// A non-nil return is an anomaly; the caller should cancel the run.
func (r *opRunner) runOnce(ctx context.Context) error {
	h := r.handles[r.rng.Intn(len(r.handles))]
	op := r.ops[r.rng.Intn(len(r.ops))]
	offset := r.rng.Int63n(r.nBlocks) * int64(r.blockSize)
	return op(ctx, h, offset)
}
