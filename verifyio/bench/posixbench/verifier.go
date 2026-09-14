package posixbench

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// Verifier checks data files written by Runner against the expected pattern
// derived from the manifest seed.
type Verifier struct {
	cfg Config
}

// NewVerifier creates a Verifier from cfg. cfg is typically obtained from
// ReadManifest. cfg must pass Validate and have a non-zero Seed.
func NewVerifier(cfg Config) (*Verifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Seed == 0 {
		return nil, fmt.Errorf("posixbench.NewVerifier: seed is zero")
	}
	return &Verifier{cfg: cfg}, nil
}

// Sweep is what one Verify pass examined, and what it found.
//
// The two are reported together deliberately. An anomaly count on its own
// cannot distinguish a clean sweep from one that examined nothing, so "0
// anomalies" reads as a verdict on the data when it may be a verdict on
// nothing at all -- the false PASS this pipeline exists to prevent. A caller
// that renders a verdict needs both halves, and Manifest.ExpectedBlocks says
// what Blocks should be.
type Sweep struct {
	// Anomalies is the number of findings reported through fn.
	Anomalies int
	// Regions is the number of regions opened and walked. A region whose data
	// file is missing is NOT counted here -- it contributed nothing -- and is
	// reported as an anomaly instead.
	Regions int
	// Blocks is the number of blocks read and compared byte-for-byte. A block
	// that could not be read (short file) is not counted; it is an anomaly.
	Blocks int64
}

// Verify reads every block in every region and checks it for integrity.
//
// Each block is compared byte-for-byte against the pattern regenerated from
// (seed, fileIndex, blockIndex).
//
// fn is called once per anomaly; if fn returns a non-nil error Verify stops
// and returns that error. Pass nil to count anomalies without inspecting them.
//
// Returns what the sweep covered and found, plus any error from fn or IO. The
// coverage half matters as much as the anomaly count: see Sweep.
func (v *Verifier) Verify(fn func(Anomaly) error) (Sweep, error) {
	if fn == nil {
		fn = func(Anomaly) error { return nil }
	}

	// Flatten all per-worker region assignments into a single list.
	// For n-to-1 this produces one region per worker, each with a distinct
	// fileIndex and startOff into the shared file.
	var regions []region
	for _, workerRegs := range workerRegions(v.cfg) {
		regions = append(regions, workerRegs...)
	}

	buf := make([]byte, v.cfg.BlockSize)
	var sweep Sweep

	expected := make([]byte, v.cfg.BlockSize)

	for _, reg := range regions {
		f, err := os.Open(reg.path)
		if err != nil {
			// Data that is MISSING is an anomaly and the sweep continues; any
			// other open failure is the tool being unable to do its job, and
			// still aborts.
			//
			// The first half mirrors the short-read arm below, which counts an
			// EOF and keeps going for exactly this reason: one bad file must
			// not mask corruption in the files after it. The second half is why
			// this is not simply "count every open failure" -- an EACCES, an
			// ELOOP from a symlink planted at the name, or an EIO from a wedged
			// mount would then be reported as "the data is wrong", blaming the
			// filesystem for bytes we never managed to read. cmdVerify's
			// mid-sweep error return draws the same line for the same reason.
			//
			// One anomaly per absent file rather than one per block it should
			// have held: its absence is a single fact, and Err names it as the
			// whole file.
			if errors.Is(err, os.ErrNotExist) {
				sweep.Anomalies++
				if e := fn(Anomaly{
					File: reg.path, FileIndex: reg.fileIndex, BlockIndex: -1, Offset: reg.startOff,
					Err: fmt.Errorf("missing data file"),
				}); e != nil {
					return sweep, e
				}
				continue
			}
			// No "open %s" here: err is already a *PathError naming the path.
			return sweep, fmt.Errorf("posixbench.Verify: %w", err)
		}

		sweep.Regions++
		blocks := reg.length / int64(v.cfg.BlockSize)
		for b := int64(0); b < blocks; b++ {
			seed := blockSeed(v.cfg.Seed, reg.fileIndex, b)
			if err := block.GenerateBody(v.cfg.Kind, seed, expected); err != nil {
				f.Close()
				return sweep, fmt.Errorf("posixbench.Verify: generate: %w", err)
			}
			off := reg.startOff + b*int64(v.cfg.BlockSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					// A short read means the block is truncated or missing. Count
					// it as an anomaly and keep going rather than aborting the run,
					// so one bad file can't mask corruption in later files.
					sweep.Anomalies++
					if e := fn(Anomaly{
						File: reg.path, FileIndex: reg.fileIndex, BlockIndex: b, Offset: off,
						Err: fmt.Errorf("short read (truncated/missing block): %w", err),
					}); e != nil {
						f.Close()
						return sweep, e
					}
					continue
				}
				f.Close()
				return sweep, fmt.Errorf("posixbench.Verify: read %s at %d: %w",
					reg.path, off, err)
			}
			sweep.Blocks++
			if !bytes.Equal(expected, buf) {
				sweep.Anomalies++
				if err := fn(Anomaly{
					File:       reg.path,
					FileIndex:  reg.fileIndex,
					BlockIndex: b,
					Offset:     off,
					Err:        fmt.Errorf("data mismatch"),
				}); err != nil {
					f.Close()
					return sweep, err
				}
			}
		}
		f.Close()
	}
	return sweep, nil
}
