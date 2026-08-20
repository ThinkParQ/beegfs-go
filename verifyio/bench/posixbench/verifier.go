package posixbench

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// eccTable is the CRC32C polynomial used for ECC stripe verification.
var eccTable = crc32.MakeTable(crc32.Castagnoli)

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

// Verify reads every block in every region and checks it for integrity.
//
// When cfg.NoXattr is false (the default), each block is compared byte-for-byte
// against the pattern regenerated from (seed, fileIndex, blockIndex).
//
// When cfg.NoXattr is true, each block is verified using the per-stripe CRC32C
// checksums embedded in the data file (ECC-only; no body regeneration).
//
// fn is called once per anomaly; if fn returns a non-nil error Verify stops
// and returns that error. Pass nil to count anomalies without inspecting them.
//
// Returns the total number of anomalies found and any error from fn or IO.
func (v *Verifier) Verify(fn func(Anomaly) error) (int, error) {
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
	anomalies := 0

	if v.cfg.NoXattr {
		return v.verifyECC(regions, buf, fn)
	}

	expected := make([]byte, v.cfg.BlockSize)

	for _, reg := range regions {
		f, err := os.Open(reg.path)
		if err != nil {
			return anomalies, fmt.Errorf("posixbench.Verify: open %s: %w", reg.path, err)
		}

		blocks := reg.length / int64(v.cfg.BlockSize)
		for b := int64(0); b < blocks; b++ {
			seed := blockSeed(v.cfg.Seed, reg.fileIndex, b)
			if err := block.GenerateBody(v.cfg.Kind, seed, expected); err != nil {
				f.Close()
				return anomalies, fmt.Errorf("posixbench.Verify: generate: %w", err)
			}
			off := reg.startOff + b*int64(v.cfg.BlockSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					// A short read means the block is truncated or missing. Count
					// it as an anomaly and keep going rather than aborting the run,
					// so one bad file can't mask corruption in later files.
					anomalies++
					if e := fn(Anomaly{
						File: reg.path, FileIndex: reg.fileIndex, BlockIndex: b, Offset: off,
						Err: fmt.Errorf("short read (truncated/missing block): %w", err),
					}); e != nil {
						f.Close()
						return anomalies, e
					}
					continue
				}
				f.Close()
				return anomalies, fmt.Errorf("posixbench.Verify: read %s at %d: %w",
					reg.path, off, err)
			}
			if !bytes.Equal(expected, buf) {
				anomalies++
				if err := fn(Anomaly{
					File:       reg.path,
					FileIndex:  reg.fileIndex,
					BlockIndex: b,
					Offset:     off,
					Err:        fmt.Errorf("data mismatch"),
				}); err != nil {
					f.Close()
					return anomalies, err
				}
			}
		}
		f.Close()
	}
	return anomalies, nil
}

// verifyECC checks per-stripe CRC32C checksums for all blocks in regions.
// Each block is laid out as [body: bodyLen bytes][stripe CRCs: numStripes*4 bytes]
// where the split point is derived from cfg.BlockSize.
//
// This is the throughput-benchmark (NoXattr) verification path: the in-file CRC
// stripes detect bit-rot / in-place corruption only — not misdirected, torn, or
// stale writes. That is by design; --no-xattr trades verification depth for write
// speed. Use the default (xattr) mode for full content verification.
func (v *Verifier) verifyECC(regions []region, buf []byte, fn func(Anomaly) error) (int, error) {
	bodyLen, err := block.BodyLen(v.cfg.BlockSize)
	if err != nil {
		return 0, fmt.Errorf("posixbench.Verify: %w", err)
	}
	numStripes := block.NumStripes(bodyLen)
	anomalies := 0

	for _, reg := range regions {
		f, err := os.Open(reg.path)
		if err != nil {
			return anomalies, fmt.Errorf("posixbench.Verify: open %s: %w", reg.path, err)
		}

		blocks := reg.length / int64(v.cfg.BlockSize)
		for b := int64(0); b < blocks; b++ {
			off := reg.startOff + b*int64(v.cfg.BlockSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					// Truncated/missing block: count and continue (see default path).
					anomalies++
					if e := fn(Anomaly{
						File: reg.path, FileIndex: reg.fileIndex, BlockIndex: b, Offset: off,
						Err: fmt.Errorf("short read (truncated/missing block): %w", err),
					}); e != nil {
						f.Close()
						return anomalies, e
					}
					continue
				}
				f.Close()
				return anomalies, fmt.Errorf("posixbench.Verify: read %s at %d: %w",
					reg.path, off, err)
			}
			body := buf[:bodyLen]
			crcSlice := buf[bodyLen : bodyLen+numStripes*4]
			for s := range numStripes {
				start := s * block.StripeSize
				end := start + block.StripeSize
				if end > bodyLen {
					end = bodyLen
				}
				want := binary.LittleEndian.Uint32(crcSlice[s*4 : (s+1)*4])
				got := crc32.Checksum(body[start:end], eccTable)
				if want != got {
					anomalies++
					if err := fn(Anomaly{
						File:       reg.path,
						FileIndex:  reg.fileIndex,
						BlockIndex: b,
						Offset:     off,
						Err:        fmt.Errorf("stripe %d CRC mismatch", s),
					}); err != nil {
						f.Close()
						return anomalies, err
					}
					break // one anomaly per block is enough
				}
			}
		}
		f.Close()
	}
	return anomalies, nil
}
