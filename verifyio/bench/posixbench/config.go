// Package posixbench implements a POSIX IO benchmark that writes deterministic
// data patterns to a directory and optionally reads them back for throughput
// measurement.
//
// A manifest (posixbench.json, or posixbench-{hostname}.json for multi-node
// runs) is written alongside the data files so a subsequent Verifier can
// regenerate the exact expected bytes for every block using (seed, fileIndex,
// blockIndex) — no out-of-band state is needed.
//
// Typical usage:
//
//	cfg := posixbench.DefaultConfig()
//	cfg.Path = "/mnt/beegfs/bench"
//	cfg.EnsureSeed()
//	r, _ := posixbench.NewRunner(cfg)
//	result, _ := r.Run(true)   // true = also run read phase
//
//	v, _ := posixbench.NewVerifier(cfg)
//	anomalies, _ := v.Verify(nil)
package posixbench

import (
	"fmt"
	mrand "math/rand/v2"
	"path/filepath"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// Layout controls how data files are distributed across workers.
type Layout string

const (
	// Layout1to1 assigns each worker its own set of files.
	Layout1to1 Layout = "1-to-1"
	// LayoutNto1 has all workers share one file, each writing a
	// non-overlapping region.
	LayoutNto1 Layout = "n-to-1"
)

// DefaultBlockSize is 4 MiB — a common large-IO transfer size.
const DefaultBlockSize = 4 * 1024 * 1024

// Config is a fully resolved benchmark configuration.
// Construct with DefaultConfig, override fields, then call EnsureSeed and
// Validate before passing to NewRunner or NewVerifier.
type Config struct {
	Path           string // target directory for data files and manifest
	Hostname       string // included in data filenames; allows multiple nodes to share one path
	Threads        int    // number of concurrent worker goroutines
	BlockSize      int    // IO transfer size in bytes (total on-disk footprint, including ECC stripes when NoXattr=true)
	FileSize       int64  // per-worker data volume in bytes
	FilesPerWorker int    // files per worker; ignored when Layout is n-to-1
	Layout         Layout
	Kind           block.Kind // data-region pattern
	Seed           uint64     // must be non-zero before Run or Verify
	// NoXattr is the throughput-benchmark mode. posixbench never calls
	// setxattr/getxattr in either mode -- it verifies purely through the
	// run-level manifest, regenerating each block's expected content from
	// its (seed, kind) and comparing byte-for-byte against what's on disk,
	// which catches misdirected, torn, or stale writes as well as bit-rot.
	// NoXattr trades that depth for speed: instead of the default mode's
	// full regenerate-and-compare, it writes per-stripe CRC32C checksums
	// (ECC) directly into the data files and verification is limited to
	// checking those in-file stripes for internal self-consistency, which
	// detects bit-rot / in-place corruption but NOT misdirected, torn, or
	// stale writes. BlockSize is embedded in the data filenames so the
	// verifier can locate the ECC region. Use the default mode when full
	// content verification is required.
	NoXattr bool
}

// DefaultConfig returns a Config with sensible defaults. Set Path before use.
func DefaultConfig() Config {
	return Config{
		Threads:        4,
		BlockSize:      DefaultBlockSize,
		FileSize:       1 * 1024 * 1024 * 1024,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
	}
}

// EnsureSeed fills Seed with a random value if it is zero. The chosen seed
// is written into the manifest so Verify can reconstruct expected data.
func (c *Config) EnsureSeed() {
	for c.Seed == 0 {
		c.Seed = mrand.Uint64()
	}
}

// Validate returns the first configuration error found.
func (c *Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("posixbench: path is required")
	}
	if c.Threads <= 0 {
		return fmt.Errorf("posixbench: threads must be > 0 (got %d)", c.Threads)
	}
	if c.BlockSize <= 0 {
		return fmt.Errorf("posixbench: blockSize must be > 0 (got %d)", c.BlockSize)
	}
	if c.FileSize <= 0 {
		return fmt.Errorf("posixbench: fileSize must be > 0 (got %d)", c.FileSize)
	}
	if c.FileSize%int64(c.BlockSize) != 0 {
		return fmt.Errorf("posixbench: fileSize (%d) must be a multiple of blockSize (%d)",
			c.FileSize, c.BlockSize)
	}
	if c.NoXattr {
		if _, err := block.BodyLen(c.BlockSize); err != nil {
			return fmt.Errorf("posixbench: --no-xattr requires a block-size that partitions evenly into body+ECC stripes: %w", err)
		}
	}
	switch c.Layout {
	case Layout1to1:
		if c.FilesPerWorker <= 0 {
			return fmt.Errorf("posixbench: filesPerWorker must be > 0 (got %d)", c.FilesPerWorker)
		}
	case LayoutNto1:
		// FilesPerWorker is unused; no additional constraint.
	default:
		return fmt.Errorf("posixbench: unknown layout %q (want 1-to-1 or n-to-1)", c.Layout)
	}
	return nil
}

// region describes a contiguous byte range within a single file that one
// worker owns for reading or writing.
type region struct {
	path      string // path to the data file
	fileIndex int    // stable identifier for this region, used by blockSeed
	startOff  int64  // first byte this worker reads or writes
	length    int64  // number of bytes in this region
	exclusive bool   // true when this worker owns the whole file (1-to-1)
}

// pbenchFilePrefix returns the filename prefix workerRegions and Run's
// n-to-1 preallocation step both derive shared/per-file names from -- kept
// in one place so the two can't drift out of sync (they did once: Run's
// preallocation omitted the "-bs<N>" suffix NoXattr adds here, so every
// worker opened a shared-file path that preallocate had never created).
func pbenchFilePrefix(cfg Config) string {
	prefix := "pbench"
	if cfg.Hostname != "" {
		prefix = "pbench-" + cfg.Hostname
	}
	if cfg.NoXattr {
		prefix = fmt.Sprintf("%s-bs%d", prefix, cfg.BlockSize)
	}
	return prefix
}

// workerRegions returns the regions assigned to each worker for cfg.
// Element [i] is the slice of regions that worker i owns.
func workerRegions(cfg Config) [][]region {
	out := make([][]region, cfg.Threads)
	prefix := pbenchFilePrefix(cfg)

	switch cfg.Layout {
	case LayoutNto1:
		shared := filepath.Join(cfg.Path, prefix+"-shared.dat")
		for w := 0; w < cfg.Threads; w++ {
			out[w] = []region{{
				path:      shared,
				fileIndex: w, // each worker's region has a distinct seed namespace
				startOff:  int64(w) * cfg.FileSize,
				length:    cfg.FileSize,
				exclusive: false, // shared file; pre-allocated by Runner
			}}
		}
	default: // Layout1to1
		for w := 0; w < cfg.Threads; w++ {
			regions := make([]region, cfg.FilesPerWorker)
			for f := 0; f < cfg.FilesPerWorker; f++ {
				fi := w*cfg.FilesPerWorker + f
				regions[f] = region{
					path: filepath.Join(cfg.Path,
						fmt.Sprintf("%s-w%03d-f%03d.dat", prefix, w, f)),
					fileIndex: fi,
					startOff:  0,
					length:    cfg.FileSize,
					exclusive: true,
				}
			}
			out[w] = regions
		}
	}
	return out
}

// blockSeed returns the deterministic seed for the block at (fileIndex,
// blockIndex within the region). Two calls with identical arguments always
// return the same value — the invariant that lets Verifier regenerate
// expected data without any side-channel storage.
//
// Assumption: fileIndex < 2^32 and blockIndex < 2^32. At DefaultBlockSize
// those limits imply files up to 16 TiB each, well beyond practical sizes.
func blockSeed(userSeed uint64, fileIndex int, blockIndex int64) uint64 {
	return userSeed ^ uint64(fileIndex)<<32 ^ uint64(blockIndex)
}
