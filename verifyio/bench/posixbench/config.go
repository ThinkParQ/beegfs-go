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
//	result, _ := r.Run(ctx, true)   // true = also run read phase
//
//	v, _ := posixbench.NewVerifier(cfg)
//	sweep, _ := v.Verify(nil)
package posixbench

import (
	"fmt"
	mrand "math/rand/v2"
	"path/filepath"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/fileops"

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

// Pattern is the order in which a worker visits the blocks of its region.
type Pattern string

const (
	PatternSequential Pattern = "sequential"
	PatternRandom     Pattern = "random"
)

// blockOrder returns the block indices for one region in visit order.
//
// Sequential returns them ascending and allocates nothing. Random returns a
// permutation derived from the run seed and the region, so a rerun with the
// same seed issues the same IO in the same order -- a performance result that
// cannot be reproduced is not much use for regression tracking.
func blockOrder(cfg Config, fileIndex int, blocks int64) []int64 {
	order := make([]int64, blocks)
	for i := range order {
		order[i] = int64(i)
	}
	if cfg.Pattern != PatternRandom {
		return order
	}
	// Derived from, but distinct from, the block seeds: mixing the region in
	// keeps two regions of one run from marching in lockstep.
	rng := mrand.New(mrand.NewPCG(cfg.Seed, uint64(fileIndex)+0x9E3779B97F4A7C15))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

// DefaultBlockSize is 4 MiB — a common large-IO transfer size.
const DefaultBlockSize = 4 * 1024 * 1024

// Largest block size for a single write()
const MaxBlockSize = 1 << 30 // 1 GiB

// MaxThreads and MaxRegions bound the two manifest fields that size an
// allocation: workerRegions allocates Threads * FilesPerWorker regions from
// values an untrusted manifest supplies.
const (
	MaxThreads = 4096
	MaxRegions = 1 << 18
)

// MaxBlocksPerRegion caps the block count Verify derives from FileSize. The cap
// is on the derived count rather than on FileSize, which means nothing without
// its block size.
const MaxBlocksPerRegion = 1 << 24

// maxHostnameLen bounds Config.Hostname, which is interpolated into every data
// filename. 253 is the maximum length of a DNS name.
const maxHostnameLen = 253

// validPathElement reports whether s is safe to interpolate into a path element.
//
// Guards both Hostname and RunID, which are the two operator- or
// caller-supplied strings woven into every data filename.
//
// Hostname reaches pbenchFilePrefix ("pbench-" + Hostname) and from there into
// filepath.Join, and it arrives from the same untrusted manifest as the bounded
// fields above -- but Validate never looked at it. filepath.Join does not
// contain the escape, because Join cleans the result: a hostname spelled
// "x/../../../../../../tmp/escaped" cancels the real path elements and leaves
// the bench directory entirely, resolving to /tmp/escaped-w000-f000.dat.
//
// In-tree the reach is read-only -- verify renders a verdict on files outside
// --path -- but the documented ReadManifest -> NewRunner re-run flow makes it a
// root O_CREATE|O_TRUNC at an attacker-chosen path.
//
// A conservative charset is the whole fix; rejecting '/' costs nothing
// legitimate. Note that the bare "../outside" spelling does NOT escape on its
// own, since "pbench-../outside" leaves "pbench-.." as a single literal path
// element -- a test asserting only that spelling passes while the bug remains.
func validPathElement(s string) bool {
	for _, r := range s {
		allowed := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if !allowed {
			return false
		}
	}
	return true
}

// Config is a fully resolved benchmark configuration.
// Construct with DefaultConfig, override fields, then call EnsureSeed and
// Validate before passing to NewRunner or NewVerifier.
type Config struct {
	Path     string // target directory for data files and manifest
	Hostname string // included in data filenames; allows multiple nodes to share one path
	// RunID identifies this run and is woven into every filename it produces,
	// so one directory can hold several runs without them overwriting each
	// other. EnsureRunID stamps it from the clock when unset; Verify reads it
	// back from the manifest, which is why it must round-trip exactly.
	//
	// For a multi-node workload it is the WORKLOAD id: set the same value on
	// every node and let Hostname separate them. See RunRecord for what a
	// collector can and cannot do with the results that produces.
	RunID          string
	Threads        int   // number of concurrent worker goroutines
	BlockSize      int   // IO transfer size in bytes
	FileSize       int64 // per-worker data volume in bytes
	FilesPerWorker int   // files per worker; ignored when Layout is n-to-1
	Layout         Layout
	Kind           block.Kind // data-region pattern
	Seed           uint64     // must be non-zero before Run or Verify
	// IOType selects how bytes reach the file. The zero value means buffered,
	// so a Config literal need not set it and a manifest written before the
	// field existed still reads.
	//
	// Only buffered is implemented; Validate rejects the rest BY NAME rather
	// than silently falling back, because a benchmark that quietly measured
	// something other than what was asked for is worse than one that refuses.
	IOType fileops.IOType
	// Pattern selects the order blocks are visited in; the zero value means
	// sequential. It changes nothing about what each block CONTAINS --
	// blockSeed keys on the block index, not on write order -- so verification
	// is identical for every pattern and needs no knowledge of which was used.
	Pattern Pattern
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
		IOType:         fileops.IOTypeBuffered,
		Pattern:        PatternSequential,
	}
}

// EnsureSeed fills Seed with a random value if it is zero. The chosen seed
// is written into the manifest so Verify can reconstruct expected data.
func (c *Config) EnsureSeed() {
	for c.Seed == 0 {
		c.Seed = mrand.Uint64()
	}
}

// RunIDLayout is the time format EnsureRunID stamps: sortable, filename-safe,
// and free of the colons RFC3339 would need quoting for in a shell.
const RunIDLayout = "20060102T150405Z"

// EnsureRunID sets RunID from the current UTC time if it is unset. Call once,
// before Validate, the same way EnsureSeed is called.
func (c *Config) EnsureRunID() {
	if c.RunID == "" {
		c.RunID = time.Now().UTC().Format(RunIDLayout)
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
	if c.Threads > MaxThreads {
		return fmt.Errorf("posixbench: threads %d exceeds MaxThreads (%d)", c.Threads, MaxThreads)
	}
	if c.BlockSize <= 0 {
		return fmt.Errorf("posixbench: blockSize must be > 0 (got %d)", c.BlockSize)
	}
	if c.BlockSize > MaxBlockSize {
		return fmt.Errorf("posixbench: blockSize %d exceeds MaxBlockSize (%d)", c.BlockSize, MaxBlockSize)
	}
	if c.FileSize <= 0 {
		return fmt.Errorf("posixbench: fileSize must be > 0 (got %d)", c.FileSize)
	}
	if c.FileSize%int64(c.BlockSize) != 0 {
		return fmt.Errorf("posixbench: fileSize (%d) must be a multiple of blockSize (%d)",
			c.FileSize, c.BlockSize)
	}
	if blocks := c.FileSize / int64(c.BlockSize); blocks > MaxBlocksPerRegion {
		return fmt.Errorf("posixbench: fileSize %d over blockSize %d implies %d blocks per region, "+
			"exceeding MaxBlocksPerRegion (%d)",
			c.FileSize, c.BlockSize, blocks, MaxBlocksPerRegion)
	}
	if len(c.Hostname) > maxHostnameLen {
		return fmt.Errorf("posixbench: hostname is %d bytes, exceeding %d",
			len(c.Hostname), maxHostnameLen)
	}
	if !validPathElement(c.Hostname) {
		return fmt.Errorf("posixbench: hostname %q contains characters not allowed in a filename; "+
			"it is interpolated into every data-file path, so only letters, digits, '-', '_' and '.' are accepted",
			c.Hostname)
	}
	if !validPathElement(c.RunID) {
		return fmt.Errorf("posixbench: runID %q contains characters not allowed in a filename; "+
			"it is interpolated into every data-file path, so only letters, digits, '-', '_' and '.' are accepted",
			c.RunID)
	}
	switch c.IOType {
	case 0, fileops.IOTypeBuffered:
	case fileops.IOTypeODirect, fileops.IOTypeMmap, fileops.IOTypePwritev:
		return fmt.Errorf("posixbench: ioType %s is not implemented yet: %w",
			c.IOType, fileops.ErrIOTypeNotSupported)
	default:
		return fmt.Errorf("posixbench: unknown ioType %d", c.IOType)
	}
	switch c.Pattern {
	case "", PatternSequential, PatternRandom:
	default:
		return fmt.Errorf("posixbench: unknown pattern %q (want %s or %s)",
			c.Pattern, PatternSequential, PatternRandom)
	}
	switch c.Layout {
	case Layout1to1:
		if c.FilesPerWorker <= 0 {
			return fmt.Errorf("posixbench: filesPerWorker must be > 0 (got %d)", c.FilesPerWorker)
		}
		// Expressed as a division so the product cannot overflow int on its way
		// to being compared. Threads is already bounded above and > 0 here, so
		// this can neither divide by zero nor wrap.
		if c.FilesPerWorker > MaxRegions/c.Threads {
			return fmt.Errorf("posixbench: threads*filesPerWorker (%d*%d) exceeds MaxRegions (%d)",
				c.Threads, c.FilesPerWorker, MaxRegions)
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

// pbenchFilePrefix returns the filename prefix workerRegions and Run's n-to-1
// preallocation step both derive shared/per-file names from -- kept in one
// place so the two cannot drift out of sync, which they have done before.
func pbenchFilePrefix(cfg Config) string {
	prefix := "pbench"
	if cfg.RunID != "" {
		prefix += "-" + cfg.RunID
	}
	if cfg.Hostname != "" {
		prefix += "-" + cfg.Hostname
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
// Assumes fileIndex and blockIndex both fit in 32 bits, which the configured
// bounds keep them well inside.
func blockSeed(userSeed uint64, fileIndex int, blockIndex int64) uint64 {
	return userSeed ^ uint64(fileIndex)<<32 ^ uint64(blockIndex)
}

// ioTypeOrDefault resolves the zero value to what it means. Kept in one place
// so the "zero means buffered" rule cannot be honoured in some paths and not
// others -- the manifest recorded "iotype(0)" until it existed.
func (c Config) ioTypeOrDefault() fileops.IOType {
	if c.IOType == 0 {
		return fileops.IOTypeBuffered
	}
	return c.IOType
}
