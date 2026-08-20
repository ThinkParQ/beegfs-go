package posixbench

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// Runner executes a posixbench write (and optionally read) workload.
type Runner struct {
	cfg Config
}

// NewRunner creates a Runner from cfg. cfg must pass Validate and have a
// non-zero Seed before this call.
func NewRunner(cfg Config) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Seed == 0 {
		return nil, fmt.Errorf("posixbench.NewRunner: seed is zero; call cfg.EnsureSeed() first")
	}
	return &Runner{cfg: cfg}, nil
}

// Run executes the write phase and, if doRead is true, a sequential read
// phase for bandwidth measurement. The manifest is written before IO begins
// so Verify can always find the run parameters.
//
// If any worker encounters a fatal IO error, Run returns the first such error
// alongside a partial Result (bytes written/read up to that point are still
// reported).
func (r *Runner) Run(doRead bool) (Result, error) {
	if err := os.MkdirAll(r.cfg.Path, 0755); err != nil {
		return Result{}, fmt.Errorf("posixbench.Run: mkdir %s: %w", r.cfg.Path, err)
	}
	if err := writeManifest(r.cfg.Path, configToManifest(r.cfg)); err != nil {
		return Result{}, fmt.Errorf("posixbench.Run: %w", err)
	}

	// For n-to-1, the shared file must exist at full size before workers
	// begin writing to arbitrary offsets within it.
	if r.cfg.Layout == LayoutNto1 {
		sharedPath := filepath.Join(r.cfg.Path, pbenchFilePrefix(r.cfg)+"-shared.dat")
		totalSize := int64(r.cfg.Threads) * r.cfg.FileSize
		if err := preallocate(sharedPath, totalSize); err != nil {
			return Result{}, fmt.Errorf("posixbench.Run: preallocate: %w", err)
		}
	}

	assignments := workerRegions(r.cfg)
	workers := make([]WorkerResult, r.cfg.Threads)

	// Write phase.
	var wg sync.WaitGroup
	writeStart := time.Now()
	for i := 0; i < r.cfg.Threads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workers[id].WorkerID = id
			t := time.Now()
			workers[id].BytesWritten, workers[id].WriteErr = writeRegions(r.cfg, assignments[id])
			workers[id].WriteElapsed = time.Since(t)
		}(i)
	}
	wg.Wait()

	res := Result{Workers: workers, WriteElapsed: time.Since(writeStart)}
	for i := range workers {
		res.TotalWritten += workers[i].BytesWritten
	}
	for i := range workers {
		if workers[i].WriteErr != nil {
			return res, fmt.Errorf("posixbench.Run: worker %d: %w", i, workers[i].WriteErr)
		}
	}

	if !doRead {
		return res, nil
	}

	// Read phase (bandwidth measurement only; no verification).
	readStart := time.Now()
	for i := 0; i < r.cfg.Threads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			t := time.Now()
			workers[id].BytesRead, workers[id].ReadErr = readRegions(r.cfg, assignments[id])
			workers[id].ReadElapsed = time.Since(t)
		}(i)
	}
	wg.Wait()

	res.ReadElapsed = time.Since(readStart)
	for i := range workers {
		res.TotalRead += workers[i].BytesRead
	}
	for i := range workers {
		if workers[i].ReadErr != nil {
			return res, fmt.Errorf("posixbench.Run: read worker %d: %w", i, workers[i].ReadErr)
		}
	}

	return res, nil
}

// preallocate creates (or truncates) the file at path to exactly size bytes.
// On Linux this produces a sparse file; actual disk blocks are allocated on
// first write.
func preallocate(path string, size int64) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	return f.Truncate(size)
}

// writeRegions iterates over all regions assigned to one worker and returns
// the total bytes written.
func writeRegions(cfg Config, regions []region) (int64, error) {
	buf := make([]byte, cfg.BlockSize)
	var total int64
	for _, reg := range regions {
		n, err := writeRegion(cfg, reg, buf)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func writeRegion(cfg Config, reg region, buf []byte) (written int64, err error) {
	var flags int
	if reg.exclusive {
		flags = os.O_CREATE | os.O_RDWR | os.O_TRUNC
	} else {
		flags = os.O_RDWR
	}
	f, err := os.OpenFile(reg.path, flags, 0644)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", reg.path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	var bodyLen int
	if cfg.NoXattr {
		bodyLen, err = block.BodyLen(cfg.BlockSize)
		if err != nil {
			return 0, fmt.Errorf("block size %d: %w", cfg.BlockSize, err)
		}
	}

	blocks := reg.length / int64(cfg.BlockSize)
	for b := int64(0); b < blocks; b++ {
		seed := blockSeed(cfg.Seed, reg.fileIndex, b)
		off := reg.startOff + b*int64(cfg.BlockSize)
		if cfg.NoXattr {
			var hdr block.Header
			if err := block.MakeBlock(buf, &hdr, cfg.Kind, seed, bodyLen); err != nil {
				return written, fmt.Errorf("generate ECC block %d in %s: %w", b, reg.path, err)
			}
		} else {
			if err := block.GenerateBody(cfg.Kind, seed, buf); err != nil {
				return written, fmt.Errorf("generate block %d in %s: %w", b, reg.path, err)
			}
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			return written, fmt.Errorf("write block %d in %s: %w", b, reg.path, err)
		}
		written += int64(cfg.BlockSize)
	}
	return written, nil
}

// readRegions reads all blocks in the assigned regions for bandwidth
// measurement. Data is not verified here; use Verifier for that.
func readRegions(cfg Config, regions []region) (int64, error) {
	buf := make([]byte, cfg.BlockSize)
	var total int64
	for _, reg := range regions {
		f, err := os.Open(reg.path)
		if err != nil {
			return total, fmt.Errorf("open %s for read: %w", reg.path, err)
		}
		blocks := reg.length / int64(cfg.BlockSize)
		for b := int64(0); b < blocks; b++ {
			off := reg.startOff + b*int64(cfg.BlockSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				f.Close()
				return total, fmt.Errorf("read block %d in %s: %w", b, reg.path, err)
			}
			total += int64(cfg.BlockSize)
		}
		f.Close()
	}
	return total, nil
}
