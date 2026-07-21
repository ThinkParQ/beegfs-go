package posixbench

import (
	"math"
	"time"
)

// WorkerResult holds the outcome of a single worker goroutine.
type WorkerResult struct {
	WorkerID     int
	BytesWritten int64
	WriteElapsed time.Duration
	WriteErr     error
	BytesRead    int64
	ReadElapsed  time.Duration
	ReadErr      error
}

// workerMBps returns the worker's bandwidth in MB/s (SI: 1 MB = 10^6 bytes)
// for the given bytes and elapsed time, or 0 if elapsed is non-positive.
func workerMBps(bytes int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / 1_000_000 / elapsed.Seconds()
}

// BandwidthStats summarises per-worker bandwidth across all workers.
type BandwidthStats struct {
	MinMBps    float64
	MaxMBps    float64
	MeanMBps   float64
	StdDevMBps float64
}

// Result aggregates outcomes from all workers for a Run.
type Result struct {
	Workers      []WorkerResult
	TotalWritten int64
	WriteElapsed time.Duration // wall-clock time for the whole write phase
	TotalRead    int64
	ReadElapsed  time.Duration // wall-clock time for the read phase; zero if skipped
}

// WriteMBps returns aggregate write bandwidth in MB/s (SI: 1 MB = 10^6 bytes).
func (r Result) WriteMBps() float64 {
	if r.WriteElapsed <= 0 {
		return 0
	}
	return float64(r.TotalWritten) / 1_000_000 / r.WriteElapsed.Seconds()
}

// ReadMBps returns aggregate read bandwidth in MB/s (SI: 1 MB = 10^6 bytes).
// Returns 0 if no read phase was run.
func (r Result) ReadMBps() float64 {
	if r.ReadElapsed <= 0 {
		return 0
	}
	return float64(r.TotalRead) / 1_000_000 / r.ReadElapsed.Seconds()
}

// WriteStats returns per-worker write bandwidth statistics.
func (r Result) WriteStats() BandwidthStats {
	return bandwidthStats(r.Workers, func(w WorkerResult) (int64, time.Duration) {
		return w.BytesWritten, w.WriteElapsed
	})
}

// ReadStats returns per-worker read bandwidth statistics. Returns a zero
// BandwidthStats if no read phase was run.
func (r Result) ReadStats() BandwidthStats {
	return bandwidthStats(r.Workers, func(w WorkerResult) (int64, time.Duration) {
		return w.BytesRead, w.ReadElapsed
	})
}

func bandwidthStats(workers []WorkerResult, field func(WorkerResult) (int64, time.Duration)) BandwidthStats {
	if len(workers) == 0 {
		return BandwidthStats{}
	}
	var vals []float64
	for _, w := range workers {
		bytes, elapsed := field(w)
		if elapsed > 0 {
			vals = append(vals, workerMBps(bytes, elapsed))
		}
	}
	if len(vals) == 0 {
		return BandwidthStats{}
	}
	min, max, sum := vals[0], vals[0], 0.0
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	mean := sum / float64(len(vals))
	var variance float64
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	variance /= float64(len(vals))
	return BandwidthStats{
		MinMBps:    min,
		MaxMBps:    max,
		MeanMBps:   mean,
		StdDevMBps: math.Sqrt(variance),
	}
}

// Anomaly describes a single verification failure.
type Anomaly struct {
	File       string // path of the affected file
	FileIndex  int    // region index passed to blockSeed
	BlockIndex int64  // block index within the region
	Offset     int64  // byte offset within the file
	Err        error
}
