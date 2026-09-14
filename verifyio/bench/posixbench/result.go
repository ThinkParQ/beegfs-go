package posixbench

import (
	"math"
	"time"
)

// WorkerResult holds the outcome of a single worker goroutine.
type WorkerResult struct {
	WorkerID     int
	BytesWritten int64
	// WriteElapsed is the wall-clock time for this worker's whole write
	// phase: open, pattern generation, WriteAt, fsync, and close. It is NOT
	// storage throughput on its own -- see GenerateElapsed and
	// WriteIOElapsed.
	WriteElapsed time.Duration
	// GenerateElapsed is the portion of WriteElapsed spent regenerating each
	// block's pattern (block.GenerateBody / block.MakeBlock), not writing it.
	// Pattern generation is CPU-bound work this tool does, not the target
	// filesystem's work, and it is not always cheap relative to storage.
	// Without separating it out, the reported write rate silently caps at
	// whichever is slower of the storage and the generator for the chosen
	// -kind.
	GenerateElapsed time.Duration
	WriteErr        error
	BytesRead       int64
	ReadElapsed     time.Duration
	ReadErr         error
}

// WriteIOElapsed returns WriteElapsed with GenerateElapsed subtracted out --
// the portion of this worker's write phase spent actually writing rather than
// regenerating pattern data. Use this, not WriteElapsed, for a throughput
// figure that reflects storage rather than the pattern generator. Clamped to
// zero: generation and IO are timed as two separate windows on the same
// goroutine so the subtraction should never go negative, but a rate computed
// from a negative duration would be nonsense, not merely wrong.
func (w WorkerResult) WriteIOElapsed() time.Duration {
	d := w.WriteElapsed - w.GenerateElapsed
	if d < 0 {
		return 0
	}
	return d
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
	MinMBps    float64 `json:"min"`
	MaxMBps    float64 `json:"max"`
	MeanMBps   float64 `json:"mean"`
	StdDevMBps float64 `json:"stdDev"`
}

// Result aggregates outcomes from all workers for a Run.
type Result struct {
	Workers      []WorkerResult
	TotalWritten int64
	// WriteElapsed is the wall-clock time for the whole write phase,
	// including pattern generation -- see WriteIOElapsed for the figure that
	// isolates storage throughput.
	WriteElapsed time.Duration
	// WriteIOElapsed is the slowest worker's WorkerResult.WriteIOElapsed --
	// the write phase's duration with pattern generation excluded. WriteMBps
	// is computed from this, not WriteElapsed, so the reported rate reflects
	// the storage rather than the pattern generator. Taking the worker max
	// (rather than, say, summing and subtracting from the phase-level
	// WriteElapsed) is the dimensionally correct generalization: workers run
	// concurrently, so the phase as a whole finishes only once its slowest
	// worker does, same as WriteElapsed itself.
	WriteIOElapsed time.Duration
	// GenerateElapsed is the slowest worker's WorkerResult.GenerateElapsed --
	// how much of the write phase went to pattern generation rather than IO.
	// A large fraction of WriteElapsed here means WriteMBps would have been
	// generator-bound, not storage-bound, without the WriteIOElapsed split.
	GenerateElapsed time.Duration
	TotalRead       int64
	ReadElapsed     time.Duration // wall-clock time for the read phase; zero if skipped
	// WriteLatency and ReadLatency are per-OPERATION timings, merged from the
	// per-worker histograms. They answer a question bandwidth cannot: a phase
	// with a good aggregate rate and a bad tail is the shape most storage
	// complaints actually take.
	WriteLatency Latency
	ReadLatency  Latency
}

// WriteMBps returns aggregate write bandwidth in MB/s (SI: 1 MB = 10^6 bytes),
// computed from WriteIOElapsed so pattern-generation time doesn't inflate the
// apparent throughput ceiling or deflate the reported rate below what the
// storage actually sustained.
func (r Result) WriteMBps() float64 {
	if r.WriteIOElapsed <= 0 {
		return 0
	}
	return float64(r.TotalWritten) / 1_000_000 / r.WriteIOElapsed.Seconds()
}

// ReadMBps returns aggregate read bandwidth in MB/s (SI: 1 MB = 10^6 bytes).
// Returns 0 if no read phase was run.
//
// This is not necessarily a measurement of storage. The read phase tries to
// evict the client's cached pages first (see dropCache), but that is best
// effort and does nothing under BeeGFS's default cache mode -- and it can never
// reach the STORAGE SERVERS' page cache, so a read that misses on the client
// may still be served from server RAM. Comparing two runs is sound; quoting
// this as device throughput is not, unless the run used O_DIRECT.
func (r Result) ReadMBps() float64 {
	if r.ReadElapsed <= 0 {
		return 0
	}
	return float64(r.TotalRead) / 1_000_000 / r.ReadElapsed.Seconds()
}

// WriteStats returns per-worker write bandwidth statistics.
func (r Result) WriteStats() BandwidthStats {
	return bandwidthStats(r.Workers, func(w WorkerResult) (int64, time.Duration) {
		return w.BytesWritten, w.WriteIOElapsed()
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
	File      string // path of the affected file
	FileIndex int    // region index passed to blockSeed
	// BlockIndex is the block within the region, or -1 when the anomaly is
	// about the whole file rather than one block: a data file that is not
	// there at all has no block to name.
	BlockIndex int64
	Offset     int64 // byte offset within the file
	Err        error
}
