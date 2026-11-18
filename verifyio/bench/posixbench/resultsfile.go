package posixbench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// resultsVersion is the schema version of the results file. It is deliberately
// independent of manifestVersion: the manifest describes data that must be
// re-derivable byte-for-byte, so a bump there means "this build cannot verify
// that directory". A results bump means only "this record has a different
// shape", which never invalidates anything.
const resultsVersion = 1

// EnvUnknown is what every Environment field holds unless a caller set it.
// A record always carries the keys, so a reader can tell "we did not capture
// this" from "this run had no such thing".
const EnvUnknown = "unknown"

// Environment records what the run ran against, so a number in a historical
// series is interpretable a year later.
//
// The library populates NONE of this and never will: what is discoverable
// differs per deployment, and a wrong auto-detected value is worse than an
// honest "unknown". Capture is the calling tool's job; this type is the slot.
type Environment struct {
	ToolVersion   string `json:"toolVersion"`
	BeeGFSVersion string `json:"beegfsVersion"`
	Kernel        string `json:"kernel"`
	MountOptions  string `json:"mountOptions"`
	// External is operator-supplied context passed straight through -- a ticket
	// id, a cluster name, the change under test.
	External string `json:"external,omitempty"`
	// Extra carries anything a future capture step learns that has no field
	// here yet, so adding one is not a schema change.
	Extra map[string]string `json:"extra,omitempty"`
	// ClientConfig is the target filesystem client's effective settings.
	//
	// Stored as key/value rather than as text so comparing two runs is a
	// field-by-field diff instead of a diff over formatting. That is the point:
	// answering "did option A beat option B" a year from now needs the setting
	// you did not think to record today, so capture everything and let the
	// reader filter.
	//
	// Empty unless the caller supplied it; the library reads nothing. On BeeGFS
	// the source worth capturing is /proc/fs/beegfs/<client>/config -- the
	// EFFECTIVE runtime config, since mount options override the file on disk
	// and one node can hold several mounts with different settings.
	ClientConfig map[string]string `json:"clientConfig,omitempty"`
}

// LoadEnvKV reads a "key = value" file into a map, the shape both
// /proc/fs/beegfs/<client>/config and an ordinary conf file use.
//
// Blank lines and # comments are skipped; whitespace around the key and value
// is trimmed; a line with no "=" is skipped rather than failing, so pointing
// this at a file with a banner or a trailing note still works. A repeated key
// takes the last value, matching how the config files themselves are read.
func LoadEnvKV(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("posixbench.LoadEnvKV: %w", err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

// DefaultEnvironment returns an Environment with every known field set to
// EnvUnknown. Start here and fill what you can.
func DefaultEnvironment() Environment {
	return Environment{
		ToolVersion:   EnvUnknown,
		BeeGFSVersion: EnvUnknown,
		Kernel:        EnvUnknown,
		MountOptions:  EnvUnknown,
	}
}

// RunRecord is the persisted record of a completed run: everything that
// described it, plus the numbers it produced.
//
// Self-contained on purpose. The intended use is copying these out of the bench
// directory into a historical archive -- a weekly regression series, say -- and
// neither the data files nor the manifest follow, so the record embeds the
// whole Manifest rather than pointing at it. A record read a year later must be
// interpretable with nothing else on hand.
//
// Written only when a run completes, so a failed or interrupted run usually
// leaves no record. Do not read the converse: the record and the manifest's
// FinishedAt are two separate writes and cannot be made atomic, so a results
// write that fails leaves FinishedAt stamped with no record beside it. See
// Run's finish closure, which writes the manifest first.
//
// # Collecting across nodes
//
// The run ID is the WORKLOAD id, not a per-node one. Give every node in one
// workload the same Config.RunID and let Hostname separate them; records then
// fan in on (RunID, Hostname), which is also how they are named on disk. A
// harness that lets each node stamp its own RunID gets records it cannot group,
// and nothing here will stop it.
//
// Because each record embeds its whole Manifest, a collector can check that
// every node ran the SAME configuration before aggregating. Do that: summing
// throughput over nodes that used different block sizes produces a number with
// no meaning.
//
// A node that was expected and left no record needs looking at -- a signal
// rather than a gap, though only the harness knows the roster. Check its
// manifest before concluding the run died: FinishedAt is the authority on
// whether it finished, and a stamped manifest with no record beside it means
// the run completed and only the record write failed.
//
// # Which figure to aggregate
//
// These answer different questions and can differ by a lot on short runs, so
// pick deliberately; the record carries what both need.
//
//   - Summing MBPerSecond across nodes answers "what did each node's storage
//     sustain". It divides by IOElapsedSeconds, which excludes pattern
//     generation and counts only the slowest worker.
//   - Total Bytes over the wall-clock span from the earliest StartedAt to the
//     latest FinishedAt answers "what did the cluster push end to end". It
//     includes process startup, manifest writes, fsync and teardown.
//
// StartedAt and FinishedAt come from each node's own clock, so any span
// computed across nodes assumes they agree. Nothing here can detect skew.
//
// Bytes and seconds are the authoritative fields. The MB/s figures are derived
// (SI: 1 MB = 10^6 bytes) and carried anyway so a series is directly comparable
// without every consumer reimplementing the arithmetic -- in particular the
// write rate excludes pattern-generation time, which a naive
// bytes/wall-clock would not.
type RunRecord struct {
	Version     int         `json:"version"`
	Manifest    Manifest    `json:"manifest"`
	Environment Environment `json:"environment"`
	Write       RecordPhase `json:"write"`
	// Read is absent unless the run included a read phase.
	Read    *RecordPhase   `json:"read,omitempty"`
	Workers []RecordWorker `json:"workers"`
}

// RecordPhase is one phase's aggregate numbers as persisted. Distinct from
// Result, which is the in-memory shape.
type RecordPhase struct {
	Bytes int64 `json:"bytes"`
	// ElapsedSeconds is wall-clock for the phase. For the write phase this
	// INCLUDES pattern generation, so it is not the throughput denominator.
	ElapsedSeconds float64 `json:"elapsedSeconds"`
	// IOElapsedSeconds is ElapsedSeconds with generation excluded, and is what
	// MBPerSecond divides by. Equal to ElapsedSeconds for the read phase.
	IOElapsedSeconds float64 `json:"ioElapsedSeconds"`
	// GenerateSeconds is the slowest worker's pattern-generation time. A large
	// fraction of ElapsedSeconds means the run was generator-bound, and the
	// storage figure below is the one to trust.
	GenerateSeconds float64        `json:"generateSeconds"`
	MBPerSecond     float64        `json:"mbPerSecond"`
	PerWorker       BandwidthStats `json:"perWorkerMBPerSecond"`
	// Latency is per-operation, in milliseconds. Bandwidth and latency answer
	// different questions and a phase can look healthy in one and not the
	// other, which is the shape most storage complaints take.
	Latency RecordLatency `json:"latencyMs"`
}

// RecordLatency is one phase's per-operation timing, in milliseconds.
//
// Count, Min, Max and Mean are exact. P50/P95/P99 come from a log-scale
// histogram and read HIGH by at most one sub-bucket's width -- see Latency.
// They are for comparing
// runs to each other, not for quoting an absolute tail.
type RecordLatency struct {
	Count uint64  `json:"count"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func newRecordLatency(l *Latency) RecordLatency {
	r := RecordLatency{Count: l.count}
	if l.count == 0 {
		return r
	}
	r.Min = ms(time.Duration(l.min))
	r.Max = ms(time.Duration(l.max))
	r.Mean = ms(time.Duration(l.sum / l.count))
	r.P50 = ms(l.quantile(0.50))
	r.P95 = ms(l.quantile(0.95))
	r.P99 = ms(l.quantile(0.99))
	return r
}

// RecordWorker is one worker's contribution as persisted, kept so a straggler
// is visible rather than averaged away. Distinct from WorkerResult, which is
// the in-memory shape.
//
// No omitempty on the numeric fields, deliberately: it would make a measured
// zero indistinguishable from one never populated, and a measured zero here
// means something went wrong and is worth seeing. Absence and zero must not
// collide in a record meant to be read a year later.
type RecordWorker struct {
	WorkerID        int     `json:"workerID"`
	BytesWritten    int64   `json:"bytesWritten"`
	WriteSeconds    float64 `json:"writeSeconds"`
	WriteIOSeconds  float64 `json:"writeIOSeconds"`
	GenerateSeconds float64 `json:"generateSeconds"`
	BytesRead       int64   `json:"bytesRead"`
	ReadSeconds     float64 `json:"readSeconds"`
}

// resultsStem is deliberately NOT a "posixbench-" name. ListManifests globs
// posixbench*.json and a manifest is posixbench-<runID>-<host>.json, so a
// results file called posixbench-results-<runID>-<host>.json sat inside the
// manifest namespace and had to be filtered out of it by prefix -- which then
// ate any manifest whose runID began "results". Two namespaces separated by a
// string prefix on an operator-controlled field cannot be told apart; disjoint
// stems can, and need no filter at all.
const resultsStem = "pbresults"

func resultsPath(dir, runID, hostname string) string {
	name := resultsStem
	if runID != "" {
		name += "-" + runID
	}
	if hostname != "" {
		name += "-" + hostname
	}
	return filepath.Join(dir, name+".json")
}

// ResultsPath returns where a run's results file is written, mirroring
// ManifestPath.
func ResultsPath(dir, runID, hostname string) string { return resultsPath(dir, runID, hostname) }

func secs(d time.Duration) float64 { return d.Seconds() }

func newRunRecord(m Manifest, env Environment, r Result, didRead bool) RunRecord {
	rec := RunRecord{
		Version:     resultsVersion,
		Manifest:    m,
		Environment: env,
		Write: RecordPhase{
			Bytes:            r.TotalWritten,
			ElapsedSeconds:   secs(r.WriteElapsed),
			IOElapsedSeconds: secs(r.WriteIOElapsed),
			GenerateSeconds:  secs(r.GenerateElapsed),
			MBPerSecond:      r.WriteMBps(),
			PerWorker:        r.WriteStats(),
			Latency:          newRecordLatency(&r.WriteLatency),
		},
		Workers: make([]RecordWorker, 0, len(r.Workers)),
	}
	if didRead {
		rec.Read = &RecordPhase{
			Bytes:            r.TotalRead,
			ElapsedSeconds:   secs(r.ReadElapsed),
			IOElapsedSeconds: secs(r.ReadElapsed),
			MBPerSecond:      r.ReadMBps(),
			PerWorker:        r.ReadStats(),
			Latency:          newRecordLatency(&r.ReadLatency),
		}
	}
	for _, w := range r.Workers {
		rec.Workers = append(rec.Workers, RecordWorker{
			WorkerID:        w.WorkerID,
			BytesWritten:    w.BytesWritten,
			WriteSeconds:    secs(w.WriteElapsed),
			WriteIOSeconds:  secs(w.WriteIOElapsed()),
			GenerateSeconds: secs(w.GenerateElapsed),
			BytesRead:       w.BytesRead,
			ReadSeconds:     secs(w.ReadElapsed),
		})
	}
	return rec
}

func writeRunRecord(dir string, rec RunRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("posixbench: marshal results: %w", err)
	}
	p := resultsPath(dir, rec.Manifest.RunID, rec.Manifest.Hostname)
	if err := writeJSONFile(p, append(data, '\n')); err != nil {
		return fmt.Errorf("posixbench: write results %s: %w", p, err)
	}
	return nil
}

// ReadRunRecord reads the results file a completed run left in dir.
func ReadRunRecord(dir, runID, hostname string) (RunRecord, error) {
	p := resultsPath(dir, runID, hostname)
	data, err := os.ReadFile(p)
	if err != nil {
		return RunRecord{}, fmt.Errorf("posixbench.ReadRunRecord: %w", err)
	}
	var rec RunRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return RunRecord{}, fmt.Errorf("posixbench.ReadRunRecord: parse %s: %w", p, err)
	}
	return rec, nil
}
