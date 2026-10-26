// This is a unit test.
//
// Coverage: the results file's WIRE SCHEMA -- every json key a written record
// carries -- and that ReadRunRecord reads back what writeRunRecord wrote,
// including both of its error arms.
//
// The schema half exists because a round trip cannot replace it. Marshal and
// unmarshal read the SAME struct tag, so renaming one moves both sides together
// and a write-then-read test still passes. Measured, not assumed. What a rename
// actually breaks is an EXTERNAL reader -- jq, a spreadsheet, a collector -- and
// the break is silent, because encoding/json reports no error for a key the
// struct no longer names. It zero-fills, so a historical series reads 0 MB/s for
// a run that measured fine. Only pinning the key names catches that.
//
// Deliberately NOT a version gate. resultsVersion's doc states the asymmetry
// with manifestVersion is intentional -- a results bump "never invalidates
// anything" -- so refusing an unequal version here would contradict it.
package posixbench

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// fullRunRecord is a record with every field set to a distinct non-zero value.
//
// Non-zero throughout is what keeps the schema list honest: an omitempty field
// left at its zero emits no key, so a half-filled fixture would silently shrink
// the schema this test claims to pin.
func fullRunRecord() RunRecord {
	started := time.Date(2026, 3, 4, 5, 6, 7, 8, time.UTC)
	finished := started.Add(90 * time.Second)
	phase := func(scale float64) RecordPhase {
		return RecordPhase{
			Bytes:            int64(1 << 20 * scale),
			ElapsedSeconds:   1.5 * scale,
			IOElapsedSeconds: 1.25 * scale,
			GenerateSeconds:  0.25 * scale,
			MBPerSecond:      800 * scale,
			PerWorker: BandwidthStats{
				MinMBps: 700 * scale, MaxMBps: 900 * scale,
				MeanMBps: 800 * scale, StdDevMBps: 12.5 * scale,
			},
			Latency: RecordLatency{
				Count: uint64(1000 * scale), Min: 0.1 * scale, Max: 9.9 * scale,
				Mean: 1.1 * scale, P50: 1.0 * scale, P95: 4.0 * scale, P99: 8.0 * scale,
			},
		}
	}
	read := phase(2)
	return RunRecord{
		Version: resultsVersion,
		Manifest: Manifest{
			Version: manifestVersion, Seed: 0xFEEDFACE, BlockSize: 4096,
			Kind: "posixbench", FilesPerWorker: 3, FileSize: 1 << 24,
			WorkerCount: 4, Layout: "1-to-1", Hostname: "nodeA", RunID: "r1",
			IOType: "buffered", Pattern: "sequential",
			StartedAt: started, FinishedAt: &finished,
		},
		Environment: Environment{
			ToolVersion: "v8.4.1", BeeGFSVersion: "8.4.1", Kernel: "7.1.13",
			MountOptions: "rw,relatime", External: "TICKET-1",
			Extra:        map[string]string{"note": "schema fixture"},
			ClientConfig: map[string]string{"tuneFileCacheType": "buffered"},
		},
		Write: phase(1),
		Read:  &read,
		Workers: []RecordWorker{{
			WorkerID: 1, BytesWritten: 1 << 20, WriteSeconds: 1.5,
			WriteIOSeconds: 1.25, GenerateSeconds: 0.25,
			BytesRead: 1 << 21, ReadSeconds: 0.75,
		}},
	}
}

// resultsSchema is the results file's public key surface, as dotted paths.
//
// This list is the pin: changing a json tag, adding a field or dropping one
// fails this test until the list is edited to match, which is the moment to ask
// whether an external reader has to be told. Map VALUES are not walked -- Extra
// and ClientConfig are open by design -- but the maps' own keys are.
var resultsSchema = []string{
	"environment", "environment.beegfsVersion", "environment.clientConfig",
	"environment.external", "environment.extra", "environment.kernel",
	"environment.mountOptions", "environment.toolVersion",
	"manifest", "manifest.blockSize", "manifest.fileSize",
	"manifest.filesPerWorker", "manifest.finishedAt", "manifest.hostname",
	"manifest.ioType", "manifest.kind", "manifest.layout", "manifest.pattern",
	"manifest.runID", "manifest.seed", "manifest.startedAt", "manifest.version",
	"manifest.workerCount",
	"read", "read.bytes", "read.elapsedSeconds", "read.generateSeconds",
	"read.ioElapsedSeconds", "read.latencyMs", "read.latencyMs.count",
	"read.latencyMs.max", "read.latencyMs.mean", "read.latencyMs.min",
	"read.latencyMs.p50", "read.latencyMs.p95", "read.latencyMs.p99",
	"read.mbPerSecond", "read.perWorkerMBPerSecond",
	"read.perWorkerMBPerSecond.max", "read.perWorkerMBPerSecond.mean",
	"read.perWorkerMBPerSecond.min", "read.perWorkerMBPerSecond.stdDev",
	"version",
	"workers", "workers[].bytesRead", "workers[].bytesWritten",
	"workers[].generateSeconds", "workers[].readSeconds",
	"workers[].workerID", "workers[].writeIOSeconds", "workers[].writeSeconds",
	"write", "write.bytes", "write.elapsedSeconds", "write.generateSeconds",
	"write.ioElapsedSeconds", "write.latencyMs", "write.latencyMs.count",
	"write.latencyMs.max", "write.latencyMs.mean", "write.latencyMs.min",
	"write.latencyMs.p50", "write.latencyMs.p95", "write.latencyMs.p99",
	"write.mbPerSecond", "write.perWorkerMBPerSecond",
	"write.perWorkerMBPerSecond.max", "write.perWorkerMBPerSecond.mean",
	"write.perWorkerMBPerSecond.min", "write.perWorkerMBPerSecond.stdDev",
}

// keyPaths walks decoded JSON and returns every key path it contains.
//
// An array contributes one "[]" segment rather than an index, so a record with
// two workers yields the same paths as one with a single worker.
func keyPaths(v any, prefix string, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, sub := range t {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			*out = append(*out, p)
			keyPaths(sub, p, out)
		}
	case []any:
		for _, sub := range t {
			keyPaths(sub, prefix+"[]", out)
		}
	}
}

// TestResultsFileSchemaIsPinned fails when a written record's key surface moves.
func TestResultsFileSchemaIsPinned(t *testing.T) {
	b, err := json.Marshal(fullRunRecord())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	var got []string
	keyPaths(decoded, "", &got)
	// Extra and ClientConfig are open maps; their contents are not schema.
	got = slices.DeleteFunc(got, func(p string) bool {
		return strings.HasPrefix(p, "environment.extra.") ||
			strings.HasPrefix(p, "environment.clientConfig.")
	})
	sort.Strings(got)
	got = slices.Compact(got)

	want := slices.Clone(resultsSchema)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("results file key surface changed.\nmissing: %v\nunexpected: %v\n"+
			"If this is deliberate, update resultsSchema -- and consider whether an\n"+
			"external reader of these files has to be told.",
			missing(want, got), missing(got, want))
	}
}

func missing(want, got []string) []string {
	var out []string
	for _, w := range want {
		if !slices.Contains(got, w) {
			out = append(out, w)
		}
	}
	return out
}

// TestRunRecordRoundTrips pins ReadRunRecord against the writer, and both of the
// error arms that no other test reaches.
func TestRunRecordRoundTrips(t *testing.T) {
	dir := t.TempDir()
	rec := fullRunRecord()
	if err := writeRunRecord(dir, rec); err != nil {
		t.Fatalf("writeRunRecord: %v", err)
	}

	got, err := ReadRunRecord(dir, rec.Manifest.RunID, rec.Manifest.Hostname)
	if err != nil {
		t.Fatalf("ReadRunRecord: %v", err)
	}
	if !reflect.DeepEqual(got, rec) {
		t.Errorf("round trip changed the record.\n got: %+v\nwant: %+v", got, rec)
	}

	t.Run("a missing file is an error, not an empty record", func(t *testing.T) {
		if _, err := ReadRunRecord(dir, "no-such-run", "nodeA"); err == nil {
			t.Fatal("ReadRunRecord on a missing file returned nil error")
		}
	})

	t.Run("unparseable contents are an error, not an empty record", func(t *testing.T) {
		p := resultsPath(dir, "bad", "nodeA")
		if err := writeFileForTest(p, "{not json"); err != nil {
			t.Fatalf("seed bad file: %v", err)
		}
		_, err := ReadRunRecord(dir, "bad", "nodeA")
		if err == nil {
			t.Fatal("ReadRunRecord on unparseable contents returned nil error")
		}
		if !strings.Contains(err.Error(), "parse") {
			t.Errorf("error = %q, want it to name the parse failure", err)
		}
	})
}

func writeFileForTest(path, contents string) error {
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
