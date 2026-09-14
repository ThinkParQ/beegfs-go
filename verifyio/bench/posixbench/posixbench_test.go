// This is a unit test.
//
// Coverage: Config.Validate (including that FilesPerWorker is exempt from
// its >0 constraint under n-to-1 layout); blockSeed determinism; manifest
// JSON round-trip; an end-to-end write-then-verify pass on a clean run for
// both 1-to-1 and n-to-1 layouts, with multiple files per worker and an
// optional read phase; and the verifier correctly flagging injected
// corruption. Also the truncated/missing-block ("torn write") anomaly path --
// a file shortened mid-block must be reported as an anomaly and the sweep must
// continue past it, not abort or silently stop counting.
package posixbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"errors"
	"github.com/thinkparq/beegfs-go/verifyio/block"
	"syscall"
	"time"
)

func TestConfigValidate(t *testing.T) {
	good := Config{
		Path:           "/tmp/test",
		Threads:        2,
		BlockSize:      4096,
		FileSize:       4096 * 4,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           1,
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("good config: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty path", func(c *Config) { c.Path = "" }},
		{"zero threads", func(c *Config) { c.Threads = 0 }},
		{"zero blockSize", func(c *Config) { c.BlockSize = 0 }},
		{"oversized blockSize", func(c *Config) { c.BlockSize = MaxBlockSize + 1 }},
		{"zero fileSize", func(c *Config) { c.FileSize = 0 }},
		{"unaligned fileSize", func(c *Config) { c.FileSize = 4096*4 + 1 }},
		{"bad layout", func(c *Config) { c.Layout = Layout("bad") }},
		{"zero filesPerWorker", func(c *Config) { c.FilesPerWorker = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := good
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Errorf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

func TestNto1ValidateNoFilesPerWorkerConstraint(t *testing.T) {
	// n-to-1 does not require FilesPerWorker > 0.
	cfg := Config{
		Path:      "/tmp/test",
		Threads:   2,
		BlockSize: 4096,
		FileSize:  4096 * 4,
		Layout:    LayoutNto1,
		Kind:      block.KindDecimal,
		Seed:      1,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("n-to-1 with zero FilesPerWorker: %v", err)
	}
}

func TestBlockSeedDeterministic(t *testing.T) {
	const userSeed = uint64(0xdeadbeefcafe1234)

	a := blockSeed(userSeed, 3, 100)
	b := blockSeed(userSeed, 3, 100)
	if a != b {
		t.Errorf("blockSeed not deterministic: %x != %x", a, b)
	}
	// Different fileIndex must produce a different seed.
	if c := blockSeed(userSeed, 4, 100); a == c {
		t.Errorf("blockSeed identical for fileIndex 3 and 4")
	}
	// Different blockIndex must produce a different seed.
	if d := blockSeed(userSeed, 3, 101); a == d {
		t.Errorf("blockSeed identical for blockIndex 100 and 101")
	}
	// Different userSeed must produce a different seed.
	if e := blockSeed(userSeed+1, 3, 100); a == e {
		t.Errorf("blockSeed identical for different userSeeds")
	}
}

// TestManifestRoundTrip pins that every Config field the manifest carries comes
// back out of it.
//
// It used to assert six of eleven, and the gaps were not harmless: dropping
// FilesPerWorker made Verify examine one file per worker and report PASS over
// the rest, and dropping RunID or Hostname pointed it at filenames no run ever
// wrote. Assert the whole set, so adding a field and forgetting to read it back
// fails here rather than in a verdict.
func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        3,
		BlockSize:      8192,
		FileSize:       8192 * 10,
		FilesPerWorker: 2,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           0xabcdef1234,
		Hostname:       "nodeA",
		RunID:          "20260903T120000Z",
		Pattern:        PatternRandom,
	}
	if err := writeManifest(dir, configToManifest(cfg)); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	// Read through the addressed path, since a run with a runID and a hostname
	// is what the CLI always writes -- EnsureRunID guarantees it.
	_, got, err := ReadManifestAt(dir, cfg.RunID, cfg.Hostname)
	if err != nil {
		t.Fatalf("ReadManifestAt: %v", err)
	}
	if got.Seed != cfg.Seed {
		t.Errorf("Seed: got %x, want %x", got.Seed, cfg.Seed)
	}
	if got.BlockSize != cfg.BlockSize {
		t.Errorf("BlockSize: got %d, want %d", got.BlockSize, cfg.BlockSize)
	}
	if got.FileSize != cfg.FileSize {
		t.Errorf("FileSize: got %d, want %d", got.FileSize, cfg.FileSize)
	}
	if got.Layout != cfg.Layout {
		t.Errorf("Layout: got %q, want %q", got.Layout, cfg.Layout)
	}
	if got.Kind != cfg.Kind {
		t.Errorf("Kind: got %v, want %v", got.Kind, cfg.Kind)
	}
	if got.Threads != cfg.Threads {
		t.Errorf("Threads: got %d, want %d", got.Threads, cfg.Threads)
	}
	if got.FilesPerWorker != cfg.FilesPerWorker {
		t.Errorf("FilesPerWorker: got %d, want %d -- Verify examines this many files "+
			"per worker, so losing it means PASS over the ones it skipped",
			got.FilesPerWorker, cfg.FilesPerWorker)
	}
	if got.Hostname != cfg.Hostname {
		t.Errorf("Hostname: got %q, want %q -- it is woven into every data filename",
			got.Hostname, cfg.Hostname)
	}
	if got.RunID != cfg.RunID {
		t.Errorf("RunID: got %q, want %q -- it is woven into every data filename",
			got.RunID, cfg.RunID)
	}
	if got.Pattern != cfg.Pattern {
		t.Errorf("Pattern: got %q, want %q", got.Pattern, cfg.Pattern)
	}
	// Path is supplied by the CALLER of ReadManifest, not carried in the
	// document -- that is what keeps a manifest's contents from redirecting the
	// data path outside -path. Assert it, so the day someone persists Path the
	// omission is a deliberate change rather than a silent one.
	if got.Path != dir {
		t.Errorf("Path: got %q, want the caller's %q", got.Path, dir)
	}
	// IOType is deliberately NOT read back: only buffered is implemented and
	// Validate rejects every other value by name, so the zero value is correct
	// and unambiguous. If a second IOType ever lands, this becomes a real gap.
	if got.IOType != 0 {
		t.Errorf("IOType: got %v, want the zero value; a second IOType has landed and "+
			"manifestToConfig now needs to read it back", got.IOType)
	}
}

// goldenManifest is a manifest carrying its version as a LITERAL rather than as
// the symbol.
//
// That literal is the point: TestManifestRoundTrip builds its input through
// configToManifest, which stamps whatever manifestVersion says, so both sides
// move together and the gate pins nothing. It matters because the default verify
// path regenerates every body from the manifest seed -- a directory written by a
// build with a different GenerateBody reports a data mismatch on every block,
// indistinguishable from real corruption, and one un-upgraded node sharing the
// directory is enough to produce it.
const goldenManifest = `{
  "version": 3,
  "seed": 737894400,
  "blockSize": 4096,
  "kind": "decimal",
  "filesPerWorker": 1,
  "fileSize": 16384,
  "workerCount": 2,
  "layout": "1-to-1",
  "startedAt": "2026-09-02T10:00:00Z",
  "finishedAt": "2026-09-02T10:00:41Z"
}
`

// TestGoldenManifest pins the manifest format against its literal version.
//
// A deliberate bump is meant to break this: replace the document above with a
// freshly captured one. Rejection of the superseded version needs no document
// of its own -- TestManifestVersionGate covers it as `manifestVersion - 1`.
func TestGoldenManifest(t *testing.T) {
	if manifestVersion != 3 {
		t.Fatalf("manifestVersion is %d, but this golden document describes version 3.\n"+
			"If the bump is deliberate, capture a new document. If not, revert it -- the gate "+
			"is what keeps a body-derivation change from reporting as corruption on every "+
			"block.", manifestVersion)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestFileName), []byte(goldenManifest), 0644); err != nil {
		t.Fatalf("write golden manifest: %v", err)
	}
	_, got, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest on the golden document: %v", err)
	}
	if got.Seed != 737894400 || got.BlockSize != 4096 || got.FileSize != 16384 ||
		got.Threads != 2 || got.FilesPerWorker != 1 || got.Layout != Layout1to1 ||
		got.Kind != block.KindDecimal {
		t.Errorf("golden document decoded to an unexpected config: %+v", got)
	}
}

// TestManifestVersionGate pins the gate at manifest.go: a manifest whose
// version is not this build's is refused, in either direction.
//
// Verify regenerates every body from the seed, so a directory written by a
// build with a different GenerateBody would otherwise report "data mismatch"
// on every block -- indistinguishable from real corruption on the filesystem
// under test. A future version is refused for the same reason: this build
// cannot know what it means.
func TestManifestVersionGate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int
		wantErr bool
	}{
		{"current version", manifestVersion, false},
		{"stale version", manifestVersion - 1, true},
		{"future version", manifestVersion + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			m := Manifest{
				Version: tc.version, Seed: 1, BlockSize: 4096, Kind: "decimal",
				FilesPerWorker: 1, FileSize: 16384, WorkerCount: 2,
				Layout: string(Layout1to1),
			}
			data, err := json.MarshalIndent(m, "", "  ")
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, manifestFileName), data, 0644); err != nil {
				t.Fatalf("write: %v", err)
			}
			_, _, err = ReadManifest(dir)
			if tc.wantErr && err == nil {
				t.Errorf("version %d was accepted; want rejected", tc.version)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("version %d was rejected: %v", tc.version, err)
			}
		})
	}
}

// TestVerifyRejectsOversizedManifestBlockSize pins MaxBlockSize as a bound on
// untrusted input. Verify sizes two buffers directly from cfg.BlockSize, which
// comes from a manifest file -- and on a shared, multi-writer bench directory a
// manifest is not trusted input. Without the bound, a multi-GiB or multi-TiB
// blockSize reaches those allocations directly: 2^31 drives RSS to ~2.1 GB, and
// 2^40 is an uncatchable "fatal error: runtime: out of memory". NewVerifier must
// reject the manifest before either buffer is allocated.
func TestVerifyRejectsOversizedManifestBlockSize(t *testing.T) {
	// int64 literals narrowed per-case rather than a []int: 1<<31 and 1<<40 do
	// not fit an int on a 32-bit build, so as untyped constants in an []int
	// they are a compile error there -- which took `GOOS=linux GOARCH=386 go
	// vet ./verifyio/...` down for the whole package, including the tests that
	// exist specifically to check 32-bit truncation behaviour.
	for _, blockSize64 := range []int64{1 << 31, 1 << 40} {
		blockSize := int(blockSize64) // truncates on 32-bit; the value is still oversized
		t.Run(fmt.Sprintf("blockSize=%d", blockSize64), func(t *testing.T) {
			if int64(blockSize) != blockSize64 {
				t.Skipf("blockSize %d does not fit an int on this platform", blockSize64)
			}
			dir := t.TempDir()
			cfg := Config{
				Path:           dir,
				Threads:        1,
				BlockSize:      blockSize,
				FileSize:       blockSize64,
				FilesPerWorker: 1,
				Layout:         Layout1to1,
				Kind:           block.KindDecimal,
				Seed:           1,
			}
			// writeManifest marshals directly with no validation, matching how
			// a hand-crafted or corrupted manifest file reaches disk.
			if err := writeManifest(dir, configToManifest(cfg)); err != nil {
				t.Fatalf("writeManifest: %v", err)
			}
			_, got, err := ReadManifest(dir)
			if err != nil {
				t.Fatalf("ReadManifest: %v", err)
			}
			if _, err := NewVerifier(got); err == nil {
				t.Fatalf("NewVerifier accepted blockSize=%d, want an error before "+
					"any buffer is allocated", blockSize)
			}
		})
	}
}

// TestVerifyRejectsOversizedManifestFileSize is the sibling of the test above
// for the one manifest field that was left unbounded.
//
// The failure mode is different from its three bounded siblings, and worse.
// BlockSize, Threads and FilesPerWorker each size an ALLOCATION, so an absurd
// value is an OOM: loud, immediate, unmistakably a failure. FileSize sizes the
// verify LOOP (blocks := reg.length / BlockSize), so an absurd value costs the
// verdict entirely -- `iotest-posixbench verify` on the repro value below exits
// only on a 20s timeout, having printed its banner and nothing else: no
// Elapsed, no PASS, no FAIL. Under -verbose it emits invented short-read
// anomalies at ~260k blocks/s, which fills the filesystem under test if stdout
// is redirected onto it.
func TestVerifyRejectsOversizedManifestFileSize(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blockSize int
		fileSize  int64
		wantErr   bool
	}{
		// The review's reproduction: 2.3e15 blocks per region.
		{"absurd", 4096, 9223372036854771712, true},
		{"just over the ceiling", 4096, int64(MaxBlocksPerRegion+1) * 4096, true},
		{"at the ceiling", 4096, int64(MaxBlocksPerRegion) * 4096, false},
		// The ceiling is on the derived count, not on a byte figure, so a
		// large region paired with a large block size stays legal.
		{"64 TiB at the default block size", DefaultBlockSize,
			int64(MaxBlocksPerRegion) * DefaultBlockSize, false},
		{"ordinary", 4096, 4096 * 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{
				Path: dir, Threads: 1, BlockSize: tc.blockSize, FileSize: tc.fileSize,
				FilesPerWorker: 1, Layout: Layout1to1, Kind: block.KindDecimal, Seed: 1,
			}
			// writeManifest marshals with no validation, matching how a
			// hand-crafted or corrupted manifest reaches disk.
			if err := writeManifest(dir, configToManifest(cfg)); err != nil {
				t.Fatalf("writeManifest: %v", err)
			}
			_, got, err := ReadManifest(dir)
			if err != nil {
				t.Fatalf("ReadManifest: %v", err)
			}
			_, err = NewVerifier(got)
			if tc.wantErr && err == nil {
				t.Errorf("NewVerifier accepted fileSize=%d (%d blocks per region), want an error "+
					"before the verify loop is entered",
					tc.fileSize, tc.fileSize/int64(tc.blockSize))
			}
			if !tc.wantErr && err != nil {
				t.Errorf("NewVerifier rejected a legitimate fileSize=%d: %v", tc.fileSize, err)
			}
		})
	}
}

// TestPathElementsCannotEscapeBenchDir pins that BOTH operator-supplied strings
// woven into a data-file path are validated before they get there.
//
// manifestToConfig copies m.Hostname and m.RunID straight into Config,
// pbenchFilePrefix builds "pbench-"+RunID+"-"+Hostname, and workerRegions joins
// that into the path -- while Validate once checked Path, Threads, BlockSize,
// FileSize and Layout and none of these. filepath.Join does not contain them,
// because Join cleans: the "/../" spelling cancels the real path elements and
// leaves the bench directory.
//
// Every case runs against BOTH fields on purpose. validPathElement was renamed
// and generalised from validHostname when RunID became its second caller, and
// the test was not: removing the RunID check left both gates green, while RunID
// reaches manifestPath, resultsPath AND pbenchFilePrefix -- the same reach the
// hostname had when it was a live escape.
//
// The precision case matters as much as the escape. "../outside" on its own
// does NOT escape, because "pbench-.." is a single literal path element -- so a
// test asserting only that spelling would pass while the bug remained. Both
// spellings are here for that reason.
func TestPathElementsCannotEscapeBenchDir(t *testing.T) {
	fields := []struct {
		name string
		set  func(*Config, string)
	}{
		{"hostname", func(c *Config, v string) { c.Hostname = v }},
		{"runID", func(c *Config, v string) { c.RunID = v }},
	}
	for _, tc := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"escapes via /../", "x/../../../../../../tmp/escaped", true},
		{"bare dotdot does not escape but is still refused", "../outside", true},
		{"absolute path", "/etc/passwd", true},
		{"newline", "node\n", true},
		{"nul", "node\x00", true},
		{"ordinary value", "t94", false},
		{"dotted value", "test-r9-04.lab.example.com", false},
		{"underscore is allowed", "node_1", false},
		{"empty means the element is left out of the name", "", false},
	} {
		for _, f := range fields {
			t.Run(f.name+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				cfg := Config{
					Path: dir, Threads: 1, BlockSize: 4096,
					FileSize: 4096 * 4, FilesPerWorker: 1, Layout: Layout1to1,
					Kind: block.KindDecimal, Seed: 1,
				}
				f.set(&cfg, tc.value)
				err := cfg.Validate()
				if tc.wantErr && err == nil {
					escaped := filepath.Clean(workerRegions(cfg)[0][0].path)
					t.Errorf("Validate accepted %s %q; data path resolves to %s",
						f.name, tc.value, escaped)
				}
				if !tc.wantErr {
					if err != nil {
						t.Fatalf("Validate rejected a legitimate %s %q: %v", f.name, tc.value, err)
					}
					// A value that validates must also stay inside --path.
					got := filepath.Clean(workerRegions(cfg)[0][0].path)
					if !strings.HasPrefix(got, filepath.Clean(dir)+string(filepath.Separator)) {
						t.Errorf("%s %q produced a path outside the bench dir: %s",
							f.name, tc.value, got)
					}
				}
			})
		}
	}
}

// TestPathElementLengthIsBounded pins the length ceiling separately from the
// character set, because only one of the two fields carries it: maxHostnameLen
// guards Hostname and nothing bounds RunID, so an over-long -run fails late at
// ENAMETOOLONG from the filesystem rather than early in Validate. Recorded here
// as the known asymmetry rather than left for someone to rediscover.
func TestPathElementLengthIsBounded(t *testing.T) {
	base := func() Config {
		return Config{
			Path: "/tmp/x", Threads: 1, BlockSize: 4096, FileSize: 4096 * 4,
			FilesPerWorker: 1, Layout: Layout1to1, Kind: block.KindDecimal, Seed: 1,
		}
	}
	t.Run("hostname at the bound is accepted", func(t *testing.T) {
		c := base()
		c.Hostname = strings.Repeat("a", maxHostnameLen)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate rejected a hostname of exactly maxHostnameLen: %v", err)
		}
	})
	t.Run("hostname one over the bound is refused", func(t *testing.T) {
		c := base()
		c.Hostname = strings.Repeat("a", maxHostnameLen+1)
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a hostname one byte over maxHostnameLen")
		}
	})
	t.Run("runID is deliberately unbounded, and this records that", func(t *testing.T) {
		// Not a bug being pinned in place: an over-long runID is refused by the
		// filesystem with ENAMETOOLONG, which is loud and names the path. If a
		// bound is ever added, delete this case rather than inverting it.
		c := base()
		c.RunID = strings.Repeat("a", maxHostnameLen*4)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate grew a runID length bound; the test that documented "+
				"its absence needs deleting, not inverting: %v", err)
		}
	})
}

func TestRunnerAndVerifierClean(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 8
		threads   = 2
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        threads,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           42,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := int64(threads * fileSize); res.TotalWritten != want {
		t.Errorf("TotalWritten=%d, want %d", res.TotalWritten, want)
	}
	if res.WriteElapsed == 0 {
		t.Errorf("WriteElapsed is zero")
	}

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sweep, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	n := sweep.Anomalies
	if n != 0 {
		t.Errorf("Verify: %d anomalies, want 0", n)
	}
}

// TestWorkerResultWriteIOElapsed pins WriteIOElapsed's subtraction and its zero
// clamp -- the arithmetic WriteMBps and WriteStats depend on to report storage
// throughput rather than a figure capped by the pattern generator.
func TestWorkerResultWriteIOElapsed(t *testing.T) {
	cases := []struct {
		name            string
		writeElapsed    time.Duration
		generateElapsed time.Duration
		want            time.Duration
	}{
		{"generation is a fraction of write time", 100 * time.Millisecond, 30 * time.Millisecond, 70 * time.Millisecond},
		{"no generation cost", 100 * time.Millisecond, 0, 100 * time.Millisecond},
		{"clamped at zero, never negative", 10 * time.Millisecond, 50 * time.Millisecond, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := WorkerResult{WriteElapsed: tc.writeElapsed, GenerateElapsed: tc.generateElapsed}
			if got := w.WriteIOElapsed(); got != tc.want {
				t.Errorf("WriteIOElapsed() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestRunTracksGenerateElapsedSeparatelyFromWriteIO pins that Run times pattern
// generation separately from the write itself. Timing them in one window caps the
// reported write rate at whichever is slower of the storage and the pattern
// generator for the chosen -kind, and moves that cap every time -kind changes --
// so WriteIOElapsed, and therefore WriteMBps, must reflect IO alone.
func TestRunTracksGenerateElapsedSeparatelyFromWriteIO(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 16
		threads   = 2
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        threads,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal, // the default; not free to generate
		Seed:           7,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	// doRead=true also exercises readRegions' cache-drop path end to end.
	res, err := r.Run(context.Background(), true)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GenerateElapsed <= 0 {
		t.Error("GenerateElapsed is zero -- pattern-generation time is not being tracked")
	}
	if res.WriteIOElapsed <= 0 {
		t.Error("WriteIOElapsed is zero")
	}
	// Deliberately NOT "WriteIOElapsed <= WriteElapsed": WriteIOElapsed() is
	// max(0, WriteElapsed-GenerateElapsed), so that can never fail and pins
	// nothing. The real claim is that generation was subtracted at all.
	if res.WriteIOElapsed >= res.WriteElapsed {
		t.Errorf("WriteIOElapsed (%s) is not less than WriteElapsed (%s) -- generation time "+
			"was not subtracted, so the reported write rate still includes the pattern generator",
			res.WriteIOElapsed, res.WriteElapsed)
	}
	// The exact identity Result's doc states -- "the slowest worker's
	// WorkerResult.WriteIOElapsed" -- not just a bound. The inequalities above
	// are one-sided, so any worker duration that happens to be positive and
	// under the phase wall clock satisfies them: measured, sourcing the
	// aggregate from GenerateElapsed OR from the worker's full WriteElapsed
	// both left the whole suite green, and the second silently puts pattern
	// generation back into WriteMBps -- the exact defect this test exists to
	// prevent, restored one field away from where it was closed.
	var wantIO, wantGen time.Duration
	for _, w := range res.Workers {
		if io := w.WriteIOElapsed(); io > wantIO {
			wantIO = io
		}
		if w.GenerateElapsed > wantGen {
			wantGen = w.GenerateElapsed
		}
	}
	if res.WriteIOElapsed != wantIO {
		t.Errorf("WriteIOElapsed = %s, want %s (the slowest worker's WriteIOElapsed) -- the "+
			"aggregate is not sourced from the field it documents", res.WriteIOElapsed, wantIO)
	}
	if res.GenerateElapsed != wantGen {
		t.Errorf("GenerateElapsed = %s, want %s (the slowest worker's GenerateElapsed)",
			res.GenerateElapsed, wantGen)
	}

	for _, w := range res.Workers {
		if w.GenerateElapsed <= 0 {
			t.Errorf("worker %d: GenerateElapsed is zero", w.WorkerID)
		}
		// Same reasoning as above: strictly less, or the subtraction did not
		// happen. `>` is unreachable by construction.
		if w.WriteIOElapsed() >= w.WriteElapsed {
			t.Errorf("worker %d: WriteIOElapsed() (%s) is not less than WriteElapsed (%s)",
				w.WorkerID, w.WriteIOElapsed(), w.WriteElapsed)
		}
	}
}

func TestVerifierDetectsCorruption(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 4
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        1,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           99,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Corrupt one byte in the first block. KindDecimal produces bytes in
	// ['0'-'9', ' '] so 0xFF is always a detectable mismatch.
	dataFile := filepath.Join(dir, "pbench-w000-f000.dat")
	f, err := os.OpenFile(dataFile, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 100); err != nil {
		t.Fatalf("corrupt byte: %v", err)
	}
	f.Close()

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	var anomalies []Anomaly
	sweep, err := v.Verify(func(a Anomaly) error {
		anomalies = append(anomalies, a)
		return nil
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	n := sweep.Anomalies
	// Exactly one: one byte was flipped in one block. "> 0" was satisfied by
	// any over-count, so double-counting an anomaly survived every gate, and a
	// verdict that inflates real findings trains an operator to discount them.
	if n != 1 || len(anomalies) != 1 {
		t.Fatal("Verify: expected at least one anomaly after corruption, got none")
	}
	if anomalies[0].BlockIndex != 0 {
		t.Errorf("first anomaly: BlockIndex=%d, want 0", anomalies[0].BlockIndex)
	}
}

// TestVerifierDetectsMisdirectedWrite pins that a block written to the wrong
// place is caught, in every layout and at every granularity the tool has.
//
// Two things are being pinned. The DEFAULT kind must detect it: blockSeed folds
// a region-distinguishing index in at bit 32, so a Kind reading only its seed's
// low bits would report zero anomalies after one region's data was overwritten
// with another's -- see block.mixSeed. And the region indices must actually be
// DISTINCT, which is workerRegions' "each worker's region has a distinct seed
// namespace" claim.
//
// That claim was true and unpinned in both layouts. A single 1-to-1 case with
// one file per worker reaches neither of the two ways it can break: n-to-1
// handing every region fileIndex 0, and 1-to-1 keying on the worker instead of
// on worker*filesPerWorker+file, so a worker's own files share a namespace.
// Both survived every gate, and both turn a misdirected write into PASS --
// which is what the ECC-removal decision cited as the reason to keep
// byte-for-byte verification in the first place.
func TestVerifierDetectsMisdirectedWrite(t *testing.T) {
	const (
		blockSize = 1024
		fileSize  = 1024 * 4
		perFile   = fileSize / blockSize
	)
	cases := []struct {
		name string
		cfg  func(dir string) Config
		// misdirect copies one region's bytes over another's and returns how
		// many blocks that should make anomalous.
		misdirect func(t *testing.T, dir string) int
	}{
		{
			name: "1-to-1, one worker's file over another's",
			cfg: func(dir string) Config {
				return Config{Path: dir, Threads: 2, BlockSize: blockSize, FileSize: fileSize,
					FilesPerWorker: 1, Layout: Layout1to1, Kind: block.KindDecimal, Seed: 123456789}
			},
			misdirect: func(t *testing.T, dir string) int {
				copyOver(t, filepath.Join(dir, "pbench-w000-f000.dat"), filepath.Join(dir, "pbench-w001-f000.dat"))
				return perFile
			},
		},
		{
			// S5: with fileIndex keyed on the worker rather than on
			// worker*filesPerWorker+file, a worker's OWN files share a seed
			// namespace and this swap becomes undetectable.
			name: "1-to-1, one of a worker's own files over another",
			cfg: func(dir string) Config {
				return Config{Path: dir, Threads: 1, BlockSize: blockSize, FileSize: fileSize,
					FilesPerWorker: 3, Layout: Layout1to1, Kind: block.KindDecimal, Seed: 123456789}
			},
			misdirect: func(t *testing.T, dir string) int {
				copyOver(t, filepath.Join(dir, "pbench-w000-f000.dat"), filepath.Join(dir, "pbench-w000-f001.dat"))
				return perFile
			},
		},
		{
			// S4: with every n-to-1 region handed fileIndex 0, one region's
			// bytes verify happily at another region's offset.
			name: "n-to-1, one region of the shared file over another",
			cfg: func(dir string) Config {
				return Config{Path: dir, Threads: 3, BlockSize: blockSize, FileSize: fileSize,
					FilesPerWorker: 1, Layout: LayoutNto1, Kind: block.KindDecimal, Seed: 123456789}
			},
			misdirect: func(t *testing.T, dir string) int {
				shared := filepath.Join(dir, "pbench-shared.dat")
				f, err := os.OpenFile(shared, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("open shared file: %v", err)
				}
				defer f.Close()
				region0 := make([]byte, fileSize)
				if _, err := f.ReadAt(region0, 0); err != nil {
					t.Fatalf("read region 0: %v", err)
				}
				// Region 1 lives at offset 1*FileSize.
				if _, err := f.WriteAt(region0, fileSize); err != nil {
					t.Fatalf("write region 0's bytes over region 1: %v", err)
				}
				return perFile
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := tc.cfg(dir)
			r, err := NewRunner(cfg)
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			if _, err := r.Run(context.Background(), false); err != nil {
				t.Fatalf("Run: %v", err)
			}
			// Control: it verified clean before the misdirection, so a later
			// FAIL cannot be blamed on the fixture.
			v, err := NewVerifier(cfg)
			if err != nil {
				t.Fatalf("NewVerifier: %v", err)
			}
			if before, err := v.Verify(nil); err != nil || before.Anomalies != 0 {
				t.Fatalf("the fixture did not verify clean before the misdirection: %d anomalies, err=%v",
					before.Anomalies, err)
			}

			want := tc.misdirect(t, dir)
			sweep, err := v.Verify(nil)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			// Exact, not "at least one": an under-count means most of the
			// misdirected region verified against the wrong namespace, which is
			// the defect, and an over-count is a false FAIL.
			if sweep.Anomalies != want {
				t.Errorf("Verify: %d anomalies, want %d (every block of the misdirected region)",
					sweep.Anomalies, want)
			}
		})
	}
}

// copyOver replaces dst's contents with src's, wholesale.
func copyOver(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		t.Fatalf("overwrite %s: %v", dst, err)
	}
}

// TestVerifierDetectsTruncation covers the short-read ("torn write") anomaly
// branch in Verify (verifier.go), which TestVerifierDetectsCorruption doesn't
// exercise -- that test only flips a byte in an otherwise-intact file. A
// truncated file must be reported as anomalies, not a hard error, and the
// sweep must continue past the first bad block rather than stopping there.
func TestVerifierDetectsTruncation(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 4
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        1,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           99,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Truncate mid-block-2, so block 2 is a genuine short read (some bytes,
	// then EOF) and block 3 is entirely absent (zero bytes at that offset) --
	// covering both short-read shapes the code's errors.Is check handles.
	dataFile := filepath.Join(dir, "pbench-w000-f000.dat")
	if err := os.Truncate(dataFile, 2*blockSize+blockSize/2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	var anomalies []Anomaly
	sweep, err := v.Verify(func(a Anomaly) error {
		anomalies = append(anomalies, a)
		return nil
	})
	if err != nil {
		t.Fatalf("Verify: %v (truncation must be reported as an anomaly, not abort the sweep)", err)
	}
	n := sweep.Anomalies
	// Exactly two: the file was cut mid-block-2, so blocks 2 and 3 are short
	// and blocks 0 and 1 are intact. "at least 2" was satisfied by any
	// over-count, so counting each short read twice survived every gate -- and
	// an inflated anomaly count on real damage is still a wrong number in a
	// verdict someone acts on.
	if n != 2 {
		t.Fatalf("Verify: got %d anomalies, want exactly 2 (blocks 2 and 3 short, 0 and 1 intact)", n)
	}
	seen := map[int64]bool{}
	for _, a := range anomalies {
		seen[a.BlockIndex] = true
	}
	if !seen[2] || !seen[3] {
		t.Errorf("anomalies = %v, want both block 2 (partial) and block 3 (missing) flagged", anomalies)
	}
}

func TestRunnerNto1(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 4
		threads   = 3
	)
	dir := t.TempDir()
	cfg := Config{
		Path:      dir,
		Threads:   threads,
		BlockSize: blockSize,
		FileSize:  fileSize,
		Layout:    LayoutNto1,
		Kind:      block.KindDecimal,
		Seed:      77,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := int64(threads) * fileSize; res.TotalWritten != want {
		t.Errorf("TotalWritten=%d, want %d", res.TotalWritten, want)
	}

	info, err := os.Stat(filepath.Join(dir, "pbench-shared.dat"))
	if err != nil {
		t.Fatalf("stat shared file: %v", err)
	}
	if want := int64(threads) * fileSize; info.Size() != want {
		t.Errorf("shared file size=%d, want %d", info.Size(), want)
	}

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sweep, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	n := sweep.Anomalies
	if n != 0 {
		t.Errorf("n-to-1 Verify: %d anomalies, want 0", n)
	}
}

func TestRunnerWithReadPhase(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 4
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        2,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           55,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(context.Background(), true)
	if err != nil {
		t.Fatalf("Run(read=true): %v", err)
	}
	want := int64(2 * fileSize)
	if res.TotalWritten != want {
		t.Errorf("TotalWritten=%d, want %d", res.TotalWritten, want)
	}
	if res.TotalRead != want {
		t.Errorf("TotalRead=%d, want %d", res.TotalRead, want)
	}
	if res.ReadElapsed == 0 {
		t.Errorf("ReadElapsed=0 after read phase")
	}
}

func TestMultipleFilesPerWorker(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 2
		threads   = 2
		fpw       = 3
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        threads,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: fpw,
		Layout:         Layout1to1,
		Kind:           block.KindPRNG,
		Seed:           123,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := int64(threads*fpw) * fileSize; res.TotalWritten != want {
		t.Errorf("TotalWritten=%d, want %d", res.TotalWritten, want)
	}

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sweep, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	n := sweep.Anomalies
	if n != 0 {
		t.Errorf("Verify: %d anomalies, want 0", n)
	}
}

// cancellableConfig returns a config large enough that a cancellation landing
// mid-run is unambiguous: many blocks per worker, so stopping after a short
// delay leaves most of them unwritten.
func cancellableConfig(dir string) Config {
	return Config{
		Path:           dir,
		Threads:        2,
		BlockSize:      4096,
		FileSize:       4096 * 20000, // ~80 MiB per worker, 20k blocks
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           7,
	}
}

// TestRunCancelledBeforeStartWritesNothing pins that an already-cancelled
// context is honoured immediately. Before ctx was plumbed through, Run had no
// way to observe cancellation at all: BenchTool.Run referenced neither its ctx
// nor cfg.StopSignal, so `iotest stop` and Ctrl+C were silently ignored and the
// caller blocked for the whole Threads x FileSize write and read.
func TestRunCancelledBeforeStartWritesNothing(t *testing.T) {
	r, err := NewRunner(cancellableConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := r.Run(ctx, true)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if res.TotalWritten != 0 {
		t.Errorf("TotalWritten = %d, want 0 on an already-cancelled run", res.TotalWritten)
	}
	if res.TotalRead != 0 {
		t.Errorf("TotalRead = %d, want 0 -- a cancelled write phase must not start the read phase", res.TotalRead)
	}
}

// TestRunCancelMidWriteStopsDuringIO is the load-bearing case: cancellation must
// take effect *inside* the block loop, not at the next phase boundary. The check
// is per block precisely because a region is a whole per-worker extent -- a
// region-level check would never fire mid-run, which for the 1 GiB/worker
// default means no effective cancellation at all.
func TestRunCancelMidWriteStopsDuringIO(t *testing.T) {
	cfg := cancellableConfig(t.TempDir())
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	res, err := r.Run(ctx, true)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	full := int64(cfg.Threads) * cfg.FileSize
	if res.TotalWritten >= full {
		t.Errorf("TotalWritten = %d, want well under the full %d -- cancellation "+
			"must interrupt the write, not merely be noticed after it finishes",
			res.TotalWritten, full)
	}
	if res.TotalRead != 0 {
		t.Errorf("TotalRead = %d, want 0 -- the read phase must be skipped", res.TotalRead)
	}
	// Generous bound: the point is that it returns promptly rather than running
	// to completion, not that it hits a precise deadline on a loaded CI box.
	if elapsed > 30*time.Second {
		t.Errorf("took %s; cancellation did not interrupt the run", elapsed)
	}
}

// TestRunCancelDuringReadPhase covers the second loop: readRegions has its own
// per-block check, so a stop arriving while only the read phase is left must
// also take effect rather than running the read to completion.
func TestRunCancelDuringReadPhase(t *testing.T) {
	cfg := cancellableConfig(t.TempDir())
	cfg.FileSize = 4096 * 2000 // smaller, so the write phase completes quickly
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	// Write with a live context so the write phase finishes.
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("write phase: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := r.Run(ctx, true)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if res.TotalRead != 0 {
		t.Errorf("TotalRead = %d, want 0", res.TotalRead)
	}
}

// TestBlockSeedNoStructuredCollision is the regression test for the seed
// collision that survived the first misdirected-write fix.
//
// blockSeed composes userSeed ^ fileIndex<<32 ^ blockIndex, and the XOR-fold
// that KindCountUp and KindDecimal used to reduce that to a small starting
// value is linear -- so it collapsed to C ^ fileIndex ^ blockIndex, and every
// pair of blocks with equal fileIndex^blockIndex generated byte-identical
// bodies. File 0/block 1 against file 1/block 0 is the smallest instance, and
// it is a one-block-off misdirected write between two files: precisely what the
// fold was added to detect. mixSeed's avalanche is what breaks the alignment
// between the caller's seed layout and the reduction.
//
// Verified to fail against the pre-fix code: with the XOR-fold, both seeds
// below reduced to 276 for KindDecimal.
func TestBlockSeedNoStructuredCollision(t *testing.T) {
	const (
		userSeed = 123456789
		bodyLen  = 4096
	)
	// Each pair has equal fileIndex^blockIndex, the invariant the old fold
	// collapsed on.
	pairs := []struct {
		aFile  int
		aBlock int64
		bFile  int
		bBlock int64
	}{
		{0, 1, 1, 0},
		{2, 3, 3, 2},
		{0, 5, 5, 0},
		{1, 6, 6, 1},
	}
	for _, kind := range []block.Kind{block.KindCountUp, block.KindDecimal} {
		for _, p := range pairs {
			name := fmt.Sprintf("%s/f%db%d_vs_f%db%d", kind, p.aFile, p.aBlock, p.bFile, p.bBlock)
			t.Run(name, func(t *testing.T) {
				a := make([]byte, bodyLen)
				b := make([]byte, bodyLen)
				if err := block.GenerateBody(kind, blockSeed(userSeed, p.aFile, p.aBlock), a); err != nil {
					t.Fatalf("GenerateBody(a): %v", err)
				}
				if err := block.GenerateBody(kind, blockSeed(userSeed, p.bFile, p.bBlock), b); err != nil {
					t.Fatalf("GenerateBody(b): %v", err)
				}
				if bytes.Equal(a, b) {
					t.Errorf("%s: (file %d, block %d) and (file %d, block %d) generated identical "+
						"bodies -- a misdirected write between them is undetectable",
						kind, p.aFile, p.aBlock, p.bFile, p.bBlock)
				}
			})
		}
	}
}

// TestPhaseErrRanksRealErrorsAboveCancel is the regression test for a cancelled
// run swallowing its workers' IO errors.
//
// The scenario: worker 1 hits ENOSPC partway in, the other workers keep going
// (Run does not cancel on first error), and the operator then Ctrl+Cs. Run used
// to return early on ctx.Err() before collecting worker errors at all, so it
// returned bare context.Canceled -- and the CLI branches on errors.Is(err,
// context.Canceled) to print "STOPPED (partial run; no results reported)" and
// exit 0. The ENOSPC was never printed anywhere.
//
// The errors.Is assertion is the load-bearing one: simply joining the cancel in
// alongside the real error would still satisfy "the error is reported" while
// leaving the CLI's check matching, and the run would still exit 0.
func TestPhaseErrRanksRealErrorsAboveCancel(t *testing.T) {
	writeErr := func(w WorkerResult) error { return w.WriteErr }
	enospc := errors.New("write /mnt/bench/pbench-w001-f000.dat: no space left on device")

	t.Run("real error survives a cancel", func(t *testing.T) {
		workers := []WorkerResult{
			{WriteErr: context.Canceled}, // stopped by the cancel, not a failure
			{WriteErr: enospc},           // the actual finding
			{WriteErr: context.Canceled},
		}
		err := phaseErr(workers, context.Canceled, "worker", writeErr)
		if err == nil {
			t.Fatal("cancelled phase with a failing worker returned nil")
		}
		if !errors.Is(err, enospc) {
			t.Errorf("ENOSPC did not survive the cancel: %v", err)
		}
		if errors.Is(err, context.Canceled) {
			t.Errorf("error still matches context.Canceled, so the CLI reports STOPPED and exits 0: %v", err)
		}
		if !strings.Contains(err.Error(), "worker 1") {
			t.Errorf("error does not identify the failing worker: %v", err)
		}
	})

	t.Run("a bare cancel is reported once", func(t *testing.T) {
		workers := []WorkerResult{
			{WriteErr: context.Canceled},
			{WriteErr: context.Canceled},
		}
		err := phaseErr(workers, context.Canceled, "worker", writeErr)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel not reported: %v", err)
		}
		// N joined copies of one stop would bury anything real in a longer run.
		if strings.Contains(err.Error(), "worker") {
			t.Errorf("per-worker copies of the cancel leaked into the error: %v", err)
		}
	})

	t.Run("a deadline counts as a cancel", func(t *testing.T) {
		workers := []WorkerResult{{WriteErr: context.DeadlineExceeded}}
		err := phaseErr(workers, context.DeadlineExceeded, "worker", writeErr)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline not reported: %v", err)
		}
		if strings.Contains(err.Error(), "worker") {
			t.Errorf("per-worker copies of the deadline leaked into the error: %v", err)
		}
	})

	t.Run("every failing worker is named", func(t *testing.T) {
		other := errors.New("input/output error")
		workers := []WorkerResult{{WriteErr: enospc}, {}, {WriteErr: other}}
		err := phaseErr(workers, nil, "worker", writeErr)
		if !errors.Is(err, enospc) || !errors.Is(err, other) {
			t.Errorf("joined error lost one of its causes: %v", err)
		}
	})

	t.Run("a clean phase returns nil", func(t *testing.T) {
		if err := phaseErr([]WorkerResult{{}, {}}, nil, "worker", writeErr); err != nil {
			t.Errorf("clean phase returned %v", err)
		}
	})
}

// TestConfigValidateBoundsAllocatingFields pins the ceilings on the two
// manifest fields that size an allocation.
//
// Config arrives from posixbench.json, which on a shared bench directory is not
// trusted input -- the reason MaxBlockSize exists. Threads and FilesPerWorker
// reach the same allocations (workerRegions makes [][]region of Threads, then
// []region of FilesPerWorker each) but were checked only for > 0, so a manifest
// claiming "workerCount": 100000000 allocated multiple GB, or panicked in
// makeslice, before Verify read a single data byte.
func TestConfigValidateBoundsAllocatingFields(t *testing.T) {
	base := func() Config {
		c := DefaultConfig()
		c.Path = "/tmp/test"
		return c
	}

	t.Run("threads beyond MaxThreads", func(t *testing.T) {
		c := base()
		c.Threads = MaxThreads + 1
		if err := c.Validate(); err == nil {
			t.Fatalf("Validate accepted threads=%d", c.Threads)
		}
	})

	t.Run("region count beyond MaxRegions", func(t *testing.T) {
		c := base()
		c.Threads = 64
		c.FilesPerWorker = MaxRegions/64 + 1
		if err := c.Validate(); err == nil {
			t.Fatalf("Validate accepted %d*%d regions", c.Threads, c.FilesPerWorker)
		}
	})

	// At-the-ceiling cases. Of the four ceilings Validate carries, only
	// MaxBlocksPerRegion had one, so flipping ">" to ">=" on the others
	// survived every gate. The failure mode is a false refusal of a legal
	// config rather than a false verdict, which is why it went unnoticed --
	// nothing legitimate in the suite sat exactly on a bound.
	t.Run("threads exactly at MaxThreads is accepted", func(t *testing.T) {
		c := base()
		c.Threads = MaxThreads
		c.FilesPerWorker = 1
		if err := c.Validate(); err != nil {
			t.Errorf("Validate refused threads=MaxThreads (%d): %v", MaxThreads, err)
		}
	})

	t.Run("blockSize exactly at MaxBlockSize is accepted", func(t *testing.T) {
		c := base()
		c.BlockSize = MaxBlockSize
		// FileSize must stay a multiple of BlockSize and inside the per-region
		// block ceiling, so scale it with the block size rather than leaving
		// the default and tripping a different bound.
		c.FileSize = int64(MaxBlockSize) * 2
		if err := c.Validate(); err != nil {
			t.Errorf("Validate refused blockSize=MaxBlockSize (%d): %v", MaxBlockSize, err)
		}
	})

	t.Run("region count exactly at MaxRegions is accepted", func(t *testing.T) {
		c := base()
		c.Threads = 64
		c.FilesPerWorker = MaxRegions / 64
		if err := c.Validate(); err != nil {
			t.Errorf("Validate refused exactly MaxRegions (%d*%d): %v",
				c.Threads, c.FilesPerWorker, err)
		}
	})

	t.Run("n-to-1 still bounds threads", func(t *testing.T) {
		// FilesPerWorker is unused under n-to-1, but Threads still sizes the
		// outer slice, so the ceiling has to apply on this path too.
		c := base()
		c.Layout = LayoutNto1
		c.Threads = MaxThreads + 1
		if err := c.Validate(); err == nil {
			t.Fatalf("Validate accepted threads=%d under n-to-1", c.Threads)
		}
	})

	t.Run("a large but legal config is still accepted", func(t *testing.T) {
		// The ceilings must not narrow real use: this is far beyond any actual
		// run and must still validate.
		c := base()
		c.Threads = 256
		c.FilesPerWorker = 256
		if err := c.Validate(); err != nil {
			t.Errorf("Validate rejected a legitimate %d*%d config: %v",
				c.Threads, c.FilesPerWorker, err)
		}
	})
}

// TestRunRecordNumbers pins every number the results file publishes.
//
// This is the tool's OUTPUT -- the figures someone quotes in a ticket a year
// from now -- and until this test existed none of them was checked by anything.
// Thirteen of fourteen arithmetic mutations survived both gates: WriteMBps
// returning zero, dividing by the wrong duration, or using 1<<20 where the doc
// says SI 1e6; workerMBps returning a constant; bandwidthStats returning the
// zero value, swapping min and max, or reporting variance as standard
// deviation; Merge losing its bucket accumulation or its sum; and the record
// wiring p99 to the p95 quantile. The exercise script's numeric checks are
// PRESENCE needles, so every one of those was invisible end to end too.
//
// The inputs are synthetic and the expectations hand-computed, so the test does
// not restate the implementation. The numbers are chosen so each mutation lands
// somewhere visibly different: the two per-worker rates give an exact standard
// deviation, and the phase wall clock differs from the IO duration so dividing
// by the wrong one cannot coincide.
func TestRunRecordNumbers(t *testing.T) {
	// Per worker, write phase:
	//   w0: 2 MB over (3s wall - 1s generating) = 2s IO -> 1.0 MB/s
	//   w1: 6 MB over (3s wall - 1s generating) = 2s IO -> 3.0 MB/s
	// so min 1, max 3, mean 2, population stddev exactly 1.
	// Read phase: w0 1 MB / 1s -> 1.0 ; w1 2 MB / 0.5s -> 4.0
	// so min 1, max 4, mean 2.5, population stddev exactly 1.5.
	workers := []WorkerResult{
		{
			WorkerID: 0, BytesWritten: 2_000_000,
			WriteElapsed: 3 * time.Second, GenerateElapsed: 1 * time.Second,
			BytesRead: 1_000_000, ReadElapsed: 1 * time.Second,
		},
		{
			WorkerID: 1, BytesWritten: 6_000_000,
			WriteElapsed: 3 * time.Second, GenerateElapsed: 1 * time.Second,
			BytesRead: 2_000_000, ReadElapsed: 500 * time.Millisecond,
		},
	}

	// Latencies are merged the way Run merges them, so Merge is on the path
	// rather than beside it.
	//
	// The distribution puts q0.50, q0.90, q0.95 and q0.99 each in a DIFFERENT
	// bucket, which is what lets the percentiles be asserted by value below. An
	// earlier fixture had 95 of its 100 samples at or below 10ms, so q0.90 and
	// q0.95 shared a bucket and P95 wired to quantile(0.90) reported the right
	// number for the wrong reason.
	//
	// 50 at 1ms, 40 at 10ms, 5 at 100ms, 4 at 1000ms, 1 at 10000ms: count 100,
	// sum 1.495e10 ns, so the mean is exactly 149.5ms.
	var lat0, lat1 Latency
	for i := 0; i < 50; i++ {
		lat0.Observe(1 * time.Millisecond)
	}
	for i := 0; i < 40; i++ {
		lat0.Observe(10 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		lat1.Observe(100 * time.Millisecond)
	}
	for i := 0; i < 4; i++ {
		lat1.Observe(1000 * time.Millisecond)
	}
	lat1.Observe(10000 * time.Millisecond)
	var merged Latency
	merged.Merge(&lat0)
	merged.Merge(&lat1)

	res := Result{
		Workers:      workers,
		TotalWritten: 8_000_000,
		// Phase wall clock is deliberately NOT the sum or the max of the
		// workers': MBPerSecond must divide by IOElapsed, and a mutation that
		// divides by this instead has to produce a different number.
		WriteElapsed:    4 * time.Second,
		WriteIOElapsed:  2 * time.Second,
		GenerateElapsed: 1 * time.Second,
		TotalRead:       3_000_000,
		ReadElapsed:     1500 * time.Millisecond,
		WriteLatency:    merged,
		ReadLatency:     merged,
	}
	rec := newRunRecord(Manifest{Version: manifestVersion, RunID: "r", Hostname: "h"},
		Environment{ToolVersion: "test"}, res, true)

	eq := func(t *testing.T, what string, got, want float64) {
		t.Helper()
		// Exact rather than within a tolerance, which here would only hide a
		// defect. The bandwidth figures are hand-computed values doubles hold
		// exactly. The percentile figures are not -- 1.048576 is 2^14/5^6 --
		// but the comparison is still exact either way: ms() is one correctly
		// rounded division of an exact nanosecond count by 1e6, and the literal
		// is the decimal for that same rational, so both sides land on the same
		// double.
		if got != want {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}

	t.Run("write phase", func(t *testing.T) {
		w := rec.Write
		if w.Bytes != 8_000_000 {
			t.Errorf("bytes = %d, want 8000000", w.Bytes)
		}
		eq(t, "elapsedSeconds", w.ElapsedSeconds, 4)
		eq(t, "ioElapsedSeconds", w.IOElapsedSeconds, 2)
		eq(t, "generateSeconds", w.GenerateSeconds, 1)
		// 8 MB / 2s IO. Not 2.0 (that would be the 4s wall clock) and not
		// 3.814... (that would be MiB rather than the documented SI MB).
		eq(t, "mbPerSecond", w.MBPerSecond, 4)
		eq(t, "perWorker.min", w.PerWorker.MinMBps, 1)
		eq(t, "perWorker.max", w.PerWorker.MaxMBps, 3)
		eq(t, "perWorker.mean", w.PerWorker.MeanMBps, 2)
		eq(t, "perWorker.stdDev", w.PerWorker.StdDevMBps, 1)
	})

	t.Run("read phase", func(t *testing.T) {
		if rec.Read == nil {
			t.Fatal("read phase is absent from a record built with didRead=true")
		}
		r := *rec.Read
		if r.Bytes != 3_000_000 {
			t.Errorf("bytes = %d, want 3000000", r.Bytes)
		}
		eq(t, "elapsedSeconds", r.ElapsedSeconds, 1.5)
		// Documented to equal ElapsedSeconds for the read phase: there is no
		// generation to subtract.
		eq(t, "ioElapsedSeconds", r.IOElapsedSeconds, 1.5)
		eq(t, "mbPerSecond", r.MBPerSecond, 2)
		eq(t, "perWorker.min", r.PerWorker.MinMBps, 1)
		eq(t, "perWorker.max", r.PerWorker.MaxMBps, 4)
		eq(t, "perWorker.mean", r.PerWorker.MeanMBps, 2.5)
		eq(t, "perWorker.stdDev", r.PerWorker.StdDevMBps, 1.5)
	})

	t.Run("latency, merged from per-worker histograms", func(t *testing.T) {
		l := rec.Write.Latency
		if l.Count != 100 {
			t.Errorf("count = %d, want 100 -- Merge lost samples", l.Count)
		}
		eq(t, "min", l.Min, 1)
		eq(t, "max", l.Max, 10000)
		// sum/count = 1.495e10ns/100 = 149.5ms exactly. Merge dropping its sum
		// accumulation shows up here and nowhere else.
		eq(t, "mean", l.Mean, 149.5)
		// Each percentile by VALUE. Ordering and a lower bound only say the
		// field holds a plausible latency; they cannot say WHICH percentile it
		// holds, and which one it holds is the field's entire meaning. These
		// are the bucket uppers quantile() reports for the distribution above:
		// shifting its bucket search by one moves all three, and wiring a
		// field to a neighbouring quantile moves that one.
		eq(t, "p50", l.P50, 1.048576)
		eq(t, "p95", l.P95, 100.663296)
		eq(t, "p99", l.P99, 1073.741824)
	})

	t.Run("per-worker rows reconcile with the phase aggregates", func(t *testing.T) {
		if len(rec.Workers) != 2 {
			t.Fatalf("got %d worker rows, want 2", len(rec.Workers))
		}
		var sum int64
		for _, w := range rec.Workers {
			sum += w.BytesWritten
		}
		if sum != rec.Write.Bytes {
			t.Errorf("worker bytes sum to %d, phase says %d", sum, rec.Write.Bytes)
		}
		// WriteIOSeconds is WriteSeconds with generation removed: 3s - 1s.
		eq(t, "worker0.writeIOSeconds", rec.Workers[0].WriteIOSeconds, 2)
		eq(t, "worker0.writeSeconds", rec.Workers[0].WriteSeconds, 3)
		eq(t, "worker0.generateSeconds", rec.Workers[0].GenerateSeconds, 1)

		var readSum int64
		for _, w := range rec.Workers {
			readSum += w.BytesRead
		}
		if readSum != rec.Read.Bytes {
			t.Errorf("worker read bytes sum to %d, phase says %d", readSum, rec.Read.Bytes)
		}
		// Row 1, because every assertion above reads row 0: a field wired to a
		// constant, or to its own write-side counterpart, is indistinguishable at
		// row 0 and wrong on every row after it.
		if rec.Workers[1].WorkerID != 1 {
			t.Errorf("workers[1].workerID = %d, want 1; an unattributable row cannot "+
				"show which worker straggled", rec.Workers[1].WorkerID)
		}
		eq(t, "worker1.readSeconds", rec.Workers[1].ReadSeconds, 0.5)
	})

	t.Run("no read phase leaves the read block out entirely", func(t *testing.T) {
		rec := newRunRecord(Manifest{Version: manifestVersion}, Environment{ToolVersion: "test"}, res, false)
		if rec.Read != nil {
			t.Error("read block present for a run with no read phase; a reader " +
				"cannot tell a skipped phase from one that measured zero")
		}
	})

	t.Run("a zero-duration phase reports zero, not NaN or Inf", func(t *testing.T) {
		empty := Result{Workers: []WorkerResult{{WorkerID: 0}}}
		rec := newRunRecord(Manifest{Version: manifestVersion}, Environment{ToolVersion: "test"}, empty, true)
		for _, c := range []struct {
			name string
			got  float64
		}{
			{"write mbPerSecond", rec.Write.MBPerSecond},
			{"read mbPerSecond", rec.Read.MBPerSecond},
			{"write perWorker.mean", rec.Write.PerWorker.MeanMBps},
		} {
			if math.IsNaN(c.got) || math.IsInf(c.got, 0) {
				t.Errorf("%s = %v; a record that has to be parsed a year later "+
					"cannot carry NaN or Inf, which is not even valid JSON", c.name, c.got)
			}
			if c.got != 0 {
				t.Errorf("%s = %v, want 0", c.name, c.got)
			}
		}
	})

	t.Run("a worker with no IO time is excluded from the spread, as documented", func(t *testing.T) {
		// bandwidthStats skips workers whose elapsed is non-positive -- a
		// worker whose whole wall clock went to generation. Pinned because it
		// means min/max/mean describe a SUBSET, which the record does not say.
		ws := []WorkerResult{
			{WorkerID: 0, BytesWritten: 2_000_000, WriteElapsed: 3 * time.Second, GenerateElapsed: 1 * time.Second},
			{WorkerID: 1, BytesWritten: 6_000_000, WriteElapsed: 3 * time.Second, GenerateElapsed: 1 * time.Second},
			{WorkerID: 2, BytesWritten: 1_000_000, WriteElapsed: 1 * time.Second, GenerateElapsed: 1 * time.Second},
		}
		got := bandwidthStats(ws, func(w WorkerResult) (int64, time.Duration) {
			return w.BytesWritten, w.WriteIOElapsed()
		})
		eq(t, "min over the two contributing workers", got.MinMBps, 1)
		eq(t, "max over the two contributing workers", got.MaxMBps, 3)
		eq(t, "mean over the two contributing workers", got.MeanMBps, 2)
	})
}

// TestExpectedBlocksAgreesWithASweep pins the two coverage derivations against
// each other, in both layouts and with more than one file per worker.
//
// ExpectedBlocks earns its keep only by DISAGREEING with a sweep once
// manifestToConfig has narrowed something away, so the risk it carries is the
// opposite one: an ExpectedBlocks that is merely wrong would report every
// healthy run as short, which is a false alarm on good data. These cases are
// what stop that.
func TestExpectedBlocksAgreesWithASweep(t *testing.T) {
	cases := []struct {
		name           string
		layout         Layout
		threads        int
		filesPerWorker int
	}{
		{"1-to-1, one file each", Layout1to1, 3, 1},
		{"1-to-1, three files each", Layout1to1, 2, 3},
		{"n-to-1, one shared file", LayoutNto1, 3, 1},
		{"single worker", Layout1to1, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{
				Path:           dir,
				Threads:        tc.threads,
				BlockSize:      4096,
				FileSize:       4096 * 4,
				FilesPerWorker: tc.filesPerWorker,
				Layout:         tc.layout,
				Kind:           block.KindDecimal,
				Seed:           515,
			}
			r, err := NewRunner(cfg)
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			if _, err := r.Run(context.Background(), false); err != nil {
				t.Fatalf("Run: %v", err)
			}

			m, readCfg, err := ReadManifest(dir)
			if err != nil {
				t.Fatalf("ReadManifest: %v", err)
			}
			v, err := NewVerifier(readCfg)
			if err != nil {
				t.Fatalf("NewVerifier: %v", err)
			}
			sweep, err := v.Verify(nil)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if sweep.Anomalies != 0 {
				t.Fatalf("a freshly written run reported %d anomalies", sweep.Anomalies)
			}
			if got, want := sweep.Blocks, m.ExpectedBlocks(); got != want {
				t.Errorf("swept %d blocks, manifest describes %d: a healthy run must not "+
					"look short, or the guard cries wolf on good data", got, want)
			}
			wantRegions := tc.threads
			if tc.layout != LayoutNto1 {
				wantRegions = tc.threads * tc.filesPerWorker
			}
			if sweep.Regions != wantRegions {
				t.Errorf("Regions = %d, want %d", sweep.Regions, wantRegions)
			}
		})
	}
}

// TestWriteManifestTruncatesAShorterRewrite pins writeJSONFile's O_TRUNC.
//
// Not a hypothetical flag check: writeManifest runs twice per run, and the
// start-of-run document is SHORTER than a completed one because it carries no
// finishedAt. So rerunning the same -run/-hostname rewrites a long document
// with a short one, and without O_TRUNC the old tail survives and the manifest
// stops being valid JSON.
//
// Measured with O_TRUNC dropped: 288 bytes rewritten by 238 bytes stayed 288,
// ReadManifestAt failed with `invalid character '"' after top-level value`, and
// both gates stayed green. End to end the damage needs the rerun to be
// INTERRUPTED -- a completing rerun's final write is the longest and papers
// over its own start -- and an interrupted one then exits 5 where it should
// have reported an unfinished run.
func TestWriteManifestTruncatesAShorterRewrite(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Path: dir, Threads: 2, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
		Layout: Layout1to1, Kind: block.KindDecimal, Seed: 4242,
		Hostname: "nodeA", RunID: "F1",
	}
	m := configToManifest(cfg)
	m.StartedAt = time.Now().UTC()

	// A completed run: the long document.
	done := m.StartedAt.Add(time.Second)
	m.FinishedAt = &done
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("writeManifest (completed): %v", err)
	}
	p := filepath.Join(dir, "posixbench-F1-nodeA.json")
	long, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	// Rerunning the same -run: the short document, no finishedAt.
	m.FinishedAt = nil
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("writeManifest (start of rerun): %v", err)
	}
	short, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(short) >= len(long) {
		t.Fatalf("the rewrite is %d bytes and the original %d; this test needs the "+
			"second document to be SHORTER or it proves nothing", len(short), len(long))
	}

	// The assertion that dies without O_TRUNC: what is on disk is the short
	// document and nothing else.
	if got, _, err := ReadManifestAt(dir, cfg.RunID, cfg.Hostname); err != nil {
		t.Errorf("ReadManifestAt after a shorter rewrite: %v -- the previous "+
			"document's tail was left behind and the manifest is no longer JSON", err)
	} else if got.FinishedAt != nil {
		t.Errorf("finishedAt survived the rewrite (%v); the rerun has not finished", got.FinishedAt)
	}
}

// TestExpectedBlocksRejectsUnusableSizing pins the guard on the fields
// workerRegions allocates and divides on.
//
// ExpectedBlocks is the first EXPORTED door into workerRegions that skips
// Validate, so unlike the rest of this package it has to survive a manifest
// nobody validated -- one hand-edited, truncated, or written by another build.
// Measured before the guard was widened: a negative WorkerCount or
// FilesPerWorker panicked in make(), and a negative FileSize returned a
// NEGATIVE count that classify reads as a shortfall. Deleting the guard
// entirely is an integer divide by zero, and survived every gate.
func TestExpectedBlocksRejectsUnusableSizing(t *testing.T) {
	healthy := Manifest{Version: manifestVersion, WorkerCount: 2, FilesPerWorker: 1,
		FileSize: 16384, BlockSize: 4096, Layout: string(Layout1to1)}
	if got, want := healthy.ExpectedBlocks(), int64(8); got != want {
		t.Fatalf("the healthy manifest this table mutates gives %d, want %d", got, want)
	}

	for _, tc := range []struct {
		name string
		mut  func(*Manifest)
	}{
		{"blockSize zero", func(m *Manifest) { m.BlockSize = 0 }},
		{"blockSize negative", func(m *Manifest) { m.BlockSize = -4096 }},
		{"workerCount negative", func(m *Manifest) { m.WorkerCount = -1 }},
		{"workerCount negative, n-to-1", func(m *Manifest) {
			m.WorkerCount = -1
			m.Layout = string(LayoutNto1)
		}},
		{"filesPerWorker negative", func(m *Manifest) { m.FilesPerWorker = -1 }},
		// -16384 rather than -1 on purpose: the division truncates toward zero,
		// so a FileSize smaller in magnitude than one block returns 0 whether
		// the guard is there or not and would pass for the wrong reason. The
		// unguarded value here is -8.
		{"fileSize negative", func(m *Manifest) { m.FileSize = -16384 }},
		{"fileSize negative, n-to-1", func(m *Manifest) {
			m.FileSize = -16384
			m.Layout = string(LayoutNto1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := healthy
			tc.mut(&m)
			// No recover(): a panic here IS the failure, and letting it kill the
			// test names the field. A guard that returns a wrong number rather
			// than crashing is caught by the comparison below.
			if got := m.ExpectedBlocks(); got != 0 {
				t.Errorf("ExpectedBlocks = %d, want 0 -- sizing this manifest cannot "+
					"describe must not produce a block count the verdict trusts", got)
			}
		})
	}
}

// TestEnsureSeedAndRunID pins the two fill-in-the-blank helpers, which had zero
// coverage between them despite deciding what every filename and every block of
// data in a run is keyed on.
//
// Both are idempotent by contract -- their doc says "call once, before
// Validate" -- and both must leave an operator's explicit value alone, since
// -seed is how a run gets reproduced and -run is how one gets named.
func TestEnsureSeedAndRunID(t *testing.T) {
	t.Run("EnsureSeed fills a zero seed and never leaves it zero", func(t *testing.T) {
		var c Config
		c.EnsureSeed()
		if c.Seed == 0 {
			t.Fatal("EnsureSeed left Seed at zero; NewVerifier refuses a zero seed, " +
				"so a run would be written and then be unverifiable")
		}
		first := c.Seed
		c.EnsureSeed()
		if c.Seed != first {
			t.Errorf("EnsureSeed changed an already-set seed from %d to %d", first, c.Seed)
		}
	})

	t.Run("EnsureSeed leaves an operator's seed alone", func(t *testing.T) {
		c := Config{Seed: 42}
		c.EnsureSeed()
		if c.Seed != 42 {
			t.Errorf("Seed = %d, want the explicit 42: -seed is how a result gets reproduced", c.Seed)
		}
	})

	t.Run("EnsureRunID stamps a sortable, filename-safe id", func(t *testing.T) {
		var c Config
		c.EnsureRunID()
		if c.RunID == "" {
			t.Fatal("EnsureRunID left RunID empty")
		}
		if _, err := time.Parse(RunIDLayout, c.RunID); err != nil {
			t.Errorf("RunID %q does not parse as RunIDLayout (%q): %v", c.RunID, RunIDLayout, err)
		}
		// It goes straight into filenames, so it has to survive the same guard
		// every other path element does.
		if !validPathElement(c.RunID) {
			t.Errorf("the stamped RunID %q is not a valid path element", c.RunID)
		}
		first := c.RunID
		c.EnsureRunID()
		if c.RunID != first {
			t.Errorf("EnsureRunID overwrote an already-set RunID %q with %q", first, c.RunID)
		}
	})

	t.Run("EnsureRunID leaves an operator's run id alone", func(t *testing.T) {
		c := Config{RunID: "nightly"}
		c.EnsureRunID()
		if c.RunID != "nightly" {
			t.Errorf("RunID = %q, want the explicit \"nightly\"", c.RunID)
		}
	})
}

// TestListManifestsOrdersRunsForSelection pins ListManifests' documented order
// -- "newest RunID last" -- because that order is what an operator reads.
//
// pickRun prints the refs verbatim as the selector list when a directory holds
// more than one run, so a reversed sort presents the oldest run as the newest.
// Nothing pinned it, and the default RunID is a sortable UTC timestamp
// precisely so that lexical order IS chronological order.
func TestListManifestsOrdersRunsForSelection(t *testing.T) {
	dir := t.TempDir()
	// Written out of order deliberately, and with two hosts sharing a runID so
	// the secondary sort is exercised too.
	for _, r := range []struct{ runID, host string }{
		{"20260903T120000Z", "nodeB"},
		{"20260901T090000Z", "nodeA"},
		{"20260903T120000Z", "nodeA"},
		{"20260902T235959Z", "nodeA"},
	} {
		if err := writeManifest(dir, Manifest{Version: manifestVersion, RunID: r.runID, Hostname: r.host}); err != nil {
			t.Fatalf("writeManifest: %v", err)
		}
	}
	refs, err := ListManifests(dir)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	got := make([]string, 0, len(refs))
	for _, r := range refs {
		got = append(got, r.RunID+"/"+r.Hostname)
	}
	want := []string{
		"20260901T090000Z/nodeA",
		"20260902T235959Z/nodeA",
		"20260903T120000Z/nodeA",
		"20260903T120000Z/nodeB",
	}
	if !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v (oldest first, newest last, hostname breaking ties)", got, want)
	}
}

// TestListManifestsNamespace pins what the manifest glob does and does not
// claim. ListManifests had no unit test at all, and it decides which runs a
// verify can even see.
//
// Two defects live here, both of them "the listing does not describe the
// directory". A results file used to share the posixbench-*.json namespace and
// be excluded from it by name, so a manifest whose runID began "results" was
// filtered out and the tool reported no manifest over one sitting right there.
// And parsing was treated as proof of manifest-hood, so any JSON object in the
// namespace became a run with an empty runID.
func TestListManifestsNamespace(t *testing.T) {
	write := func(t *testing.T, dir, runID, host string) {
		t.Helper()
		if err := writeManifest(dir, Manifest{Version: manifestVersion, RunID: runID, Hostname: host}); err != nil {
			t.Fatalf("writeManifest %s/%s: %v", runID, host, err)
		}
		if err := writeRunRecord(dir, RunRecord{
			Version:  resultsVersion,
			Manifest: Manifest{Version: manifestVersion, RunID: runID, Hostname: host},
		}); err != nil {
			t.Fatalf("writeRunRecord %s/%s: %v", runID, host, err)
		}
	}

	t.Run("a results file beside a manifest is not itself listed", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "r1", "nodeA")
		refs, err := ListManifests(dir)
		if err != nil {
			t.Fatalf("ListManifests: %v", err)
		}
		if len(refs) != 1 {
			t.Fatalf("got %d refs, want 1 (the manifest only): %+v", len(refs), refs)
		}
		if refs[0].RunID != "r1" || refs[0].Hostname != "nodeA" {
			t.Errorf("ref = %+v, want runID r1 host nodeA", refs[0])
		}
	})

	t.Run("a runID beginning 'results' is still listed", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "results1", "nodeA")
		refs, err := ListManifests(dir)
		if err != nil {
			t.Fatalf("ListManifests: %v", err)
		}
		if len(refs) != 1 {
			t.Fatalf("got %d refs, want 1: a runID sharing a prefix with the results "+
				"file must not be filtered out of its own namespace: %+v", len(refs), refs)
		}
		if refs[0].RunID != "results1" {
			t.Errorf("ref = %+v, want runID results1", refs[0])
		}
	})

	t.Run("a stray JSON object in the namespace is reported, not listed", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "r1", "nodeA")
		stray := filepath.Join(dir, "posixbench-stray.json")
		if err := os.WriteFile(stray, []byte("{}\n"), 0644); err != nil {
			t.Fatalf("write stray: %v", err)
		}
		refs, err := ListManifests(dir)
		if err == nil {
			t.Fatalf("ListManifests accepted a stray {} and returned %+v; it lists as a "+
				"run with an empty runID and makes the real run ambiguous", refs)
		}
		if !strings.Contains(err.Error(), "posixbench-stray.json") {
			t.Errorf("err = %v, want it to name the offending file", err)
		}
	})

	t.Run("a FUTURE version in the namespace is reported, not listed", func(t *testing.T) {
		// Both directions, not just the old-manifest one. TestManifestVersionGate
		// covers both for ReadManifestAt, but this gate was pinned only from
		// below: relaxing it to `m.Version < manifestVersion` survived every gate,
		// and it degrades a precise "not a version N manifest" at exit 5 into an
		// ambiguous-run refusal at exit 2 once a newer build's manifest shares a
		// directory with this one's.
		dir := t.TempDir()
		write(t, dir, "r1", "nodeA")
		future := filepath.Join(dir, "posixbench-r2-nodeA.json")
		if err := writeJSONFile(future, []byte(fmt.Sprintf(
			`{"version":%d,"runId":"r2","hostname":"nodeA"}`+"\n", manifestVersion+1))); err != nil {
			t.Fatalf("write future manifest: %v", err)
		}
		refs, err := ListManifests(dir)
		if err == nil {
			t.Fatalf("ListManifests accepted a version %d manifest and returned %+v; "+
				"a manifest this build cannot read must be refused by name, not "+
				"offered as a run", manifestVersion+1, refs)
		}
		if !strings.Contains(err.Error(), "posixbench-r2-nodeA.json") {
			t.Errorf("err = %v, want it to name the offending file", err)
		}
	})

	t.Run("a stale-version manifest is reported by the listing, not offered as a run", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "r1", "nodeA")
		if err := writeManifest(dir, Manifest{Version: manifestVersion - 1, RunID: "old", Hostname: "nodeA"}); err != nil {
			t.Fatalf("writeManifest: %v", err)
		}
		if _, err := ListManifests(dir); err == nil {
			t.Error("a manifest this build cannot read was offered as a selectable run")
		}
	})
}

// TestVerifySeparatesUnreadableFromWrong pins the one split both of Verify's
// error arms make: "the tool could not read it" is never "the data is wrong".
// Each arm has an anomaly side that counts and carries on, and an abort side
// that stops -- and each side is a subtest here.
//
//	open (verifier.go)   ErrNotExist -> anomaly    | anything else -> abort
//	read (verifier.go)   EOF         -> anomaly    | anything else -> abort
//
// The abort this replaces was worse than it looked. A deleted file exited 5,
// "the tool could not do its job", while the SAME data loss by truncation
// exited 1; and because the sweep stopped at the first unopenable file, real
// corruption in the files after it went entirely unreported, -verbose and all.
//
// The two abort subtests are what keep the fix honest, and they are the halves
// that rot quietly: counting EVERY failure as an anomaly would close the
// masking and report an unreadable mount as corrupt data -- an invented anomaly
// being just as much a false verdict as a missed one, and the one that teaches
// an operator to disbelieve the real ones. Measured before the read arm's
// subtest existed: widening it to accept every read error left both packages
// green, and verify then printed "FAIL 4 anomaly(s)" over four blocks it had
// never read.
//
// Note the read arm's predicate also names io.ErrUnexpectedEOF, which no
// subtest here drives and none can: os.File.ReadAt returns plain io.EOF for
// both a partial read and a past-the-end read, so that clause is unreachable
// at this call site. A mutation deleting it survives correctly, and is not a
// coverage gap to close.
func TestVerifySeparatesUnreadableFromWrong(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 4
		threads   = 3
	)
	newRun := func(t *testing.T) (string, Config) {
		t.Helper()
		dir := t.TempDir()
		cfg := Config{
			Path:           dir,
			Threads:        threads,
			BlockSize:      blockSize,
			FileSize:       fileSize,
			FilesPerWorker: 1,
			Layout:         Layout1to1,
			Kind:           block.KindDecimal,
			Seed:           4242,
		}
		r, err := NewRunner(cfg)
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}
		if _, err := r.Run(context.Background(), false); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return dir, cfg
	}
	// Returns the whole Sweep, not just its anomaly count: Regions and Blocks
	// are what the "Verified:" line reports, and their documented "not counted
	// on failure" clauses can only be checked on a sweep that failed.
	runSweep := func(t *testing.T, cfg Config) (Sweep, []Anomaly, error) {
		t.Helper()
		v, err := NewVerifier(cfg)
		if err != nil {
			t.Fatalf("NewVerifier: %v", err)
		}
		var got []Anomaly
		sweep, err := v.Verify(func(a Anomaly) error {
			got = append(got, a)
			return nil
		})
		return sweep, got, err
	}

	t.Run("a missing file is counted and does not mask later corruption", func(t *testing.T) {
		dir, cfg := newRun(t)
		// Worker 0's file goes away entirely; worker 2 gets three genuinely
		// corrupt blocks AFTER it in the sweep order.
		if err := os.Remove(filepath.Join(dir, "pbench-w000-f000.dat")); err != nil {
			t.Fatalf("remove worker 0's file: %v", err)
		}
		victim := filepath.Join(dir, "pbench-w002-f000.dat")
		f, err := os.OpenFile(victim, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open worker 2's file: %v", err)
		}
		for _, off := range []int64{100, blockSize + 100, 2*blockSize + 100} {
			if _, err := f.WriteAt([]byte{0xFF}, off); err != nil {
				t.Fatalf("corrupt byte at %d: %v", off, err)
			}
		}
		f.Close()

		sweep, got, err := runSweep(t, cfg)
		if err != nil {
			t.Fatalf("Verify returned an error (%v); a missing file must not abort the sweep", err)
		}
		// One for the absent file plus one per corrupt block. Asserting the
		// exact count, because the defect here is under-reporting and a
		// "> 0" assertion would be satisfied by the absent file alone.
		if want := 4; sweep.Anomalies != want {
			t.Errorf("anomalies = %d, want %d (1 missing file + 3 corrupt blocks)", sweep.Anomalies, want)
		}
		// Coverage on a FAILING sweep, which is the only place the increments'
		// position can matter: w0's file is gone so it contributes no region,
		// and the other two contribute 4 blocks each. Over clean data both
		// counters are right wherever the ++ sits, so every other assertion on
		// them in this package is blind to a sweep that silently shrank -- and
		// over-reporting here would make the "Verified:" line vouch for regions
		// it never opened.
		if want := 2; sweep.Regions != want {
			t.Errorf("regions = %d, want %d -- the absent file must not be counted as walked",
				sweep.Regions, want)
		}
		if want := int64(8); sweep.Blocks != want {
			t.Errorf("blocks = %d, want %d (2 surviving files x 4 blocks; a mismatched "+
				"block WAS read and does count)", sweep.Blocks, want)
		}
		var sawMissing, sawMismatch int
		for _, a := range got {
			switch {
			case strings.Contains(a.Err.Error(), "missing data file"):
				sawMissing++
				if a.BlockIndex != -1 {
					t.Errorf("missing-file anomaly has blockIndex %d, want -1: no block is named", a.BlockIndex)
				}
			case strings.Contains(a.Err.Error(), "data mismatch"):
				sawMismatch++
			}
		}
		if sawMissing != 1 {
			t.Errorf("missing-file anomalies = %d, want 1", sawMissing)
		}
		// The point of the whole fix: the sweep reached worker 2.
		if sawMismatch != 3 {
			t.Errorf("data-mismatch anomalies = %d, want 3 -- the sweep stopped before "+
				"reaching the corrupt file, so its corruption went unreported", sawMismatch)
		}
	})

	t.Run("a truncated file's unread blocks are anomalies and are not counted", func(t *testing.T) {
		// The short-read arm is the missing-file arm's sibling -- Verify's own
		// comment says so -- and Blocks carries the same "not counted on
		// failure" clause as Regions. Truncation is how that clause is reached
		// without deleting anything: the region opens, so it IS counted, while
		// three of its four blocks cannot be read and must not be.
		dir, cfg := newRun(t)
		victim := filepath.Join(dir, "pbench-w001-f000.dat")
		if err := os.Truncate(victim, blockSize); err != nil {
			t.Fatalf("truncate worker 1's file to one block: %v", err)
		}

		sweep, got, err := runSweep(t, cfg)
		if err != nil {
			t.Fatalf("Verify returned an error (%v); a short read must not abort the sweep", err)
		}
		if want := 3; sweep.Anomalies != want {
			t.Errorf("anomalies = %d, want %d (blocks 1..3 of the truncated file)", sweep.Anomalies, want)
		}
		if want := 3; sweep.Regions != want {
			t.Errorf("regions = %d, want %d -- a truncated file still opened and was walked",
				sweep.Regions, want)
		}
		// 4 + 1 + 4. The three blocks past the truncation point were never
		// compared, so counting them would report 48 KiB verified out of a
		// file set holding 36 KiB of readable data.
		if want := int64(9); sweep.Blocks != want {
			t.Errorf("blocks = %d, want %d -- a block that could not be read must not "+
				"count toward what the sweep verified", sweep.Blocks, want)
		}
		for _, a := range got {
			if !strings.Contains(a.Err.Error(), "short read") {
				t.Errorf("anomaly %q is not a short read; a truncated file must not "+
					"report as a data mismatch", a.Err)
			}
		}
	})

	t.Run("a file that cannot be read is an error, not an anomaly", func(t *testing.T) {
		dir, cfg := newRun(t)
		// A symlink to itself: open fails with ELOOP rather than ENOENT, so it
		// stands in for every "could not read it" failure -- EACCES, EIO from a
		// wedged metadata server -- without needing root or a real fault.
		p := filepath.Join(dir, "pbench-w001-f000.dat")
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if err := os.Symlink(p, p); err != nil {
			t.Fatalf("symlink loop: %v", err)
		}

		_, _, err := runSweep(t, cfg)
		if err == nil {
			t.Fatal("Verify returned nil for an unreadable file; an unreadable file is not a verdict on the data")
		}
		if errors.Is(err, os.ErrNotExist) {
			t.Errorf("err = %v, want something other than ErrNotExist -- the file is there, it cannot be read", err)
		}
		if !errors.Is(err, syscall.ELOOP) {
			t.Errorf("err = %v, want it to wrap ELOOP", err)
		}
	})

	t.Run("a read that fails is an error, not an anomaly", func(t *testing.T) {
		dir, cfg := newRun(t)
		// A directory at the data file's name. open SUCCEEDS and the failure
		// arrives from the read as EISDIR, which is the whole point: it is the
		// one case no open-arm predicate can catch, so it reaches the read arm
		// and nothing else does. Same spirit as the ELOOP twin above -- a real
		// errno, no root, no injected fault.
		p := filepath.Join(dir, "pbench-w001-f000.dat")
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if err := os.Mkdir(p, 0755); err != nil {
			t.Fatalf("mkdir at the data file's name: %v", err)
		}

		sweep, got, err := runSweep(t, cfg)
		if err == nil {
			t.Fatal("Verify returned nil for a file it could not read; that is not a verdict on the data")
		}
		if !errors.Is(err, syscall.EISDIR) {
			t.Errorf("err = %v, want it to wrap EISDIR", err)
		}
		// The counts, not just the error: widening the predicate turns each
		// unread block into an anomaly, so the tool reports FAIL over blocks it
		// never read. That is a false verdict in the opposite direction from a
		// missed one, and it is the reason this arm cannot simply count
		// everything.
		if sweep.Anomalies != 0 {
			t.Errorf("anomalies = %d, want 0 -- a block that could not be READ must not be "+
				"reported as data that is WRONG", sweep.Anomalies)
		}
		for _, a := range got {
			t.Errorf("unexpected anomaly %q: an unreadable file accuses the filesystem of "+
				"nothing", a.Err)
		}
	})
}

// TestManifestRecordsWhetherTheRunFinished pins the completion flag end to end:
// Run stamps FinishedAt only when it completes, and ReadManifestAt hands that
// back so a caller can act on it.
//
// Both halves need pinning and neither had it. The producer could stop writing
// FinishedAt with every gate green, and the flag had no reader at all -- Config
// has no completion field, so verify reported the blocks an interrupted run
// never reached as corrupt data.
//
// Note what this deliberately does NOT assert: that an unstamped manifest
// implies short data. A cancel in the read phase skips the completion write
// over a dataset the write phase finished, so the flag describes the RUN and
// not the bytes. TestRunCancelDuringReadPhase covers that path.
func TestManifestRecordsWhetherTheRunFinished(t *testing.T) {
	t.Run("a completed run is stamped, and the stamp reads back", func(t *testing.T) {
		dir := t.TempDir()
		cfg := Config{
			Path:      dir,
			Threads:   2,
			BlockSize: 4096,
			FileSize:  4096 * 2,
			// Layout1to1 requires FilesPerWorker; Validate rejects zero.
			FilesPerWorker: 1,
			Layout:         Layout1to1,
			Kind:           block.KindDecimal,
			Seed:           31,
		}
		r, err := NewRunner(cfg)
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}
		if _, err := r.Run(context.Background(), false); err != nil {
			t.Fatalf("Run: %v", err)
		}

		m, _, err := ReadManifest(dir)
		if err != nil {
			t.Fatalf("ReadManifest: %v", err)
		}
		if m.FinishedAt == nil {
			t.Fatal("FinishedAt is nil after a run that completed; a reader cannot " +
				"distinguish this run from an interrupted one")
		}
		if m.FinishedAt.Before(m.StartedAt) {
			t.Errorf("FinishedAt %s is before StartedAt %s", m.FinishedAt, m.StartedAt)
		}
	})

	t.Run("a cancelled run is not stamped", func(t *testing.T) {
		dir := t.TempDir()
		r, err := NewRunner(cancellableConfig(dir))
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(15 * time.Millisecond)
			cancel()
		}()
		if _, err := r.Run(ctx, true); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}

		m, _, err := ReadManifest(dir)
		if err != nil {
			t.Fatalf("ReadManifest: %v", err)
		}
		if m.FinishedAt != nil {
			t.Errorf("FinishedAt = %s after a cancelled run, want nil: its absence is "+
				"the only record that the run did not complete", m.FinishedAt)
		}
	})
}

// TestWriteSitesRefuseSymlinks pins O_NOFOLLOW at every site in this package
// that creates or truncates a file.
//
// Each of those paths is a name the run composed itself under the operator's
// -path, and on a shared mount that directory is usually writable by others. A
// symlink planted at one redirects a privileged write out of -path and destroys
// what it points at while the run still reports success.
//
// Table-driven over every site rather than one test per site, deliberately. The
// n-to-1 branch alone used to open without O_NOFOLLOW and got this test; the
// guard then reached three sites of five with coverage on that one, so dropping
// it from the 1-to-1 default, from preallocate, or from either JSON writer left
// both gates green. Adding a site here is cheaper than noticing that later.
func TestWriteSitesRefuseSymlinks(t *testing.T) {
	cfg := Config{BlockSize: 4096, Kind: block.KindDecimal, Seed: 1}
	const runID, host = "run1", "host1"

	cases := []struct {
		name string
		// target names the file the site under test will write, so the symlink
		// is planted at the name the tool composes rather than a stand-in.
		target func(dir string) string
		write  func(dir string) error
	}{
		{
			name:   "preallocate, the n-to-1 shared file",
			target: func(dir string) string { return filepath.Join(dir, "shared.dat") },
			write: func(dir string) error {
				return preallocate(filepath.Join(dir, "shared.dat"), int64(cfg.BlockSize))
			},
		},
		{
			name:   "writeRegion, exclusive: the 1-to-1 default",
			target: func(dir string) string { return filepath.Join(dir, "w000.dat") },
			write: func(dir string) error {
				reg := region{path: filepath.Join(dir, "w000.dat"), length: int64(cfg.BlockSize), exclusive: true}
				_, _, err := writeRegion(context.Background(), cfg, reg, make([]byte, cfg.BlockSize), &Latency{})
				return err
			},
		},
		{
			name:   "writeRegion, shared: n-to-1",
			target: func(dir string) string { return filepath.Join(dir, "shared.dat") },
			write: func(dir string) error {
				reg := region{path: filepath.Join(dir, "shared.dat"), length: int64(cfg.BlockSize), exclusive: false}
				_, _, err := writeRegion(context.Background(), cfg, reg, make([]byte, cfg.BlockSize), &Latency{})
				return err
			},
		},
		{
			name:   "writeManifest",
			target: func(dir string) string { return manifestPath(dir, runID, host) },
			write: func(dir string) error {
				return writeManifest(dir, Manifest{Version: manifestVersion, RunID: runID, Hostname: host})
			},
		},
		{
			name:   "writeRunRecord",
			target: func(dir string) string { return resultsPath(dir, runID, host) },
			write: func(dir string) error {
				return writeRunRecord(dir, RunRecord{
					Version:  resultsVersion,
					Manifest: Manifest{Version: manifestVersion, RunID: runID, Hostname: host},
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			canary := filepath.Join(dir, "canary")
			const original = "do not touch"
			if err := os.WriteFile(canary, []byte(original), 0644); err != nil {
				t.Fatalf("write canary: %v", err)
			}
			if err := os.Symlink(canary, tc.target(dir)); err != nil {
				t.Fatalf("symlink: %v", err)
			}

			err := tc.write(dir)
			if err == nil {
				t.Fatal("wrote through a planted symlink and reported success")
			}
			// ELOOP specifically. Asserting merely "an error" would pass with
			// the guard gone and something else failing for its own reason.
			if !errors.Is(err, syscall.ELOOP) {
				t.Errorf("err = %v, want it to wrap ELOOP", err)
			}

			got, err := os.ReadFile(canary)
			if err != nil {
				t.Fatalf("read canary: %v", err)
			}
			if string(got) != original {
				t.Errorf("canary = %q, want %q: the write followed the symlink", got, original)
			}
		})
	}
}

// TestLatencyExactFieldsAndBoundedPercentiles pins both halves of Latency's
// contract: the scalar fields are exact, and the percentiles -- which come from
// a log-scale histogram -- read HIGH but never by more than the documented
// bucket width. A histogram whose error is unbounded is worse than no
// percentile at all, because it looks authoritative.
func TestLatencyExactFieldsAndBoundedPercentiles(t *testing.T) {
	var l Latency
	const n = 10000
	for i := 1; i <= n; i++ {
		l.Observe(time.Duration(i) * time.Microsecond)
	}
	if l.count != n {
		t.Errorf("count = %d, want %d", l.count, n)
	}
	if got, want := time.Duration(l.min), 1*time.Microsecond; got != want {
		t.Errorf("min = %s, want %s", got, want)
	}
	if got, want := time.Duration(l.max), n*time.Microsecond; got != want {
		t.Errorf("max = %s, want %s", got, want)
	}
	// Sum is exact, so the mean is too.
	wantSum := uint64(n) * (n + 1) / 2 * uint64(time.Microsecond)
	if l.sum != wantSum {
		t.Errorf("sum = %d, want %d", l.sum, wantSum)
	}

	// With a uniform 1..n distribution the true q-quantile is q*n microseconds.
	// Assert the reported value never UNDER-reports (a tail that looks better
	// than it is would be the dangerous direction) and is within the bucket
	// width the doc promises.
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		truth := time.Duration(q*n) * time.Microsecond
		got := l.quantile(q)
		if got < truth {
			t.Errorf("q%.2f = %s, under-reports the true %s", q, got, truth)
		}
		// The overshoot is bounded by one sub-bucket's width. The widest is
		// the first of an octave, whose width relative to its own lower edge
		// is 1/latSubBuckets -- derived here rather than written as a figure,
		// so it tracks latSubBits instead of going stale against it.
		if over := float64(got-truth) / float64(truth); over > 1.0/latSubBuckets {
			t.Errorf("q%.2f = %s, %.1f%% above the true %s; the bound is one sub-bucket's width, %.1f%%",
				q, got, over*100, truth, 100.0/latSubBuckets)
		}
	}
}

// TestLatencyBucketBoundsHoldForEveryOctave pins the two properties every
// reported percentile rests on: a bucket's bound is strictly above every sample
// the bucket can hold, and the bounds rise with the index. Containment is what
// stops a percentile coming out below the exact min; monotonicity is what stops
// p99 coming out below p95. Both are impossible records, and a reader has no
// way to tell either from a merely strange workload.
//
// This is a property rather than a fixture on purpose. The defect it was
// written for lived in ONE of the four sub-bucket arms, so any distribution
// whose probed quantiles miss that arm passes while a quarter of the range
// reports its own lower edge -- which is exactly how it shipped.
func TestLatencyBucketBoundsHoldForEveryOctave(t *testing.T) {
	for n := uint64(0); n < 1<<20; n++ {
		if i := latBucket(n); latBucketUpper(i) <= n {
			t.Fatalf("n=%d sits in bucket %d, whose upper bound is %d: not above its own sample",
				n, i, latBucketUpper(i))
		}
	}
	// The sweep above covers the low octaves densely. Walk every sub-bucket
	// edge of the higher ones too, since the arm a defect hides in is the one
	// a sparser fixture misses.
	for exp := 20; exp < 64; exp++ {
		step := uint64(1) << (exp - latSubBits)
		for sub := uint64(0); sub < latSubBuckets; sub++ {
			lo := uint64(1)<<exp + sub*step
			for _, n := range []uint64{lo, lo + step/2, lo + step - 1} {
				i := latBucket(n)
				// The last bucket's true bound is one past the top of uint64,
				// so it saturates and containment cannot be strict there.
				// Unreachable from Observe either way: time.Duration is int64.
				if latBucketUpper(i) == math.MaxUint64 {
					continue
				}
				if latBucketUpper(i) <= n {
					t.Fatalf("exp=%d sub=%d: n=%d sits in bucket %d, whose upper bound is %d",
						exp, sub, n, i, latBucketUpper(i))
				}
			}
		}
	}
	for i := 1; i < latBuckets; i++ {
		if latBucketUpper(i) < latBucketUpper(i-1) {
			t.Fatalf("bucket %d's upper bound (%d) is below bucket %d's (%d); a later percentile could report lower than an earlier one",
				i, latBucketUpper(i), i-1, latBucketUpper(i-1))
		}
	}
}

func TestLatencyMergeAndEmpty(t *testing.T) {
	var empty Latency
	if got := empty.quantile(0.99); got != 0 {
		t.Errorf("quantile on an empty histogram = %s, want 0", got)
	}
	empty.Merge(&Latency{})
	if empty.count != 0 || empty.min != 0 || empty.max != 0 {
		t.Errorf("merging empty into empty left count=%d min=%d max=%d, want all zero",
			empty.count, empty.min, empty.max)
	}

	// Merging an EMPTY histogram into a populated one must not drag min down to
	// zero. This was the suite's one assertion-free line -- the call was made
	// with a "must not corrupt min" comment and nothing was read back, so
	// deleting Merge's own guard survived every gate.
	var populated Latency
	populated.Observe(5 * time.Millisecond)
	populated.Merge(&Latency{})
	if got, want := time.Duration(populated.min), 5*time.Millisecond; got != want {
		t.Errorf("min = %s after merging an empty histogram in, want %s", got, want)
	}
	if populated.count != 1 {
		t.Errorf("count = %d after merging an empty histogram in, want 1", populated.count)
	}

	var a, b Latency
	a.Observe(5 * time.Millisecond)
	b.Observe(1 * time.Millisecond)
	b.Observe(9 * time.Millisecond)
	a.Merge(&b)
	if a.count != 3 {
		t.Errorf("merged count = %d, want 3", a.count)
	}
	if got, want := time.Duration(a.min), 1*time.Millisecond; got != want {
		t.Errorf("merged min = %s, want %s", got, want)
	}
	if got, want := time.Duration(a.max), 9*time.Millisecond; got != want {
		t.Errorf("merged max = %s, want %s", got, want)
	}
}

// TestLatencyObserveCountsEveryOperation pins Observe's stated contract, which
// is a decision rather than an accident: "Non-positive durations are counted at
// zero rather than dropped, so Count always equals the number of operations."
//
// A monotonic clock should not produce a non-positive duration, but if one ever
// does, dropping it would silently decouple Count from the operation count --
// and Count is the denominator of the reported mean. Skipping such samples
// survived every gate.
func TestLatencyObserveCountsEveryOperation(t *testing.T) {
	var l Latency
	l.Observe(3 * time.Millisecond)
	l.Observe(0)
	l.Observe(-1 * time.Millisecond)
	if l.count != 3 {
		t.Errorf("count = %d after three Observe calls, want 3: a dropped sample "+
			"makes Count stop meaning the number of operations", l.count)
	}
	if l.min != 0 {
		t.Errorf("min = %d, want 0: a non-positive duration is recorded as zero", l.min)
	}
	if got, want := time.Duration(l.max), 3*time.Millisecond; got != want {
		t.Errorf("max = %s, want %s", got, want)
	}
}

// TestBlockOrder pins what a Pattern actually changes -- and what it must not.
//
// The end-to-end check that a randomly written file still verifies would pass
// even if -pattern random were a silent no-op, because content is keyed on
// block index. So the discriminating assertions are here: random must really
// reorder, must be a permutation rather than a resampling (every block written
// exactly once), must be reproducible from the seed, and must not march two
// regions in lockstep.
func TestBlockOrder(t *testing.T) {
	const blocks = 512
	seq := blockOrder(Config{Pattern: PatternSequential, Seed: 7}, 0, blocks)
	for i, v := range seq {
		if v != int64(i) {
			t.Fatalf("sequential order[%d] = %d, want %d", i, v, i)
		}
	}
	if zero := blockOrder(Config{Seed: 7}, 0, blocks); !slices.Equal(zero, seq) {
		t.Error("the zero-value Pattern must behave as sequential")
	}

	rnd := blockOrder(Config{Pattern: PatternRandom, Seed: 7}, 0, blocks)
	if slices.Equal(rnd, seq) {
		t.Error("random order is identical to sequential; the pattern is a no-op")
	}
	// A permutation, not a resampling: every block exactly once, or the run
	// would silently write some blocks twice and others never.
	sorted := slices.Clone(rnd)
	slices.Sort(sorted)
	if !slices.Equal(sorted, seq) {
		t.Error("random order is not a permutation of 0..n-1")
	}

	if again := blockOrder(Config{Pattern: PatternRandom, Seed: 7}, 0, blocks); !slices.Equal(again, rnd) {
		t.Error("random order is not reproducible from the same seed; a perf result could not be rerun")
	}
	if other := blockOrder(Config{Pattern: PatternRandom, Seed: 7}, 1, blocks); slices.Equal(other, rnd) {
		t.Error("two regions of one run got the same order; they would march in lockstep")
	}
	if otherSeed := blockOrder(Config{Pattern: PatternRandom, Seed: 8}, 0, blocks); slices.Equal(otherSeed, rnd) {
		t.Error("a different run seed produced the same order")
	}
}

// TestLoadEnvKV pins the parser against the shape it exists to read:
// /proc/fs/beegfs/<client>/config, which is "key = value" with spaces, plus the
// junk a real file accumulates. Skipping a malformed line rather than failing
// is deliberate -- a banner or a trailing note must not cost a run its context.
func TestLoadEnvKV(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config")
	body := "# a comment\n\n" +
		"cfgFile = /etc/beegfs/beegfs-client.conf\n" +
		"tuneFileCacheType = buffered\n" +
		"connMaxInternodeNum = 12\n" +
		"a line with no equals sign\n" +
		"  spaced   =   value with spaces  \n" +
		"empty =\n" +
		"dup = first\ndup = last\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadEnvKV(p)
	if err != nil {
		t.Fatalf("LoadEnvKV: %v", err)
	}
	want := map[string]string{
		"cfgFile":             "/etc/beegfs/beegfs-client.conf",
		"tuneFileCacheType":   "buffered",
		"connMaxInternodeNum": "12",
		"spaced":              "value with spaces",
		"empty":               "",
		"dup":                 "last", // last wins, as the config files themselves do
	}
	if len(got) != len(want) {
		t.Errorf("parsed %d keys, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q = %q, want %q", k, got[k], v)
		}
	}
	if _, err := LoadEnvKV(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Error("a missing file must be an error, not an empty map: a run would " +
			"otherwise record no context and look identical to one that had none")
	}
}

// TestRunMergesEveryWorkersLatencies pins that both phases' per-worker latency
// histograms reach the Result.
//
// The histograms are the only record of the tail: RunRecord's p50/p95/p99 come
// from these, and they are built per worker and merged once at the end of each
// phase. Deleting either merge loop left both packages green, and the results
// file then reported latencyMs with count 0 and every percentile zero -- a
// published number that is silently absent rather than wrong, which nothing was
// looking at.
//
// The count is asserted exactly rather than as "> 0", because "> 0" is
// satisfied by merging only the first worker, which is the same defect at a
// different size. writeRegion and readRegion Observe once per block, so a
// completed phase has exactly one sample per block -- and the block count comes
// from the Result's own byte total, so this stays an identity rather than a
// restatement of the test's configuration.
func TestRunMergesEveryWorkersLatencies(t *testing.T) {
	const (
		blockSize = 4096
		fileSize  = 4096 * 8
		threads   = 3 // more than one, so merging a single worker is visibly short
	)
	dir := t.TempDir()
	cfg := Config{
		Path:           dir,
		Threads:        threads,
		BlockSize:      blockSize,
		FileSize:       fileSize,
		FilesPerWorker: 1,
		Layout:         Layout1to1,
		Kind:           block.KindDecimal,
		Seed:           11,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(context.Background(), true)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, tc := range []struct {
		phase string
		lat   *Latency
		bytes int64
	}{
		{"write", &res.WriteLatency, res.TotalWritten},
		{"read", &res.ReadLatency, res.TotalRead},
	} {
		want := uint64(tc.bytes / int64(blockSize))
		if want == 0 {
			t.Fatalf("%s phase moved no bytes; the test cannot pin anything", tc.phase)
		}
		if tc.lat.count != want {
			t.Errorf("%s latency count = %d, want %d (one sample per block, %d bytes at %d) -- "+
				"a short count means some workers' samples never reached the Result, so the "+
				"published percentiles describe only part of the run",
				tc.phase, tc.lat.count, want, tc.bytes, blockSize)
		}
	}
}
