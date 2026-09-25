package posixbench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"golang.org/x/sys/unix"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// manifestVersion gates whether a manifest's run can be verified by this build.
//
// It has to move with the body derivation, not just with the manifest's own
// JSON shape: the default verify path regenerates every body from the manifest
// seed and compares byte-for-byte, so a directory written by a build with a
// different GenerateBody reports "data mismatch" on every block --
// indistinguishable from real corruption on the filesystem under test. Per-node
// manifests are designed to share one directory, so a single un-upgraded node
// is enough to produce that. Rejecting the manifest names the real problem.
//
// 2: block.HeaderVersion 4, GenerateBody's mixSeed change.
// 3: dropped the ECC-only verification mode and its "noXattr" key.
const manifestVersion = 3
const manifestFileName = "posixbench.json"

// ManifestPath returns the path of the manifest file for dir, runID and hostname.
// When hostname is non-empty the filename is posixbench-{hostname}.json so
// that multiple nodes can share a directory without clobbering each other.
func ManifestPath(dir, runID, hostname string) string {
	return manifestPath(dir, runID, hostname)
}

// Manifest is the on-disk record of a run. Most of it is what Verifier needs to
// reconstruct the expected data for every block; StartedAt/FinishedAt are not,
// and exist for whoever is reading a set of these across nodes.
type Manifest struct {
	Version        int    `json:"version"`
	Seed           uint64 `json:"seed"`
	BlockSize      int    `json:"blockSize"`
	Kind           string `json:"kind"`
	FilesPerWorker int    `json:"filesPerWorker"`
	FileSize       int64  `json:"fileSize"`
	WorkerCount    int    `json:"workerCount"`
	Layout         string `json:"layout"`
	Hostname       string `json:"hostname,omitempty"`
	RunID          string `json:"runID,omitempty"`
	// IOType and Pattern describe HOW the run wrote, not what it wrote. Verify
	// does not need either -- content is keyed on block index -- but a reader
	// comparing two records does, since the same bandwidth under sequential and
	// random means very different things.
	IOType  string `json:"ioType,omitempty"`
	Pattern string `json:"pattern,omitempty"`

	// StartedAt and FinishedAt bracket the run in wall-clock time.
	//
	// FinishedAt is written by a second manifest write once the run completes,
	// so its ABSENCE is meaningful: the run was interrupted or failed partway.
	//
	// It does NOT follow that the data is short. A cancel during the READ phase
	// skips the completion write over a dataset the write phase finished, so an
	// unstamped manifest can sit beside data that verifies clean. A reader may
	// therefore report the run as unfinished, but must not refuse or condemn its
	// data on that basis alone.
	//
	// That also makes these the only
	// record of whether a set of per-node runs actually overlapped, which is
	// what a multi-node aggregate needs before summing anything -- the numbers
	// each node prints say nothing about when it ran.
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
}

// manifestPath composes dir/posixbench[-runID][-hostname].json. The two
// optional elements are what let one directory hold several runs and several
// nodes; both are charset-checked by Validate before anything is written.
func manifestPath(dir, runID, hostname string) string {
	name := "posixbench"
	if runID != "" {
		name += "-" + runID
	}
	if hostname != "" {
		name += "-" + hostname
	}
	return filepath.Join(dir, name+".json")
}

func configToManifest(c Config) Manifest {
	return Manifest{
		Version:        manifestVersion,
		Seed:           c.Seed,
		BlockSize:      c.BlockSize,
		Kind:           c.Kind.String(),
		FilesPerWorker: c.FilesPerWorker,
		FileSize:       c.FileSize,
		WorkerCount:    c.Threads,
		Layout:         string(c.Layout),
		Hostname:       c.Hostname,
		RunID:          c.RunID,
		IOType:         c.ioTypeOrDefault().String(),
		Pattern:        string(c.Pattern),
	}
}

func manifestToConfig(m Manifest, dir string) (Config, error) {
	kind, err := block.KindFromString(m.Kind)
	if err != nil {
		return Config{}, fmt.Errorf("posixbench manifest: kind: %w", err)
	}
	return Config{
		Path:           dir,
		Threads:        m.WorkerCount,
		BlockSize:      m.BlockSize,
		FileSize:       m.FileSize,
		FilesPerWorker: m.FilesPerWorker,
		Layout:         Layout(m.Layout),
		Kind:           kind,
		Seed:           m.Seed,
		Hostname:       m.Hostname,
		RunID:          m.RunID,
		Pattern:        Pattern(m.Pattern),
	}, nil
}

// writeJSONFile writes data to path, refusing to follow a symlink found there.
//
// O_NOFOLLOW: every file this package writes is a name the run composed itself
// under the operator's -path, and on a shared mount that directory is usually
// writable by others. A symlink planted at one of those names redirects the
// write out of -path and truncates whatever it points at -- and these tools run
// privileged, so root governs who may run them, not who may write the directory
// they write into. runner.go's preallocate and writeRegion carry the same guard
// with the same reasoning; this is the single seam for the JSON artifacts, so it
// cannot be present at the manifest and missing at the results file.
//
// Deliberately NOT written to a temporary name and renamed into place. A rename
// would also sidestep the symlink, and would make a failed rewrite atomic -- but
// it replaces a planted link silently where this refuses loudly, and a torn
// write is something we want to see happen rather than paper over.
func writeJSONFile(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_NOFOLLOW, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	_, err = f.Write(data)
	return err
}

// ExpectedBlocks returns how many blocks a complete verify of this run must
// compare, derived from the manifest's own recorded fields.
//
// This exists to be an INDEPENDENT second opinion, and it is worth being precise
// about what it is independent OF. It builds the region layout straight from the
// manifest and reuses workerRegions, so it duplicates no layout arithmetic; what
// it skips is manifestToConfig, the one narrowing step that can silently hand
// Verify a Config describing fewer files than the manifest does. A sweep that
// then reports no anomalies has verified a subset and called it clean, and
// nothing else in the pipeline can see the discrepancy.
//
// So: do NOT "simplify" this to run through manifestToConfig. That deletes the
// only thing it checks. It is deliberately blind to a bug in workerRegions
// itself, since both sides would move together -- that case is covered by tests
// instead.
func (m Manifest) ExpectedBlocks() int64 {
	cfg := Config{
		Threads:        m.WorkerCount,
		FilesPerWorker: m.FilesPerWorker,
		FileSize:       m.FileSize,
		BlockSize:      m.BlockSize,
		Layout:         Layout(m.Layout),
	}
	// Guard the SIGN of every field the derivation below allocates or divides
	// on, not just BlockSize. This is the first EXPORTED entry into
	// workerRegions that does not pass Validate first -- NewRunner and
	// NewVerifier both do -- so a hand-edited, truncated or foreign manifest
	// arrives here unchecked. A negative WorkerCount or FilesPerWorker panics in
	// make(); a negative FileSize returns a negative block count, which classify
	// reads as a shortfall. Zero is deliberately left to flow through: it
	// describes a run of no blocks and 0 is the correct answer for it.
	//
	// MAGNITUDE is deliberately NOT checked, and the limit is worth stating:
	// Validate bounds Threads, FilesPerWorker and the per-region block count
	// against MaxThreads, MaxRegions and MaxBlocksPerRegion, and this path skips
	// all three. A WorkerCount in the billions passes the signs above, reaches
	// workerRegions and dies in the allocator -- "runtime: out of memory", which
	// is unrecoverable rather than a panic a caller can catch. Left open because
	// the shipped path cannot reach it (iotest-posixbench validates through
	// NewVerifier before calling this) and a manifest that large does not arrive
	// by accident. A caller feeding this method manifests from an untrusted
	// source owns that check itself.
	if cfg.BlockSize <= 0 || cfg.Threads < 0 || cfg.FilesPerWorker < 0 || cfg.FileSize < 0 {
		return 0
	}
	var blocks int64
	for _, regions := range workerRegions(cfg) {
		for _, reg := range regions {
			blocks += reg.length / int64(cfg.BlockSize)
		}
	}
	return blocks
}

func writeManifest(dir string, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("posixbench: marshal manifest: %w", err)
	}
	p := manifestPath(dir, m.RunID, m.Hostname)
	if err := writeJSONFile(p, append(data, '\n')); err != nil {
		return fmt.Errorf("posixbench: write manifest %s: %w", p, err)
	}
	return nil
}

// ReadManifest reads and parses the manifest written by a previous Run in dir
// that was started without a runID or hostname. Use ReadManifestAt when the run
// was started with either set.
func ReadManifest(dir string) (Manifest, Config, error) {
	return ReadManifestAt(dir, "", "")
}

// ReadManifestAt reads and parses the manifest identified by runID and
// hostname, either of which may be empty. Use ListManifests to discover what a
// directory holds rather than guessing.
//
// The whole Manifest comes back alongside the Config, and not just the Config,
// because the two answer different questions. Config is what Verify needs to
// regenerate the expected data; the Manifest additionally records whether the
// run that wrote that data finished (see FinishedAt). A caller given only the
// Config cannot tell a half-written run from a corrupt one, and will report the
// blocks an interrupted run never reached as bad data.
func ReadManifestAt(dir, runID, hostname string) (Manifest, Config, error) {
	p := manifestPath(dir, runID, hostname)
	data, err := os.ReadFile(p)
	if err != nil {
		return Manifest{}, Config{}, fmt.Errorf("posixbench.ReadManifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, Config{}, fmt.Errorf("posixbench.ReadManifest: parse %s: %w", p, err)
	}
	// Verify regenerates every body from the seed, so a directory written by a
	// build whose GenerateBody differs reports "data mismatch" on every block,
	// indistinguishable from real corruption. Refuse it rather than report that.
	if m.Version != manifestVersion {
		return Manifest{}, Config{}, fmt.Errorf("posixbench.ReadManifest: unsupported version %d (want %d)",
			m.Version, manifestVersion)
	}
	cfg, err := manifestToConfig(m, dir)
	if err != nil {
		return Manifest{}, Config{}, err
	}
	return m, cfg, nil
}

// ManifestRef identifies one run's manifest within a directory.
type ManifestRef struct {
	RunID    string
	Hostname string
	Path     string
}

// ListManifests returns every manifest in dir, newest RunID last.
//
// Runs are stamped and per-node, so a directory can legitimately hold several.
// A caller that wants "the one manifest here" must decide what to do when
// there is more than one; this deliberately does not choose, because silently
// picking the newest is how a verify reports on a run nobody asked about.
func ListManifests(dir string) ([]ManifestRef, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "posixbench*.json"))
	if err != nil {
		return nil, fmt.Errorf("posixbench.ListManifests: %w", err)
	}
	refs := make([]ManifestRef, 0, len(entries))
	for _, p := range entries {
		// No name-based filtering here on purpose. The results file used to
		// share this glob and be excluded by prefix, and the prefix ate any
		// manifest whose runID began "results" -- see resultsStem. Its stem is
		// now disjoint, so everything this glob matches is meant to be a
		// manifest.
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("posixbench.ListManifests: %w", err)
		}
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			// Report rather than skip. The glob is this tool's own namespace,
			// so a file matching it is ours; skipping one turned "your manifest
			// is corrupt" into "there is no manifest here", which sends an
			// operator looking for a missing file that is sitting right there.
			return nil, fmt.Errorf("posixbench.ListManifests: parse %s: %w", p, err)
		}
		// Parsing is not evidence the file IS a manifest: every JSON object
		// unmarshals into one, leaving every field at its zero value. A stray
		// {} in this namespace listed as a run with an empty runID and made
		// pickRun ambiguous, so it refused to verify a perfectly good run
		// sitting beside it. The version is the cheapest sound discriminator
		// and it is the same gate ReadManifestAt applies, so catching it here
		// means the listing only ever offers runs that can actually be read.
		if m.Version != manifestVersion {
			return nil, fmt.Errorf("posixbench.ListManifests: %s is not a version %d manifest (version %d)",
				p, manifestVersion, m.Version)
		}
		refs = append(refs, ManifestRef{RunID: m.RunID, Hostname: m.Hostname, Path: p})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].RunID != refs[j].RunID {
			return refs[i].RunID < refs[j].RunID
		}
		return refs[i].Hostname < refs[j].Hostname
	})
	return refs, nil
}
