package posixbench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

const manifestVersion = 1
const manifestFileName = "posixbench.json"

// ManifestPath returns the path of the manifest file for dir and hostname.
// When hostname is non-empty the filename is posixbench-{hostname}.json so
// that multiple nodes can share a directory without clobbering each other.
func ManifestPath(dir, hostname string) string {
	return manifestPath(dir, hostname)
}

// Manifest is the on-disk record of a run. It contains everything Verifier
// needs to reconstruct the expected data for every block.
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
	NoXattr        bool   `json:"noXattr,omitempty"`
}

func manifestPath(dir, hostname string) string {
	if hostname != "" {
		return filepath.Join(dir, fmt.Sprintf("posixbench-%s.json", hostname))
	}
	return filepath.Join(dir, manifestFileName)
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
		NoXattr:        c.NoXattr,
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
		NoXattr:        m.NoXattr,
	}, nil
}

func writeManifest(dir string, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("posixbench: marshal manifest: %w", err)
	}
	p := manifestPath(dir, m.Hostname)
	if err := os.WriteFile(p, append(data, '\n'), 0644); err != nil {
		return fmt.Errorf("posixbench: write manifest %s: %w", p, err)
	}
	return nil
}

// ReadManifest reads and parses the manifest written by a previous Run in dir
// that was started without a hostname (i.e. the CLI bench run command).
// Use ReadManifestHost when the run was started with a hostname set.
func ReadManifest(dir string) (Config, error) {
	return ReadManifestHost(dir, "")
}

// ReadManifestHost reads and parses the manifest for the given hostname.
// When hostname is non-empty it looks for posixbench-{hostname}.json;
// when empty it looks for posixbench.json.
func ReadManifestHost(dir, hostname string) (Config, error) {
	p := manifestPath(dir, hostname)
	data, err := os.ReadFile(p)
	if err != nil {
		return Config{}, fmt.Errorf("posixbench.ReadManifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Config{}, fmt.Errorf("posixbench.ReadManifest: parse %s: %w", p, err)
	}
	if m.Version != manifestVersion {
		return Config{}, fmt.Errorf("posixbench.ReadManifest: unsupported version %d (want %d)",
			m.Version, manifestVersion)
	}
	return manifestToConfig(m, dir)
}
