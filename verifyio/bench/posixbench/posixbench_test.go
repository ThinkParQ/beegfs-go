// This is a unit test.
//
// Coverage: Config.Validate (including that FilesPerWorker is exempt from
// its >0 constraint under n-to-1 layout); blockSeed determinism; manifest
// JSON round-trip; an end-to-end write-then-verify pass on a clean run for
// both 1-to-1 and n-to-1 layouts, with multiple files per worker and an
// optional read phase; the verifier correctly flagging injected corruption;
// and n-to-1 combined with NoXattr, a regression test for Run's
// preallocation step and workerRegions independently deriving the shared
// filename and drifting out of sync.
package posixbench

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
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
	}
	if err := writeManifest(dir, configToManifest(cfg)); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	got, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
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
	res, err := r.Run(false)
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
	n, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n != 0 {
		t.Errorf("Verify: %d anomalies, want 0", n)
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
	if _, err := r.Run(false); err != nil {
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
	n, err := v.Verify(func(a Anomaly) error {
		anomalies = append(anomalies, a)
		return nil
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n == 0 || len(anomalies) == 0 {
		t.Error("Verify: expected at least one anomaly after corruption, got none")
	}
	if anomalies[0].BlockIndex != 0 {
		t.Errorf("first anomaly: BlockIndex=%d, want 0", anomalies[0].BlockIndex)
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
	res, err := r.Run(false)
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
	n, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n != 0 {
		t.Errorf("n-to-1 Verify: %d anomalies, want 0", n)
	}
}

// TestRunnerNto1NoXattr is a regression test: Run's n-to-1 preallocation
// step and workerRegions used to derive the shared filename independently,
// and only workerRegions accounted for NoXattr's "-bs<N>" suffix, so every
// worker opened a shared-file path preallocate had never created. Both now
// go through the single pbenchFilePrefix helper.
func TestRunnerNto1NoXattr(t *testing.T) {
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
		NoXattr:   true,
		Kind:      block.KindDecimal,
		Seed:      77,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res, err := r.Run(false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := int64(threads) * fileSize; res.TotalWritten != want {
		t.Errorf("TotalWritten=%d, want %d", res.TotalWritten, want)
	}

	wantPath := filepath.Join(dir, fmt.Sprintf("pbench-bs%d-shared.dat", blockSize))
	info, err := os.Stat(wantPath)
	if err != nil {
		t.Fatalf("stat shared file %s: %v", wantPath, err)
	}
	if want := int64(threads) * fileSize; info.Size() != want {
		t.Errorf("shared file size=%d, want %d", info.Size(), want)
	}

	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	n, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n != 0 {
		t.Errorf("n-to-1 NoXattr Verify: %d anomalies, want 0", n)
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
	res, err := r.Run(true)
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
	res, err := r.Run(false)
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
	n, err := v.Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n != 0 {
		t.Errorf("Verify: %d anomalies, want 0", n)
	}
}
