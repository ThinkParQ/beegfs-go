// This is a unit test.
//
// Coverage: iotest-dump refuses a FIFO target promptly rather than wedging in
// open(2) waiting for a writer -- the regression this tool has already had
// once, since it opened its target directly with os.Open instead of through
// fileops.
package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestDumpRefusesFifo mirrors fileops.TestOpenRefusesNonRegularFile's shape,
// including its timeout guard: a regression here is a hang, not a wrong
// value, so the test must fail on a deadline rather than hanging the run.
func TestDumpRefusesFifo(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "iotest-dump")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	fifo := filepath.Join(dir, "afifo")
	if err := syscall.Mkfifo(fifo, 0666); err != nil {
		t.Skipf("mkfifo unsupported on this filesystem: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "-path", fifo).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("iotest-dump did not exit within the deadline -- it must not block on a "+
			"non-regular file: %s", out)
	}
	if err == nil {
		t.Fatalf("iotest-dump against a FIFO: got exit 0, want a refusal; output: %s", out)
	}
	if !strings.Contains(string(out), "not a regular file") {
		t.Errorf("output=%q, want it to mention 'not a regular file'", out)
	}
}
