// This is a unit test.
//
// Coverage: iotest-smoke's exit-code contract, driven end to end against the
// built binary. Before this file the tool had no Go test at all, and its
// verdict tokens and exit codes were pinned by NEITHER gate: the three success
// checks in scripts/exercise-smoke-verify-dump.sh match a per-verdict count
// line ('OK: +8$'), not the PASS/FAIL token, and the script is in neither
// `make test` nor CI. So the collision this file exists to prevent -- a usage
// error and a data FAIL sharing exit 1 -- could regress in silence.
//
// The harness follows iotest-verify's (verify_endtoend_linux_test.go): build
// the tool into t.TempDir() and exec it. iotest-posixbench's alternative --
// re-exec the test binary with an env marker -- needs a callable args-taking
// entry point, and smoke has only func main().
//
// WHAT THIS FILE CANNOT REACH, stated so nobody reads it as complete. smoke
// writes and verifies in ONE process with no verify-only mode, so there is no
// window in which an external test can corrupt, truncate or unlink anything.
// That makes three arms un-drivable through the binary: the FAIL verdict
// itself, and the two absence counters (RECORD_MISSING, SHORT_READ). FAIL's
// code is pinned by TestExitCodes below; the two counters are pinned by
// nothing, and closing that needs the read loop extracted into a callable
// function -- deliberately not done here.
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildSmoke builds the tool once for the calling test and returns its path.
func buildSmoke(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "iotest-smoke")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// run executes the binary and returns its combined output and exit code.
func run(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return string(out), ee.ExitCode()
		}
		t.Fatalf("run %v: %v\n%s", args, err, out)
	}
	return string(out), 0
}

// TestExitCodes pins the two codes this package declares, as LITERALS.
//
// Literals, not the exitX constants: an expectation written in terms of the
// constant follows a rename and pins nothing -- the same hole c43e505 closed in
// iotest-posixbench's TestClassify. And this is the only thing pinning
// exitFail == 1, because the FAIL arm cannot be driven through the binary (see
// the file comment), so a renumbering that reintroduced the collision would
// otherwise pass every gate.
func TestExitCodes(t *testing.T) {
	if exitPass != 0 {
		t.Errorf("exitPass = %d, want 0", exitPass)
	}
	if exitFail != 1 {
		t.Errorf("exitFail = %d, want 1 -- 1 is the FAIL code this tool's whole "+
			"exit contract reserves; moving it silently changes what every "+
			"wrapper branching on $? concludes", exitFail)
	}
}

// TestExitCodeContract drives every failure arm reachable through the binary
// and pins which code it produces.
//
// The load-bearing case is the pair "blocks <= 0" and "a path that is not ours"
// against exitFail: before this fix both exited 1, indistinguishable from "the
// data read back wrong". Measured at the pre-fix commit, every row below marked
// want 2 or 5 returned 1 instead.
func TestExitCodeContract(t *testing.T) {
	bin := buildSmoke(t)
	dir := t.TempDir()

	// A path the destroy guard refuses: a real file that no verifyio tool made.
	// Must not be named iotest-*, which the guard accepts by prefix.
	notOurs := filepath.Join(dir, "not-a-verifyio-file.txt")
	if err := os.WriteFile(notOurs, []byte("written by something else\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	aDir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(aDir, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name       string
		args       []string
		wantCode   int
		wantSubstr string
	}{
		{
			name:       "a clean run passes",
			args:       []string{"-path", filepath.Join(dir, "iotest-clean.dat"), "-blocks", "4", "-blocksize", "1024"},
			wantCode:   0,
			wantSubstr: "PASS",
		},
		{
			name:       "blocks <= 0 is usage",
			args:       []string{"-path", filepath.Join(dir, "iotest-a.dat"), "-blocks", "0"},
			wantCode:   2,
			wantSubstr: "records must be > 0",
		},
		{
			name:       "an unknown -kind is usage",
			args:       []string{"-path", filepath.Join(dir, "iotest-b.dat"), "-blocks", "2", "-kind", "nosuchkind"},
			wantCode:   2,
			wantSubstr: "unknown pattern",
		},
		{
			name:       "an invalid -blocksize is usage",
			args:       []string{"-path", filepath.Join(dir, "iotest-c.dat"), "-blocks", "2", "-blocksize", "3"},
			wantCode:   2,
			wantSubstr: "invalid -blocksize",
		},
		{
			// The destroy guard's operator-error arms are usage, not
			// environment: nothing is broken and retyping fixes it.
			name:       "a path that is not a verifyio artifact is usage",
			args:       []string{"-path", notOurs, "-blocks", "2", "-blocksize", "1024"},
			wantCode:   2,
			wantSubstr: "refusing to destroy",
		},
		{
			name:       "a target that is not a regular file is usage",
			args:       []string{"-path", aDir, "-blocks", "2", "-blocksize", "1024"},
			wantCode:   2,
			wantSubstr: "not a regular file",
		},
		{
			// The guard passes (nothing to destroy: ENOENT) and the open then
			// fails, which is the tool being unable to do its job.
			name:       "a path under a missing directory is environment",
			args:       []string{"-path", filepath.Join(dir, "nosuchdir", "iotest-d.dat"), "-blocks", "2", "-blocksize", "1024"},
			wantCode:   5,
			wantSubstr: "no such file or directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code := run(t, bin, tc.args...)
			if code != tc.wantCode {
				t.Errorf("exit = %d, want %d; output:\n%s", code, tc.wantCode, out)
			}
			if !strings.Contains(out, tc.wantSubstr) {
				t.Errorf("output does not contain %q; got:\n%s", tc.wantSubstr, out)
			}
			// The property F3 is about, asserted separately from the values so
			// the INTENT is pinned and not just the numbers: no arm where the
			// tool failed may exit with the code that means "the data is bad".
			if tc.wantCode != 0 && code == exitFail {
				t.Errorf("a tool failure exited %d, the FAIL code -- a wrapper "+
					"branching on $? cannot tell it from corrupt data", exitFail)
			}
		})
	}
}

// TestUsageErrorsAndFailDoNotShareACode is F3 stated as one assertion rather
// than as a property of a table, so deleting a row above cannot quietly delete
// the guarantee.
func TestUsageErrorsAndFailDoNotShareACode(t *testing.T) {
	bin := buildSmoke(t)
	dir := t.TempDir()

	_, usage := run(t, bin, "-path", filepath.Join(dir, "iotest-x.dat"), "-blocks", "0")
	if usage == exitFail {
		t.Fatalf("a usage error exits %d, the same code as FAIL", usage)
	}
	_, env := run(t, bin, "-path", filepath.Join(dir, "nodir", "iotest-y.dat"), "-blocks", "2", "-blocksize", "1024")
	if env == exitFail {
		t.Fatalf("an environment failure exits %d, the same code as FAIL", env)
	}
	if usage == env {
		t.Errorf("usage and environment failures share exit %d; the two are "+
			"different questions for an operator (retype the command vs. "+
			"investigate the machine)", usage)
	}
}
