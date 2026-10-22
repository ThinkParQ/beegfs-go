package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/bench/posixbench"
	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// TestClassify pins the verdict table. Every arm is a claim about a filesystem
// that someone acts on, and the two that matter most are the ones where two
// conditions are true at once:
//
//   - anomalies over an unfinished run must not read as "the data is wrong",
//     because the missing blocks are as likely to be ones the run never reached;
//   - a MISSING data file produces an anomaly AND a short sweep, so if the
//     shortfall outranked anomalies the most ordinary data loss there is would
//     report as a tool failure instead of a verdict.
func TestClassify(t *testing.T) {
	const expected = 12
	cases := []struct {
		name       string
		sweep      posixbench.Sweep
		expected   int64
		incomplete bool
		// Literals, not the verdictX constants: the token is what a reader
		// greps, so an expectation written in terms of the constant follows a
		// rename and pins nothing.
		wantVerdict string
		wantCode    int
	}{
		{
			name:        "clean and complete",
			sweep:       posixbench.Sweep{Regions: 3, Blocks: expected},
			expected:    expected,
			wantVerdict: "PASS", wantCode: exitPass,
		},
		{
			name:        "clean over a run that did not finish still passes on what it wrote",
			sweep:       posixbench.Sweep{Regions: 3, Blocks: expected},
			expected:    expected,
			incomplete:  true,
			wantVerdict: "PASS", wantCode: exitPass,
		},
		{
			name:        "corruption in a finished run is FAIL",
			sweep:       posixbench.Sweep{Anomalies: 1, Regions: 3, Blocks: expected},
			expected:    expected,
			wantVerdict: "FAIL", wantCode: exitFail,
		},
		{
			name:        "anomalies over an unfinished run are INCOMPLETE, not FAIL",
			sweep:       posixbench.Sweep{Anomalies: 5754, Regions: 3, Blocks: 4},
			expected:    expected,
			incomplete:  true,
			wantVerdict: "INCOMPLETE", wantCode: exitIncomplete,
		},
		{
			name: "a missing data file: anomaly AND short, and the anomaly must win",
			// The region whose file is gone reports one anomaly and contributes
			// no blocks, so both conditions hold. FAIL, not SHORT.
			sweep:       posixbench.Sweep{Anomalies: 1, Regions: 2, Blocks: 8},
			expected:    expected,
			wantVerdict: "FAIL", wantCode: exitFail,
		},
		{
			name:        "fewer blocks than the manifest describes, and nothing found: SHORT",
			sweep:       posixbench.Sweep{Regions: 1, Blocks: 4},
			expected:    expected,
			wantVerdict: "SHORT", wantCode: exitError,
		},
		{
			name:        "MORE blocks than the manifest describes is equally wrong",
			sweep:       posixbench.Sweep{Regions: 3, Blocks: expected * 2},
			expected:    expected,
			wantVerdict: "SHORT", wantCode: exitError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verdict, code := classify(tc.sweep, tc.expected, tc.incomplete)
			if verdict != tc.wantVerdict || code != tc.wantCode {
				t.Errorf("classify(%+v, expected=%d, incomplete=%t) = (%s, %d), want (%s, %d)",
					tc.sweep, tc.expected, tc.incomplete, verdict, code, tc.wantVerdict, tc.wantCode)
			}
		})
	}
}

// TestExitCodes pins the exit codes to their literal values. The numbers
// themselves are the contract -- with every wrapping script, and with
// cmd/iotest-verify, whose codes these deliberately match so the same number
// cannot mean "corrupt data" in one tool and "could not read it" in the other.
// That parity is asserted in prose at the const block and was pinned on
// iotest-verify's side only; renumbering here would have gone unnoticed.
//
// Deliberately literal rather than written in terms of the constants: a test
// that says exitIncomplete == exitIncomplete passes no matter what the constant
// becomes. Measured -- every one of the six could be renumbered with the whole
// Go suite green.
//
// This replaces TestExitCodesAreDistinct, which compared the constants only to
// each other. Asserting the numbers also asserts they stay distinct, which is
// the property that test existed for: two outcomes sharing a code is the defect
// this tool was rewritten to remove, the since-deleted climain.Die having
// hardcoded 1, which is FAIL, so a target it could not READ and a file full of
// corruption exited identically.
//
// Kept identical in shape to iotest-verify's TestExitCodes on purpose; the two
// tools are one contract and should be read side by side.
func TestExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"pass", exitPass, 0},
		{"fail", exitFail, 1},
		{"usage", exitUsage, 2},
		{"incomplete", exitIncomplete, 3},
		{"no_data", exitNoData, 4},
		{"error", exitError, 5},
	} {
		if tc.got != tc.want {
			t.Errorf("%s exit code = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// TestVerifyReportsWhetherTheRunFinished pins cmdVerify's sole consumer of
// Manifest.FinishedAt.
//
// TestClassify pins classify in isolation, but nothing pinned the VALUE fed to
// its third argument. Measured: inverting `incomplete := m.FinishedAt == nil`
// left the whole Go suite green and was caught only by
// exercise-posixbench.sh (55 passed / 11 failed) -- so the entire finishedAt
// mechanism rested on a shell script nothing in `make test` runs.
//
// cmdVerify exits the process, so it is driven in a subprocess: re-exec the
// test binary with a marker in the environment. That is also closer to what an
// operator sees than calling classify would be, since the caveat is printed by
// the rendering rather than returned by classify.
func TestVerifyReportsWhetherTheRunFinished(t *testing.T) {
	if dir := os.Getenv("PB_VERIFY_DIR"); dir != "" {
		cmdVerify([]string{"-path", dir})
		// Exit rather than return: cmdVerify only returns on PASS, and letting the
		// test framework run on appends its own bare "PASS" to the buffer the parent
		// parses -- landing directly after "Verified:", the slot the tool's verdict
		// occupies. The parent could then not tell a printed verdict from none at all.
		os.Exit(exitPass)
	}

	// One real run, so the manifest and the data agree and the verdict is PASS
	// on both paths; only the finishedAt stamp differs between the subtests.
	dir := t.TempDir()
	cfg := posixbench.Config{
		Path: dir, Threads: 1, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
		Layout: posixbench.Layout1to1, Kind: block.KindDecimal, Seed: 4242,
		Hostname: "nodeA", RunID: "F1",
	}
	r, err := posixbench.NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}
	manifest := filepath.Join(dir, "posixbench-F1-nodeA.json")

	run := func(t *testing.T) (string, int) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=TestVerifyReportsWhetherTheRunFinished")
		cmd.Env = append(os.Environ(), "PB_VERIFY_DIR="+dir)
		out, err := cmd.CombinedOutput()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("running the subprocess: %v", err)
		}
		return string(out), code
	}

	// The verdict is the line right after the "Verified:" coverage line, which
	// is how cmdVerify orders its output. Located rather than grepped on
	// purpose: `go test` prints its own bare "PASS" line into this same buffer,
	// so a Contains(out, verdictPass) would hold even if the tool printed no
	// verdict at all.
	verdictLine := func(t *testing.T, out string) string {
		t.Helper()
		lines := strings.Split(out, "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, "Verified:") && i+1 < len(lines) {
				return lines[i+1]
			}
		}
		t.Fatalf("no \"Verified:\" line followed by a verdict in:\n%s", out)
		return ""
	}

	const caveat = "the run did not finish"

	t.Run("a finished run passes with no caveat", func(t *testing.T) {
		out, code := run(t)
		if code != exitPass {
			t.Fatalf("exit = %d, want %d; output:\n%s", code, exitPass, out)
		}
		if got := verdictLine(t, out); got != "PASS" {
			t.Errorf("verdict line = %q, want exactly %q -- a run with finishedAt "+
				"stamped must carry no caveat, which makes an operator distrust "+
				"data that is complete", got, "PASS")
		}
	})

	t.Run("a run with no finishedAt passes WITH the caveat", func(t *testing.T) {
		// Strip the stamp the way an interrupted run would have left it: the
		// data is intact, so the verdict must stay PASS and only the caveat
		// changes. A cancel in the read phase leaves exactly this on disk.
		raw, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		if _, ok := m["finishedAt"]; !ok {
			t.Fatal("the completed run left no finishedAt; this subtest has nothing to strip")
		}
		delete(m, "finishedAt")
		edited, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal manifest: %v", err)
		}
		if err := os.WriteFile(manifest, edited, 0644); err != nil {
			t.Fatalf("write manifest: %v", err)
		}

		out, code := run(t)
		if code != exitPass {
			t.Fatalf("exit = %d, want %d; an unfinished run still passes on the data "+
				"it did write; output:\n%s", code, exitPass, out)
		}
		got := verdictLine(t, out)
		if !strings.HasPrefix(got, "PASS") {
			t.Errorf("verdict line = %q, want it to start with %q", got, "PASS")
		}
		if !strings.Contains(got, caveat) {
			t.Errorf("verdict line = %q, want it to report the unfinished run; an "+
				"operator cannot otherwise tell a complete dataset from a truncated one", got)
		}
	})
}

// TestPickRunHonoursAnExplicitHostname pins the distinction between a hostname
// the operator typed and the one -hostname defaults to.
//
// pickRun falls back to every run in the directory when the hostname filter
// matches nothing, which is what lets a plain `verify -path D` read a directory
// produced on another node. Applied to a hostname the operator SUPPLIED, the
// same fallback discards the only thing they said about which node's data they
// wanted and then verifies whatever is left: a two-node run where one node never
// started, or a typo, verified the other node's files and exited 0. A PASS over
// data nobody checked is the worst outcome this tool has, and nothing pinned it
// at either layer -- no test called pickRun, and the exercise script's two
// -hostname verifies both pass a hostname matching their own run, so it stayed
// 65/0 on the broken binary.
//
// The first two cases are the pair that makes the distinction load-bearing:
// drop either half of the condition and one of them fails.
func TestPickRunHonoursAnExplicitHostname(t *testing.T) {
	// A real run per host, so the manifests are the ones the tool writes.
	newRun := func(t *testing.T, dir, runID, hostname string) {
		t.Helper()
		r, err := posixbench.NewRunner(posixbench.Config{
			Path: dir, Threads: 1, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
			Layout: posixbench.Layout1to1, Kind: block.KindDecimal, Seed: 4242,
			Hostname: hostname, RunID: runID,
		})
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}
		if _, err := r.Run(context.Background(), false); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}

	oneRun := t.TempDir()
	newRun(t, oneRun, "W1", "nodeA")

	twoRuns := t.TempDir()
	newRun(t, twoRuns, "W1", "nodeA")
	newRun(t, twoRuns, "W1", "nodeB")

	cases := []struct {
		name       string
		dir        string
		hostname   string
		explicit   bool
		wantErr    error
		wantHost   string
		wantReason string
	}{
		{
			name: "an explicit hostname with no run of its own finds nothing",
			dir:  oneRun, hostname: "nodeB", explicit: true,
			wantErr:    errNoManifest,
			wantReason: "verifying nodeA's files and reporting PASS is a clean verdict over data nobody asked about",
		},
		{
			name: "a defaulted hostname still falls back to the only run present",
			dir:  oneRun, hostname: "some-other-box", explicit: false,
			wantHost:   "nodeA",
			wantReason: "verifying a directory produced on another node is the ordinary case and must keep working",
		},
		{
			name: "an explicit hostname selects its own run",
			dir:  twoRuns, hostname: "nodeB", explicit: true,
			wantHost: "nodeB",
		},
		{
			name: "an explicitly empty hostname means any host",
			dir:  oneRun, hostname: "", explicit: true,
			wantHost: "nodeA",
		},
		{
			name: "a defaulted hostname matching neither of two runs stays ambiguous",
			dir:  twoRuns, hostname: "some-other-box", explicit: false,
			wantErr:    errAmbiguousRun,
			wantReason: "the fallback widens the choice, it does not make one",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pickRun(tc.dir, "W1", tc.hostname, tc.explicit)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("pickRun(-hostname %q, explicit=%v) = %+v, %v; want error %v -- %s",
						tc.hostname, tc.explicit, got, err, tc.wantErr, tc.wantReason)
				}
				return
			}
			if err != nil {
				t.Fatalf("pickRun(-hostname %q, explicit=%v): %v", tc.hostname, tc.explicit, err)
			}
			if got.Hostname != tc.wantHost {
				t.Errorf("pickRun(-hostname %q, explicit=%v) selected host %q, want %q -- %s",
					tc.hostname, tc.explicit, got.Hostname, tc.wantHost, tc.wantReason)
			}
		})
	}
}

// TestVerifyWiresTheExplicitHostnameThrough pins the argument, not the
// predicate. TestPickRunHonoursAnExplicitHostname calls pickRun directly, so it
// holds whatever cmdVerify passes it -- and measured, changing the flag name
// fs.Visit looks for from "hostname" to "host" restores the false PASS end to
// end with that test still green. This is the case the tool actually ships:
// real argv, real flag parsing, real exit code.
func TestVerifyWiresTheExplicitHostnameThrough(t *testing.T) {
	if dir := os.Getenv("PB_WIRE_DIR"); dir != "" {
		args := append([]string{"-path", dir}, strings.Fields(os.Getenv("PB_WIRE_ARGS"))...)
		cmdVerify(args)
		// cmdVerify only returns on PASS; see TestVerifyReportsWhetherTheRunFinished.
		os.Exit(exitPass)
	}

	// One node's run, in a directory an operator might reasonably think holds
	// two.
	dir := t.TempDir()
	r, err := posixbench.NewRunner(posixbench.Config{
		Path: dir, Threads: 1, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
		Layout: posixbench.Layout1to1, Kind: block.KindDecimal, Seed: 4242,
		Hostname: "nodeA", RunID: "W1",
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}

	verify := func(t *testing.T, args string) (string, int) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=TestVerifyWiresTheExplicitHostnameThrough")
		cmd.Env = append(os.Environ(), "PB_WIRE_DIR="+dir, "PB_WIRE_ARGS="+args)
		out, err := cmd.CombinedOutput()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("running the subprocess: %v", err)
		}
		return string(out), code
	}

	t.Run("-hostname naming a node that did not run reports no data", func(t *testing.T) {
		out, code := verify(t, "-run W1 -hostname nodeB")
		if code != exitNoData {
			t.Fatalf("exit = %d, want %d -- nodeB wrote nothing here, so anything else is a "+
				"verdict about nodeA's files in answer to a question about nodeB's; output:\n%s",
				code, exitNoData, out)
		}
	})

	t.Run("an omitted -hostname still verifies the only run present", func(t *testing.T) {
		out, code := verify(t, "-run W1")
		if code != exitPass {
			t.Fatalf("exit = %d, want %d -- a defaulted hostname must still fall back; output:\n%s",
				code, exitPass, out)
		}
		// The run it chose, not just that it chose one: the selection line is
		// what makes a PASS auditable, and without it this subtest could not
		// tell a fallback to nodeA from a fallback to anything else.
		if want := "run=W1 hostname=nodeA"; !strings.Contains(out, want) {
			t.Errorf("output does not name the selected run (%q); got:\n%s", want, out)
		}
	})
}

// TestVerifyExitsNonZeroOnAFailedVerdict pins that the verdict's exit code
// reaches the process.
//
// TestClassify pins which code each verdict maps to, and TestExitCodes pins
// what those codes are, but neither drives the one statement that hands the
// number to the operating system. Measured: replacing cmdVerify's
// `os.Exit(code)` with `os.Exit(exitPass)` left the whole Go suite green, and
// verify then printed FAIL over corrupt data while exiting 0 -- collapsing
// every verdict-carrying failure at once. A harness branching on $?, which is
// the documented way this tool is driven, reads a corrupt filesystem as clean.
//
// One corrupt-data case is enough to hold it: the mutation flattens all of the
// non-zero codes together, so any one of them catches it. FAIL is the cheapest
// to stage and the most consequential to get wrong. exitError has its own case
// in TestVerifyExitsErrorOnAnUnreadableTarget, because "the data is wrong" and
// "the tool could not read it" are the pair that must not collapse into each
// other.
func TestVerifyExitsNonZeroOnAFailedVerdict(t *testing.T) {
	if dir := os.Getenv("PB_FAIL_DIR"); dir != "" {
		cmdVerify([]string{"-path", dir})
		// cmdVerify only returns on PASS; see TestVerifyReportsWhetherTheRunFinished.
		os.Exit(exitPass)
	}

	dir := t.TempDir()
	r, err := posixbench.NewRunner(posixbench.Config{
		Path: dir, Threads: 1, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
		Layout: posixbench.Layout1to1, Kind: block.KindDecimal, Seed: 4242,
		Hostname: "nodeA", RunID: "W1",
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// One flipped byte in the first block. KindDecimal produces bytes in
	// ['0'-'9', ' '], so 0xFF is always a detectable mismatch -- the same
	// staging TestVerifierDetectsCorruption uses.
	data := filepath.Join(dir, "pbench-W1-nodeA-w000-f000.dat")
	f, err := os.OpenFile(data, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 100); err != nil {
		t.Fatalf("corrupt byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close data file: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestVerifyExitsNonZeroOnAFailedVerdict")
	cmd.Env = append(os.Environ(), "PB_FAIL_DIR="+dir)
	out, err := cmd.CombinedOutput()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running the subprocess: %v", err)
	}

	if code != exitFail {
		t.Errorf("exit = %d, want %d -- the data is corrupt, so a wrapper reading $? alone "+
			"must not conclude the filesystem is clean; output:\n%s", code, exitFail, string(out))
	}
	// Both halves, because the printed verdict and the exit code are separate
	// writes: FAIL on stdout with a zero status is the failure this pins.
	//
	// The literal, not verdictFail: the token is what a reader greps, so an
	// assertion written in terms of the constant follows it wherever it goes
	// and pins nothing. Measured -- renaming the token survived this test
	// while it used the constant.
	if !strings.Contains(string(out), "FAIL") {
		t.Errorf("output does not contain %q; got:\n%s", "FAIL", string(out))
	}
}

// TestVerifyExitsErrorOnAnUnreadableTarget pins the other end of the exit
// vocabulary: a target the tool could not READ exits 5, not 1.
//
// This is the pair TestVerifyExitsNonZeroOnAFailedVerdict deliberately left
// open. Corrupt data and an unreadable target are the two outcomes that look
// alike from outside and mean opposite things -- the first is a verdict about
// the filesystem, the second is the tool declining to give one -- and telling
// them apart with $? alone is the reason this tool has a code per outcome
// rather than one shared code (the since-deleted climain.Die's hardcoded 1).
//
// posixbench.Verify's read arm is what draws the line; the bench package's
// TestVerifySeparatesUnreadableFromWrong pins the predicate itself. This pins
// that the distinction survives the trip out to the process.
func TestVerifyExitsErrorOnAnUnreadableTarget(t *testing.T) {
	if dir := os.Getenv("PB_ERR_DIR"); dir != "" {
		cmdVerify([]string{"-path", dir})
		// cmdVerify only returns on PASS; see TestVerifyReportsWhetherTheRunFinished.
		os.Exit(exitPass)
	}

	dir := t.TempDir()
	r, err := posixbench.NewRunner(posixbench.Config{
		Path: dir, Threads: 2, BlockSize: 4096, FileSize: 16384, FilesPerWorker: 1,
		Layout: posixbench.Layout1to1, Kind: block.KindDecimal, Seed: 4242,
		Hostname: "nodeA", RunID: "W1",
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if _, err := r.Run(context.Background(), false); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A directory where a data file belongs: open succeeds, the read fails with
	// EISDIR. See TestVerifySeparatesUnreadableFromWrong for why this is the
	// staging that reaches the read arm.
	victim := filepath.Join(dir, "pbench-W1-nodeA-w001-f000.dat")
	if err := os.Remove(victim); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Mkdir(victim, 0755); err != nil {
		t.Fatalf("mkdir at the data file's name: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestVerifyExitsErrorOnAnUnreadableTarget")
	cmd.Env = append(os.Environ(), "PB_ERR_DIR="+dir)
	out, err := cmd.CombinedOutput()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running the subprocess: %v", err)
	}

	if code != exitError {
		t.Errorf("exit = %d, want %d -- the tool could not read the target, so it must not "+
			"report a verdict about the data; %d in particular would say the data is corrupt; "+
			"output:\n%s", code, exitError, exitFail, string(out))
	}
	// The literal, for the reason given in TestVerifyExitsNonZeroOnAFailedVerdict.
	if strings.Contains(string(out), "FAIL") {
		t.Errorf("output says FAIL for a target it could not read; got:\n%s", string(out))
	}
}
