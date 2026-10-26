package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

// TestCountSpanCoverage pins which spans count as coverage.
//
// The gap rows are the regression. A gap span's bytes are read, so it is
// tempting to count it as "checked" -- but no record claims those bytes, so
// reading them says nothing about written data. Counting them did exactly that,
// and one all-zero gap span was enough to make classify's INCOMPLETE arm
// unreachable: a sweep that verified no record printed PASS at exit 0.
func TestCountSpanCoverage(t *testing.T) {
	for _, tc := range []struct {
		name string
		span verifier.Span
		want counts
	}{
		{
			name: "clean record is coverage",
			span: verifier.Span{Coverage: verifier.CoverageOne, Verdict: block.VerdictOK},
			want: counts{records: 1},
		},
		{
			name: "failed record is coverage and an anomaly",
			span: verifier.Span{Coverage: verifier.CoverageOne, Verdict: block.VerdictBodyCorrupt},
			want: counts{records: 1, anomalies: 1},
		},
		{
			// The regression: read, but not verified. Never coverage.
			name: "all-zero gap is not coverage",
			span: verifier.Span{Coverage: verifier.CoverageNone, AllZero: true},
			want: counts{gaps: 1},
		},
		{
			name: "non-zero gap is not coverage but is an anomaly",
			span: verifier.Span{Coverage: verifier.CoverageNone, AllZero: false},
			want: counts{gaps: 1, anomalies: 1},
		},
		{
			name: "overlap is coverage and an anomaly",
			span: verifier.Span{Coverage: verifier.CoverageMany},
			want: counts{records: 1, anomalies: 1},
		},
		{
			// Being locked out is not a finding about the data, so a contended
			// span is neither coverage nor an anomaly -- only a reason to
			// come back later, which is what classify's INCOMPLETE arm says.
			name: "contended is neither coverage nor an anomaly",
			span: verifier.Span{Coverage: verifier.CoverageContended},
			want: counts{contended: 1},
		},
		{
			name: "unknown coverage is not coverage but is an anomaly",
			span: verifier.Span{Coverage: verifier.Coverage(99)},
			want: counts{anomalies: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c counts
			ok, line, _ := countSpan(&c, tc.span)
			if c != tc.want {
				t.Errorf("counts = %+v, want %+v", c, tc.want)
			}
			if ok != (tc.want.anomalies == 0) {
				t.Errorf("ok = %v, want %v", ok, tc.want.anomalies == 0)
			}
			if line == "" {
				t.Error("no line rendered for the span")
			}
		})
	}
}

// TestClassify pins the mapping from a completed sweep's counts to its verdict
// and exit code. Every code is distinct so a wrapper can branch on the code
// alone: PASS means the data was checked and is good, INCOMPLETE means ask
// again, and the rest mean stop.
func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name        string
		c           counts
		wantVerdict string
		wantCode    int
	}{
		{
			name: "clean", c: counts{records: 3},
			wantVerdict: verdictPass, wantCode: exitPass,
		},
		{
			name: "anomaly", c: counts{records: 3, anomalies: 1},
			wantVerdict: verdictFail, wantCode: exitFail,
		},
		{
			// An anomaly outranks a contended span: something is definitely
			// wrong, which is a stronger statement than "some spans were
			// skipped".
			name: "anomaly outranks contended", c: counts{records: 1, contended: 2, anomalies: 1},
			wantVerdict: verdictFail, wantCode: exitFail,
		},
		{
			name: "all contended", c: counts{contended: 4},
			wantVerdict: verdictIncomplete, wantCode: exitIncomplete,
		},
		{
			// The regression, stated in counts: gaps are not coverage, so a
			// sweep of nothing but gaps and contended spans is INCOMPLETE. Read
			// as PASS at exit 0 while gaps counted toward coverage.
			name: "gaps do not rescue a contended sweep", c: counts{gaps: 1, contended: 4},
			wantVerdict: verdictIncomplete, wantCode: exitIncomplete,
		},
		{
			// Some spans were verified and were clean. Still a pass -- the
			// contended count is printed so the operator sees it was partial.
			name: "partially contended", c: counts{records: 2, contended: 2},
			wantVerdict: verdictPass, wantCode: exitPass,
		},
		{
			name: "empty file", c: counts{},
			wantVerdict: verdictNoData, wantCode: exitNoData,
		},
		{
			// Nothing verified and nothing skipped. Unreachable today --
			// checkXattrPresence exits first on a non-empty file with no
			// records -- but it must not fall through to PASS if it ever is.
			name: "nothing but gaps", c: counts{gaps: 3},
			wantVerdict: verdictNoData, wantCode: exitNoData,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, code := classify(tc.c)
			if verdict != tc.wantVerdict {
				t.Errorf("verdict = %s, want %s", verdict, tc.wantVerdict)
			}
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d", code, tc.wantCode)
			}
		})
	}
}

// TestClassifySweepErr pins which VerifyFile errors are findings about the data
// and which stopped the sweep. Getting the split backwards produces no visible
// difference on a healthy run, so nothing else in the tree can catch it: with
// every error treated as malformed-entries, a read error mid-sweep reports as
// FAIL with partial counts rendered as though the sweep had finished, instead
// of ERROR. On BeeGFS an EIO during a post-run sweep is the realistic way in.
func TestClassifySweepErr(t *testing.T) {
	malformed := &verifier.MalformedEntriesError{
		Target:  "/tmp/x.dat",
		Entries: []xattrstore.MalformedEntry{{Name: "user.verifyio.100--50"}, {Name: "user.verifyio.z"}},
	}
	for _, tc := range []struct {
		name        string
		err         error
		wantEntries int // -1 means "expect no finding"
		wantFatal   bool
	}{
		{"no error", nil, -1, false},
		{"malformed entries", malformed, 2, false},
		{"malformed entries, wrapped", fmt.Errorf("verify: %w", malformed), 2, false},
		{"a real failure", errors.New("read /tmp/x.dat: input/output error"), -1, true},
		{"a real failure, wrapped", fmt.Errorf("verifier: %w", errors.New("EIO")), -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finding, fatal := classifySweepErr(tc.err)
			if fatal != tc.wantFatal {
				t.Errorf("fatal = %v, want %v", fatal, tc.wantFatal)
			}
			if tc.wantEntries < 0 {
				if finding != nil {
					t.Errorf("finding = %v, want nil", finding)
				}
				return
			}
			if finding == nil {
				t.Fatalf("finding = nil, want one naming %d entries", tc.wantEntries)
			}
			if got := len(finding.Entries); got != tc.wantEntries {
				t.Errorf("finding names %d entries, want %d", got, tc.wantEntries)
			}
		})
	}
}

// TestExitCodes pins the exit codes to their literal values, because the
// numbers themselves are the contract with every wrapping script -- renumbering
// one silently rewrites what those scripts conclude.
//
// Deliberately literal rather than written in terms of the constants: a test
// that says exitIncomplete == exitIncomplete passes no matter what the constant
// becomes. Asserting the numbers also asserts they stay distinct, which is what
// lets a wrapper branch on the code alone.
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

// TestSweepVerdictOutput pins the shape a wrapping script parses: the last line
// of stdout is always the machine-readable summary, its field set is fixed
// whatever the verdict, and path comes last because it may contain spaces.
func TestSweepVerdictOutput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		c          counts
		wantSubstr string
		wantLine   string
	}{
		{
			name: "pass", c: counts{records: 12, gaps: 3},
			wantSubstr: "PASS",
			wantLine:   "verdict=PASS records=12 gaps=3 contended=0 anomalies=0 path=/tmp/x.dat",
		},
		{
			name: "fail", c: counts{records: 3, anomalies: 1},
			wantSubstr: "FAIL",
			wantLine:   "verdict=FAIL records=3 gaps=0 contended=0 anomalies=1 path=/tmp/x.dat",
		},
		{
			name: "incomplete", c: counts{contended: 4},
			wantSubstr: "INCOMPLETE",
			wantLine:   "verdict=INCOMPLETE records=0 gaps=0 contended=4 anomalies=0 path=/tmp/x.dat",
		},
		{
			name: "no data", c: counts{},
			wantSubstr: "NO DATA READ",
			wantLine:   "verdict=NO_DATA records=0 gaps=0 contended=0 anomalies=0 path=/tmp/x.dat",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, _ := sweepVerdict("/tmp/x.dat", tc.c)
			if !strings.Contains(out, tc.wantSubstr) {
				t.Errorf("output does not contain %q:\n%s", tc.wantSubstr, out)
			}
			lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
			if got := lines[len(lines)-1]; got != tc.wantLine {
				t.Errorf("last line  = %q\nwant       = %q\nfull output:\n%s", got, tc.wantLine, out)
			}
			// A skipped span is never silently dropped from the prose either:
			// hiding the count is how an incomplete sweep reads as a whole one.
			if tc.c.contended > 0 && !strings.Contains(out, "contended") {
				t.Errorf("output does not report %d contended span(s):\n%s", tc.c.contended, out)
			}
			// Only PASS means the data was checked and is good, so no other
			// verdict may put that word in front of a script grepping for it.
			if tc.wantSubstr != "PASS" && strings.Contains(out, "PASS") {
				t.Errorf("non-PASS verdict %s printed PASS:\n%s", tc.name, out)
			}
		})
	}
}
