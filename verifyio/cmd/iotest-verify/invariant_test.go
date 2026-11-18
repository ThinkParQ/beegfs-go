// This is a unit test.
//
// Coverage: structural invariants about this binary's exit paths, which no
// behavioural test can pin. dieError has five call sites and only the open()
// one was reachable from a test; rewriting each of the other four to FAIL --
// "a file I could not read is corrupt data" -- left the whole suite and
// exercise-smoke-verify-dump.sh green, and all four together left dieError with
// one live caller. Three of the four need a fault injector to reach at runtime,
// so this walks the source instead.
//
// SCOPE, because this comment twice overstated it: the two rules below match
// one spelling -- a call to exitWithLine -- in this directory's non-test files.
// A later call site is covered by construction when it is written that way, and
// not otherwise. dieError's doc lists the spellings that escape and why; keep
// the two lists in step.
//
// These two rules are this tool's own vocabulary -- exitWithLine, dieError,
// checkXattrPresence -- and are deliberately package-scoped. The spellings that
// escape them because they are not written in that vocabulary at all
// (log.Fatal*, syscall.Exit, an inline os.Exit on an error path, climain.Fail)
// are banned across every tool by verifyio/internal/exitguard, which parses
// every tool directory under cmd/. That is also where every os.Exit rule now
// lives: this file used to carry a third rule banning os.Exit with a failure
// constant or a bare literal, and exitguard's argument allowlist subsumes it --
// os.Exit here may name only exitUsage and the identifier code. Note the
// allowlist reads the SPELLING, so reusing an allowed spelling for a failure
// escapes it; exitguard's doc has the list.
// Neither file subsumes the other: this one knows which function is allowed to
// report a verdict, and that one knows the exit is happening at all.
package main

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/internal/exitguard"
)

// loadNonTestFiles parses every non-test .go file in this directory.
//
// Test files are excluded deliberately: the invariants below describe the
// shipped binary's exit paths, and a helper in a _test.go file is not one.
func loadNonTestFiles(t *testing.T) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	tool, err := exitguard.LoadDir(".")
	if err != nil {
		t.Fatalf("parse this package: %v", err)
	}
	return tool.Fset, tool.Files
}

// TestOnlyDieErrorAndCheckXattrPresenceExit pins dieError's doc comment: every
// failure exit routes through dieError, so a target the tool could not read
// cannot reach a wrapper as FAIL.
//
// This is the invariant, not a coverage proxy. A scripted forensic sweep over
// many files branches on the exit code alone, so ERROR (5) and FAIL (1) are the
// difference between "this file was unreadable" and "this file is corrupt".
func TestOnlyDieErrorAndCheckXattrPresenceExit(t *testing.T) {
	fset, files := loadNonTestFiles(t)
	allowed := map[string]bool{
		// The one wrapper. Names verdictError/exitError itself.
		"dieError": true,
		// The sole deliberate exception: FAIL for corrupt metadata in
		// verifyio's own namespace, ERROR for a file with no records at all.
		// Both are verdicts about the data, which is why they are not dieError.
		"checkXattrPresence": true,
	}
	calls := exitguard.CallsTo(fset, files, "exitWithLine")
	if len(calls) == 0 {
		t.Fatal("no exitWithLine calls found; the walk is not looking at what it thinks it is")
	}
	for fn, positions := range calls {
		if allowed[fn] {
			continue
		}
		t.Errorf("%s calls exitWithLine directly (%s).\n"+
			"A failure exit must go through dieError so it reports ERROR, not FAIL --\n"+
			"see dieError's doc comment. If this exit is a verdict about the DATA rather\n"+
			"than a failure to read it, add the function to allowed and say why.",
			fn, strings.Join(positions, ", "))
	}
}

// TestNoErrorPathPicksItsOwnVerdict is the other half of the invariant above,
// and it exists because the first half alone is not enough.
//
// checkXattrPresence is allowed to call exitWithLine, because two of its exits
// are verdicts about the DATA. But it also has two ERROR exits -- f.Size() and
// listxattr failing -- and those must still route through dieError. Allowing
// the function wholesale lets exactly those two rewrite to FAIL unnoticed,
// which is measured, not hypothetical: both survived the first test.
//
// So: no exitWithLine anywhere inside an "if" whose condition mentions err.
// That keys on the name, which is a convention rather than a guarantee -- but
// it is the convention this file follows at every one of its error sites, and
// the alternative (pinning an exact list of call sites) fails the next time
// someone adds a legitimate exit.
func TestNoErrorPathPicksItsOwnVerdict(t *testing.T) {
	fset, files := loadNonTestFiles(t)
	checked := 0
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			body, ok := exitguard.ErrConditionedBody(n)
			if !ok {
				return true
			}
			checked++
			ast.Inspect(body, func(m ast.Node) bool {
				call, ok := m.(*ast.CallExpr)
				if !ok || exitguard.CalleeName(call.Fun) != "exitWithLine" {
					return true
				}
				t.Errorf("an error path calls exitWithLine directly at %s.\n"+
					"An error means no verdict about the data is possible, so it must go\n"+
					"through dieError and report ERROR. Reporting FAIL here tells a wrapper\n"+
					"the file is corrupt when the tool simply could not read it.",
					fset.Position(call.Pos()))
				return true
			})
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no error-testing if statements found; the walk is not looking at what it thinks it is")
	}
}
