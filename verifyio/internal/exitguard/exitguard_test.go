// This is a unit test.
//
// Coverage: structural invariants over the exit paths of every binary under
// verifyio/cmd, which no behavioural test can pin. Most of these error paths
// need a fault injector to reach at runtime -- an fsync that fails, a listxattr
// that returns E2BIG mid-sweep -- so this walks the source instead.
//
// WHAT EACH RULE KEYS ON. Stated as mechanism, not as a list of what is covered:
// three previous versions of this paragraph claimed completeness -- first as
// "covered by construction", then as a count of escapes -- and a round of review
// falsified each one. A mechanism description does not go stale when someone
// finds a new spelling.
//
// Four of the five key on a SOURCE SPELLING: a callee spelled log.Fatal*,
// syscall.Exit, climain.Fail or os.Exit; and an os.Exit ARGUMENT as spelled,
// against the tool's policy. The fifth keys on the filesystem instead: a
// directory under cmd/ with no policy at all. Rule 3, and only rule 3, also
// keys on lexical position -- inside the BODY of a statement whose condition
// mentions the identifier err. The argument rule is deliberately
// position-blind, which is why it catches a package-level func literal that
// rule 3 cannot.
//
// What that leaves out is in this package's doc comment, which holds the only
// copy of the escape list. Do not restate it here or at a call site.
//
// cmd/iotest-verify's invariant_test.go carries two further rules in that tool's
// own vocabulary; they are not duplicated here.
package exitguard

import (
	"fmt"
	"go/ast"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// cmdRoot is verifyio/cmd relative to this package's directory, which is the
// working directory `go test` gives us.
const cmdRoot = "../../cmd"

// toolPolicy says what one binary may do on an exit path.
type toolPolicy struct {
	// errPathExits are the os.Exit arguments this tool may use from inside an
	// error-conditioned body, mapped to why it is not a failure report.
	// Everything else routes through the tool's reporter.
	errPathExits map[string]string

	// exitArgs are every os.Exit argument this tool may name, anywhere,
	// mapped to why it is legitimate. This is an allowlist on purpose: the exit
	// vocabulary is closed and small, so naming what is allowed catches the
	// codes nobody thought to forbid -- including one reached through a
	// variable, which is otherwise a go/types dataflow problem.
	exitArgs map[string]string

	// noClimainFail, when non-empty, bans climain.Fail in this tool and says
	// why. Empty means Fail is this tool's reporter and is correct.
	noClimainFail string
}

// policy is the whole of cmd/, by name. A tool missing from this map fails
// TestEveryToolHasAPolicy rather than being silently unguarded -- that omission
// is how this axis reopened after each of the last six rounds.
var policy = map[string]toolPolicy{
	"iotest-verify": {
		exitArgs: map[string]string{
			"exitUsage": "a usage error, before any verdict is possible",
			"code": "two sites: sweepVerdict's computed code in main, and " +
				"exitWithLine's code PARAMETER -- so this permits any value a " +
				"caller passes exitWithLine. What bounds it is invariant_test.go's " +
				"caller allowlist, not this entry",
		},
		noClimainFail: "iotest-verify reserves five exit codes and promises a " +
			"summary line as genuinely the last line of stdout; Fail produces 2 or 4 " +
			"and writes to stderr, so it breaks the code mapping and the output " +
			"contract at once. Route failures through dieError",
	},
	"iotest-posixbench": {
		exitArgs: map[string]string{
			"exitUsage":      "a usage error, before any run happens",
			"exitIncomplete": "a cancelled run, which is neither pass nor fail",
			"code":           "the code classify computed; the verdict path's own exit",
		},
		errPathExits: map[string]string{
			// The signal is the operator's own, so no verdict about the data is
			// implied and nothing was misread -- the run simply stopped.
			"exitIncomplete": "a cancelled run is incomplete, not failed",
		},
	},
	"iotest-smoke": {
		exitArgs: map[string]string{
			"exitPass": "every record read back OK",
			"exitFail": "the verdict path: at least one record did not",
		},
	},
	// Both are fully migrated onto climain.Fail; the one bare 2 each keeps is a
	// usage exit next to flag.Usage, which Fail cannot replace without also
	// taking over the usage message.
	"iotest-dump": {exitArgs: map[string]string{"2": "usage, alongside flag.Usage"}},
	"iotest-util": {exitArgs: map[string]string{"2": "usage, alongside flag.Usage"}},
}

// TestEveryToolHasAPolicy is the rule that makes the others hold for a tool
// nobody has written yet.
//
// Every previous round on this axis found the same shape: a new exit surface
// appeared -- a new tool, a new helper, a third directory -- and the guard did
// not cover it because nothing forced the question. A new directory under cmd/
// now fails here until someone states its policy, which is the only part of this
// file that cannot be escaped by writing the exit a different way.
func TestEveryToolHasAPolicy(t *testing.T) {
	tools, err := LoadTools(cmdRoot)
	if err != nil {
		t.Fatalf("load %s: %v", cmdRoot, err)
	}
	for _, tool := range tools {
		if _, ok := policy[tool.Name]; !ok {
			t.Errorf("cmd/%s has no entry in policy.\n"+
				"Add one saying which os.Exit spellings its error paths may use and\n"+
				"whether climain.Fail is its reporter. Until then none of the rules in\n"+
				"this file describe it.", tool.Name)
		}
	}
	for name := range policy {
		found := false
		for _, tool := range tools {
			if tool.Name == name {
				found = true
			}
		}
		if !found {
			t.Errorf("policy names %q but cmd/%s does not exist; drop the stale entry", name, name)
		}
	}
}

// TestNoForeignExitHelper bans the two stdlib spellings that exit without any
// of this tree's exit vocabulary being visible at the call site.
//
// log.Fatal* hardcodes 1, which is FAIL in all three verdict-bearing tools, and
// its os.Exit sits inside package log where no rule keyed on a call to os.Exit
// can see it. syscall.Exit is the same shape with an arbitrary code. Neither is
// used anywhere in verifyio today, so this is a ban on re-introducing them
// rather than a finding about live code -- and it is the rule that catches the
// spelling cmd/iotest-verify's own rules structurally cannot.
func TestNoForeignExitHelper(t *testing.T) {
	tools, err := LoadTools(cmdRoot)
	if err != nil {
		t.Fatalf("load %s: %v", cmdRoot, err)
	}
	inspected := 0
	for _, tool := range tools {
		for path, f := range tool.Files {
			inspected++
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee := CalleeName(call.Fun)
				if !strings.HasPrefix(callee, "log.Fatal") && callee != "syscall.Exit" {
					return true
				}
				t.Errorf("%s calls %s at %s.\n"+
					"log.Fatal* exits 1, which is FAIL -- \"the data read back wrong\" --\n"+
					"and syscall.Exit names any code at all. Both exit from inside another\n"+
					"package, so no rule keyed on os.Exit can see them. Route the failure\n"+
					"through this tool's reporter instead.",
					filepath.Base(path), callee, tool.Fset.Position(call.Pos()))
				return true
			})
		}
	}
	if inspected == 0 {
		t.Fatal("no files inspected; the walk is not looking at what it thinks it is")
	}
}

// TestNoErrorPathExitsDirectly is the policy itself: an error path reports
// through the tool's reporter, never by exiting on its own.
//
// A reporter -- climain.Fail, or dieError in iotest-verify -- maps the failure
// to a code that is not the tool's FAIL, and in iotest-verify also emits the
// summary line a wrapper parses. An os.Exit written inline bypasses both, and
// reads as perfectly fine in isolation, which is why every round of this has
// found one.
//
// Keying on the exit's lexical position rather than its argument is what makes
// this hold for the indirected spelling -- c := exitFail; os.Exit(c) -- that the
// per-tool rules need go/types dataflow to see. The argument does not matter:
// inside an error-conditioned body, no direct exit is correct unless the policy
// says so by name.
func TestNoErrorPathExitsDirectly(t *testing.T) {
	tools, err := LoadTools(cmdRoot)
	if err != nil {
		t.Fatalf("load %s: %v", cmdRoot, err)
	}
	checked := 0
	for _, tool := range tools {
		allowed := policy[tool.Name].errPathExits
		for _, f := range tool.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				body, ok := ErrConditionedBody(n)
				if !ok {
					return true
				}
				checked++
				ast.Inspect(body, func(m ast.Node) bool {
					call, ok := m.(*ast.CallExpr)
					if !ok || CalleeName(call.Fun) != "os.Exit" || len(call.Args) != 1 {
						return true
					}
					if _, ok := allowed[argName(call.Args[0])]; ok {
						return true
					}
					t.Errorf("an error path in cmd/%s calls os.Exit directly at %s.\n"+
						"An error means no verdict about the data is possible, so the exit\n"+
						"belongs to this tool's reporter (climain.Fail, or dieError in\n"+
						"iotest-verify) -- reporting a verdict code here tells a wrapper the\n"+
						"data is wrong when the tool simply could not do its job. If this exit\n"+
						"genuinely is not a failure report, add its code to the tool's\n"+
						"errPathExits with the reason.",
						tool.Name, tool.Fset.Position(call.Pos()))
					return true
				})
				return true
			})
		}
	}
	if checked == 0 {
		t.Fatal("no error-testing if statements found; the walk is not looking at what it thinks it is")
	}
}

// argName renders an os.Exit argument as it is spelled in the source, for
// matching against a policy's errPathExits. Anything that is not a bare
// identifier or integer literal returns "", which no policy names, so an
// indirected or computed code is never allowed by accident.
func argName(e ast.Expr) string {
	switch a := e.(type) {
	case *ast.Ident:
		return a.Name
	case *ast.BasicLit:
		return a.Value
	}
	return ""
}

// TestReporterBanIsHonoured pins the half of the exit contract that is about
// output, not codes.
//
// climain.Fail is the tree-wide idiom and is correct for four of the five tools.
// It is wrong for iotest-verify, and wrong in a way no reader notices: Fail is
// already imported there for ExitIfNoArgs, so adopting it is a one-token change
// that passes every gate, produces exit 2 or 4 -- "nothing to verify" -- for a
// file the tool could not read, and drops the summary line main promises is
// genuinely the last line of stdout. This is the escape climain.Die used to be,
// on the helper that replaced it.
func TestReporterBanIsHonoured(t *testing.T) {
	tools, err := LoadTools(cmdRoot)
	if err != nil {
		t.Fatalf("load %s: %v", cmdRoot, err)
	}
	banned := 0
	for _, tool := range tools {
		reason := policy[tool.Name].noClimainFail
		if reason == "" {
			continue
		}
		banned++
		for fn, positions := range CallsTo(tool.Fset, tool.Files, "climain.Fail") {
			t.Errorf("%s calls climain.Fail in cmd/%s (%s).\n%s.",
				fn, tool.Name, strings.Join(positions, ", "), reason)
		}
	}
	if banned == 0 {
		t.Fatal("no tool bans climain.Fail; the walk is not looking at what it thinks it is")
	}
}

// TestOnlyPolicyNamedExitCodes is the other half of the exit rule, and the half
// that does not care where the exit sits.
//
// TestNoErrorPathExitsDirectly keys on lexical position, so it misses an exit
// whose enclosing condition tests something derived from the error rather than
// err itself -- iotest-verify's "if fatal", where fatal came from
// classifySweepErr(err). This rule keys on the argument instead and allows only
// what each tool's policy names, so a code reached through a variable is caught
// without go/types: the spelling os.Exit(c) is not in any allowlist.
//
// It walks whole files rather than *ast.FuncDecl bodies, which also closes the
// package-level "var bail = func() { os.Exit(exitFail) }" escape that the
// per-tool rules list as one they cannot see.
//
// A new legitimate exit fails this test until someone adds it with a reason.
// That is the intent: the exit vocabulary is closed, and an addition to it is
// exactly the decision worth making deliberately.
func TestOnlyPolicyNamedExitCodes(t *testing.T) {
	tools, err := LoadTools(cmdRoot)
	if err != nil {
		t.Fatalf("load %s: %v", cmdRoot, err)
	}
	seen := 0
	for _, tool := range tools {
		allowed := policy[tool.Name].exitArgs
		for _, f := range tool.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || CalleeName(call.Fun) != "os.Exit" || len(call.Args) != 1 {
					return true
				}
				seen++
				arg := argName(call.Args[0])
				if arg == "0" || arg == "exitPass" {
					// Runs before the policy check below, so a success code on a
					// failure path is out of scope by construction. Disclosed in
					// exitguard.go's escape list; not fixed.
					return true
				}
				if _, ok := allowed[arg]; ok {
					return true
				}
				t.Errorf("cmd/%s calls os.Exit(%s) at %s, which its policy does not name.\n"+
					"Allowed here: %s.\n"+
					"A failure exit belongs to this tool's reporter (climain.Fail, or\n"+
					"dieError in iotest-verify), which maps it to a code that is not this\n"+
					"tool's FAIL. If this exit genuinely is legitimate, add its spelling to\n"+
					"the tool's exitArgs with the reason -- and check first that the code it\n"+
					"names is not a verdict about data nobody read.",
					tool.Name, exprText(call.Args[0]), tool.Fset.Position(call.Pos()),
					strings.Join(sortedKeys(allowed), ", "))
				return true
			})
		}
	}
	if seen == 0 {
		t.Fatal("no os.Exit calls found; the walk is not looking at what it thinks it is")
	}
}

// exprText renders an argument for a message, falling back to a node label for
// anything argName cannot spell.
func exprText(e ast.Expr) string {
	if s := argName(e); s != "" {
		return s
	}
	return fmt.Sprintf("%T", e)
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
