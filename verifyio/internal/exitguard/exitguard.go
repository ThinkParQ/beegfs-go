// Package exitguard parses the iotest tools' source so a test can assert
// structural invariants about their exit paths, and holds the AST vocabulary
// those tests share.
//
// # Why this is cmd/-wide
//
// 1 is FAIL in every verdict-bearing tool -- "the data read back wrong" -- so an
// error path that exits 1 reports a target the tool could not READ as corrupt
// data, and a scripted sweep over many files branching on $? cannot tell the two
// apart. Six rounds tried to pin that with a guard inside one tool's package.
// Each time the hole reappeared one layer out, and always for the same reason:
// filepath.Glob("*.go") in cmd/iotest-verify never parses the tool next to it,
// and never parses the shared helper doing the exiting. Scope was the defect, so
// this package takes every immediate subdirectory of cmd/ as its unit, and a new
// tool directory is in scope by default rather than by someone remembering. It
// does not recurse: a package nested under a tool, or a .go file directly in
// cmd/, is parsed by no rule.
//
// # What escapes these rules
//
// This is the ONLY copy of this list. exitguard_test.go, climain.go,
// cmd/iotest-verify/main.go, cmd/iotest-verify/invariant_test.go and
// cmd/iotest-posixbench/main.go point here rather than restating it.
//
// Grouped by WHAT each one escapes, because the groups differ and a flat list
// reads as "escapes everything" for all of them.
//
// Escapes every rule. Measured in round 9, each through gofmt, build, vet and
// the full test run:
//
//   - An aliased import. Every rule compares a rendered callee against a literal
//     string, so `import cm ".../climain"; cm.Fail(...)` and
//     `import o "os"; o.Exit(exitFail)` match nothing -- including on an
//     ordinary `if err != nil` path, which is rule 3's core case.
//   - A function VALUE rather than a call: `var bail = os.Exit; bail(exitFail)`.
//     There is no os.Exit call expression to find. The func LITERAL form,
//     `var bail = func() { os.Exit(exitFail) }`, IS caught, by rule 4, which
//     walks whole files and reads the argument.
//   - An os.Exit argument spelled the same as one the policy allows:
//     `code := exitFail; os.Exit(code)` in iotest-verify. Or one the policy
//     allows for another reason, under a condition rule 3 cannot read:
//     `if fatal { os.Exit(exitUsage) }`.
//   - A success code on a failure path. Rule 4 opens with an unconditional skip
//     of `0` and exitPass -- before it consults the policy at all -- so
//     `if fatal { os.Exit(exitPass) }` is out of scope by construction, and no
//     policy entry can bring it back. Rule 3 does not fire either: the condition
//     is fatal, not err. This is the one entry on this list that yields a false
//     PASS rather than a mislabelled failure.
//   - A new exiting helper in a third package. The bans are by name, so one
//     nobody has written yet cannot be named.
//
// Escapes the err-POSITION rule only -- rule 3 here, and invariant_test.go's
// TestNoErrorPathPicksItsOwnVerdict, which calls the same predicate. An os.Exit
// naming a code the tool's policy does not allow is still caught by rule 4, so
// these matter for exits the argument rule cannot judge. Read off
// ErrConditionedBody and MentionsErr rather than measured:
//
//   - An exit in the ELSE arm of an err-conditioned if: ErrConditionedBody
//     returns Body and never Else.
//   - An error held in a variable not named err: MentionsErr matches the
//     identifier err and nothing else.
//   - A type switch on the error: its case clauses hold types, and a type name
//     is not the identifier err.
//
// Escapes parsing altogether, so no rule sees it. Read off LoadTools:
//
//   - A package nested under a tool directory, or a .go file directly in cmd/.
//
// Do not add a count to this list and do not call it complete. Four rounds of
// review have each added an entry.
//
// # Why the rules are lexical
//
// Every rule compares a rendered callee or argument against a literal string,
// which is what the alias entry above defeats. Resolving identity instead --
// go/types, via x/tools -- would close the alias and dot-import cases and
// nothing else here: the scope entries are scope, the argument entries are
// policy, and a function value needs a call graph. Declined 2026-09 on cost --
// it would be this module's first x/tools dependency and a second AST technique
// in a test-only package, spent on the one entry that does not arrive by
// accident. Reopen only for an escape that identity resolution would catch.
//
// The invariants themselves are in exitguard_test.go; this file is only the
// parsing and the two predicates they walk with.
package exitguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Tool is one directory's worth of parsed, non-test source.
//
// Test files are excluded deliberately: the invariants describe the shipped
// binaries' exit paths, and a helper in a _test.go file is not one. Excluding
// them also keeps a re-exec'd test subprocess -- which legitimately calls
// os.Exit(1) to stand in for a failing tool -- from reading as a violation.
type Tool struct {
	Name  string // directory name, e.g. "iotest-verify"
	Fset  *token.FileSet
	Files map[string]*ast.File // keyed by the path passed to the parser
}

// LoadDir parses every non-test .go file in dir.
//
// An empty result is an error rather than an empty Tool: every rule below walks
// what it is given, so a glob that silently matched nothing would let all of
// them pass vacuously. That is not hypothetical -- it is the failure a
// package-local glob produces the moment the test moves.
func LoadDir(dir string) (Tool, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return Tool{}, fmt.Errorf("glob %s: %w", dir, err)
	}
	t := Tool{
		Name:  filepath.Base(dir),
		Fset:  token.NewFileSet(),
		Files: make(map[string]*ast.File, len(paths)),
	}
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(t.Fset, p, nil, parser.ParseComments)
		if err != nil {
			return Tool{}, fmt.Errorf("parse %s: %w", p, err)
		}
		t.Files[p] = f
	}
	if len(t.Files) == 0 {
		return Tool{}, fmt.Errorf("no non-test .go files in %s", dir)
	}
	return t, nil
}

// LoadTools parses every immediate subdirectory of root, in name order.
func LoadTools(root string) ([]Tool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}
	var tools []Tool
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		t, err := LoadDir(filepath.Join(root, e.Name()))
		if err != nil {
			return nil, err
		}
		tools = append(tools, t)
	}
	if len(tools) == 0 {
		return nil, fmt.Errorf("no tool directories under %s", root)
	}
	return tools, nil
}

// CalleeName renders a call's callee as "f" or "pkg.f", and "" for anything
// else (a method value, a func literal). Only the two named forms can be one of
// the exits the rules look for, so returning "" for the rest is not a gap --
// except for a func value, which the package doc lists as a known escape.
func CalleeName(e ast.Expr) string {
	switch f := e.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			return x.Name + "." + f.Sel.Name
		}
	}
	return ""
}

// CallsTo maps enclosing function name -> positions of every call to name.
func CallsTo(fset *token.FileSet, files map[string]*ast.File, name string) map[string][]string {
	found := make(map[string][]string)
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fd, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || CalleeName(call.Fun) != name {
					return true
				}
				found[fd.Name.Name] = append(found[fd.Name.Name], fset.Position(call.Pos()).String())
				return true
			})
		}
	}
	return found
}

// ErrConditionedBody returns the body of n when n is a statement whose
// condition tests an error, and reports whether it is one.
//
// Both spellings count. An "if err != nil" is the common one; a
// "switch { case err != nil: }" is the same decision written differently, and
// keying only on *ast.IfStmt let it through -- measured, not hypothetical.
// ast.Inspect visits a CaseClause directly, so matching it here needs no
// special handling of the enclosing switch, and covers "switch err := f(); {"
// as well because the condition still lands in the case expression.
func ErrConditionedBody(n ast.Node) (ast.Node, bool) {
	switch st := n.(type) {
	case *ast.IfStmt:
		if MentionsErr(st.Cond) {
			return st.Body, true
		}
	case *ast.CaseClause:
		for _, e := range st.List {
			if MentionsErr(e) {
				return &ast.BlockStmt{List: st.Body}, true
			}
		}
	}
	return nil, false
}

// MentionsErr reports whether e references an identifier named err.
//
// Keying on the name is a convention rather than a guarantee: an error held in
// a variable named anything else escapes. That is deliberate -- the alternative
// is pinning an exact call-site list, which stops holding the first time someone
// adds a legitimate exit -- and it is the convention every error site in these
// tools follows.
func MentionsErr(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "err" {
			found = true
		}
		return !found
	})
	return found
}
