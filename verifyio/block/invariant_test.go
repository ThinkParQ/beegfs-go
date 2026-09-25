// This is a unit test.
//
// Coverage: structural invariants that doc comments in this package assert but
// no behavioural test can pin -- claims of the form "every X in this package
// does Y", which stay true only until someone adds a new X.
package block

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// loadPkg parses every .go file in this directory, build tags included. An
// invariant has to hold on files this GOARCH excludes too, which is why this
// globs rather than using go/packages or the deprecated parser.ParseDir.
func loadPkg(t *testing.T) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob *.go: %v (%d files)", err, len(paths))
	}
	fset := token.NewFileSet()
	files := make(map[string]*ast.File, len(paths))
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		files[p] = f
	}
	return fset, files
}

// TestEveryBodyLenWriterIsBounded pins the claim MaxBodyLen's doc makes: that
// the ceiling is a property of the format rather than a check each writer has
// to remember.
//
// That claim was false for three review rounds -- MakeBlock stamped h.BodyLen
// while enforcing nothing, so a block written exactly as asked self-checked
// SIZE_MISMATCH with an explanation blaming its xattr name. A behavioural test
// pins the writers that exist; this pins the ones that do not exist yet, which
// is how the original defect arrived.
//
// Deliberately shallow: it asserts a writer REFERENCES the ceiling, not that it
// enforces it correctly. TestMakeBlockEnforcesMaxBodyLen covers the second half.
func TestEveryBodyLenWriterIsBounded(t *testing.T) {
	// UnmarshalHeader writes BodyLen straight off disk on purpose: that is the
	// untrusted read path, and HeaderImpliedSize is where it gets bounded.
	exempt := map[string]string{
		"UnmarshalHeader": "untrusted read path, bounded at HeaderImpliedSize",
	}

	fset, files := loadPkg(t)
	checked := 0
	for name, f := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var writes, bounded bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range as.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "BodyLen" {
							writes = true
						}
					}
				}
				// An Ident match, so a comment naming MaxBodyLen cannot satisfy this.
				if id, ok := n.(*ast.Ident); ok && id.Name == "MaxBodyLen" {
					bounded = true
				}
				return true
			})
			if !writes {
				continue
			}
			if why, ok := exempt[fn.Name.Name]; ok {
				t.Logf("%s: exempt (%s)", fn.Name.Name, why)
				continue
			}
			checked++
			if !bounded {
				t.Errorf("%s (%s): writes Header.BodyLen but never mentions MaxBodyLen; "+
					"MaxBodyLen's doc claims every writer enforces it",
					fn.Name.Name, fset.Position(fn.Pos()))
			}
		}
	}
	// Without this the walk silently passing would look identical to the walk
	// matching nothing after a refactor.
	if checked == 0 {
		t.Fatal("no BodyLen writers found at all -- the AST walk has stopped matching")
	}
	t.Logf("checked %d BodyLen writer(s)", checked)
}
