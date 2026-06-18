package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRemoteExecutor_MalformedRowArity: a row split across two output lines
// (a result value containing a newline) must surface as an error from wait()
// instead of being emitted as two silently corrupted rows.
func TestRemoteExecutor_MalformedRowArity(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "gufi_sqlite3")
	// Two lines with different column counts (\037 = unit separator).
	script := "#!/bin/sh\nprintf 'a\\037b\\037c\\nd\\037e\\n'\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &RemoteExecutor{Host: "node1", Sqlite3Bin: bin}
	rows, wait, err := e.Execute(context.Background(), QuerySpec{IndexRoot: "/idx", SQLEntries: "SELECT 1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for range rows {
	}
	if err := wait(); err == nil || !strings.Contains(err.Error(), "columns") {
		t.Fatalf("wait() = %v, want malformed-row arity error", err)
	}
}

// TestWriteVTParams_InlinesPredicates: every gufi_query SQL fragment is passed to gufi_vt verbatim inside a double-quote wrapper with trailing space.
func TestWriteVTParams_InlinesPredicates(t *testing.T) {
	spec := QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE name NOT LIKE '.%'",
		SQLSummary: "SELECT name FROM summary WHERE (totfiles+totlinks)=0",
		IndexRoot:  "/mnt/index",
	}
	var sb strings.Builder
	if err := writeVTParams(&sb, spec, "node1", ""); err != nil {
		t.Fatalf("writeVTParams: %v", err)
	}
	got := sb.String()

	wantE := `E="SELECT name FROM entries WHERE name NOT LIKE '.%' "`
	if !strings.Contains(got, wantE) {
		t.Errorf("E param not inlined verbatim\n got: %s\nwant substring: %s", got, wantE)
	}
	wantS := `S="SELECT name FROM summary WHERE (totfiles+totlinks)=0 "`
	if !strings.Contains(got, wantS) {
		t.Errorf("S param not inlined verbatim\n got: %s\nwant substring: %s", got, wantS)
	}
	if strings.Contains(got, "WHERE 1,") || strings.Contains(got, `WHERE 1 "`) {
		t.Errorf("unexpected WHERE-1 extraction sentinel (extraction should be gone): %s", got)
	}
}

// TestWriteVTParams_CommaInLiteral: double-quote wrapper preserves commas in single-quoted literals.
func TestWriteVTParams_CommaInLiteral(t *testing.T) {
	spec := QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE name = 'unusual, name'",
		IndexRoot:  "/mnt/index",
	}
	var sb strings.Builder
	if err := writeVTParams(&sb, spec, "node1", ""); err != nil {
		t.Fatalf("writeVTParams: %v", err)
	}
	got := sb.String()

	want := `E="SELECT name FROM entries WHERE name = 'unusual, name' "`
	if !strings.Contains(got, want) {
		t.Errorf("comma-bearing literal not kept intact\n got: %s\nwant substring: %s", got, want)
	}
}

// TestBuildVTSQL_BothTiers: E and S predicates coexist; outer SELECT is plain pass-through.
func TestBuildVTSQL_BothTiers(t *testing.T) {
	spec := QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE type='f' AND size=0",
		SQLSummary: "SELECT name FROM summary WHERE (totfiles+totlinks)=0",
		IndexRoot:  "/mnt/index",
	}
	sql, err := buildVTSQL(spec, "node1", "")
	if err != nil {
		t.Fatalf("buildVTSQL returned error for E+S spec: %v", err)
	}
	if !strings.Contains(sql, `E="SELECT name FROM entries WHERE type='f' AND size=0 "`) {
		t.Errorf("E not inlined: %s", sql)
	}
	if !strings.Contains(sql, `S="SELECT name FROM summary WHERE (totfiles+totlinks)=0 "`) {
		t.Errorf("S not inlined: %s", sql)
	}
	if !strings.HasSuffix(sql, "SELECT * FROM temp.q0;") {
		t.Errorf("outer SELECT not a plain pass-through: %s", sql)
	}
	if strings.Contains(sql, " WHERE ") && strings.HasSuffix(sql, "SELECT * FROM temp.q0;") {
		outer := sql[strings.LastIndex(sql, "SELECT * FROM temp.q0"):]
		if strings.Contains(outer, "WHERE") {
			t.Errorf("outer SELECT unexpectedly carries a WHERE: %s", outer)
		}
	}
}

// TestBuildVTSQL_DoubleQuoteRejected: a raw SQL fragment containing '"' cannot survive the
// gufi_vt double-quote wrapper, so buildVTSQL rejects it with a clear error.
func TestBuildVTSQL_DoubleQuoteRejected(t *testing.T) {
	spec := QuerySpec{
		SQLEntries: `SELECT name FROM entries WHERE name = "quoted"`,
		IndexRoot:  "/mnt/index",
	}
	_, err := buildVTSQL(spec, "node1", "")
	if err == nil {
		t.Fatal("expected error for double quote in SQL fragment, got nil")
	}
	if !strings.Contains(err.Error(), "-E SQL contains a double quote") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestVTParamConstructors locks in the escaping contract of each vtParam
// constructor — the boundary that keeps user/path values from reaching the
// remote shell unescaped.
func TestVTParamConstructors(t *testing.T) {
	t.Run("output", func(t *testing.T) {
		cases := []struct{ name, got, want string }{
			{"rawParam verbatim", rawParam("remote_cmd='ssh'").s, "remote_cmd='ssh'"},
			{"intParam", intParam("threads", 4).s, "threads=4"},
			{"sqlParam dq-escapes + wraps", sqlParam("E", `x '$(id)'`).s, `E="x '\$(id)' "`},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if c.got != c.want {
					t.Errorf("got %q, want %q", c.got, c.want)
				}
			})
		}
	})

	t.Run("bareArgParam escapes and rejects", func(t *testing.T) {
		got, err := bareArgParam("index", "/mnt/a b")
		if err != nil || got.s != `index='/mnt/a\ b '` {
			t.Errorf("got (%q, %v), want index='/mnt/a\\ b ', nil", got.s, err)
		}
		if _, err := bareArgParam("index", "/mnt/o'brien"); err == nil {
			t.Error("bareArgParam must reject a single quote")
		}
		if _, err := bareArgParam("index", "/mnt/a\nb"); err == nil {
			t.Error("bareArgParam must reject a newline")
		}
	})

	t.Run("sqConstParam rejects single quote", func(t *testing.T) {
		got, err := sqConstParam("plugin", "/opt/beegfs/lib/plugin.so")
		if err != nil || got.s != "plugin='/opt/beegfs/lib/plugin.so'" {
			t.Errorf("got (%q, %v), want plugin='/opt/beegfs/lib/plugin.so', nil", got.s, err)
		}
		if _, err := sqConstParam("plugin", "/opt/p'lugin"); err == nil {
			t.Error("sqConstParam must reject a single quote")
		}
	})
}

// TestWriteVTParams_PluginSingleQuoteRejected: a plugin path with a single quote
// would break out of gufi_vt's single-quoted word on the remote shell, so it is
// rejected rather than escaped.
func TestWriteVTParams_PluginSingleQuoteRejected(t *testing.T) {
	var sb strings.Builder
	err := writeVTParams(&sb, QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE 1",
		IndexRoot:  "/i",
		PluginPath: "/opt/p'lugin",
	}, "node1", "")
	if err == nil {
		t.Fatal("expected error for plugin path with single quote, got nil")
	}
	if !strings.Contains(err.Error(), "plugin path") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRemoteExecutor_SkipFileRejected: remote executor rejects skip file (gufi_vt has no skip parameter).
func TestRemoteExecutor_SkipFileRejected(t *testing.T) {
	e := &RemoteExecutor{Host: "node1", Sqlite3Bin: "/opt/beegfs/bin/index/gufi_sqlite3"}
	_, _, err := e.Execute(context.Background(), QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE 1",
		SkipFile:   "/tmp/skip.txt",
	})
	if err == nil {
		t.Fatal("expected error for SkipFile on remote executor, got nil")
	}
	if !strings.Contains(err.Error(), "--skip is not supported") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestWriteVTParams_EscapesRemoteShellExpansion: $, ` and \ in SQL fragments are
// backslash-escaped so the remote login shell (which re-parses gufi_vt's flattened
// -E "<sql>" inside double quotes) treats them as literals instead of expanding them.
func TestWriteVTParams_EscapesRemoteShellExpansion(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "command-substitution",
			sql:  `SELECT name FROM entries WHERE name GLOB '$(touch /tmp/pwned)'`,
			want: `E="SELECT name FROM entries WHERE name GLOB '\$(touch /tmp/pwned)' "`,
		},
		{
			name: "backtick",
			sql:  "SELECT name FROM entries WHERE name GLOB '`id`'",
			want: `E="SELECT name FROM entries WHERE name GLOB '\` + "`" + `id\` + "`" + `' "`,
		},
		{
			name: "backslash",
			sql:  `SELECT name FROM entries WHERE name GLOB 'a\b'`,
			want: `E="SELECT name FROM entries WHERE name GLOB 'a\\b' "`,
		},
		{
			name: "variable-expansion",
			sql:  `SELECT name FROM entries WHERE name = '${HOME}'`,
			want: `E="SELECT name FROM entries WHERE name = '\${HOME}' "`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sb strings.Builder
			err := writeVTParams(&sb, QuerySpec{SQLEntries: c.sql, IndexRoot: "/mnt/index"}, "node1", "")
			if err != nil {
				t.Fatalf("writeVTParams: %v", err)
			}
			if got := sb.String(); !strings.Contains(got, c.want) {
				t.Errorf("SQL not shell-escaped\n got: %s\nwant substring: %s", got, c.want)
			}
		})
	}
}

// TestWriteVTParams_EscapesIndexRoot: index roots are flattened into the ssh
// command unquoted by gufi_vt, so every shell metacharacter must be escaped;
// single quotes and newlines cannot be transported and are rejected.
func TestWriteVTParams_EscapesIndexRoot(t *testing.T) {
	cases := []struct {
		name    string
		root    string
		want    string // substring of the generated params; empty means expect error
		wantErr bool
	}{
		{name: "plain", root: "/mnt/index", want: `index='/mnt/index '`},
		{name: "space", root: "/mnt/my index", want: `index='/mnt/my\ index '`},
		{name: "dollar", root: "/mnt/$x", want: `index='/mnt/\$x '`},
		{name: "semicolon", root: "/mnt/a;b", want: `index='/mnt/a\;b '`},
		{name: "single-quote-rejected", root: "/mnt/o'brien", wantErr: true},
		{name: "newline-rejected", root: "/mnt/a\nb", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sb strings.Builder
			err := writeVTParams(&sb, QuerySpec{SQLEntries: "SELECT name FROM entries WHERE 1", IndexRoot: c.root}, "node1", "")
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for index root %q, got params: %s", c.root, sb.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("writeVTParams: %v", err)
			}
			if got := sb.String(); !strings.Contains(got, c.want) {
				t.Errorf("index root not shell-escaped\n got: %s\nwant substring: %s", got, c.want)
			}
		})
	}
}

// TestBareArgGlobParam locks in the glob-preserving index escaper: the four glob
// metacharacters * ? [ ] survive bare (so the remote shell expands them) while
// every other metacharacter is still escaped and a single quote / newline is
// rejected — the boundary that keeps a remote-glob index root from carrying an
// injection.
func TestBareArgGlobParam(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    string // expected vtParam.s; only checked when wantErr is false
		wantErr bool
	}{
		{name: "glob metachars stay bare", val: "/idx/mnt/data/*/", want: `index='/idx/mnt/data/*/ '`},
		{name: "question mark and brackets bare", val: "/idx/d?[ab]/", want: `index='/idx/d?[ab]/ '`},
		{name: "dangerous bytes still escaped", val: "/idx/$x;(id)/*", want: `index='/idx/\$x\;\(id\)/* '`},
		{name: "space still escaped", val: "/idx/a b/*", want: `index='/idx/a\ b/* '`},
		{name: "single quote rejected", val: "/idx/o'brien/*", wantErr: true},
		{name: "newline rejected", val: "/idx/a\nb/*", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := bareArgGlobParam("index", c.val)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %q", c.val, got.s)
				}
				return
			}
			if err != nil {
				t.Fatalf("bareArgGlobParam: %v", err)
			}
			if got.s != c.want {
				t.Errorf("got %q, want %q", got.s, c.want)
			}
		})
	}
}

// TestWriteVTParams_IndexRootGlob: IndexRootGlob is the single switch that flips
// the index escaping — the glob metacharacters reach the remote shell unescaped
// when it is set, and stay backslash-escaped when it is not.
func TestWriteVTParams_IndexRootGlob(t *testing.T) {
	const root = "/idx/mnt/data/*/"

	var glob strings.Builder
	if err := writeVTParams(&glob, QuerySpec{
		SQLEntries:    "SELECT name FROM entries WHERE 1",
		IndexRoot:     root,
		IndexRootGlob: true,
	}, "node1", ""); err != nil {
		t.Fatalf("writeVTParams (glob): %v", err)
	}
	if !strings.Contains(glob.String(), `index='/idx/mnt/data/*/ '`) {
		t.Errorf("glob index root must reach the remote shell unescaped\n got: %s", glob.String())
	}

	var plain strings.Builder
	if err := writeVTParams(&plain, QuerySpec{
		SQLEntries: "SELECT name FROM entries WHERE 1",
		IndexRoot:  root,
	}, "node1", ""); err != nil {
		t.Fatalf("writeVTParams (plain): %v", err)
	}
	if !strings.Contains(plain.String(), `index='/idx/mnt/data/\*/ '`) {
		t.Errorf("without the flag the * must stay escaped\n got: %s", plain.String())
	}
}

// TestWriteVTParams_BatchModeBeforeHost: SSH transport options (BatchMode, ServerAlive)
// are injected as remote_arg words that precede the host, so ssh parses them as options
// rather than as the host or part of the remote command. Mirrors the create-path
// assertion in remote_subprocess_test.go.
func TestWriteVTParams_BatchModeBeforeHost(t *testing.T) {
	var sb strings.Builder
	if err := writeVTParams(&sb, QuerySpec{SQLEntries: "SELECT name FROM entries WHERE 1", IndexRoot: "/i"}, "node1", ""); err != nil {
		t.Fatalf("writeVTParams: %v", err)
	}
	got := sb.String()

	for _, want := range []string{
		"remote_arg='-o', remote_arg='BatchMode=yes'",
		"remote_arg='-o', remote_arg='ServerAliveInterval=5'",
		"remote_arg='-o', remote_arg='ServerAliveCountMax=2'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing transport option %q\n got: %s", want, got)
		}
	}

	bi := strings.Index(got, "remote_arg='BatchMode=yes'")
	hi := strings.Index(got, "remote_arg='node1'")
	if bi < 0 || hi < 0 || bi >= hi {
		t.Fatalf("BatchMode option must come before host\n got: %s", got)
	}
}

// TestBuildVTSQL_RemotePATHInjection: buildVTSQL prepends index bindir to remote PATH so ssh resolves version-matched binary.
func TestBuildVTSQL_RemotePATHInjection(t *testing.T) {
	spec := QuerySpec{SQLEntries: "SELECT name FROM entries WHERE 1"}

	cases := []struct {
		name       string
		binDir     string
		wantInject bool
	}{
		{name: "absolute", binDir: "/opt/beegfs/bin/index", wantInject: true},
		{name: "relative-skipped", binDir: ".", wantInject: false},
		{name: "empty-skipped", binDir: "", wantInject: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sql, err := buildVTSQL(spec, "node1", c.binDir)
			if err != nil {
				t.Fatalf("buildVTSQL: %v", err)
			}
			env := fmt.Sprintf("remote_arg='env', remote_arg='PATH=%s:$PATH'", c.binDir)
			has := strings.Contains(sql, env)
			if has != c.wantInject {
				t.Fatalf("injection=%v want=%v\n  sql: %s", has, c.wantInject, sql)
			}
			if c.wantInject {
				host := "remote_arg='node1'"
				if hi, ei := strings.Index(sql, host), strings.Index(sql, env); hi < 0 || ei < hi {
					t.Fatalf("env tokens not after host\n  sql: %s", sql)
				}
				if !strings.Contains(sql, ":$PATH'") {
					t.Fatalf("$PATH not preserved literally\n  sql: %s", sql)
				}
			}
		})
	}
}
