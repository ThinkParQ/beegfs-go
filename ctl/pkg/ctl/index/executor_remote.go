package index

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// remoteColSep avoids TLV delimiter collision (gufi_sqlite3 cannot emit TLV).
// Unit Separator (0x1F) does not appear in normal paths, unlike '|'.
const remoteColSep = "\x1f"

// RemoteExecutor runs queries via gufi_vt in gufi_sqlite3 :memory: on a remote host.
type RemoteExecutor struct {
	Host       string
	Sqlite3Bin string
}

func (e *RemoteExecutor) Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	log, _ := config.GetLogger()

	if spec.SkipFile != "" {
		return nil, nil, fmt.Errorf("--skip is not supported with a remote index (--index-addr ssh:<host>): gufi_vt cannot forward a skip file to the remote node")
	}

	sql, err := buildVTSQL(spec, e.Host, filepath.Dir(e.Sqlite3Bin))
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	// SECURITY (load-bearing): gufi_sqlite3 opens its database argument with
	// SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, so it *can* write on-disk files.
	// We deliberately point it at an in-memory database (":memory:") and touch
	// the real indexes only indirectly, through the read-only gufi_query that
	// gufi_vt spawns on the remote node. A malformed or injected query can thus
	// only scribble on this throwaway in-memory DB, never an index on disk.
	// Do NOT change ":memory:" to an on-disk path without first making
	// gufi_sqlite3 read-only; the Go-side query escaping (sqlQuote / the
	// Predicate constructors in predicates.go) is defense-in-depth, not a
	// substitute for this boundary — and `index query` passes raw user SQL that
	// the Predicate type cannot guard.
	args := []string{"-d", remoteColSep, ":memory:"}
	cmd := exec.CommandContext(ctx, e.Sqlite3Bin, args...)
	cmd.SysProcAttr = CallerSysProcAttr()
	cmd.Stdin = strings.NewReader(sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	log.Debug("running gufi_sqlite3 (remote VT)",
		zap.String("bin", e.Sqlite3Bin),
		zap.String("host", e.Host),
		zap.String("indexRoot", spec.IndexRoot),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("creating stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("starting gufi_sqlite3: %w", err)
	}

	rows := make(chan []string, chanBufSize(spec.Threads))

	// A plain errgroup suffices: the goroutine selects on ctx (cancelled by
	// wait), and nothing observes a derived context, so WithContext would add
	// only an unused cancel.
	var g errgroup.Group
	g.Go(func() error {
		defer close(rows)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64<<10), scannerMaxBuf)
		// Seed the expected arity from the caller's declared columns so even the
		// first (or only) row is validated. Without it a single mis-framed row —
		// a value containing the 0x1F column separator or a newline — would set
		// the arity itself and pass undetected. Falls back to the first row's
		// width when the caller did not declare columns (still catches later rows).
		arity := -1
		if len(spec.Columns) > 0 {
			arity = len(spec.Columns)
		}
		for scanner.Scan() {
			row := strings.Split(scanner.Text(), remoteColSep)
			if arity < 0 {
				arity = len(row)
			} else if len(row) != arity {
				_ = cmd.Process.Kill()
				waitErr := cmd.Wait()
				return errors.Join(
					fmt.Errorf("gufi_sqlite3 returned a row with %d columns, expected %d: a result value likely contains a newline or the column separator (0x1F), which the remote index path cannot transport", len(row), arity),
					waitErr,
					stderrErr(&stderr),
				)
			}
			select {
			case rows <- row:
			case <-ctx.Done():
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return ctx.Err()
			}
		}
		if scanErr := scanner.Err(); scanErr != nil {
			_ = cmd.Process.Kill()
			waitErr := cmd.Wait()
			return errors.Join(
				fmt.Errorf("scanning gufi_sqlite3 output: %w", scanErr),
				waitErr,
				stderrErr(&stderr),
			)
		}
		if err := cmd.Wait(); err != nil {
			return errors.Join(
				fmt.Errorf("gufi_sqlite3: %w", err),
				stderrErr(&stderr),
			)
		}
		return nil
	})

	wait := func() error {
		cancel()
		return g.Wait()
	}
	return rows, wait, nil
}

// buildVTSQL generates single-line SQL for gufi_sqlite3 stdin: one gufi_vt
// table over the remote host, selected as-is.
func buildVTSQL(spec QuerySpec, host string, remoteBinDir string) (string, error) {
	for _, f := range []struct{ flag, sql string }{
		{"-E", spec.SQLEntries}, {"-S", spec.SQLSummary}, {"-T", spec.SQLTreeSum},
		{"-I", spec.SQLInit}, {"-K", spec.SQLAggInit}, {"-J", spec.SQLIntermed},
		{"-G", spec.SQLAggregate}, {"-F", spec.SQLFinal},
	} {
		if strings.Contains(f.sql, `"`) {
			return "", fmt.Errorf("%s SQL contains a double quote, which cannot be passed to a remote index (gufi_vt wraps SQL parameters in double quotes); rewrite the query without it", f.flag)
		}
	}

	var sb strings.Builder
	sb.WriteString("CREATE VIRTUAL TABLE temp.q0 USING gufi_vt(")
	if err := writeVTParams(&sb, spec, host, remoteBinDir); err != nil {
		return "", err
	}
	sb.WriteString("); SELECT * FROM temp.q0;")

	return sb.String(), nil
}

// vtParam is a single gufi_vt module argument (key=value) in the CREATE VIRTUAL
// TABLE statement. A vtParam can only be produced by the constructors below,
// each of which applies the escaping required for the shell context its value
// lands in once gufi_vt's flatten_argv re-emits the gufi_query invocation onto
// the remote ssh command line (see gufi_vt.c). Because writeVTParams collects
// only vtParam values — never a bare fmt.Sprintf into the param list — a user-
// or path-derived value cannot reach the remote shell without passing through
// the matching escaper. To add a parameter that carries external input, add a
// constructor here rather than formatting it inline, so the escaping cannot be
// forgotten.
type vtParam struct{ s string }

// rawParam wraps a fully trusted, literal parameter that we control end to end
// (e.g. remote_cmd='ssh'). Its argument must contain no user- or path-derived
// data — use sqlParam/bareArgParam/sqConstParam/pathEnvParam for those.
func rawParam(s string) vtParam { return vtParam{s} }

// intParam emits key=<int>. Integers carry no shell-special bytes, so no
// escaping is needed.
func intParam(key string, val int) vtParam {
	return vtParam{fmt.Sprintf("%s=%d", key, val)}
}

// sqlParam emits a SQL fragment as key="<value> ". gufi_vt re-emits -E/-S/...
// fragments inside DOUBLE quotes on the remote shell, so the value is escaped
// for that double-quoted context: escapeRemoteShellDQ neutralizes $, ` and \,
// and buildVTSQL has already rejected any literal " (which would close the
// wrapper). The trailing space is consumed by gufi_vt's set_refstr() quote
// stripping; the remote shell then discards it during word splitting.
func sqlParam(key, sql string) vtParam {
	return vtParam{fmt.Sprintf("%s=\"%s \"", key, escapeRemoteShellDQ(sql))}
}

// bareArgParam emits key='<value> ' for a value that gufi_vt re-emits UNQUOTED
// on the remote shell (index roots). The single quotes wrap the value for
// gufi_vt's own module-arg tokenizer; escapeRemoteShellArg backslash-escapes
// every metacharacter for the unquoted remote position and rejects the two
// bytes that cannot be transported (a single quote and a newline).
func bareArgParam(key, val string) (vtParam, error) {
	esc, err := escapeRemoteShellArg(val)
	if err != nil {
		return vtParam{}, err
	}
	return vtParam{fmt.Sprintf("%s='%s '", key, esc)}, nil
}

// bareArgGlobParam is bareArgParam for an index root that intentionally carries
// a shell glob (QuerySpec.IndexRootGlob): escapeRemoteShellArgGlob leaves the
// glob metacharacters * ? [ ] unescaped so the remote login shell expands them
// into the matching index trees, while every other metacharacter is still
// escaped. The single quotes are gufi_vt's own (stripped by set_refstr); the
// value lands unquoted on the remote shell exactly as with bareArgParam.
func bareArgGlobParam(key, val string) (vtParam, error) {
	esc, err := escapeRemoteShellArgGlob(val)
	if err != nil {
		return vtParam{}, err
	}
	return vtParam{fmt.Sprintf("%s='%s '", key, esc)}, nil
}

// sqConstParam emits key='<value>' for a value placed inside SINGLE quotes on
// the remote shell (the --plugin path; also the ssh host). Single quotes
// neutralize every metacharacter except ' itself, so a literal ' is rejected
// rather than escaped — SQL-style doubling would not survive gufi_vt's quote
// stripping. Intended for constants and pre-validated values; the rejection is
// defense in depth.
func sqConstParam(key, val string) (vtParam, error) {
	if strings.Contains(val, "'") {
		return vtParam{}, fmt.Errorf("%q contains a single quote, which cannot be passed to a remote index", val)
	}
	return vtParam{fmt.Sprintf("%s='%s'", key, val)}, nil
}

// pathEnvParam emits remote_arg='PATH=<dir>:$PATH', prepending dir to the remote
// PATH so the version-matched gufi_query is resolved. dir lands unquoted on the
// remote shell (escapeRemoteShellArg), while the literal :$PATH suffix is left
// expandable by that shell.
func pathEnvParam(dir string) (vtParam, error) {
	esc, err := escapeRemoteShellArg(dir)
	if err != nil {
		return vtParam{}, err
	}
	return vtParam{fmt.Sprintf("remote_arg='PATH=%s:$PATH'", esc)}, nil
}

func writeVTParams(sb *strings.Builder, spec QuerySpec, host, remoteBinDir string) error {
	params := make([]vtParam, 0, 16)

	addSQL := func(key, val string) {
		if val != "" {
			params = append(params, sqlParam(key, val))
		}
	}
	addSQL("E", spec.SQLEntries)
	addSQL("S", spec.SQLSummary)
	addSQL("T", spec.SQLTreeSum)
	addSQL("I", spec.SQLInit)
	addSQL("K", spec.SQLAggInit)
	addSQL("J", spec.SQLIntermed)
	addSQL("G", spec.SQLAggregate)
	addSQL("F", spec.SQLFinal)

	if spec.AddUp >= 1 && spec.AddUp <= 2 {
		params = append(params, intParam("a", spec.AddUp))
	}
	if spec.Threads > 0 {
		params = append(params, intParam("threads", spec.Threads))
	}
	if spec.MinLevel > 0 {
		params = append(params, intParam("min_level", spec.MinLevel))
	}
	if spec.MaxLevel >= 0 {
		params = append(params, intParam("max_level", spec.MaxLevel))
	}

	// plugin lands inside single quotes on the remote shell; the value is a
	// build-time constant path, rejected (not escaped) if it contains a quote.
	if spec.PluginPath != "" {
		p, err := sqConstParam("plugin", spec.PluginPath)
		if err != nil {
			return fmt.Errorf("plugin path: %w", err)
		}
		params = append(params, p)
	}

	// index root lands unquoted on the remote shell. A spec flagged
	// IndexRootGlob carries a deliberate shell glob: its metacharacters are left
	// unescaped so the remote login shell expands it into the matching trees.
	var (
		idx vtParam
		err error
	)
	if spec.IndexRootGlob {
		idx, err = bareArgGlobParam("index", spec.IndexRoot)
	} else {
		idx, err = bareArgParam("index", spec.IndexRoot)
	}
	if err != nil {
		return fmt.Errorf("index path: %w", err)
	}
	params = append(params, idx)

	params = append(params,
		rawParam("remote_cmd='ssh'"),
		rawParam("remote_arg='-o'"),
		rawParam("remote_arg='BatchMode=yes'"),
		rawParam("remote_arg='-o'"),
		rawParam("remote_arg='ServerAliveInterval=5'"),
		rawParam("remote_arg='-o'"),
		rawParam("remote_arg='ServerAliveCountMax=2'"),
	)

	// host is shell-safe by construction (validateSSHHost allows only
	// [user@]host chars); sqConstParam's quote rejection is belt-and-suspenders.
	h, err := sqConstParam("remote_arg", host)
	if err != nil {
		return fmt.Errorf("ssh host: %w", err)
	}
	params = append(params, h)

	// Inject env PATH=<bindir>:$PATH to resolve the version-matched gufi_query on
	// the remote node. ssh joins its argv into one remote shell word stream, so
	// the bindir is escaped while the :$PATH suffix stays expandable.
	if filepath.IsAbs(remoteBinDir) {
		params = append(params, rawParam("remote_arg='env'"))
		pe, err := pathEnvParam(remoteBinDir)
		if err != nil {
			return fmt.Errorf("remote bin dir: %w", err)
		}
		params = append(params, pe)
	}

	parts := make([]string, len(params))
	for i, pr := range params {
		parts[i] = pr.s
	}
	sb.WriteString(strings.Join(parts, ", "))
	return nil
}

// remoteShellDQEscaper escapes the characters a POSIX shell expands inside
// double quotes. gufi_vt's remote mode flattens the whole gufi_query
// invocation into one ssh argument, emitting each SQL fragment as -E "<sql>"
// (flatten_argv in gufi_vt.c); the remote login shell re-parses that string,
// so an unescaped $, ` or \ in a user-supplied predicate value would be
// expanded — up to command execution — on the remote host. A literal " cannot
// occur here: sqlQuote emits it as char(34) and buildVTSQL rejects leftovers.
var remoteShellDQEscaper = strings.NewReplacer(`\`, `\\`, `$`, `\$`, "`", "\\`")

func escapeRemoteShellDQ(s string) string {
	return remoteShellDQEscaper.Replace(s)
}

// escapeRemoteShellArg backslash-escapes s for an unquoted position in the
// flattened remote command (index roots, PATH injection). Two characters
// cannot be transported and are rejected: a single quote, because gufi_vt's
// set_refstr() strips quote characters from parameter ends and SQLite passes
// module arguments verbatim (SQL-style quote doubling would reach the remote
// uncollapsed); and a newline, because backslash-newline is removed by the
// shell as a line continuation.
func escapeRemoteShellArg(s string) (string, error) {
	if strings.ContainsAny(s, "'\n") {
		return "", fmt.Errorf("%q contains a single quote or newline, which cannot be passed to a remote index", s)
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !remoteShellSafeRune(r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String(), nil
}

// escapeRemoteShellArgGlob is escapeRemoteShellArg with the four glob
// metacharacters * ? [ ] emitted bare instead of backslash-escaped, so the
// remote login shell expands an index-root glob into the matching trees (see
// bareArgGlobParam). Every other metacharacter ($ ` ; | & ( ) < > space …) is
// still escaped, so the injection surface is identical to escapeRemoteShellArg —
// only filename globbing is enabled — and a single quote or newline is still
// rejected as untransportable. Note: only the four bytes are bare, so unusual
// characters *inside* a bracket expression remain escaped; plain * and ? are the
// fully supported forms.
func escapeRemoteShellArgGlob(s string) (string, error) {
	if strings.ContainsAny(s, "'\n") {
		return "", fmt.Errorf("%q contains a single quote or newline, which cannot be passed to a remote index", s)
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !remoteShellSafeRune(r) && !isGlobMetaRune(r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String(), nil
}

// isGlobMetaRune reports whether r is a POSIX shell glob metacharacter that the
// remote login shell expands against the index filesystem.
func isGlobMetaRune(r rune) bool {
	return r == '*' || r == '?' || r == '[' || r == ']'
}

func remoteShellSafeRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r > 127: // non-ASCII is never special to the shell
		return true
	}
	return strings.ContainsRune("_./-:,+@%=", r)
}
