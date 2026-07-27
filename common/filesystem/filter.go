package filesystem

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

type FileInfo struct {
	Path  string    `expr:"path"`  // Full file path
	Name  string    `expr:"name"`  // Base name of the file
	Size  int64     `expr:"size"`  // File size in bytes
	Mode  uint32    `expr:"mode"`  // raw mode bits from syscall.Stat_t (type + permissions)
	Perm  uint32    `expr:"perm"`  // just the permission bits (mode & 0777)
	Mtime time.Time `expr:"mtime"` // Modification time
	Atime time.Time `expr:"atime"` // Access time
	Ctime time.Time `expr:"ctime"` // Change time
	Uid   uint32    `expr:"uid"`   // User ID
	Gid   uint32    `expr:"gid"`   // Group ID

	EntryID        string `expr:"entryid"`
	MetaNode       int    `expr:"metanode"`
	MetaMirrored   bool   `expr:"metamirrored"`
	MetaBuddyGroup int    `expr:"metabuddygroup"`

	Pattern          string `expr:"pattern"`
	ChunkSize        int64  `expr:"chunksize"`
	NumTargets       int    `expr:"numtargets"`
	Pool             int    `expr:"pool"`
	PoolName         string `expr:"poolname"`
	Mirrored         bool   `expr:"mirrored"`
	Targets          []int  `expr:"targets"`
	BuddyGroups      []int  `expr:"buddygroups"`
	AllocatedTargets []int  `expr:"allocatedtargets"`
	DataState        string `expr:"datastate"`
	Offloaded        bool   `expr:"offloaded"`
	Access           string `expr:"access"`
	Locked           bool   `expr:"locked"`
	RstIDs           []int  `expr:"rstids"`
}

// File type filter expressions and definitions
var (
	fileTypeMask  = 0o170000
	fileTypes     = map[string]uint32{"file": 0o100000, "directory": 0o040000, "symlink": 0o120000, "block": 0o060000, "char": 0o020000, "fifo": 0o010000, "socket": 0o140000}
	fileTypeNames = func() []string {
		names := make([]string, 0, len(fileTypes))
		for name := range fileTypes {
			names = append(names, name)
		}
		return names
	}()
	fileTypeGroupRe = "(?:" + strings.Join(fileTypeNames, "|") + ")"
	fileTypeRe      = regexp.MustCompile(`\b(?i)type\s*(==|!=)\s*(` + fileTypeGroupRe + `(?:\s*,\s*` + fileTypeGroupRe + `)*)\b`)
)

var (
	// modeOctRe insists on a leading 0 with 5-6 octal digits. This targets classic stat-style
	// literals like 0100644 without touching plain decimal values like 33188. The rewrite is scoped
	// to mode to avoid clobbering other fields that might also match.
	modeOctRe = regexp.MustCompile(`\b(?i)(mode)\s*(==|!=|<=|>=|<|>)\s*(0[0-7]{5,6})\b`)
	// permOctRe allows 3-4 digits with an optional leading zero. This matches the chmod style
	// notation users expect for permissions like 644 or 01644. Because the rewrite is scoped to
	// perm, relaxing the prefix doesn't risk clobbering arbitrary decimal limits (like sizes).
	permOctRe = regexp.MustCompile(`\b(?i)(perm)\s*(==|!=|<=|>=|<|>)\s*(0?[0-7]{3,4})\b`)
	timeRe    = regexp.MustCompile(`\b(?i)(mtime|atime|ctime)\s*(<=|>=|<|>)\s*([0-9]+(?:\.[0-9]+)?[smhdMyw]+)\b`)
	sizeRe    = regexp.MustCompile(`\b(?i)(size|chunksize)\s*(<=|>=|<|>|!=|=)\s*([0-9]+(?:\.[0-9]+)?(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB))\b`)
	litRe = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`)

	unitFactors = map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
	}
)

type fieldReq struct {
	stat   bool
	entry  bool
	detail bool
}

// dslFields is the single source of truth mapping each DSL field name (lowercase, as the user
// types it) to the data its evaluation requires. FileInfo's expr struct tags map these same
// names to Go fields; TestFieldTableMatchesTags asserts the two never drift apart. Fields with
// no requirement (name, path) are derived from the path alone and need no fetch.
var dslFields = map[string]fieldReq{
	"name": {}, "path": {},

	"size": {stat: true}, "mode": {stat: true}, "perm": {stat: true},
	"mtime": {stat: true}, "atime": {stat: true}, "ctime": {stat: true},
	"uid": {stat: true}, "gid": {stat: true},

	// Entry info available even when the inode is locked/rebalancing.
	"entryid": {entry: true}, "metanode": {entry: true},
	"metamirrored": {entry: true}, "metabuddygroup": {entry: true},

	// Entry details (imply entry info too; unavailable when the inode is locked).
	"pattern": {entry: true, detail: true}, "chunksize": {entry: true, detail: true},
	"numtargets": {entry: true, detail: true}, "pool": {entry: true, detail: true},
	"poolname": {entry: true, detail: true}, "mirrored": {entry: true, detail: true},
	"targets": {entry: true, detail: true}, "buddygroups": {entry: true, detail: true},
	"datastate": {entry: true, detail: true}, "offloaded": {entry: true, detail: true},
	"access": {entry: true, detail: true}, "locked": {entry: true, detail: true},
	"rstids": {entry: true, detail: true},

	// Needs both an lstat and the entry details.
	"allocatedtargets": {stat: true, entry: true, detail: true},
}

// fieldVisitor is the expr AST visitor registered via expr.Patch. In a single structural walk
// it does two jobs that used to require six regexes:
//
//   - Case-insensitivity: it lowercases every identifier node that names a known field, so
//     `MTIME` resolves through FileInfo's expr:"mtime" tag exactly like `mtime`.
//   - Needs-detection: it records which data the referenced fields require.
//
// Because it inspects typed AST nodes, a field-like word inside a string literal (e.g.
// glob(name, "*targets*")) is a StringNode, never an IdentifierNode, so it can never be
// mistaken for a field reference. That is why detection no longer needs the query text to be
// masked for string literals.
type fieldVisitor struct {
	stat, entry, detail bool
}

func (fv *fieldVisitor) Visit(node *ast.Node) {
	id, ok := (*node).(*ast.IdentifierNode)
	if !ok {
		return
	}
	lower := strings.ToLower(id.Value)
	req, ok := dslFields[lower]
	if !ok {
		return // a function name, builtin, or unknown identifier — leave it for the checker
	}
	id.Value = lower
	fv.stat = fv.stat || req.stat
	fv.entry = fv.entry || req.entry
	fv.detail = fv.detail || req.detail
}

const FilterFilesHelp = "Filter files by expression. " +
	"POSIX fields(name/path <string>, uid/gid <int>, " +
	"mode <octal[like 0100644, 0o0100644] | decimal[like 33188]>, perm <octal[like 644, 0644, 0o0644]>, " +
	"type <file|directory|symlink|block|char|fifo|socket>, " +
	"mtime/atime/ctime <duration[like 1s, 2m, 3h, 4d, 5M, 10y]>, size <bytes[like 1B, 2KB, 3MiB, 4GiB]>). " +
	"BeeGFS fields(entryid <string>, metanode/metabuddygroup <int>, metamirrored/mirrored/offloaded/locked <bool>, " +
	"pattern <raid0|raid10|buddymirror>, chunksize <bytes>, numtargets/pool <int>, poolname <string>, " +
	"targets/buddygroups/allocatedtargets/rstids <int list, use 'N in targets'>, " +
	"datastate <available|manualrestore|autorestore|delayedrestore|unavailable>, " +
	"access <unlocked|readlock|writelock|readwritelock>). " +
	"operators(==,!=,<,>,<=,>=); helpers(glob([name|path], pattern), regex([name|path], pattern)); " +
	"logic(and|or|not); Examples: --filter-files=\"mtime > 365d and type == file,symlink and glob(name, '*.txt')\", " +
	"--filter-files=\"offloaded or 5 in targets\". "

type FileInfoFilter func(FileInfo) (bool, error)

type Filter struct {
	eval             FileInfoFilter
	query            string
	needsStat        bool
	needsEntryInfo   bool
	needsEntryDetail bool
}

// NeedsStat reports whether the expression references any field populated from an lstat.
func (f *Filter) NeedsStat() bool { return f.needsStat }

// NeedsEntryInfo reports whether the expression references any BeeGFS field, and therefore
// requires the entry to be fetched before it can be evaluated.
func (f *Filter) NeedsEntryInfo() bool { return f.needsEntryInfo }

// NeedsEntryDetails reports whether the expression references a BeeGFS field that comes from
// the GetEntryInfoResponse details (which are unavailable when the inode is locked).
func (f *Filter) NeedsEntryDetails() bool { return f.needsEntryDetail }

// Evaluate runs the compiled expression against fi.
func (f *Filter) Evaluate(fi FileInfo) (bool, error) { return f.eval(fi) }

// Compile turns a DSL expression into a Filter, reporting which data (stat, entry info,
// entry details) the expression references so callers can fetch only what is needed.
func Compile(query string) (*Filter, error) {
	// Preprocess DSL: rewrite the un-lexable sugar (octal/duration/size literals and type sets)
	// into valid expr, protecting string literals while doing so.
	q := preprocessDSL(query)

	// fv both normalizes identifier case and records which data the referenced fields need,
	// during expr's own AST walk (see fieldVisitor).
	fv := &fieldVisitor{}
	prog, err := expr.Compile(q,
		expr.Env(FileInfo{}),
		expr.Patch(fv),
		expr.Function("ago", func(params ...any) (any, error) { return ago(params[0].(string)) }),
		expr.Function("bytes", func(params ...any) (any, error) { return parseBytes(params[0].(string)) }),
		expr.Function("glob", func(params ...any) (any, error) { return globMatch(params[0].(string), params[1].(string)) }),
		expr.Function("regex", func(params ...any) (any, error) { return regexMatch(params[0].(string), params[1].(string)) }),
		expr.Function("now", func(params ...any) (any, error) { return time.Now(), nil }),
	)
	if err != nil {
		return nil, err
	}

	eval := func(fi FileInfo) (bool, error) {
		out, err := expr.Run(prog, fi)
		if err != nil {
			return false, fmt.Errorf("filter eval %q on %s: %w", query, fi.Path, err)
		}
		result, ok := out.(bool)
		if !ok {
			return false, fmt.Errorf("filter expression resulted in a non-boolean value of type %T. Make sure your filter is a valid comparison (e.g., 'size>100MB')", out)
		}
		return result, nil
	}

	return &Filter{
		eval:             eval,
		query:            query,
		needsStat:        fv.stat,
		needsEntryInfo:   fv.entry,
		needsEntryDetail: fv.detail,
	}, nil
}

// CompileFilter compiles a filter for contexts that can only evaluate against stat data
// (no BeeGFS entry info is available, e.g. server-side RST walks). It returns an error if
// the expression references BeeGFS metadata fields. Callers that can fetch entry info
// should use Compile and Decide instead.
func CompileFilter(query string) (FileInfoFilter, error) {
	f, err := Compile(query)
	if err != nil {
		return nil, err
	}
	if f.needsEntryInfo {
		return nil, fmt.Errorf("filter references BeeGFS metadata fields, which are not supported in this context (only POSIX file attributes are available here)")
	}
	return f.eval, nil
}

// preprocessDSL rewrites the terse sugar that expr's lexer cannot tokenize into valid expr:
// bare octal literals, duration/size literals, and type sets. String literals are extracted
// first and restored afterwards so field-like words inside quoted values (e.g.
// poolname == "fast-targets") are never rewritten. Identifier field names are NOT rewritten
// here: expr resolves them via FileInfo's struct tags, and fieldVisitor normalizes their case.
func preprocessDSL(q string) string {
	q, lits := protectLiterals(q)
	// octal to decimal
	q = normalizeOctal(q, permOctRe)
	q = normalizeOctal(q, modeOctRe)
	// file types
	q = setFileType(q)
	// time shifts: "mtime > 365d" means "older than 365 days", i.e. an mtime earlier than the
	// cutoff, so the comparison is flipped to `mtime < ago("365d")`. The field name is emitted
	// as matched; fieldVisitor lowercases it to resolve through the struct tag.
	q = timeRe.ReplaceAllStringFunc(q, func(m string) string {
		parts := timeRe.FindStringSubmatch(m)
		f, op, val := parts[1], parts[2], parts[3]
		switch op {
		case ">":
			op = "<"
		case "<":
			op = ">"
		case ">=":
			op = "<="
		case "<=":
			op = ">="
		}
		return fmt.Sprintf("%s %s ago(%q)", f, op, val)
	})
	// size units
	q = sizeRe.ReplaceAllString(q, `$1 $2 bytes("$3")`)
	return restoreLiterals(q, lits)
}

// protectLiterals replaces every quoted string literal in q with an opaque placeholder that
// cannot match any rewrite regex, returning the masked string and the extracted literals. The
// placeholders are restored with restoreLiterals after all rewrites have run.
func protectLiterals(q string) (string, []string) {
	var lits []string
	out := litRe.ReplaceAllStringFunc(q, func(m string) string {
		i := len(lits)
		lits = append(lits, m)
		// NUL bytes never appear in the DSL and are non-word characters, so the placeholder is
		// invisible to \b-anchored identifier/unit/octal rewrites.
		return fmt.Sprintf("\x00L%d\x00", i)
	})
	return out, lits
}

// restoreLiterals reverses protectLiterals, putting the original quoted literals back in place.
func restoreLiterals(q string, lits []string) string {
	for i, lit := range lits {
		q = strings.Replace(q, fmt.Sprintf("\x00L%d\x00", i), lit, 1)
	}
	return q
}

// setFileType transform a type expressions one or more into mode filters to include or exclude
// specific file types.
func setFileType(q string) string {
	return fileTypeRe.ReplaceAllStringFunc(q, func(expr string) string {
		parts := fileTypeRe.FindStringSubmatch(expr)
		op, types := parts[1], parts[2]

		used := make(map[string]struct{})
		clauses := []string{}
		for name := range strings.SplitSeq(types, ",") {
			name = strings.TrimSpace(strings.ToLower(name))
			if _, ok := used[name]; ok {
				continue
			}
			if _, ok := fileTypes[name]; !ok {
				return expr
			}
			used[name] = struct{}{}
			clauses = append(clauses, fmt.Sprintf("bitand(int(mode), %d) == %d", fileTypeMask, fileTypes[name]))
		}

		expr = "(" + strings.Join(clauses, " or ") + ")"
		if op == "!=" {
			return fmt.Sprintf("not %s", expr)
		}
		return expr
	})
}

// normalizeOctal takes a string and parses it to a base8 integer after stripping an optional 0o
// prefix or leading 0. For example 644, 0644, and 0o644 are all converted to 420.
func normalizeOctal(q string, re *regexp.Regexp) string {
	return re.ReplaceAllStringFunc(q, func(expr string) string {
		sub := re.FindStringSubmatch(expr)
		if len(sub) != 4 {
			return expr
		}
		field, op, lit := sub[1], sub[2], sub[3]
		if strings.HasPrefix(lit, "0o") || strings.HasPrefix(lit, "0O") {
			lit = lit[2:]
		} else if strings.HasPrefix(lit, "0") {
			lit = lit[1:]
		}
		val, err := strconv.ParseInt(lit, 8, 64)
		if err != nil {
			return expr
		}
		return fmt.Sprintf("%s %s %d", field, op, val)
	})
}

func StatToFileInfo(path string, st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Path:  path,
		Name:  filepath.Base(path),
		Size:  st.Size,
		Mode:  st.Mode,
		Perm:  st.Mode & 0o7777, // special permissions bit + os.ModePerm
		Atime: time.Unix(st.Atim.Sec, st.Atim.Nsec),
		Mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Ctime: time.Unix(st.Ctim.Sec, st.Ctim.Nsec),
		Uid:   st.Uid,
		Gid:   st.Gid,
	}
}

// posixFileInfo builds a FileInfo containing only POSIX fields. When st is nil (the filter
// does not reference any stat field) only the path-derived fields are populated.
func posixFileInfo(path string, st *syscall.Stat_t) FileInfo {
	if st == nil {
		return FileInfo{Path: path, Name: filepath.Base(path)}
	}
	return StatToFileInfo(path, st)
}

// Decide applies the compiled filter f to a single path with a fetch-once short-circuit,
// returning the value produced by fetch (the zero value if the filter short-circuited before
// fetching), whether the entry should be skipped (filtered out), and any error. It is the
// shared core reused by every filtered processEntry so the short-circuit and single-fetch
// logic is not duplicated per caller.
//
//   - f == nil: fetch is always called and nothing is skipped.
//   - POSIX-only filter: evaluated from the path (and an lstat via lstat if NeedsStat)
//     WITHOUT calling fetch; a non-match skips without fetching, a match then calls fetch so
//     the caller still receives the value.
//   - filter references BeeGFS metadata: fetch is called once, then toInfo builds the full
//     FileInfo from the fetched value and the whole expression is evaluated.
//
// lstat is called at most once and only when the filter needs stat data. toInfo may return an
// error (for example when entry details are unavailable); Decide returns that error together
// with the already-fetched value so the caller can apply its own policy without re-fetching.
func Decide[T any](
	f *Filter,
	path string,
	lstat func() (*syscall.Stat_t, error),
	fetch func() (T, error),
	toInfo func(path string, st *syscall.Stat_t, fetched T) (FileInfo, error),
) (fetched T, skip bool, err error) {
	var zero T

	if f == nil {
		fetched, err = fetch()
		return fetched, false, err
	}

	var st *syscall.Stat_t
	if f.needsStat {
		if st, err = lstat(); err != nil {
			return zero, false, err
		}
	}

	// POSIX-only: evaluate before fetching so non-matching entries never trigger a fetch.
	if !f.needsEntryInfo {
		keep, err := f.eval(posixFileInfo(path, st))
		if err != nil {
			return zero, false, err
		}
		if !keep {
			return zero, true, nil
		}
		fetched, err = fetch()
		return fetched, false, err
	}

	// References BeeGFS metadata (possibly mixed with POSIX): fetch once then evaluate whole.
	if fetched, err = fetch(); err != nil {
		return zero, false, err
	}
	fi, err := toInfo(path, st, fetched)
	if err != nil {
		return fetched, false, err
	}
	keep, err := f.eval(fi)
	if err != nil {
		return fetched, false, err
	}
	if !keep {
		return fetched, true, nil
	}
	return fetched, false, nil
}

// ago returns time.Now() minus parsed duration.
func ago(durationStr string) (time.Time, error) {
	d, err := parseExtendedDuration(durationStr)
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(-d), nil
}

// parseExtendedDuration supports standard and custom units (d, M, y).
func parseExtendedDuration(s string) (time.Duration, error) {
	// fast path for Go durations
	sfx := s[len(s)-1]
	if strings.IndexByte("nsmh", sfx) != -1 {
		return time.ParseDuration(s)
	}
	var factor time.Duration
	num, unit := s[:len(s)-1], s[len(s)-1:]
	switch unit {
	case "d":
		factor = 24 * time.Hour
	case "M":
		factor = 30 * 24 * time.Hour
	case "y":
		factor = 365 * 24 * time.Hour
	default:
		return time.ParseDuration(s)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return time.Duration(f * float64(factor)), nil
}

// parseBytes converts size strings into byte counts.
func parseBytes(sizeStr string) (int64, error) {
	i := len(sizeStr)
	for i > 0 && (sizeStr[i-1] < '0' || sizeStr[i-1] > '9') {
		i--
	}
	num, unit := sizeStr[:i], strings.TrimSpace(sizeStr[i:])
	if unit == "" {
		unit = "B"
	}
	mul, ok := unitFactors[unit]
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q", unit)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", sizeStr, err)
	}
	return int64(f * mul), nil
}

// globMatch uses filepath.Match
func globMatch(s, pattern string) (bool, error) {
	return filepath.Match(pattern, s)
}

// regexMatch uses precompiled regex
func regexMatch(s, pattern string) (bool, error) {
	return regexp.MatchString(pattern, s)
}

// ApplyFilter returns whether the file should be kept. If filter==nil then (true, nil) will be
// returned.
//
// Use ApplyFilterByStatT instead of ApplyFilter when the in-mount path and file
// stat information is already known to avoid making duplicate stat calls.
func ApplyFilter(inMountPath string, filter FileInfoFilter, client Provider) (keep bool, err error) {
	if filter == nil {
		return true, nil
	}

	info, err := client.Lstat(inMountPath)
	if err != nil {
		return false, fmt.Errorf("unable to filter file: %w", err)
	}

	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unable to retrieve stat information: unsupported platform")
	}

	if keep, err = filter(StatToFileInfo(inMountPath, statT)); err != nil {
		return false, fmt.Errorf("unable to apply filter: %w", err)
	}
	return
}

// ApplyFilterByStatT returns whether the file should be kept. If filter==nil then (true, nil) will
// be returned.
//
// ApplyFilterByStatT should be used instead of ApplyFilter when the in-mount path and stat are
// already known to avoid duplicate stat calls.
func ApplyFilterByStatT(inMountPath string, statT *syscall.Stat_t, filter FileInfoFilter) (keep bool, err error) {
	if filter == nil {
		return true, nil
	}

	if keep, err = filter(StatToFileInfo(inMountPath, statT)); err != nil {
		return false, fmt.Errorf("unable to apply filter: %w", err)
	}
	return
}
