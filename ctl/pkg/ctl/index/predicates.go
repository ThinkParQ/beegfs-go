package index

import (
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type PredicateSet struct {
	Clauses []string
	// BeeGFSClauses hold predicates referencing the joined BeeGFS plugin
	// views (aliases b for beegfs_file_view, t for beegfs_file_targets_view).
	// Those aliases only exist after the join, so these clauses must go into
	// the outer WHERE, never into the vrpentries subquery.
	BeeGFSClauses []string
	NeedsBeeGFS   bool
	NeedsTargets  bool
	NeedsSummary  bool

	Empty bool

	// pathPreds hold --path/--regex/--iregex matchers against the full display
	// path. Their SQL depends on the target view's rpath() arity (see
	// entriesWhere/summaryWhere), so they are rendered per-context rather than
	// mixed into Clauses. indexRoot/searchRoot rebase rpath() onto the displayed
	// path and are captured by addPathPredicates.
	pathPreds  []pathPred
	indexRoot  string
	searchRoot string

	Errors []error
}

// pathPred is one --path/--regex/--iregex matcher. The user pattern is quoted at
// render time (via globStr/regexpStr), once the rpath() form for the target view
// is known.
type pathPred struct {
	glob bool   // GLOB when true, REGEXP otherwise
	val  string // user-supplied pattern
}

func (p PredicateSet) Err() error {
	if len(p.Errors) == 0 {
		return nil
	}
	return p.Errors[0]
}

func (p *PredicateSet) addErr(err error) {
	p.Errors = append(p.Errors, err)
}

func (p PredicateSet) WhereClause() string {
	if len(p.Clauses) == 0 {
		return "1"
	}
	return strings.Join(p.Clauses, " AND ")
}

// BeeGFSWhereClause renders the predicates that reference the joined BeeGFS
// views (b.* / t.*) for the outer WHERE position. Returns "1" when empty so
// callers can interpolate it unconditionally.
func (p PredicateSet) BeeGFSWhereClause() string {
	if len(p.BeeGFSClauses) == 0 {
		return "1"
	}
	return strings.Join(p.BeeGFSClauses, " AND ")
}

// Predicate is a single SQL boolean expression bound for a WHERE clause. A
// value of this type can only be produced by the constructors below:
//   - raw / rawf for SQL that contains NO user input (structural clauses,
//     column names, comparison operators, validated integers);
//   - eqStr / globStr / regexpStr for comparisons against a user-supplied
//     string, which they quote internally via sqlQuote.
//
// add and addBeeGFS accept only a Predicate, never a bare string, so every
// user value that reaches SQL has provably passed through sqlQuote. This makes
// the escaping a compile-time guarantee rather than a convention: forgetting
// to quote a value is no longer expressible. To introduce a new kind of string
// comparison, add a constructor here instead of formatting SQL at the call
// site.
type Predicate struct{ sql string }

// raw wraps a constant, trusted SQL fragment verbatim. Use only for SQL that
// contains no user input. The argument must be a literal, never built by
// concatenating a user-supplied string.
func raw(sql string) Predicate { return Predicate{sql} }

// rawf is raw with fmt formatting for trusted operands only: validated
// integers, column names, and comparison operators. The format string and args
// must never carry a user-supplied string value — route those through
// eqStr/globStr/regexpStr so they are quoted.
func rawf(format string, args ...any) Predicate {
	return Predicate{fmt.Sprintf(format, args...)}
}

// eqStr, globStr, and regexpStr compare a column (trusted SQL) to a
// user-supplied string value, quoting the value via sqlQuote. They are the
// only sanctioned way to introduce a string value into a predicate.
func eqStr(col, val string) Predicate     { return Predicate{col + " = " + sqlQuote(val)} }
func globStr(col, val string) Predicate   { return Predicate{col + " GLOB " + sqlQuote(val)} }
func regexpStr(col, val string) Predicate { return Predicate{col + " REGEXP " + sqlQuote(val)} }

// and joins predicates with SQL AND into a single predicate.
func and(preds ...Predicate) Predicate {
	parts := make([]string, len(preds))
	for i, pr := range preds {
		parts[i] = pr.sql
	}
	return Predicate{strings.Join(parts, " AND ")}
}

func (p *PredicateSet) add(clause Predicate) {
	p.Clauses = append(p.Clauses, clause.sql)
}

func (p *PredicateSet) addBeeGFS(clause Predicate) {
	p.BeeGFSClauses = append(p.BeeGFSClauses, clause.sql)
}

type FindCfg struct {
	GlobalCfg
	Name string
	Size []string
	Type string
	Uid  *int
	Gid  *int

	EntryID  string
	OwnerID  string
	TargetID string
	BeeGFS   bool

	MaxDepth *int
	MinDepth *int

	Mtime string
	Atime string
	Ctime string

	Mmin string
	Amin string
	Cmin string

	NewerThan *int64
	AnewThan  *int64
	CnewThan  *int64

	Iname  string
	Path   string
	Regex  string
	Iregex string
	Lname  string

	Inum    string
	InumStr string
	Links   string

	Executable bool
	Readable   bool
	Writable   bool

	Empty bool

	True  bool
	False bool

	NumResults int
	Smallest   bool
	Largest    bool

	ResolveOwnerNames bool
}

func Ptr[T any](v T) *T { return &v }

func BuildFindPredicates(cfg FindCfg) PredicateSet {
	var p PredicateSet

	addParsed := func(flag, col, expr string, parser func(string, string) (string, error)) {
		if expr == "" {
			return
		}
		clause, err := parser(col, expr)
		if err != nil {
			p.addErr(fmt.Errorf("%s %q: %w", flag, expr, err))
			return
		}
		// parser builds trusted SQL from a validated integer, so raw is safe.
		p.add(raw(clause))
	}

	addRegex := func(flag, expr, pattern string, clause Predicate) {
		if expr == "" {
			return
		}
		if _, err := regexp.Compile(pattern); err != nil {
			p.addErr(fmt.Errorf("%s %q: %w", flag, expr, err))
			return
		}
		p.add(clause)
	}

	if cfg.Name != "" {
		p.add(globStr("name", cfg.Name))
	}
	for _, s := range cfg.Size {
		addParsed("--size", "size", s, parseSizeClause)
	}
	switch cfg.Type {
	case "":
	case "d":
		p.NeedsSummary = true
	case "f", "l":
		p.add(eqStr("type", cfg.Type))
	default:
		p.addErr(fmt.Errorf("invalid --type %q: must be f (file), d (directory), or l (symlink)", cfg.Type))
	}
	if cfg.Uid != nil {
		p.add(rawf("uid = %d", *cfg.Uid))
	}
	if cfg.Gid != nil {
		p.add(rawf("gid = %d", *cfg.Gid))
	}

	if cfg.EntryID != "" {
		p.addBeeGFS(globStr("b.entry_id", cfg.EntryID))
		p.NeedsBeeGFS = true
	}

	idClause := func(col, expr string) Predicate {
		if id, err := strconv.ParseInt(expr, 10, 64); err == nil {
			return rawf("%s = %d", col, id)
		}
		return globStr(col, expr)
	}
	if cfg.OwnerID != "" {
		p.addBeeGFS(idClause("b.owner_id", cfg.OwnerID))
		p.NeedsBeeGFS = true
	}
	if cfg.TargetID != "" {
		p.addBeeGFS(idClause("t.target_or_group", cfg.TargetID))
		p.NeedsTargets = true
	}

	addParsed("--mtime", "mtime", cfg.Mtime, parseDayClause)
	addParsed("--atime", "atime", cfg.Atime, parseDayClause)
	addParsed("--ctime", "ctime", cfg.Ctime, parseDayClause)
	addParsed("--mmin", "mtime", cfg.Mmin, parseMinuteClause)
	addParsed("--amin", "atime", cfg.Amin, parseMinuteClause)
	addParsed("--cmin", "ctime", cfg.Cmin, parseMinuteClause)

	if cfg.NewerThan != nil {
		p.add(rawf("mtime > %d", *cfg.NewerThan))
	}
	if cfg.AnewThan != nil {
		p.add(rawf("atime > %d", *cfg.AnewThan))
	}
	if cfg.CnewThan != nil {
		p.add(rawf("ctime > %d", *cfg.CnewThan))
	}

	addRegex("--iname", cfg.Iname, "(?i)"+cfg.Iname,
		regexpStr("name", "(?i)"+cfg.Iname))
	// --path/--regex/--iregex match the full path and are added by Find via
	// addPathPredicates, which needs the index root and search root (unavailable
	// here) to rebase rpath() onto the displayed path.
	if cfg.Lname != "" {
		p.add(and(raw("type = 'l'"), globStr("linkname", cfg.Lname)))
	}

	if cfg.InumStr != "" {
		if _, err := strconv.ParseUint(cfg.InumStr, 10, 64); err != nil {
			p.addErr(fmt.Errorf("--samefile inode %q: %w", cfg.InumStr, err))
		} else {
			p.add(eqStr("inode", cfg.InumStr))
		}
	} else {
		addParsed("--inum", "inode", cfg.Inum, parseTextNumericClause)
	}
	addParsed("--links", "nlink", cfg.Links, parseNumericClause)

	if cfg.Executable {
		p.add(raw("(mode & 64) = 64"))
	}
	if cfg.Readable {
		p.add(raw("(mode & 256) = 256"))
	}
	if cfg.Writable {
		p.add(raw("(mode & 128) = 128"))
	}

	if cfg.Empty {
		p.Empty = true
	}

	if cfg.True {
		p.add(raw("1"))
	}
	if cfg.False {
		p.add(raw("0"))
	}

	return p
}

// rpath() forms for the two view families. A vrpentries row carries the entry
// name, so the 3-arg form yields the full file path; a vrsummary row is a
// directory whose name is its own basename, so rpath already returns the full
// directory path and the 2-arg form must be used or the last segment is doubled
// (e.g. ".../data/sub/sub"). The query templates make the same distinction.
const (
	rpathEntries = "rpath(sname, sroll, name)"
	rpathSummary = "rpath(sname, sroll)"
)

// addPathPredicates records the --path/--regex/--iregex matchers, which match
// the full path as displayed rather than the internal index-rooted rpath().
// They are stored (not emitted into Clauses) because their SQL depends on the
// target view's rpath() arity; entriesWhere/summaryWhere render them once that
// is known. indexRoot/searchRoot come from Find, which has them;
// BuildFindPredicates does not. Invalid regexes are reported eagerly.
func addPathPredicates(p *PredicateSet, cfg FindCfg, indexRoot, searchRoot string) {
	if cfg.Path == "" && cfg.Regex == "" && cfg.Iregex == "" {
		return
	}
	p.indexRoot = indexRoot
	p.searchRoot = searchRoot
	if cfg.Path != "" {
		p.pathPreds = append(p.pathPreds, pathPred{glob: true, val: cfg.Path})
	}
	if cfg.Regex != "" {
		if _, err := regexp.Compile(cfg.Regex); err != nil {
			p.addErr(fmt.Errorf("--regex %q: %w", cfg.Regex, err))
		} else {
			p.pathPreds = append(p.pathPreds, pathPred{val: cfg.Regex})
		}
	}
	if cfg.Iregex != "" {
		if _, err := regexp.Compile("(?i)" + cfg.Iregex); err != nil {
			p.addErr(fmt.Errorf("--iregex %q: %w", cfg.Iregex, err))
		} else {
			p.pathPreds = append(p.pathPreds, pathPred{val: "(?i)" + cfg.Iregex})
		}
	}
}

// pathClause renders the path matchers against the display path reconstructed
// from rpathExpr (the rpath() form of the target view). GUFI's rpath() returns
// indexRoot + "/" + <relative path> (the prefix displayPath strips), so the
// clause strips indexRoot and prepends searchRoot — the path the user queried —
// reproducing the displayed path. length() counts characters, consistent with
// substr() and correct for non-ASCII prefixes. disp embeds the two path operands
// as quoted literals, so it is a trusted column expression passed as the column
// argument to globStr/regexpStr, which quote the user pattern. Returns "" when
// there are no path matchers.
func (p PredicateSet) pathClause(rpathExpr string) string {
	if len(p.pathPreds) == 0 {
		return ""
	}
	disp := fmt.Sprintf("(%s || substr(%s, length(%s) + 1))",
		sqlQuote(filepath.Clean(p.searchRoot)), rpathExpr, sqlQuote(p.indexRoot))
	parts := make([]string, 0, len(p.pathPreds))
	for _, pp := range p.pathPreds {
		if pp.glob {
			parts = append(parts, globStr(disp, pp.val).sql)
		} else {
			parts = append(parts, regexpStr(disp, pp.val).sql)
		}
	}
	return strings.Join(parts, " AND ")
}

// entriesWhere and summaryWhere combine the POSIX clauses (WhereClause) with the
// path matchers rendered for vrpentries and vrsummary respectively. Callers must
// use entriesWhere for templates whose WHERE targets vrpentries/entries and
// summaryWhere for those targeting vrsummary/summary.
func (p PredicateSet) entriesWhere() string { return p.whereWithPath(rpathEntries) }
func (p PredicateSet) summaryWhere() string { return p.whereWithPath(rpathSummary) }

func (p PredicateSet) whereWithPath(rpathExpr string) string {
	base := p.WhereClause()
	path := p.pathClause(rpathExpr)
	if path == "" {
		return base
	}
	return base + " AND " + path
}

type LsCfg struct {
	GlobalCfg
	Name          string
	Type          string
	All           bool
	AlmostAll     bool
	IgnoreBackups bool

	BeeGFS        bool
	Long          bool
	ShowInode     bool
	NoGroup       bool
	ShowBlockSize bool
	BlockSize     string
	FullTime      bool
	SkipFile      string

	Recursive     bool
	AbsolutePaths bool

	Reverse     bool
	SortBySize  bool
	SortByMtime bool

	NumResults int

	ResolveOwnerNames bool
}

func BuildLsPredicates(cfg LsCfg) PredicateSet {
	return buildLsPredicates(cfg, true)
}

func BuildLsDirPredicates(cfg LsCfg) PredicateSet {
	return buildLsPredicates(cfg, false)
}

func buildLsPredicates(cfg LsCfg, includeType bool) PredicateSet {
	var p PredicateSet
	if !cfg.All && !cfg.AlmostAll {
		p.add(raw("name NOT LIKE '.%'"))
	}
	if cfg.IgnoreBackups {
		p.add(raw("name NOT LIKE '%~'"))
	}
	if cfg.Name != "" {
		p.add(globStr("name", cfg.Name))
	}
	if includeType && cfg.Type != "" {
		p.add(eqStr("type", cfg.Type))
	}
	return p
}

type StatCfg struct {
	GlobalCfg
	Filename string
	BeeGFS   bool
}

func BuildStatPredicates(cfg StatCfg) PredicateSet {
	var p PredicateSet
	if cfg.Filename != "" {
		p.add(eqStr("name", cfg.Filename))
	}
	return p
}

// sqlQuote renders s as a SQLite string expression for use as an operand
// (GLOB/LIKE/REGEXP/=). Embedded single quotes are doubled per SQL.
//
// A literal double quote must never appear in the output: remote (gufi_vt)
// queries wrap each SQL fragment in a double-quoted VT parameter
// (E="...", see writeVTParams in executor_remote.go), and that wrapper cannot
// transport a literal " — a raw " breaks SQLite's module-argument tokenizer,
// and a doubled "" reaches gufi_query uncollapsed (corrupting the value).
// Without this an attacker-controlled value could also break out of the
// wrapper and inject additional VT parameters. So any " is emitted as a
// char(34) concatenation, which is byte-clean through the wrapper and
// evaluates back to " on the remote.
func sqlQuote(s string) string {
	if !strings.Contains(s, "\"") {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	var b strings.Builder
	for i, seg := range strings.Split(s, "\"") {
		if i > 0 {
			b.WriteString("||char(34)||")
		}
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(seg, "'", "''"))
		b.WriteByte('\'')
	}
	return b.String()
}

func appendLimit(sql string, n int) string {
	if n > 0 {
		return sql + fmt.Sprintf(" LIMIT %d", n)
	}
	return sql
}

func parseDayClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}
	now := time.Now()

	switch expr[0] {
	case '+', '-':
		op := "<"
		if expr[0] == '-' {
			op = ">"
		}
		n, err := strconv.ParseUint(expr[1:], 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid day expression %q: %w", expr, err)
		}
		if n > math.MaxInt32 {
			return "", fmt.Errorf("day value too large")
		}
		return fmt.Sprintf("%s %s %d", col, op, now.AddDate(0, 0, -int(n)).Unix()), nil
	default:
		n, err := strconv.ParseInt(expr, 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid day expression %q: %w", expr, err)
		}
		upper := now.AddDate(0, 0, -int(n)).Unix()
		lower := now.AddDate(0, 0, -int(n)-1).Unix()
		return fmt.Sprintf("(%s <= %d AND %s > %d)", col, upper, col, lower), nil
	}
}

func parseSizeClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty size expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = ">"
		s = s[1:]
	case '-':
		op = "<"
		s = s[1:]
	}

	if s == "" {
		return "", fmt.Errorf("empty size value after operator")
	}

	var mult int64 = 1
	last := s[len(s)-1]
	switch last {
	case 'c', 'C':
		s = s[:len(s)-1]
	case 'b':
		mult = 512
		s = s[:len(s)-1]
	case 'k', 'K':
		mult = 1024
		s = s[:len(s)-1]
	case 'M':
		mult = 1024 * 1024
		s = s[:len(s)-1]
	case 'G':
		mult = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case 'T':
		mult = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	// ParseUint (not ParseInt) so a sign surviving the operator strip — e.g.
	// "+-5k" leaving "-5" — is rejected rather than silently accepted as a
	// negative size, matching parseNumericClause/parseTextNumericClause.
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid size %q: %w", expr, err)
	}
	if n > uint64(math.MaxInt64)/uint64(mult) {
		return "", fmt.Errorf("size value too large")
	}

	return fmt.Sprintf("%s %s %d", col, op, n*uint64(mult)), nil
}

// maxMinuteMagnitude caps --mmin/--amin/--cmin so time.Duration(n)*time.Minute
// (and the n+1 used for the lower bound) cannot overflow int64, mirroring the
// guard parseSizeClause applies to its unit multiply. The limit is ~1.5e8
// minutes (~292 years), far beyond any real query; the -1 leaves room for n+1.
const maxMinuteMagnitude = math.MaxInt64/int64(time.Minute) - 1

func parseMinuteClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}
	now := time.Now()

	switch expr[0] {
	case '+', '-':
		op := "<"
		if expr[0] == '-' {
			op = ">"
		}
		n, err := strconv.ParseUint(expr[1:], 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid minute expression %q: %w", expr, err)
		}
		if n > uint64(maxMinuteMagnitude) {
			return "", fmt.Errorf("minute value too large")
		}
		return fmt.Sprintf("%s %s %d", col, op, now.Add(-time.Duration(n)*time.Minute).Unix()), nil
	default:
		n, err := strconv.ParseInt(expr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid minute expression %q: %w", expr, err)
		}
		if n > maxMinuteMagnitude {
			return "", fmt.Errorf("minute value too large")
		}
		upper := now.Add(-time.Duration(n) * time.Minute).Unix()
		lower := now.Add(-time.Duration(n+1) * time.Minute).Unix()
		return fmt.Sprintf("(%s <= %d AND %s > %d)", col, upper, col, lower), nil
	}
}

// parseTextNumericClause handles +N/-N/N comparisons on a TEXT-affinity inode column.
// Since integers stored as TEXT sort lexicographically, bare equality is exact, but
// range operators must use a (length, lex) tuple: longer decimal strings are always
// numerically larger, and equal-length strings compare correctly lexicographically.
func parseTextNumericClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}
	s := expr
	sign := expr[0]
	if sign == '+' || sign == '-' {
		s = expr[1:]
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid numeric expression %q: %w", expr, err)
	}
	s = strconv.FormatUint(v, 10)
	L := len(s)
	switch sign {
	case '+':
		return fmt.Sprintf("(length(%s) > %d OR (length(%s) = %d AND %s > '%s'))", col, L, col, L, col, s), nil
	case '-':
		return fmt.Sprintf("(length(%s) < %d OR (length(%s) = %d AND %s < '%s'))", col, L, col, L, col, s), nil
	default:
		return fmt.Sprintf("%s = '%s'", col, s), nil
	}
}

func parseNumericClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = ">"
		s = s[1:]
	case '-':
		op = "<"
		s = s[1:]
	}

	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid numeric expression %q: %w", expr, err)
	}

	return fmt.Sprintf("%s %s %d", col, op, n), nil
}
