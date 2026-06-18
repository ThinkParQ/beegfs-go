package index

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhereClause_NoPredicates(t *testing.T) {
	p := PredicateSet{}
	assert.Equal(t, "1", p.WhereClause())
}

func TestWhereClause_SingleClause(t *testing.T) {
	p := PredicateSet{Clauses: []string{"type = 'f'"}}
	assert.Equal(t, "type = 'f'", p.WhereClause())
}

func TestWhereClause_MultipleClausesJoined(t *testing.T) {
	p := PredicateSet{Clauses: []string{"type = 'f'", "size > 1024"}}
	assert.Equal(t, "type = 'f' AND size > 1024", p.WhereClause())
}

func TestBuildFindPredicates_Name(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Name: "*.bin"})
	assert.Equal(t, "name GLOB '*.bin'", p.WhereClause())
	assert.False(t, p.NeedsBeeGFS)
}

func TestBuildFindPredicates_NameWithSingleQuote(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Name: "it's"})
	assert.Contains(t, p.WhereClause(), "it''s")
}

func TestSqlQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "foo", "'foo'"},
		{"single quote doubled", "it's", "'it''s'"},
		// A literal double quote must never appear in the output: it would
		// break the remote gufi_vt VT-param wrapper (E="...") and could be used
		// to inject extra VT parameters. It is emitted as char(34) concatenation.
		{"double quote", `a"b`, `'a'||char(34)||'b'`},
		{"leading double quote", `"x`, `''||char(34)||'x'`},
		{"vt param injection attempt", `a", threads="9`, `'a'||char(34)||', threads='||char(34)||'9'`},
		{"both quote kinds", `a'"b`, `'a'''||char(34)||'b'`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sqlQuote(tc.in)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, `"`, "output must contain no literal double quote")
		})
	}
}

func TestPredicateConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  Predicate
		want string
	}{
		{"raw verbatim", raw("name NOT LIKE '.%'"), "name NOT LIKE '.%'"},
		{"rawf int operand", rawf("uid = %d", 1000), "uid = 1000"},
		{"eqStr quotes value", eqStr("type", "f"), "type = 'f'"},
		{"eqStr escapes single quote", eqStr("name", "it's"), "name = 'it''s'"},
		{"globStr quotes value", globStr("name", "*.go"), "name GLOB '*.go'"},
		{"regexpStr quotes value", regexpStr("name", "(?i)x"), "name REGEXP '(?i)x'"},
		{"and joins with AND", and(raw("type = 'l'"), globStr("linkname", "*.so")),
			"type = 'l' AND linkname GLOB '*.so'"},
		// Injection attempt: the value's single quotes are doubled, so the whole
		// thing stays a single string literal and cannot break out of the GLOB.
		{"globStr neutralizes injection", globStr("name", "x' OR '1'='1"),
			"name GLOB 'x'' OR ''1''=''1'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got.sql)
		})
	}
}

func TestBuildFindPredicates_Type(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Type: "f"})
	assert.Equal(t, "type = 'f'", p.WhereClause())
}

func TestBuildFindPredicates_UidGid(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: Ptr(0), Gid: Ptr(100)})
	assert.Contains(t, p.WhereClause(), "uid = 0")
	assert.Contains(t, p.WhereClause(), "gid = 100")
}

func TestBuildFindPredicates_UidUnset_Skipped(t *testing.T) {
	p := BuildFindPredicates(FindCfg{})
	assert.Equal(t, "1", p.WhereClause())
}

func TestBuildFindPredicates_EntryID(t *testing.T) {
	p := BuildFindPredicates(FindCfg{EntryID: "D*"})
	assert.True(t, p.NeedsBeeGFS)
	assert.Contains(t, p.BeeGFSWhereClause(), "b.entry_id GLOB 'D*'")
	assert.Equal(t, "1", p.WhereClause(), "b.* clause must not leak into the subquery WHERE")
}

func TestBuildFindPredicates_OwnerID_Numeric(t *testing.T) {
	p := BuildFindPredicates(FindCfg{OwnerID: "2"})
	assert.True(t, p.NeedsBeeGFS)
	assert.Contains(t, p.BeeGFSWhereClause(), "b.owner_id = 2")
	assert.Equal(t, "1", p.WhereClause(), "b.* clause must not leak into the subquery WHERE")
}

func TestBuildFindPredicates_TargetID(t *testing.T) {
	p := BuildFindPredicates(FindCfg{TargetID: "101"})
	assert.True(t, p.NeedsTargets)
	assert.Contains(t, p.BeeGFSWhereClause(), "t.target_or_group = 101")
	assert.Equal(t, "1", p.WhereClause(), "t.* clause must not leak into the subquery WHERE")
}

func TestBuildFindPredicates_MultipleFlags(t *testing.T) {
	p := BuildFindPredicates(FindCfg{
		Name: "*.log",
		Type: "f",
		Size: []string{"+1G"},
	})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "name GLOB '*.log'"))
	assert.True(t, strings.Contains(w, "type = 'f'"))
	assert.True(t, strings.Contains(w, "size > 1073741824"))
	assert.Equal(t, 3, strings.Count(w, " AND ")+1)
}

func TestBuildFindPredicates_MultipleSize(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Size: []string{"+1023c", "-1025c"}})
	assert.Contains(t, p.WhereClause(), "size > 1023")
	assert.Contains(t, p.WhereClause(), "size < 1025")
}

func TestParseSizeClause(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"+1G", "size > 1073741824"},
		{"-100k", "size < 102400"},
		{"+1M", "size > 1048576"},
		{"5T", "size = 5497558138880"},
		{"+0", "size > 0"},
		{"100c", "size = 100"},
		{"+5b", "size > 2560"}, // b = 512-byte blocks (GNU find)
		{"-2b", "size < 1024"},
	}
	for _, tc := range tests {
		got, err := parseSizeClause("size", tc.expr)
		assert.NoError(t, err, "expr=%q", tc.expr)
		assert.Equal(t, tc.want, got, "expr=%q", tc.expr)
	}
}

func TestParseSizeClause_Invalid(t *testing.T) {
	// Each must be rejected, not silently accepted. The doubled-sign cases
	// (a sign surviving the operator strip) previously slipped through as a
	// negative size, e.g. "+-5k" -> "size > -5120".
	for _, expr := range []string{"foo", "+-5k", "-+5k", "+", "k"} {
		t.Run(expr, func(t *testing.T) {
			_, err := parseSizeClause("size", expr)
			assert.Error(t, err)
		})
	}
}

func TestParseSizeClause_Overflow(t *testing.T) {
	// The unit multiply must not silently wrap negative and match everything.
	_, err := parseSizeClause("size", "+9999999999G")
	assert.ErrorContains(t, err, "too large")
}

func TestParseDayClause_Operators(t *testing.T) {
	// Just verify the operator is applied correctly; timestamp varies.
	got, err := parseDayClause("mtime", "-7")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "mtime > "), got)

	got, err = parseDayClause("mtime", "+7")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "mtime < "), got)
}

func TestParseDayClause_DoubledSignRejected(t *testing.T) {
	for _, expr := range []string{"+-5", "-+5", "++5", "--5"} {
		_, err := parseDayClause("mtime", expr)
		assert.Error(t, err, "expr %q must be rejected", expr)
	}
}

func TestParseDayClause_OverflowRejected(t *testing.T) {
	for _, expr := range []string{"+4294967295", "+2147483648", "-2147483648"} {
		_, err := parseDayClause("mtime", expr)
		assert.Error(t, err, "expr %q must be rejected", expr)
	}
	_, err := parseDayClause("mtime", "+2147483647")
	assert.NoError(t, err)
}

func TestParseMinuteClause_DoubledSignRejected(t *testing.T) {
	for _, expr := range []string{"+-5", "-+5", "++5", "--5"} {
		_, err := parseMinuteClause("mtime", expr)
		assert.Error(t, err, "expr %q must be rejected", expr)
	}
}

func TestParseDayClause_BareNWindow(t *testing.T) {
	// Bare N must compile to a one-day window, not a single-second equality
	// (which never matches). floor((now-col)/day) == N.
	got, err := parseDayClause("mtime", "7")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "(mtime <= "), got)
	assert.Contains(t, got, " AND mtime > ", got)
	upper, lower := parseWindowBounds(t, got)
	assert.Greater(t, upper, lower, "upper bound must be later than lower bound")
}

func TestParseMinuteClause_BareNWindow(t *testing.T) {
	got, err := parseMinuteClause("mtime", "5")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "(mtime <= "), got)
	assert.Contains(t, got, " AND mtime > ", got)
	upper, lower := parseWindowBounds(t, got)
	// Window is exactly one minute wide; minute arithmetic has no DST ambiguity.
	assert.Equal(t, int64(60), upper-lower, "minute window must span 60 seconds")
}

// parseWindowBounds extracts the two epoch-second bounds from a clause of the
// form "(col <= UPPER AND col > LOWER)".
func parseWindowBounds(t *testing.T, clause string) (upper, lower int64) {
	t.Helper()
	inner := strings.TrimSuffix(strings.TrimPrefix(clause, "("), ")")
	parts := strings.SplitN(inner, " AND ", 2)
	if len(parts) != 2 {
		t.Fatalf("clause %q is not a two-bound window", clause)
	}
	upperFields := strings.Fields(parts[0])
	lowerFields := strings.Fields(parts[1])
	u, err := strconv.ParseInt(upperFields[len(upperFields)-1], 10, 64)
	assert.NoError(t, err)
	l, err := strconv.ParseInt(lowerFields[len(lowerFields)-1], 10, 64)
	assert.NoError(t, err)
	return u, l
}

func TestBuildFindPredicates_TimeDayPredicates(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Atime: "-3", Ctime: "+10"})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "atime > "), w)
	assert.True(t, strings.Contains(w, "ctime < "), w)
}

func TestBuildFindPredicates_TimeMinutePredicates(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Mmin: "-5", Amin: "+30"})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "mtime > "), w)
	assert.True(t, strings.Contains(w, "atime < "), w)
}

func TestBuildFindPredicates_NewerThan(t *testing.T) {
	p := BuildFindPredicates(FindCfg{NewerThan: Ptr(int64(1700000000))})
	assert.Contains(t, p.WhereClause(), "mtime > 1700000000")
}

func TestBuildFindPredicates_NewerThanEpochZero(t *testing.T) {
	// Epoch 0 is valid; pointer-to-0 emits "mtime > 0", nil skips clause.
	p := BuildFindPredicates(FindCfg{NewerThan: Ptr(int64(0))})
	assert.Contains(t, p.WhereClause(), "mtime > 0")
}

func TestBuildFindPredicates_NewerThanUnset(t *testing.T) {
	// nil = unset; no newer-than clause.
	p := BuildFindPredicates(FindCfg{})
	assert.NotContains(t, p.WhereClause(), "mtime >")
}

func TestBuildFindPredicates_Iname(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Iname: ".*\\.bin"})
	assert.NoError(t, p.Err())
	assert.Contains(t, p.WhereClause(), "name REGEXP '(?i).*\\.bin'")
}

func TestBuildFindPredicates_InameInvalidRegex(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Iname: "*.BIN"})
	assert.Error(t, p.Err())
}

func TestAddPathPredicates_RebasesToDisplayedPath(t *testing.T) {
	// rpath() is index-rooted; --path must match the displayed path, so the clause
	// strips the index root and prepends the search root. The rpath() arity is
	// context-specific: 3-arg for entries (vrpentries) and 2-arg for summary
	// (vrsummary), where the row name is the directory's own basename and the
	// 3-arg form would double the last path segment.
	var p PredicateSet
	addPathPredicates(&p, FindCfg{Path: "/mnt/beegfs/data/*"}, "/var/idx/beegfs/data", "/mnt/beegfs/data")
	assert.NoError(t, p.Err())
	assert.Contains(t, p.entriesWhere(),
		"('/mnt/beegfs/data' || substr(rpath(sname, sroll, name), length('/var/idx/beegfs/data') + 1)) GLOB '/mnt/beegfs/data/*'")
	assert.Contains(t, p.summaryWhere(),
		"('/mnt/beegfs/data' || substr(rpath(sname, sroll), length('/var/idx/beegfs/data') + 1)) GLOB '/mnt/beegfs/data/*'")
	// The path matcher must not leak into the generic POSIX bucket, or it would be
	// emitted with the wrong rpath() arity wherever WhereClause is interpolated.
	assert.Equal(t, "1", p.WhereClause())
}

func TestAddPathPredicates_InvalidRegex(t *testing.T) {
	var p PredicateSet
	addPathPredicates(&p, FindCfg{Regex: "("}, "/idx", "/mnt")
	assert.Error(t, p.Err())
}

func TestBuildFindPredicates_Lname(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Lname: "*.so"})
	assert.Contains(t, p.WhereClause(), "type = 'l'")
	assert.Contains(t, p.WhereClause(), "linkname GLOB '*.so'")
}

func TestBuildFindPredicates_Inum(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Inum: "+1000"})
	assert.Contains(t, p.WhereClause(), "(length(inode) > 4 OR (length(inode) = 4 AND inode > '1000'))")
}

func TestBuildFindPredicates_InumLeadingZeros(t *testing.T) {
	// "+007" must canonicalize to 7 (width 1), not compare against width 3 —
	// otherwise the 1- and 2-digit inodes 8..99 (numerically > 7) are dropped.
	p := BuildFindPredicates(FindCfg{Inum: "+007"})
	assert.Contains(t, p.WhereClause(), "(length(inode) > 1 OR (length(inode) = 1 AND inode > '7'))")

	// Equality canonicalizes too: the inode column stores the unpadded decimal.
	eq := BuildFindPredicates(FindCfg{Inum: "007"})
	assert.Contains(t, eq.WhereClause(), "inode = '7'")
}

func TestBuildFindPredicates_Links(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Links: "2"})
	assert.Contains(t, p.WhereClause(), "nlink = 2")
}

func TestBuildFindPredicates_Permissions(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Executable: true, Readable: true, Writable: true})
	w := p.WhereClause()
	assert.Contains(t, w, "(mode & 64) = 64")
	assert.Contains(t, w, "(mode & 256) = 256")
	assert.Contains(t, w, "(mode & 128) = 128")
}

func TestBuildFindPredicates_Empty(t *testing.T) {
	// --empty signals p.Empty; table-specific clauses emitted in find.go for both tables.
	p := BuildFindPredicates(FindCfg{Empty: true})
	assert.True(t, p.Empty, "Empty flag should be set")
	assert.Equal(t, "1", p.WhereClause(), "no empty clause should be appended directly")
}

func TestBuildLsPredicates_DefaultHidesHidden(t *testing.T) {
	p := BuildLsPredicates(LsCfg{})
	assert.Contains(t, p.WhereClause(), "name NOT LIKE '.%'")
}

func TestBuildLsPredicates_AllShowsHidden(t *testing.T) {
	p := BuildLsPredicates(LsCfg{All: true})
	assert.Equal(t, "1", p.WhereClause())
}

func TestBuildLsPredicates_IgnoreBackups(t *testing.T) {
	p := BuildLsPredicates(LsCfg{All: true, IgnoreBackups: true})
	assert.Contains(t, p.WhereClause(), "name NOT LIKE '%~'")
}

func TestParseMinuteClause(t *testing.T) {
	got, err := parseMinuteClause("mtime", "-10")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "mtime > "), got)

	got, err = parseMinuteClause("mtime", "+10")
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "mtime < "), got)
}

func TestParseMinuteClause_Overflow(t *testing.T) {
	// n*time.Minute overflows int64 above ~1.5e8 minutes; the guard must reject it
	// rather than emit a silently wrong (wrapped-negative) timestamp.
	over := strconv.FormatInt(maxMinuteMagnitude+1, 10)
	for _, expr := range []string{over, "+" + over, "-" + over} {
		_, err := parseMinuteClause("mtime", expr)
		assert.ErrorContains(t, err, "too large", "expr %q", expr)
	}
	// The largest accepted value still produces a valid clause.
	_, err := parseMinuteClause("mtime", strconv.FormatInt(maxMinuteMagnitude, 10))
	assert.NoError(t, err)
}

func TestParseNumericClause(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"+5", "col > 5"},
		{"-5", "col < 5"},
		{"5", "col = 5"},
		{"+0", "col > 0"},
	}
	for _, tc := range tests {
		got, err := parseNumericClause("col", tc.expr)
		assert.NoError(t, err, "expr=%q", tc.expr)
		assert.Equal(t, tc.want, got, "expr=%q", tc.expr)
	}
}

func TestParseNumericClause_Invalid(t *testing.T) {
	_, err := parseNumericClause("col", "abc")
	assert.Error(t, err)
}
