package index

import (
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
	p := BuildFindPredicates(FindCfg{Name: "*.bin", Uid: -1, Gid: -1})
	assert.Equal(t, "name GLOB '*.bin'", p.WhereClause())
	assert.False(t, p.NeedsBeeGFS)
}

func TestBuildFindPredicates_NameWithSingleQuote(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Name: "it's", Uid: -1, Gid: -1})
	assert.Contains(t, p.WhereClause(), "it''s")
}

func TestBuildFindPredicates_Type(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Type: "f", Uid: -1, Gid: -1})
	assert.Equal(t, "type = 'f'", p.WhereClause())
}

func TestBuildFindPredicates_UidGid(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: 0, Gid: 100})
	assert.Contains(t, p.WhereClause(), "uid = 0")
	assert.Contains(t, p.WhereClause(), "gid = 100")
}

func TestBuildFindPredicates_UidNegative_Skipped(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1})
	assert.Equal(t, "1", p.WhereClause())
}

func TestBuildFindPredicates_EntryID(t *testing.T) {
	p := BuildFindPredicates(FindCfg{EntryID: "D*", Uid: -1, Gid: -1})
	assert.True(t, p.NeedsBeeGFS)
	assert.Contains(t, p.WhereClause(), "b.entry_id GLOB 'D*'")
}

func TestBuildFindPredicates_OwnerID_Numeric(t *testing.T) {
	p := BuildFindPredicates(FindCfg{OwnerID: "2", Uid: -1, Gid: -1})
	assert.True(t, p.NeedsBeeGFS)
	assert.Contains(t, p.WhereClause(), "b.owner_id = 2")
}

func TestBuildFindPredicates_TargetID(t *testing.T) {
	p := BuildFindPredicates(FindCfg{TargetID: "101", Uid: -1, Gid: -1})
	assert.True(t, p.NeedsTargets)
	assert.Contains(t, p.WhereClause(), "t.target_or_group = 101")
}

func TestBuildFindPredicates_MultipleFlags(t *testing.T) {
	p := BuildFindPredicates(FindCfg{
		Name: "*.log",
		Type: "f",
		Size: "+1G",
		Uid:  -1,
		Gid:  -1,
	})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "name GLOB '*.log'"))
	assert.True(t, strings.Contains(w, "type = 'f'"))
	assert.True(t, strings.Contains(w, "size > 1073741824"))
	assert.Equal(t, 3, strings.Count(w, " AND ")+1) // three clauses
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
	}
	for _, tc := range tests {
		got, err := parseSizeClause("size", tc.expr)
		assert.NoError(t, err, "expr=%q", tc.expr)
		assert.Equal(t, tc.want, got, "expr=%q", tc.expr)
	}
}

func TestParseSizeClause_Invalid(t *testing.T) {
	_, err := parseSizeClause("size", "foo")
	assert.Error(t, err)
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

func TestBuildFindPredicates_TimeDayPredicates(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Atime: "-3", Ctime: "+10"})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "atime > "), w)
	assert.True(t, strings.Contains(w, "ctime < "), w)
}

func TestBuildFindPredicates_TimeMinutePredicates(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Mmin: "-5", Amin: "+30"})
	w := p.WhereClause()
	assert.True(t, strings.Contains(w, "mtime > "), w)
	assert.True(t, strings.Contains(w, "atime < "), w)
}

func TestBuildFindPredicates_NewerThan(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, NewerThan: 1700000000})
	assert.Contains(t, p.WhereClause(), "mtime > 1700000000")
}

func TestBuildFindPredicates_Iname(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Iname: "*.BIN"})
	assert.Contains(t, p.WhereClause(), "name REGEXP '(?i)*.BIN'")
}

func TestBuildFindPredicates_Path(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Path: "/data/*"})
	assert.Contains(t, p.WhereClause(), "rpath(sname, sroll, name) GLOB '/data/*'")
}

func TestBuildFindPredicates_Lname(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Lname: "*.so"})
	assert.Contains(t, p.WhereClause(), "type = 'l'")
	assert.Contains(t, p.WhereClause(), "linkname GLOB '*.so'")
}

func TestBuildFindPredicates_Inum(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Inum: "+1000"})
	assert.Contains(t, p.WhereClause(), "inode > 1000")
}

func TestBuildFindPredicates_Links(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Links: "2"})
	assert.Contains(t, p.WhereClause(), "nlink = 2")
}

func TestBuildFindPredicates_Permissions(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Executable: true, Readable: true, Writable: true})
	w := p.WhereClause()
	assert.Contains(t, w, "(mode & 64) = 64")
	assert.Contains(t, w, "(mode & 256) = 256")
	assert.Contains(t, w, "(mode & 128) = 128")
}

func TestBuildFindPredicates_Empty(t *testing.T) {
	p := BuildFindPredicates(FindCfg{Uid: -1, Gid: -1, MaxDepth: -1, Empty: true})
	assert.Contains(t, p.WhereClause(), "type = 'f' AND size = 0")
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
