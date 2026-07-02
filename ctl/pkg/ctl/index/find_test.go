package index

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// splitFindSQL splits a generated find SQL statement into the WHERE clause of
// the vrpentries subquery and everything after the subquery (joins + outer
// WHERE).
func splitFindSQL(t *testing.T, sql string) (inner, outer string) {
	t.Helper()
	const start = "(SELECT * FROM vrpentries "
	const end = ") AS v"
	i := strings.Index(sql, start)
	require.GreaterOrEqual(t, i, 0, "expected subquery %q in: %s", start, sql)
	j := strings.Index(sql, end)
	require.Greater(t, j, i, "expected subquery alias %q in: %s", end, sql)
	return sql[i+len(start) : j], sql[j:]
}

func TestFindEntriesSQL_PredicatePlacement(t *testing.T) {
	tests := []struct {
		name      string
		cfg       FindCfg
		wantInner []string // substrings required inside the vrpentries subquery
		wantOuter []string // substrings required after the join (outer WHERE)
	}{
		{
			name:      "entryID only",
			cfg:       FindCfg{EntryID: "1-6A034344-1"},
			wantInner: []string{"WHERE 1"},
			wantOuter: []string{"WHERE b.entry_id GLOB '1-6A034344-1'"},
		},
		{
			name:      "ownerID only",
			cfg:       FindCfg{OwnerID: "1"},
			wantInner: []string{"WHERE 1"},
			wantOuter: []string{"WHERE b.owner_id = 1"},
		},
		{
			name:      "targetID only",
			cfg:       FindCfg{TargetID: "101"},
			wantInner: []string{"WHERE 1"},
			wantOuter: []string{"beegfs_file_targets_view", "WHERE t.target_or_group = 101"},
		},
		{
			name:      "ownerID combined with POSIX type",
			cfg:       FindCfg{OwnerID: "1", Type: "f"},
			wantInner: []string{"WHERE type = 'f'"},
			wantOuter: []string{"WHERE b.owner_id = 1"},
		},
		{
			name:      "entryID combined with POSIX name and size",
			cfg:       FindCfg{EntryID: "D*", Name: "*.bin", Size: []string{"+1k"}},
			wantInner: []string{"name GLOB '*.bin'", "size > 1024"},
			wantOuter: []string{"WHERE b.entry_id GLOB 'D*'"},
		},
		{
			name:      "targetID combined with ownerID",
			cfg:       FindCfg{TargetID: "101", OwnerID: "1"},
			wantInner: []string{"WHERE 1"},
			wantOuter: []string{"b.owner_id = 1", "t.target_or_group = 101", "beegfs_file_view"},
		},
		{
			name:      "beegfs output without BeeGFS predicates",
			cfg:       FindCfg{BeeGFS: true, Type: "f"},
			wantInner: []string{"WHERE type = 'f'"},
			wantOuter: []string{"WHERE 1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			preds := BuildFindPredicates(tc.cfg)
			require.NoError(t, preds.Err())

			sql := findEntriesSQL(tc.cfg, preds)
			inner, outer := splitFindSQL(t, sql)

			for _, want := range tc.wantInner {
				assert.Contains(t, inner, want)
			}
			for _, want := range tc.wantOuter {
				assert.Contains(t, outer, want)
			}

			// The join aliases do not exist inside the subquery; any b.* or
			// t.* reference there fails to prepare (SQL logic error).
			assert.NotContains(t, inner, "b.", "BeeGFS clause leaked into subquery")
			assert.NotContains(t, inner, "t.", "targets clause leaked into subquery")
		})
	}
}

func TestFindEntriesSQL_CoreHasSingleWhere(t *testing.T) {
	cfg := FindCfg{Type: "f"}
	preds := BuildFindPredicates(cfg)
	sql := findEntriesSQL(cfg, preds)
	assert.Equal(t, 1, strings.Count(sql, "WHERE"))
	assert.Contains(t, sql, "WHERE type = 'f'")
	// Owner columns are numeric when the user/group columns won't be displayed,
	// avoiding an uncached per-row uidtouser/gidtogroup lookup.
	assert.Contains(t, sql, "mode, uid, gid, nlink")
	assert.NotContains(t, sql, "uidtouser")
	assert.NotContains(t, sql, "%s", "all placeholders must be filled")
}

// TestFindEntriesSQL_ResolvesOwnerNamesOnlyWhenRequested: the GUFI name
// resolution is emitted only when ResolveOwnerNames is set (the user/group
// columns will be displayed); the WHERE placement is unaffected.
func TestFindEntriesSQL_ResolvesOwnerNamesOnlyWhenRequested(t *testing.T) {
	cfg := FindCfg{Type: "f", ResolveOwnerNames: true}
	preds := BuildFindPredicates(cfg)
	sql := findEntriesSQL(cfg, preds)
	assert.Contains(t, sql, "uidtouser(uid), gidtogroup(gid)")
	assert.Equal(t, 1, strings.Count(sql, "WHERE"))
	assert.NotContains(t, sql, "%s", "all placeholders must be filled")
}

// assertOwnerSQL checks that a generated find/ls statement filled its owner
// placeholder exactly once: numeric columns when names are not displayed, GUFI
// resolution when they are, and never a leftover %s or %!(arity) artifact.
func assertOwnerSQL(t *testing.T, sql string, resolve bool) {
	t.Helper()
	if sql == "" {
		return
	}
	assert.NotContains(t, sql, "%s", "unfilled placeholder in: %s", sql)
	assert.NotContains(t, sql, "%!", "Sprintf arity artifact in: %s", sql)
	if resolve {
		assert.Contains(t, sql, "uidtouser(", "names must resolve when displayed: %s", sql)
		assert.Contains(t, sql, "gidtogroup(", "names must resolve when displayed: %s", sql)
	} else {
		assert.NotContains(t, sql, "uidtouser(", "must not resolve when not displayed: %s", sql)
		assert.NotContains(t, sql, "gidtogroup(", "must not resolve when not displayed: %s", sql)
	}
}

// TestFind_OwnerResolutionGating: the --type d (FindDirS) and --empty
// (FindAggGSelect) paths must gate uid/gid name resolution on ResolveOwnerNames
// the same way the core entries path does.
func TestFind_OwnerResolutionGating(t *testing.T) {
	cases := []struct {
		name  string
		cfg   FindCfg
		field func(QuerySpec) string
	}{
		{"type-d summary", FindCfg{Type: "d"}, func(s QuerySpec) string { return s.SQLSummary }},
		{"empty aggregate", FindCfg{Empty: true}, func(s QuerySpec) string { return s.SQLAggregate }},
	}
	for _, c := range cases {
		for _, resolve := range []bool{false, true} {
			t.Run(c.name+"/resolve="+strconv.FormatBool(resolve), func(t *testing.T) {
				cfg := c.cfg
				cfg.ResolveOwnerNames = resolve
				rec := &specRecorder{}
				_, _, err := Find(context.Background(), rec, cfg, "/idx", "/idx", "/mnt", false)
				require.NoError(t, err)
				assertOwnerSQL(t, c.field(rec.spec), resolve)
			})
		}
	}
}

// specRecorder captures the QuerySpec passed to Execute so tests can assert
// on the generated SQL without running gufi_query.
type specRecorder struct{ spec QuerySpec }

func (r *specRecorder) Execute(_ context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	r.spec = spec
	ch := make(chan []string)
	close(ch)
	return ch, func() error { return nil }, nil
}

// TestFind_TypeDirSortUsesAggregate: --type d with --smallest/--largest must
// route through the aggregate stage; -S runs per directory, so ordering there
// alone cannot sort across directories.
func TestFind_TypeDirSortUsesAggregate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       FindCfg
		wantOrder string
	}{
		{name: "largest", cfg: FindCfg{Type: "d", Largest: true, NumResults: 5}, wantOrder: "ORDER BY size DESC"},
		{name: "smallest", cfg: FindCfg{Type: "d", Smallest: true, NumResults: 5}, wantOrder: "ORDER BY size ASC"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &specRecorder{}
			_, _, err := Find(context.Background(), rec, tc.cfg, "/idx", "/idx", "/mnt", false)
			require.NoError(t, err)
			assert.Empty(t, rec.spec.SQLEntries)
			assert.Contains(t, rec.spec.SQLSummary, "INSERT INTO "+statsIntermedTable)
			assert.Contains(t, rec.spec.SQLSummary, "isroot=1")
			assert.Contains(t, rec.spec.SQLSummary, tc.wantOrder)
			assert.Contains(t, rec.spec.SQLAggregate, tc.wantOrder)
			assert.Contains(t, rec.spec.SQLAggregate, "LIMIT 5")
		})
	}
}

func TestFind_TypeDirNoSortKeepsStreaming(t *testing.T) {
	rec := &specRecorder{}
	_, _, err := Find(context.Background(), rec, FindCfg{Type: "d"}, "/idx", "/idx", "/mnt", false)
	require.NoError(t, err)
	assert.Empty(t, rec.spec.SQLInit)
	assert.Contains(t, rec.spec.SQLSummary, "FROM vrsummary WHERE isroot=1")
}

// TestFind_AggregateBranchesDeclareColumns: the aggregate / summary find
// branches emit the 12 core columns, so they must declare spec.Columns for the
// remote executor's row-arity validation (a single mis-framed row would
// otherwise set the arity from itself and pass undetected).
func TestFind_AggregateBranchesDeclareColumns(t *testing.T) {
	cases := []struct {
		name string
		cfg  FindCfg
	}{
		{"empty", FindCfg{Empty: true}},
		{"type-dir summary", FindCfg{Type: "d"}},
		{"smallest entries", FindCfg{Smallest: true, NumResults: 5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &specRecorder{}
			_, _, err := Find(context.Background(), rec, tc.cfg, "/idx", "/idx", "/mnt", false)
			require.NoError(t, err)
			assert.Equal(t, findCoreColumns, rec.spec.Columns)
		})
	}
}

// TestFind_PathPredicateRpathArity: --path/--regex match the display path, which
// is reconstructed from rpath(). Queries against vrsummary (--type d, and the
// directory leg of --empty) must use 2-arg rpath(sname, sroll) — a summary row's
// name is the directory's own basename, so the 3-arg form would double the last
// segment and silently match nothing — while entries queries (vrpentries) use
// the 3-arg form.
func TestFind_PathPredicateRpathArity(t *testing.T) {
	t.Run("type-dir summary uses 2-arg rpath", func(t *testing.T) {
		rec := &specRecorder{}
		_, _, err := Find(context.Background(), rec, FindCfg{Type: "d", Path: "*/sub"}, "/idx", "/idx", "/mnt", false)
		require.NoError(t, err)
		assert.Contains(t, rec.spec.SQLSummary, "substr(rpath(sname, sroll), length")
		assert.NotContains(t, rec.spec.SQLSummary, "rpath(sname, sroll, name)")
	})
	t.Run("default entries uses 3-arg rpath", func(t *testing.T) {
		rec := &specRecorder{}
		_, _, err := Find(context.Background(), rec, FindCfg{Path: "*/f.txt"}, "/idx", "/idx", "/mnt", false)
		require.NoError(t, err)
		assert.Contains(t, rec.spec.SQLEntries, "substr(rpath(sname, sroll, name), length")
	})
	t.Run("empty splits arity across the dir and file legs", func(t *testing.T) {
		rec := &specRecorder{}
		_, _, err := Find(context.Background(), rec, FindCfg{Empty: true, Path: "*/x"}, "/idx", "/idx", "/mnt", false)
		require.NoError(t, err)
		assert.Contains(t, rec.spec.SQLSummary, "substr(rpath(sname, sroll), length")
		assert.NotContains(t, rec.spec.SQLSummary, "rpath(sname, sroll, name)")
		assert.Contains(t, rec.spec.SQLEntries, "substr(rpath(sname, sroll, name), length")
	})
}

// TestFind_EmptyRejectsSizeSort: the --empty aggregate pipeline carries no
// ORDER BY, so size-sort flags would be silently dropped — reject the combo.
func TestFind_EmptyRejectsSizeSort(t *testing.T) {
	tests := []struct {
		name string
		cfg  FindCfg
	}{
		{name: "largest", cfg: FindCfg{Empty: true, Largest: true}},
		{name: "smallest", cfg: FindCfg{Empty: true, Smallest: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Find(context.Background(), &specRecorder{}, tc.cfg, "/idx", "/idx", "/mnt", false)
			require.ErrorContains(t, err, "--empty cannot be combined with --smallest/--largest")
		})
	}
}
