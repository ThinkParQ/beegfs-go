package index

import (
	"context"
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
	preds := BuildFindPredicates(FindCfg{Type: "f"})
	sql := findEntriesSQL(FindCfg{Type: "f"}, preds)
	assert.Equal(t, FindCoreE[:len(FindCoreE)-2]+"type = 'f'", sql)
	assert.Equal(t, 1, strings.Count(sql, "WHERE"))
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
