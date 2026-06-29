package index

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowsExecutor returns the same fixed rows for every Execute call and counts
// the calls.
type rowsExecutor struct {
	rows  [][]string
	calls int
}

func (e *rowsExecutor) Execute(_ context.Context, _ QuerySpec) (<-chan []string, func() error, error) {
	e.calls++
	ch := make(chan []string, len(e.rows))
	for _, r := range e.rows {
		ch <- r
	}
	close(ch)
	return ch, func() error { return nil }, nil
}

// TestLs_NumResultsCapsCombinedListing: the file and directory queries each
// carry their own LIMIT N, so the merged listing can hold up to 2N rows; the
// non-recursive path must cap the combined result to N.
func TestLs_NumResultsCapsCombinedListing(t *testing.T) {
	const n = 3
	rows := make([][]string, n)
	for i := range rows {
		// Non-recursive core ls row layout: name,type,inode,size,mtime,... (12 cols).
		rows[i] = []string{"f" + strconv.Itoa(i), "f", "1", "0", "0", "0", "0", "0644", "0", "0", "1", "0"}
	}
	ex := &rowsExecutor{rows: rows}

	ch, wait, err := Ls(context.Background(), ex, LsCfg{NumResults: n}, "/idx", false)
	require.NoError(t, err)
	got := 0
	for range ch {
		got++
	}
	require.NoError(t, wait())

	assert.Equal(t, 2, ex.calls, "expected separate file and directory queries")
	assert.Equal(t, n, got, "combined listing must be capped to NumResults, not 2N")
}

// captureExecutor records every QuerySpec it receives and returns no rows.
type captureExecutor struct {
	specs []QuerySpec
}

func (e *captureExecutor) Execute(_ context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	e.specs = append(e.specs, spec)
	ch := make(chan []string)
	close(ch)
	return ch, func() error { return nil }, nil
}

// TestLsTargetIsFile_Remote: with no local stat available, a remote target is a
// file iff the summary probe at its path returns no row (a real directory, even
// empty, returns its own summary row).
func TestLsTargetIsFile_Remote(t *testing.T) {
	t.Parallel()
	cfg := LsCfg{GlobalCfg: GlobalCfg{IndexAddr: "ssh:host"}}

	t.Run("summary row present means directory", func(t *testing.T) {
		t.Parallel()
		ex := &rowsExecutor{rows: [][]string{{"data"}}}
		isFile, err := LsTargetIsFile(context.Background(), ex, cfg, "/idx/mnt/data")
		require.NoError(t, err)
		assert.False(t, isFile)
	})

	t.Run("no summary row means file", func(t *testing.T) {
		t.Parallel()
		ex := &rowsExecutor{rows: nil}
		isFile, err := LsTargetIsFile(context.Background(), ex, cfg, "/idx/mnt/data/file.c")
		require.NoError(t, err)
		assert.True(t, isFile)
	})
}

// TestLsFile_ParentAndNamePredicate: a file target lists from its parent index
// directory (level 0) with an exact name match on the basename.
func TestLsFile_ParentAndNamePredicate(t *testing.T) {
	t.Parallel()
	ex := &captureExecutor{}
	_, wait, err := LsFile(context.Background(), ex, LsCfg{}, "/idx/mnt/d/file_0.c")
	require.NoError(t, err)
	require.NoError(t, wait())

	require.Len(t, ex.specs, 1)
	spec := ex.specs[0]
	assert.Equal(t, "/idx/mnt/d", spec.IndexRoot, "walks the parent directory")
	assert.Equal(t, 0, spec.MaxLevel, "single directory, non-recursive")
	assert.Contains(t, spec.SQLEntries, "name = 'file_0.c'", "exact name match on the basename")
}

// TestLs_NonRecursiveOrdersBeforeLimit: with a sort flag and NumResults set, the
// non-recursive queries must push ORDER BY into the SQL ahead of LIMIT. If LIMIT
// ran first the database would keep arbitrary rows and the later in-memory sort
// could not recover the true top-N.
func TestLs_NonRecursiveOrdersBeforeLimit(t *testing.T) {
	ex := &captureExecutor{}
	_, wait, err := Ls(context.Background(), ex, LsCfg{SortBySize: true, NumResults: 5}, "/idx", false)
	require.NoError(t, err)
	require.NoError(t, wait())

	require.Len(t, ex.specs, 2, "expected separate file and directory queries")
	for _, spec := range ex.specs {
		sql := spec.SQLEntries + spec.SQLSummary
		obIdx := strings.Index(sql, "ORDER BY size DESC")
		limIdx := strings.Index(sql, "LIMIT 5")
		require.NotEqual(t, -1, obIdx, "missing ORDER BY: %s", sql)
		require.NotEqual(t, -1, limIdx, "missing LIMIT: %s", sql)
		assert.Less(t, obIdx, limIdx, "ORDER BY must precede LIMIT so the limit keeps the top-N: %s", sql)
	}
}

// TestLs_NonRecursiveBeeGFSQualifiesOrderBy: the non-recursive --beegfs queries
// LEFT JOIN beegfs_file_view, which also exposes name/type/inode, so a bare
// "ORDER BY name" is ambiguous and SQLite rejects it. The default (name) sort
// column must be qualified to the entries/summary subquery (e./s.). SortBySize
// happens to work even unqualified because size lives only in the subquery,
// which is why TestLs_NonRecursiveOrdersBeforeLimit did not catch this.
func TestLs_NonRecursiveBeeGFSQualifiesOrderBy(t *testing.T) {
	ex := &captureExecutor{}
	_, wait, err := Ls(context.Background(), ex, LsCfg{BeeGFS: true, NumResults: 5}, "/idx", false)
	require.NoError(t, err)
	require.NoError(t, wait())

	require.Len(t, ex.specs, 2, "expected separate file and directory queries")
	for _, spec := range ex.specs {
		sql := spec.SQLEntries + spec.SQLSummary
		assert.NotContains(t, sql, "ORDER BY name",
			"bare ORDER BY name is ambiguous against beegfs_file_view: %s", sql)
		assert.True(t,
			strings.Contains(sql, "ORDER BY e.name") || strings.Contains(sql, "ORDER BY s.name"),
			"BeeGFS sort column must be qualified (e./s.): %s", sql)
	}
}

// TestLs_RecursiveNumResultsCapsCombinedStream: the recursive file and directory
// queries each carry their own LIMIT N, so streaming both back to back can emit
// up to 2N rows; the streaming path must cap the merged stream to N total.
func TestLs_RecursiveNumResultsCapsCombinedStream(t *testing.T) {
	const perSpec = 2
	const n = 3 // between perSpec and 2*perSpec, so the cap must span both specs
	rows := make([][]string, perSpec)
	for i := range rows {
		rows[i] = []string{"/p" + strconv.Itoa(i), "name", "f", "1", "0", "0"}
	}
	ex := &rowsExecutor{rows: rows}

	ch, wait, err := Ls(context.Background(), ex, LsCfg{Recursive: true, NumResults: n}, "/idx", false)
	require.NoError(t, err)
	got := 0
	for range ch {
		got++
	}
	require.NoError(t, wait())

	assert.Equal(t, 2, ex.calls, "expected separate file and directory queries")
	assert.Equal(t, n, got, "recursive merged stream must be capped to NumResults, not 2N")
}
