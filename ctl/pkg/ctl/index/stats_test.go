package index

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildStatsSpecColumns(t *testing.T) {
	// Expected names mirror the SQL templates in templates.go (aliases or
	// source columns of the pipeline's final output stage).
	tests := []struct {
		stat StatName
		want []string
	}{
		{StatTotalFilecount, []string{"count"}},
		{StatTotalDircount, []string{"count"}},
		{StatTotalFilesize, []string{"size"}},
		{StatTotalLinkcount, []string{"count"}},
		{StatFilecount, []string{"path", "count"}},
		{StatFilesize, []string{"path", "size"}},
		{StatDepth, []string{"path", "depth"}},
		{StatLinkcount, []string{"path", "count"}},
		{StatDircount, []string{"path", "count"}},
		{StatLeafDirs, []string{"path"}},
		{StatLeafDepth, []string{"path", "level"}},
		{StatLeafFiles, []string{"path", "count"}},
		{StatLeafLinks, []string{"path", "count"}},
		{StatTotalLeafFiles, []string{"count"}},
		{StatTotalLeafLinks, []string{"count"}},
		{StatFilesPerLevel, []string{"level", "count"}},
		{StatLinksPerLevel, []string{"level", "count"}},
		{StatDirsPerLevel, []string{"level", "count"}},
		{StatAverageLeafFiles, []string{"value"}},
		{StatAverageLeafSize, []string{"value"}},
		{StatMedianLeafFiles, []string{"value"}},
		{StatMedianLeafSize, []string{"value"}},
		{StatDuplicateNames, []string{"name"}},
		{StatUidSize, []string{"uid", "size", "name"}},
		{StatGidSize, []string{"gid", "size", "name"}},
		{StatExtensions, []string{"extension", "count"}},
		{StatFilesizeLog2Bins, []string{"bin", "count"}},
		{StatFilesizeLog1024Bins, []string{"bin", "count"}},
		{StatDirfilecountLog2Bins, []string{"bin", "count"}},
		{StatDirfilecountLog1024Bins, []string{"bin", "count"}},
		{StatFileAgeAtime, []string{"bucket", "count"}},
		{StatFileAgeMtime, []string{"bucket", "count"}},
		{StatDirAgeCtime, []string{"bucket", "count"}},
	}
	for _, tc := range tests {
		t.Run(string(tc.stat), func(t *testing.T) {
			t.Parallel()
			spec, err := buildStatsSpec(StatsCfg{Stat: tc.stat, Uid: -1}, "/idx/beegfs")
			require.NoError(t, err)
			assert.Equal(t, tc.want, spec.Columns)
		})
	}
}

// TestStatsColumnsMatchSQLArity cross-checks every stat's declared column
// names against the arity of the SQL stage that actually produces the output
// rows, so the cmd layer's table/JSON printers (which panic on a column-count
// mismatch) can rely on Columns.
func TestStatsColumnsMatchSQLArity(t *testing.T) {
	for _, stat := range ValidStatNames {
		t.Run(string(stat), func(t *testing.T) {
			t.Parallel()
			spec, err := buildStatsSpec(StatsCfg{Stat: stat, Uid: -1}, "/idx/beegfs")
			require.NoError(t, err)
			require.NotEmpty(t, spec.Columns, "every stat must declare its output columns")

			// The final output stage is the aggregate SELECT when the stat
			// uses a pipeline, otherwise the per-directory summary or
			// per-entry SQL streams directly.
			final := spec.SQLAggregate
			if final == "" {
				final = spec.SQLSummary
			}
			if final == "" {
				final = spec.SQLEntries
			}
			require.NotEmpty(t, final, "stat emits no output SQL")

			assert.Equal(t, selectArity(t, final), len(spec.Columns),
				"declared columns must match the arity of the final output SQL %q", final)
		})
	}

	t.Run("unknown stat errors", func(t *testing.T) {
		t.Parallel()
		_, err := buildStatsSpec(StatsCfg{Stat: "no-such-stat", Uid: -1}, "/idx/beegfs")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown stat")
	})
}

// selectArity returns the number of top-level result columns of the first
// SELECT in sql: commas are counted at parenthesis depth zero outside
// single-quoted literals, up to the matching top-level FROM.
func selectArity(t *testing.T, sql string) int {
	t.Helper()
	const sel = "SELECT "
	i := strings.Index(sql, sel)
	require.GreaterOrEqual(t, i, 0, "no SELECT in %q", sql)
	rest := sql[i+len(sel):]

	depth, arity := 0, 1
	inQuote := false
	for j := 0; j < len(rest); j++ {
		c := rest[j]
		if inQuote {
			if c == '\'' {
				inQuote = false
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				arity++
			}
		default:
			if depth == 0 && c == 'F' && strings.HasPrefix(rest[j:], "FROM ") {
				return arity
			}
		}
	}
	return arity
}
