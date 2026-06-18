package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
)

// TestQueryOutputJSONMultiColumn reproduces the former panic: query output
// was hardcoded to a single "result" column, so any user SQL selecting more
// than one column made the cmdfmt JSON printer panic. Columns are now derived
// from the first row (col1..colN) and later rows padded/truncated to match.
func TestQueryOutputJSONMultiColumn(t *testing.T) {
	setJSONOutput(t)

	rows := [][]string{
		{"a", "b", "c"},               // first row fixes the column count at 3
		{"d", "e", "f"},               // matching arity
		{"too", "short"},              // padded
		{"way", "too", "long", "row"}, // truncated
		{},                            // empty row padded
	}

	cols := genericColumns(len(rows[0]))
	require.Equal(t, []string{"col1", "col2", "col3"}, cols)

	tbl := cmdfmt.NewPrintomatic(cols, cols)
	require.NotPanics(t, func() {
		for _, row := range rows {
			tbl.AddItem(toAny(padRow(row, len(cols)))...)
		}
	})
}

// TestQueryOutputJSONSingleColumn covers the common single-column query
// (e.g. SELECT rpath(...) FROM vrpentries), the only shape that worked before.
func TestQueryOutputJSONSingleColumn(t *testing.T) {
	setJSONOutput(t)

	cols := genericColumns(1)
	assert.Equal(t, []string{"col1"}, cols)

	tbl := cmdfmt.NewPrintomatic(cols, cols)
	require.NotPanics(t, func() {
		tbl.AddItem(toAny(padRow([]string{"/idx/beegfs/file.c"}, 1))...)
	})
}
