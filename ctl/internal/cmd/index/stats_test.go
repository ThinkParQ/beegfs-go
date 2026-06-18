package index

import (
	"context"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

// fakeExecutor satisfies indexPkg.Executor and streams canned rows, letting
// cmd-layer output handling be tested without GUFI binaries.
type fakeExecutor struct {
	rows [][]string
}

func (f fakeExecutor) Execute(_ context.Context, _ indexPkg.QuerySpec) (<-chan []string, func() error, error) {
	ch := make(chan []string, len(f.rows))
	for _, r := range f.rows {
		ch <- r
	}
	close(ch)
	return ch, func() error { return nil }, nil
}

// setJSONOutput configures the cmdfmt layer for buffered JSON output and
// restores the previous configuration on cleanup. Tests using it must not run
// in parallel: viper configuration is global state.
func setJSONOutput(t *testing.T) {
	t.Helper()
	prevOutput := viper.Get(config.OutputKey)
	prevPageSize := viper.Get(config.PageSizeKey)
	viper.Set(config.OutputKey, string(config.OutputJSON))
	// A page size larger than any test's row count keeps output buffered (and
	// unprinted, as PrintRemaining is never called) so tests stay quiet.
	viper.Set(config.PageSizeKey, 100)
	t.Cleanup(func() {
		viper.Set(config.OutputKey, prevOutput)
		viper.Set(config.PageSizeKey, prevPageSize)
	})
}

// TestStatsOutputJSONColumns reproduces the former panic: the cmd layer
// hardcoded a single "value" column while most stats emit two or three, which
// made the cmdfmt JSON printer panic on the column-count mismatch. Each
// representative stat's natural row arity must now match the declared columns
// and print without panicking.
func TestStatsOutputJSONColumns(t *testing.T) {
	setJSONOutput(t)

	tests := []struct {
		stat     indexPkg.StatName
		row      []string
		wantCols int
	}{
		{indexPkg.StatTotalFilecount, []string{"42"}, 1},
		{indexPkg.StatAverageLeafFiles, []string{"2.5"}, 1},
		{indexPkg.StatMedianLeafSize, []string{"4096"}, 1},
		{indexPkg.StatDuplicateNames, []string{"dup.txt"}, 1},
		{indexPkg.StatLeafDirs, []string{"/idx/beegfs/leaf"}, 1},
		{indexPkg.StatFilecount, []string{"/idx/beegfs/dir", "7"}, 2},
		{indexPkg.StatFilesize, []string{"/idx/beegfs/dir", "1048576"}, 2},
		{indexPkg.StatDepth, []string{"/idx/beegfs/dir", "3"}, 2},
		{indexPkg.StatLeafDepth, []string{"/idx/beegfs/leaf", "4"}, 2},
		{indexPkg.StatFilesPerLevel, []string{"2", "100"}, 2},
		{indexPkg.StatExtensions, []string{"txt", "12"}, 2},
		{indexPkg.StatFilesizeLog2Bins, []string{"[1024,2048)", "5"}, 2},
		{indexPkg.StatDirfilecountLog1024Bins, []string{"[0,1)", "9"}, 2},
		{indexPkg.StatFileAgeMtime, []string{"[1 day ago, 1 week ago)", "9"}, 2},
		{indexPkg.StatDirAgeCtime, []string{"[1 hour ago, 1 day ago)", "3"}, 2},
		{indexPkg.StatUidSize, []string{"1000", "4096", "/idx/beegfs/f.bin"}, 3},
		{indexPkg.StatGidSize, []string{"1000", "4096", "/idx/beegfs/f.bin"}, 3},
	}
	for _, tc := range tests {
		t.Run(string(tc.stat), func(t *testing.T) {
			require.Len(t, tc.row, tc.wantCols,
				"test row must use the stat's natural output arity")

			columns, rows, wait, err := indexPkg.Stats(context.Background(),
				fakeExecutor{rows: [][]string{tc.row}},
				indexPkg.StatsCfg{Stat: tc.stat, Uid: -1}, "/idx/beegfs")
			require.NoError(t, err)
			require.Len(t, columns, tc.wantCols, "declared columns must match row arity")

			tbl := cmdfmt.NewPrintomatic(columns, columns)
			require.NotPanics(t, func() {
				for row := range rows {
					tbl.AddItem(toAny(padRow(row, len(columns)))...)
				}
			})
			require.NoError(t, wait())
		})
	}
}

// TestStatsOutputJSONRowDrift verifies the defensive padding: rows whose
// arity unexpectedly disagrees with the declared columns must be normalized
// instead of reaching the JSON printer's panic.
func TestStatsOutputJSONRowDrift(t *testing.T) {
	setJSONOutput(t)

	drifted := [][]string{
		{"/idx/beegfs/dir"},                    // one column short
		{"/idx/beegfs/dir", "7", "unexpected"}, // one column extra
		{},                                     // empty row
		{"/idx/beegfs/other", "8"},             // correct arity
	}
	columns, rows, wait, err := indexPkg.Stats(context.Background(),
		fakeExecutor{rows: drifted},
		indexPkg.StatsCfg{Stat: indexPkg.StatFilecount, Uid: -1}, "/idx/beegfs")
	require.NoError(t, err)

	tbl := cmdfmt.NewPrintomatic(columns, columns)
	require.NotPanics(t, func() {
		for row := range rows {
			tbl.AddItem(toAny(padRow(row, len(columns)))...)
		}
	})
	require.NoError(t, wait())
}
