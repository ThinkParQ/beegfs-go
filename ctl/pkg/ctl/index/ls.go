package index

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/viper"
)

// Ls lists the contents of one index directory.
//
// Non-recursive: runs two queries sequentially —
//   - files/symlinks from entries at --max-level 0
//   - immediate subdirectories from summary at --min-level 1 --max-level 1
//
// Results are collected in memory, sorted as a combined set, then streamed.
// This is safe because non-recursive ls operates on a single directory whose
// entry count is always bounded.
//
// Recursive: queries vrpentries (files only) across the full subtree,
// streamed directly without buffering.
func Ls(ctx context.Context, exec Executor, cfg LsCfg, indexPath string) (<-chan []string, func() error, error) {
	preds := BuildLsPredicates(cfg)
	threads := viper.GetInt(ThreadsKey)

	if cfg.Recursive {
		tmpl := LsCoreRecursiveE
		if cfg.BeeGFS {
			tmpl = LsBeeGFSRecursiveE
		}
		entriesSQL := fmt.Sprintf(tmpl, preds.WhereClause()) + lsOrderBy(cfg)
		if cfg.NumResults > 0 {
			entriesSQL += fmt.Sprintf(" LIMIT %d", cfg.NumResults)
		}
		return exec.Execute(ctx, QuerySpec{
			IndexRoot:  indexPath,
			SQLEntries: entriesSQL,
			MaxLevel:   MaxLevelUnlimited,
			Threads:    threads,
			PluginPath: QueryPluginPath,
			Delimiter:  "|",
		})
	}

	// Non-recursive: which tiers to query.
	// --type d  → only directories (summary); skip entries.
	// --type f/l/… → only files (entries); skip summary.
	// --type "" → both.
	wantFiles := cfg.Type == "" || cfg.Type != "d"
	wantDirs := cfg.Type == "" || cfg.Type == "d"

	// Build file SQL.
	var entriesSQL string
	if wantFiles {
		tmpl := LsCoreE
		if cfg.BeeGFS {
			tmpl = LsBeeGFSE
		}
		entriesSQL = fmt.Sprintf(tmpl, preds.WhereClause())
		if cfg.NumResults > 0 {
			entriesSQL += fmt.Sprintf(" LIMIT %d", cfg.NumResults)
		}
	}

	// Build directory SQL.
	var dirSQL string
	if wantDirs {
		dirPreds := BuildLsDirPredicates(cfg)
		tmpl := LsDirS
		if cfg.BeeGFS {
			tmpl = LsBeeGFSDirS
		}
		dirSQL = fmt.Sprintf(tmpl, dirPreds.WhereClause())
		if cfg.NumResults > 0 {
			dirSQL += fmt.Sprintf(" LIMIT %d", cfg.NumResults)
		}
	}

	// Run each query sequentially and collect into a single slice so we can
	// sort the combined result before streaming. Non-recursive ls targets a
	// single directory, so the entry count is always small.
	collect := func(spec QuerySpec) ([][]string, error) {
		ch, wait, err := exec.Execute(ctx, spec)
		if err != nil {
			return nil, err
		}
		var rows [][]string
		for row := range ch {
			rows = append(rows, row)
		}
		return rows, wait()
	}

	var allRows [][]string

	if wantFiles {
		rows, err := collect(QuerySpec{
			IndexRoot:  indexPath,
			SQLEntries: entriesSQL,
			MaxLevel:   0,
			Threads:    threads,
			PluginPath: QueryPluginPath,
			Delimiter:  "|",
		})
		if err != nil {
			return nil, nil, fmt.Errorf("file query: %w", err)
		}
		allRows = append(allRows, rows...)
	}

	if wantDirs {
		rows, err := collect(QuerySpec{
			IndexRoot:  indexPath,
			SQLSummary: dirSQL,
			MinLevel:   1,
			MaxLevel:   1,
			Threads:    threads,
			PluginPath: QueryPluginPath,
			Delimiter:  "|",
		})
		if err != nil {
			return nil, nil, fmt.Errorf("directory query: %w", err)
		}
		allRows = append(allRows, rows...)
	}

	lsSortRows(allRows, cfg)

	out := make(chan []string, len(allRows)+1)
	for _, row := range allRows {
		out <- row
	}
	close(out)

	return out, func() error { return nil }, nil
}

// lsSortRows sorts rows in-place according to LsCfg sort flags.
// Column layout for non-recursive ls: name(0) type(1) inode(2) size(3) mtime(4) ...
func lsSortRows(rows [][]string, cfg LsCfg) {
	if len(rows) <= 1 {
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, rj := rows[i], rows[j]
		var less bool
		switch {
		case cfg.SortBySize:
			si, _ := strconv.ParseInt(colAt(ri, 3), 10, 64)
			sj, _ := strconv.ParseInt(colAt(rj, 3), 10, 64)
			less = si > sj // default: largest first
		case cfg.SortByMtime:
			mi, _ := strconv.ParseInt(colAt(ri, 4), 10, 64)
			mj, _ := strconv.ParseInt(colAt(rj, 4), 10, 64)
			less = mi > mj // default: newest first
		default:
			less = colAt(ri, 0) < colAt(rj, 0) // default: name ASC
		}
		if cfg.Reverse {
			return !less
		}
		return less
	})
}

func colAt(row []string, i int) string {
	if i >= 0 && i < len(row) {
		return row[i]
	}
	return ""
}

// lsOrderBy returns the ORDER BY clause for recursive ls SQL.
func lsOrderBy(cfg LsCfg) string {
	switch {
	case cfg.SortBySize && cfg.Reverse:
		return " ORDER BY size ASC"
	case cfg.SortBySize:
		return " ORDER BY size DESC"
	case cfg.SortByMtime && cfg.Reverse:
		return " ORDER BY mtime ASC"
	case cfg.SortByMtime:
		return " ORDER BY mtime DESC"
	case cfg.Reverse:
		return " ORDER BY name DESC"
	default:
		return " ORDER BY name ASC"
	}
}
