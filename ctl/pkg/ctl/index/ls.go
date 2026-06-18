package index

import (
	"context"
	"fmt"
	"sort"
	"strconv"
)

const lsStreamBuf = 256

// Declared output columns for the ls templates, in SELECT order (see Ls* in
// templates.go). Set as QuerySpec.Columns so the remote executor can validate
// row arity. Recursive templates prepend "path"; --beegfs appends the six
// BeeGFS columns. ParseLsRow reads the same positions.
var (
	lsCoreColumns            = []string{"name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "blocks"}
	lsBeeGFSColumns          = []string{"name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "blocks", "owner_id", "parent_entry_id", "entry_id", "stripe_pattern_type", "stripe_chunk_size", "stripe_num_targets"}
	lsRecursiveColumns       = []string{"path", "name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "blocks"}
	lsRecursiveBeeGFSColumns = []string{"path", "name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "blocks", "owner_id", "parent_entry_id", "entry_id", "stripe_pattern_type", "stripe_chunk_size", "stripe_num_targets"}
)

func Ls(ctx context.Context, exec Executor, cfg LsCfg, walkRoot string, glob bool) (<-chan []string, func() error, error) {
	preds := BuildLsPredicates(cfg)
	threads := cfg.Threads

	baseSpec := func() QuerySpec {
		return QuerySpec{
			IndexRoot:     walkRoot,
			IndexRootGlob: glob,
			Threads:       threads,
			PluginPath:    QueryPluginPath,
			SkipFile:      cfg.SkipFile,
		}
	}

	collect := func(spec QuerySpec) ([][]string, error) {
		ch, wait, err := exec.Execute(ctx, spec)
		if err != nil {
			return nil, err
		}
		return bufferRows(ch, wait)
	}

	stream := func(rows [][]string) (<-chan []string, func() error, error) {
		out := make(chan []string, lsStreamBuf)
		go func() {
			defer close(out)
			for _, row := range rows {
				select {
				case <-ctx.Done():
					return
				case out <- row:
				}
			}
		}()
		return out, func() error { return nil }, nil
	}

	wantFiles := cfg.Type != "d"
	wantDirs := cfg.Type == "" || cfg.Type == "d"

	if cfg.Recursive {
		var entriesSpec, dirSpec *QuerySpec

		if wantFiles {
			spec := baseSpec()
			spec.MaxLevel = MaxLevelUnlimited
			tmpl, qual, cols := LsCoreRecursiveE, "", lsRecursiveColumns
			if cfg.BeeGFS {
				tmpl, qual, cols = LsBeeGFSRecursiveE, "v.", lsRecursiveBeeGFSColumns
			}
			spec.SQLEntries = appendLimit(fmt.Sprintf(tmpl, preds.WhereClause())+lsOrderBy(cfg, qual), cfg.NumResults)
			spec.Columns = cols
			entriesSpec = &spec
		}

		if wantDirs {
			dirPreds := BuildLsDirPredicates(cfg)
			spec := baseSpec()
			spec.MinLevel = 1
			spec.MaxLevel = MaxLevelUnlimited
			tmpl, qual, cols := LsRecursiveDirS, "", lsRecursiveColumns
			if cfg.BeeGFS {
				tmpl, qual, cols = LsBeeGFSRecursiveDirS, "s.", lsRecursiveBeeGFSColumns
			}
			spec.SQLSummary = appendLimit(fmt.Sprintf(tmpl, dirPreds.WhereClause())+lsOrderBy(cfg, qual), cfg.NumResults)
			spec.Columns = cols
			dirSpec = &spec
		}

		switch {
		case entriesSpec != nil && dirSpec == nil:
			return exec.Execute(ctx, *entriesSpec)
		case dirSpec != nil && entriesSpec == nil:
			return exec.Execute(ctx, *dirSpec)
		default:
			// Each spec carries its own LIMIT, so pass NumResults to cap the
			// merged file+dir stream to N total rather than the per-spec 2*N.
			return streamSpecs(ctx, exec, cfg.NumResults, *entriesSpec, *dirSpec)
		}
	}

	cols := lsCoreColumns
	if cfg.BeeGFS {
		cols = lsBeeGFSColumns
	}

	var entriesSQL string
	if wantFiles {
		tmpl, qual := LsCoreE, ""
		if cfg.BeeGFS {
			tmpl, qual = LsBeeGFSE, "e."
		}
		entriesSQL = appendLimit(fmt.Sprintf(tmpl, preds.WhereClause())+lsOrderBy(cfg, qual), cfg.NumResults)
	}

	var dirSQL string
	if wantDirs {
		dirPreds := BuildLsDirPredicates(cfg)
		tmpl, qual := LsDirS, ""
		if cfg.BeeGFS {
			tmpl, qual = LsBeeGFSDirS, "s."
		}
		dirSQL = appendLimit(fmt.Sprintf(tmpl, dirPreds.WhereClause())+lsOrderBy(cfg, qual), cfg.NumResults)
	}

	var allRows [][]string

	if wantFiles {
		spec := baseSpec()
		spec.SQLEntries = entriesSQL
		spec.MaxLevel = 0
		spec.Columns = cols
		rows, err := collect(spec)
		if err != nil {
			return nil, nil, fmt.Errorf("file query: %w", err)
		}
		allRows = append(allRows, rows...)
	}

	if wantDirs {
		spec := baseSpec()
		spec.SQLSummary = dirSQL
		spec.MinLevel = 1
		spec.MaxLevel = 1
		spec.Columns = cols
		rows, err := collect(spec)
		if err != nil {
			return nil, nil, fmt.Errorf("directory query: %w", err)
		}
		allRows = append(allRows, rows...)
	}

	lsSortRows(allRows, cfg)

	// The file and directory queries each carry their own LIMIT, so the merged
	// set can hold up to 2*NumResults rows; cap the combined listing to the
	// requested count (NumResults <= 0 means unlimited).
	if cfg.NumResults > 0 && len(allRows) > cfg.NumResults {
		allRows = allRows[:cfg.NumResults]
	}

	return stream(allRows)
}

// streamSpecs executes specs sequentially and forwards their rows to a single
// output channel without buffering, preserving per-spec order. It backs the
// default recursive listing (files then dirs); buffering both result sets would
// hold the whole subtree in memory. When limit > 0 it forwards at most limit
// rows total across all specs, then stops and tears down the running query
// (limit <= 0 means unlimited). The returned wait func blocks until the
// goroutine finishes and reports the first error encountered.
func streamSpecs(ctx context.Context, exec Executor, limit int, specs ...QuerySpec) (<-chan []string, func() error, error) {
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan []string, lsStreamBuf)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		errCh <- func() error {
			emitted := 0
			for i := range specs {
				ch, wait, err := exec.Execute(ctx, specs[i])
				if err != nil {
					return err
				}
				for row := range ch {
					select {
					case <-ctx.Done():
						// wait() tears down the still-running query (the channel
						// was not drained), per the Executor contract.
						_ = wait()
						return ctx.Err()
					case out <- row:
						emitted++
						if limit > 0 && emitted >= limit {
							_ = wait()
							return nil
						}
					}
				}
				if err := wait(); err != nil {
					return err
				}
			}
			return nil
		}()
	}()
	return out, func() error {
		cancel()
		return <-errCh
	}, nil
}

func lsSortRows(rows [][]string, cfg LsCfg) {
	if len(rows) <= 1 {
		return
	}
	less := func(a, b []string) bool {
		switch {
		case cfg.SortBySize:
			sa, _ := strconv.ParseInt(colAt(a, 3), 10, 64)
			sb, _ := strconv.ParseInt(colAt(b, 3), 10, 64)
			return sa > sb
		case cfg.SortByMtime:
			ma, _ := strconv.ParseInt(colAt(a, 4), 10, 64)
			mb, _ := strconv.ParseInt(colAt(b, 4), 10, 64)
			return ma > mb
		default:
			return colAt(a, 0) < colAt(b, 0)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if cfg.Reverse {
			return less(rows[j], rows[i])
		}
		return less(rows[i], rows[j])
	})
}

func colAt(row []string, i int) string {
	if i >= 0 && i < len(row) {
		return row[i]
	}
	return ""
}

// lsOrderBy renders the ORDER BY clause for a listing query. qual qualifies
// the sort column (e.g. "e."/"s."/"v.") for templates that join
// beegfs_file_view, where a bare "name" is ambiguous between the entries
// subquery and the view; pass "" for un-joined templates.
func lsOrderBy(cfg LsCfg, qual string) string {
	switch {
	case cfg.SortBySize && cfg.Reverse:
		return " ORDER BY " + qual + "size ASC"
	case cfg.SortBySize:
		return " ORDER BY " + qual + "size DESC"
	case cfg.SortByMtime && cfg.Reverse:
		return " ORDER BY " + qual + "mtime ASC"
	case cfg.SortByMtime:
		return " ORDER BY " + qual + "mtime DESC"
	case cfg.Reverse:
		return " ORDER BY " + qual + "name DESC"
	default:
		return " ORDER BY " + qual + "name ASC"
	}
}
