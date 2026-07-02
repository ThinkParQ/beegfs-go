package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func IsIndexDir(path string) bool {
	info, err := os.Stat(filepath.Join(path, "db.db"))
	return err == nil && !info.IsDir()
}

func StatIndexDir(entryPath string) string {
	if IsIndexDir(entryPath) {
		return entryPath
	}
	return filepath.Dir(entryPath)
}

func statColumns(beegfs bool) []string {
	cols := []string{"name", "type", "inode", "size", "blocks", "mode",
		"uid", "user", "gid", "group", "nlink", "atime", "mtime", "ctime"}
	if beegfs {
		cols = append(cols, "owner_id", "parent_entry_id", "entry_id",
			"stripe_pattern_type", "stripe_chunk_size", "stripe_num_targets")
	}
	return cols
}

func statDirSpec(cfg StatCfg, entryPath string, glob bool) QuerySpec {
	tmpl := StatDirS
	if cfg.BeeGFS {
		tmpl = StatBeeGFSDirS
	}
	return QuerySpec{
		IndexRoot:     entryPath,
		IndexRootGlob: glob,
		SQLSummary:    tmpl,
		MaxLevel:      0,
		Threads:       cfg.Threads,
		PluginPath:    QueryPluginPath,
		Columns:       statColumns(cfg.BeeGFS),
	}
}

// statFileSpec stats a single named entry in its parent index directory. It is
// never used for a glob (a glob is dir-only and handled by statDirSpec), so it
// takes no glob flag — entryPath is always a literal path.
func statFileSpec(cfg StatCfg, entryPath string) QuerySpec {
	cfg.Filename = filepath.Base(entryPath)
	preds := BuildStatPredicates(cfg)
	tmpl := StatCoreE
	if cfg.BeeGFS {
		tmpl = StatBeeGFSE
	}
	return QuerySpec{
		IndexRoot:  filepath.Dir(entryPath),
		SQLEntries: fmt.Sprintf(tmpl, preds.WhereClause()),
		MaxLevel:   0,
		Threads:    cfg.Threads,
		PluginPath: QueryPluginPath,
		Columns:    statColumns(cfg.BeeGFS),
	}
}

func bufferRows(rows <-chan []string, errWait func() error) ([][]string, error) {
	var out [][]string
	for r := range rows {
		out = append(out, r)
	}
	return out, errWait()
}

func staticRows(data [][]string) (<-chan []string, func() error, error) {
	ch := make(chan []string, len(data))
	for _, r := range data {
		ch <- r
	}
	close(ch)
	return ch, func() error { return nil }, nil
}

// Stat resolves and stats a single entry. entryPath is the path handed to
// gufi_query; when glob is true it carries a shell glob the remote host expands,
// matching directories only (mirroring the local dir-only glob). A glob is
// therefore stat'd through statDirSpec alone — there is no single-file fallback,
// which would strip the dir-only trailing slash and match each index dir's db.db.
// glob is only ever set for a remote index.
func Stat(ctx context.Context, ex Executor, cfg StatCfg, entryPath string, glob bool) (<-chan []string, func() error, error) {
	if IsRemoteAddr(cfg.IndexAddr) {
		if glob {
			return ex.Execute(ctx, statDirSpec(cfg, entryPath, true))
		}
		rows, errWait, err := ex.Execute(ctx, statDirSpec(cfg, entryPath, false))
		if err != nil {
			return nil, nil, err
		}
		buffered, werr := bufferRows(rows, errWait)
		if werr != nil {
			return nil, nil, werr
		}
		if len(buffered) > 0 {
			return staticRows(buffered)
		}
		return ex.Execute(ctx, statFileSpec(cfg, entryPath))
	}

	// Local: a cheap os.Stat decides dir-vs-file up front (globs never reach here).
	if IsIndexDir(entryPath) {
		return ex.Execute(ctx, statDirSpec(cfg, entryPath, false))
	}
	return ex.Execute(ctx, statFileSpec(cfg, entryPath))
}
