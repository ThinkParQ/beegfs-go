package index

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/spf13/viper"
)

// Stat retrieves metadata for a single entry.
// entryPath is the full BeeGFS-relative path to the file or directory;
// Stat splits it into parent index directory + basename predicate.
func Stat(ctx context.Context, exec Executor, cfg StatCfg, entryPath string) (<-chan []string, func() error, error) {
	// gufi_query takes a directory as IndexRoot. For a file entry we need
	// the parent directory and filter by name.
	indexPath := filepath.Dir(entryPath)
	cfg.Filename = filepath.Base(entryPath)

	preds := BuildStatPredicates(cfg)

	tmpl := StatCoreE
	if cfg.BeeGFS {
		tmpl = StatBeeGFSE
	}

	spec := QuerySpec{
		IndexRoot:  indexPath,
		SQLEntries: fmt.Sprintf(tmpl, preds.WhereClause()),
		MaxLevel:   0, // process only the parent directory's db.db
		Threads:    viper.GetInt(ThreadsKey),
		PluginPath: QueryPluginPath,
		Delimiter:  "|",
	}

	return exec.Execute(ctx, spec)
}
