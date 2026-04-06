package index

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
)

// Find searches the GUFI index for entries matching cfg.
// It does not know or care whether exec is local or remote —
// that is purely the executor's concern.
func Find(ctx context.Context, exec Executor, cfg FindCfg, indexPath string) (<-chan []string, func() error, error) {
	preds := BuildFindPredicates(cfg)

	tmpl := FindCoreE
	if preds.NeedsTargets {
		tmpl = FindTargetsE
	} else if preds.NeedsBeeGFS || cfg.BeeGFS {
		tmpl = FindBeeGFSE
	}

	entriesSQL := fmt.Sprintf(tmpl, preds.WhereClause())
	if cfg.Smallest {
		entriesSQL += " ORDER BY size ASC"
	} else if cfg.Largest {
		entriesSQL += " ORDER BY size DESC"
	}
	if cfg.NumResults > 0 {
		entriesSQL += fmt.Sprintf(" LIMIT %d", cfg.NumResults)
	}

	spec := QuerySpec{
		IndexRoot:  indexPath,
		SQLEntries: entriesSQL,
		Threads:    viper.GetInt(ThreadsKey),
		PluginPath: QueryPluginPath,
		Delimiter:  "|",
		MaxLevel:   MaxLevelUnlimited,
	}
	if cfg.MaxDepth >= 0 {
		spec.MaxLevel = cfg.MaxDepth
	}
	if cfg.MinDepth > 0 {
		spec.MinLevel = cfg.MinDepth
	}

	return exec.Execute(ctx, spec)
}
