package index

import (
	"context"
)

type QueryCfg struct {
	GlobalCfg
	SQLEntries   string
	SQLSummary   string
	SQLTreeSum   string
	SQLInit      string
	SQLAggInit   string
	SQLIntermed  string
	SQLAggregate string
	SQLFinal     string
	AddUp        int
}

func Query(ctx context.Context, exec Executor, cfg QueryCfg, indexPath string) (<-chan []string, func() error, error) {
	spec := QuerySpec{
		IndexRoot:    indexPath,
		SQLEntries:   cfg.SQLEntries,
		SQLSummary:   cfg.SQLSummary,
		SQLTreeSum:   cfg.SQLTreeSum,
		SQLInit:      cfg.SQLInit,
		SQLAggInit:   cfg.SQLAggInit,
		SQLIntermed:  cfg.SQLIntermed,
		SQLAggregate: cfg.SQLAggregate,
		SQLFinal:     cfg.SQLFinal,
		AddUp:        cfg.AddUp,
		Threads:      cfg.Threads,
		PluginPath:   QueryPluginPath,
		MaxLevel:     MaxLevelUnlimited,
	}
	return exec.Execute(ctx, spec)
}
