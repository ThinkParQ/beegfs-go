package index

import (
	"context"

	"github.com/spf13/viper"
)

// QueryCfg holds backend configuration for the raw query command.
type QueryCfg struct {
	SQLEntries   string // -E
	SQLSummary   string // -S
	SQLInit      string // -I
	SQLAggInit   string // -K
	SQLIntermed  string // -J
	SQLAggregate string // -G
	SQLFinal     string // -F
}

// Query runs a raw user-supplied SQL statement against the GUFI index.
// It is a pass-through: the caller provides the SQL directly.
func Query(ctx context.Context, exec Executor, cfg QueryCfg, indexPath string) (<-chan []string, func() error, error) {
	spec := QuerySpec{
		IndexRoot:    indexPath,
		SQLEntries:   cfg.SQLEntries,
		SQLSummary:   cfg.SQLSummary,
		SQLInit:      cfg.SQLInit,
		SQLAggInit:   cfg.SQLAggInit,
		SQLIntermed:  cfg.SQLIntermed,
		SQLAggregate: cfg.SQLAggregate,
		SQLFinal:     cfg.SQLFinal,
		Threads:      viper.GetInt(ThreadsKey),
		PluginPath:   QueryPluginPath,
		Delimiter:    "|",
		MaxLevel:     MaxLevelUnlimited,
	}
	return exec.Execute(ctx, spec)
}
