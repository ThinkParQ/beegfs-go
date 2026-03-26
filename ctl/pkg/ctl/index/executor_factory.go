package index

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// NewExecutor reads IndexAddrKey from viper and returns the appropriate Executor.
// Called once per command invocation from common.go's newExecutor().
//
//	""  / "local"       → LocalExecutor
//	"ssh:<host>"        → RemoteExecutor (single host)
func NewExecutor(cfg *viper.Viper) (Executor, error) {
	addr := cfg.GetString(IndexAddrKey)
	switch {
	case addr == "" || addr == "local":
		return &LocalExecutor{QueryBin: cfg.GetString(QueryBinKey)}, nil

	case strings.HasPrefix(addr, "ssh:"):
		host := strings.TrimPrefix(addr, "ssh:")
		if host == "" {
			return nil, fmt.Errorf("invalid index-addr %q: missing host after ssh:", addr)
		}
		return &RemoteExecutor{
			Hosts:      []string{host},
			Sqlite3Bin: cfg.GetString(Sqlite3BinKey),
		}, nil

	default:
		return nil, fmt.Errorf(
			"invalid index-addr %q: expected local or ssh:<host>",
			addr,
		)
	}
}
