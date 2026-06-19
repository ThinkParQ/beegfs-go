package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const InfoTreesumSQL = `SELECT totfiles, totsubdirs, totlinks, totsize, depth, minuid, maxuid, mingid, maxgid, minsize, maxsize, minmtime, maxmtime, totzero, totblocks FROM treesummary`

type InfoCfg struct {
	GlobalCfg
	IndexPath string
}

func Info(ctx context.Context, executor Executor, cfg InfoCfg) (<-chan []string, func() error, error) {
	spec := QuerySpec{
		IndexRoot:  cfg.IndexPath,
		SQLTreeSum: InfoTreesumSQL,
		MaxLevel:   0,
		Threads:    cfg.Threads,
		Columns: []string{"totfiles", "totsubdirs", "totlinks", "totsize", "depth",
			"minuid", "maxuid", "mingid", "maxgid", "minsize", "maxsize",
			"minmtime", "maxmtime", "totzero", "totblocks"},
	}

	tsRows, errWait, err := executor.Execute(ctx, spec)
	if err != nil {
		return nil, nil, err
	}

	var tsRow []string
	for row := range tsRows {
		if tsRow == nil {
			tsRow = row
		}
	}
	if err := errWait(); err != nil {
		return nil, nil, fmt.Errorf("query treesummary: %w", err)
	}

	lastUpdated := getDBMtime(cfg)

	addr := cfg.IndexAddr
	if addr == "" {
		addr = "local"
	}

	row := make([]string, 18)
	row[0] = cfg.IndexPath
	row[1] = addr
	row[2] = lastUpdated
	if len(tsRow) >= 15 {
		copy(row[3:], tsRow[:15])
	}

	ch := make(chan []string, 1)
	ch <- row
	close(ch)

	return ch, func() error { return nil }, nil
}

func getDBMtime(cfg InfoCfg) string {
	if IsRemoteAddr(cfg.IndexAddr) {
		return ""
	}
	fi, err := os.Stat(filepath.Join(cfg.IndexPath, "db.db"))
	if err != nil {
		return ""
	}
	return fi.ModTime().Format(time.DateTime)
}
