package index

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// InfoTreesumSQL selects key columns from the treesummary table at the index root.
const InfoTreesumSQL = `SELECT totfiles, totsubdirs, totlinks, totsize, depth, minuid, maxuid, mingid, maxgid, minsize, maxsize, minmtime, maxmtime, totzero, totblocks FROM treesummary`

// InfoCfg holds configuration for the info command.
type InfoCfg struct {
	IndexRoot string // resolved index path
	IndexAddr string // "local", "", or "ssh:<host>"
}

// Info returns index configuration and tree summary statistics as a single-row
// channel compatible with cmdfmt.Printomatic.
//
// Row positions: index_root(0), index_addr(1), last_updated(2),
// total_files(3), total_dirs(4), total_links(5), total_size(6), depth(7),
// min_uid(8), max_uid(9), min_gid(10), max_gid(11),
// min_size(12), max_size(13), min_mtime(14), max_mtime(15),
// zero_files(16), total_blocks(17)
func Info(ctx context.Context, executor Executor, cfg InfoCfg) (<-chan []string, func() error, error) {
	// Query treesummary at the index root.
	spec := QuerySpec{
		IndexRoot:  cfg.IndexRoot,
		SQLTreeSum: InfoTreesumSQL,
		MaxLevel:   0,
		Threads:    viper.GetInt(ThreadsKey),
		Delimiter:  "|",
	}

	tsRows, errWait, err := executor.Execute(ctx, spec)
	if err != nil {
		return nil, nil, err
	}

	// Collect the single treesummary row (may be empty if table doesn't exist).
	var tsRow []string
	for row := range tsRows {
		tsRow = row
	}
	// Treesummary may not exist — that's fine, we still return config info.
	_ = errWait()

	// Get last updated: mtime of root db.db.
	lastUpdated := getDBMtime(cfg)

	// Build combined result row.
	addr := cfg.IndexAddr
	if addr == "" {
		addr = "local"
	}

	row := make([]string, 18)
	row[0] = cfg.IndexRoot
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

// getDBMtime returns the formatted mtime of the root db.db file.
// For local mode it stats the file directly; for SSH it runs a remote stat.
// Returns "" if the mtime cannot be determined.
func getDBMtime(cfg InfoCfg) string {
	dbPath := filepath.Join(cfg.IndexRoot, "db.db")

	if cfg.IndexAddr == "" || cfg.IndexAddr == "local" {
		fi, err := os.Stat(dbPath)
		if err != nil {
			return ""
		}
		return fi.ModTime().Format(time.DateTime)
	}

	// SSH mode: ssh <host> stat -c %Y <path>/db.db
	if strings.HasPrefix(cfg.IndexAddr, "ssh:") {
		host := strings.TrimPrefix(cfg.IndexAddr, "ssh:")
		out, err := exec.Command("ssh", host, "stat", "-c", "%Y", dbPath).Output()
		if err != nil {
			return ""
		}
		epoch := strings.TrimSpace(string(out))
		var ts int64
		if _, err := fmt.Sscanf(epoch, "%d", &ts); err != nil {
			return ""
		}
		return time.Unix(ts, 0).Format(time.DateTime)
	}

	return ""
}
