// Package index provides backend logic for GUFI-based index commands.
package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

// Paths to the GUFI binaries shipped with the BeeGFS index packages. These are
// fixed install locations, not user configuration.
const (
	QueryBin     = "/opt/beegfs/bin/index/gufi_query"
	Sqlite3Bin   = "/opt/beegfs/bin/index/gufi_sqlite3"
	Dir2IndexBin = "/opt/beegfs/bin/index/gufi_dir2index"
	TreesumBin   = "/opt/beegfs/bin/index/gufi_treesummary"
)

const (
	IndexPluginPath = "beegfs_index_ops:/opt/beegfs/lib/libbeegfs_indexing.so"
	QueryPluginPath = "beegfs_query_ops:/opt/beegfs/lib/libbeegfs_querying.so"
)

const DotIndexFileName = ".beegfs.index"

// ErrIndexRootNotSet reports that the index tree root was not provided. There
// is no built-in default: it must come from --index-root or a .beegfs.index
// entry at the BeeGFS mount root.
var ErrIndexRootNotSet = errors.New("index root not set: pass --index-root or configure it in .beegfs.index at the BeeGFS mount root")

// GlobalCfg carries the settings shared by all index sub-commands. Zero
// values mean "not set by the user": unset fields are filled from the
// .beegfs.index file at the BeeGFS mount root (see ApplyDotIndexOverrides),
// after which explicit flags always win. IndexRoot has no built-in default and
// is required (see ErrIndexRootNotSet); only Threads falls back to a computed
// default (DefaultThreads).
type GlobalCfg struct {
	// IndexAddr selects where the GUFI binaries run: "" or "local" for the
	// local host, or "ssh:<host>" for a remote index.
	IndexAddr string
	// IndexRoot is the root directory of the index tree
	// (<index-root>/<mount-name>/...).
	IndexRoot string
	// MountPath is the BeeGFS mount point this index was resolved against, when a
	// local mount is known (from a .beegfs.index match or mount discovery). Its
	// basename is the <mount-name> segment of the index layout, so it is the
	// authoritative source for that segment — for a remote index, whose root
	// cannot be probed on the local host, it is the only one. Empty when no local
	// mount is available; not a user flag.
	MountPath string
	// Threads is the thread count handed to the GUFI binaries. It is not a
	// flag: it is taken from the matching .beegfs.index "threads" entry, else
	// resolved via DefaultThreads (num-workers locally, a single worker for a
	// remote index). 0 makes the executors omit the flag so the GUFI binaries
	// use their own default.
	Threads int
}

type dotIndexConfig struct {
	Index dotIndexEntry `toml:"index"`
}

type dotIndexEntry struct {
	Path    string `toml:"path"`
	Root    string `toml:"root"`
	Addr    string `toml:"addr"`
	Threads int    `toml:"threads"`
}

// loadDotIndex reads and parses the .beegfs.index file at mountPath. ok is false
// when mountPath is empty or the file is missing or malformed, in which case
// callers leave their configuration unchanged.
func loadDotIndex(mountPath string) (cfg dotIndexConfig, ok bool) {
	if mountPath == "" {
		return cfg, false
	}
	path := filepath.Join(mountPath, DotIndexFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		// A missing file is the normal "no .beegfs.index configured" case and stays
		// silent; an existing-but-unreadable file is a real misconfiguration worth
		// surfacing rather than failing later as an unrelated "index root not set".
		if !os.IsNotExist(err) {
			log, _ := config.GetLogger()
			log.Warn("ignoring unreadable .beegfs.index", zap.String("path", path), zap.Error(err))
		}
		return cfg, false
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		log, _ := config.GetLogger()
		log.Warn("ignoring malformed .beegfs.index", zap.String("path", path), zap.Error(err))
		return cfg, false
	}
	return cfg, true
}

// DotIndexPath returns the "path" of the [index] entry in the .beegfs.index
// file at mountPath, or "" when mountPath is empty or the file is missing,
// malformed, or configures no path. It is the filesystem path 'beegfs index
// create' indexes when --fs-path is omitted.
func DotIndexPath(mountPath string) string {
	fileCfg, ok := loadDotIndex(mountPath)
	if !ok {
		return ""
	}
	return fileCfg.Index.Path
}

// ApplyDotIndexOverrides returns cfg with unset fields filled from the [index]
// entry of the .beegfs.index file at mountPath. Fields the user set explicitly
// are never overridden, and a missing or malformed file leaves cfg unchanged.
func ApplyDotIndexOverrides(cfg GlobalCfg, mountPath string) GlobalCfg {
	if cfg.IndexRoot != "" && cfg.IndexAddr != "" && cfg.Threads > 0 {
		return cfg
	}
	fileCfg, ok := loadDotIndex(mountPath)
	if !ok {
		return cfg
	}
	e := fileCfg.Index
	if cfg.IndexRoot == "" {
		cfg.IndexRoot = e.Root
	}
	if cfg.IndexAddr == "" {
		cfg.IndexAddr = e.Addr
	}
	if cfg.Threads <= 0 && e.Threads > 0 {
		cfg.Threads = e.Threads
	}
	return cfg
}

// DefaultThreads resolves the GUFI thread count when neither a flag nor a
// .beegfs.index "threads" entry set it: the configured num-workers (else the
// local CPU count) for a local index, and a single worker for a remote index.
// A remote count is deliberately not probed over ssh — only the gufi binaries
// reach the remote host — so set "threads" in .beegfs.index to raise it. ctx is
// retained for call-site signature stability; no I/O is performed here.
func DefaultThreads(ctx context.Context, indexAddr string) int {
	if IsRemoteAddr(indexAddr) {
		return 1
	}
	if n := viper.GetInt(config.NumWorkersKey); n > 0 {
		return n
	}
	return runtime.NumCPU()
}
