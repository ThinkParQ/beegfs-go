// Package index provides backend logic for GUFI-based index commands.
package index

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

// Viper keys for GUFI configuration. Prefixed with "index-" to avoid collisions
// with global beegfs-go keys (e.g. config.NumWorkersKey).
const (
	IndexAddrKey  = "index-addr"
	IndexRootKey  = "index-root"
	QueryBinKey   = "query-bin"
	Sqlite3BinKey = "gufi-sqlite3"
	ThreadsKey    = "index-threads"
	IndexBinKey   = "index-bin"
	TreesumBinKey = "treesummary-bin"
)

// Hardcoded plugin specs installed by the GUFI BeeGFS package.
// Format required by gufi_dir2index --plugin: "entrypoint:path/to/lib.so"
const (
	IndexPluginPath = "beegfs_index_ops:/opt/beegfs/lib/libbeegfs_index_plugin.so"
	QueryPluginPath = "beegfs_query_ops:/opt/beegfs/lib/libbeegfs_query_plugin.so"
)

// DotIndexFileName is the per-mount TOML config file that stores index location info.
const DotIndexFileName = ".beegfs.index"

// dotIndexConfig is the top-level structure of .beegfs.index.
type dotIndexConfig struct {
	Index []dotIndexEntry `mapstructure:"index"`
}

// dotIndexEntry is one [[index]] entry in .beegfs.index.
type dotIndexEntry struct {
	Path string `mapstructure:"path"` // BeeGFS-relative prefix, e.g. "/" or "/project-b"
	Root string `mapstructure:"root"` // index-root for this subtree
	Addr string `mapstructure:"addr"` // index-addr: "local" or "ssh:<host>"
}

// LoadDotIndexFile reads <mountPath>/.beegfs.index (TOML), finds the [[index]]
// entry whose path is the longest prefix of relPath, and registers its root/addr
// as viper defaults (overriding LoadGUFIConfig values). CLI flags always win.
// Missing, malformed, or non-matching file is silently ignored.
func LoadDotIndexFile(mountPath string, relPath string) {
	p := filepath.Join(mountPath, DotIndexFileName)
	v := viper.New()
	v.SetConfigFile(p)
	v.SetConfigType("toml")
	if err := v.ReadInConfig(); err != nil {
		return
	}
	var cfg dotIndexConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return
	}

	// Find the entry with the longest matching path prefix.
	var best *dotIndexEntry
	bestLen := -1
	for i := range cfg.Index {
		e := &cfg.Index[i]
		prefix := filepath.Clean(e.Path)
		if !pathHasPrefix(relPath, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			best = e
			bestLen = len(prefix)
		}
	}
	if best == nil {
		return
	}
	if best.Root != "" {
		viper.SetDefault(IndexRootKey, best.Root)
	}
	if best.Addr != "" {
		viper.SetDefault(IndexAddrKey, best.Addr)
	}
}

// pathHasPrefix reports whether path starts with prefix at a path boundary.
// pathHasPrefix("/project/sub", "/project") = true
// pathHasPrefix("/project-b/sub", "/project") = false
func pathHasPrefix(path, prefix string) bool {
	if prefix == "/" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// LoadGUFIConfig registers default values for all GUFI-related viper keys.
// CLI flags (registered after this call) will override these values automatically.
func LoadGUFIConfig() {
	viper.SetDefault(IndexAddrKey, "")
	viper.SetDefault(IndexRootKey, "/search")
	viper.SetDefault(QueryBinKey, "/opt/beegfs/bin/index/gufi_query")
	viper.SetDefault(Sqlite3BinKey, "/opt/beegfs/bin/index/gufi_sqlite3")
	viper.SetDefault(ThreadsKey, runtime.NumCPU())
	viper.SetDefault(IndexBinKey, "/opt/beegfs/bin/index/gufi_dir2index")
	viper.SetDefault(TreesumBinKey, "/opt/beegfs/bin/index/gufi_treesummary")
}
