package index

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

var (
	indexConfig       = "/etc/GUFI/config"
	beeBinary         = "/usr/local/bin/gufi_dir2index"
	treeSummaryBinary = "/usr/local/bin/gufi_treesummary"
	lsBinary          = "/usr/local/bin/gufi_ls"
	statsBinary       = "/usr/local/bin/gufi_stats"
	statBinary        = "/usr/local/bin/gufi_stat"
	findBinary        = "/usr/local/bin/gufi_find"
	sqlite3Binary     = "/usr/local/bin/gufi_sqlite3"
	queryBinary       = "/usr/local/bin/gufi_query"
)

var indexAddr string

var commonIndexFlags = []bflag.FlagWrapper{
	bflag.GlobalFlag(config.NumWorkersKey, "-n"),
	bflag.Flag("min-level", "", "Minimum level to go down", "--min-level", ""),
	bflag.Flag("max-level", "z", "Maximum level to go down", "--max-level", ""),
	bflag.Flag("path-list", "", "File containing paths at single level to walk (not including starting path). If --min-level > 0, prepend each line of the file with the index path.", "--path-list", ""),
	bflag.Flag("xattrs", "x", "Index xattrs", "-x", false),
	bflag.Flag("skip-file", "", "File containing directory names to skip", "--skip-file", ""),
	bflag.Flag("check-external-dbs", "q", "Check that external databases are valid before tracking during indexing", "-q", false),
	bflag.Flag("plugin", "", "Plugin library for modifying db entries", "--plugin", ""),
	bflag.Flag("max-memory", "X", "Target memory utilization (soft limit)", "--target-memory", ""),
	bflag.Flag("swap-prefix", "", "File name prefix for swap files", "--swap-prefix", ""),
	bflag.Flag("subdir-limit", "", "Number of subdirectories allowed to be enqueued for parallel processing. Any remainders will be processed serially", "--subdir-limit", ""),
	bflag.Flag("compress", "", "Compress work items", "--compress", false),
	bflag.Flag("version", "v", "Print version and exit", "-v", false),
	bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
	bflag.GlobalFlag(config.DebugKey, "-H"),
}

func checkIndexConfig(backend indexBackend, binaryPath string) error {
	if !backend.isLocal() {
		return nil
	}
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		return fmt.Errorf("GUFI index mode requires %s to be installed", binaryPath)
	} else if err != nil {
		return err
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		return fmt.Errorf("error: required configuration file %s is"+
			" missing. Verify that GUFI is properly installed and configured", indexConfig)
	} else if err != nil {
		return err
	}

	return nil
}

func defaultIndexPaths(backend indexBackend, args []string) ([]string, error) {
	if len(args) > 0 {
		return args, nil
	}
	if !backend.isLocal() {
		return nil, fmt.Errorf("remote index-addr requires explicit path arguments")
	}
	if indexRoot, ok := getGUFIConfigValue("IndexRoot"); ok && indexRoot != "" {
		return []string{indexRoot}, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return []string{cwd}, nil
}

func defaultIndexPath(backend indexBackend, args []string) (string, error) {
	paths, err := defaultIndexPaths(backend, args)
	if err != nil {
		return "", err
	}
	return paths[0], nil
}

func getGUFIConfigValue(key string) (string, bool) {
	configValues, err := readGUFIConfig()
	if err != nil {
		return "", false
	}
	value, ok := configValues[key]
	if !ok || value == "" {
		return "", false
	}
	return value, true
}

var gufiConfigCache struct {
	mu     sync.Mutex
	loaded bool
	path   string
	values map[string]string
	err    error
}

func readGUFIConfig() (map[string]string, error) {
	gufiConfigCache.mu.Lock()
	defer gufiConfigCache.mu.Unlock()

	if gufiConfigCache.loaded && gufiConfigCache.path == indexConfig {
		return gufiConfigCache.values, gufiConfigCache.err
	}

	values, err := readGUFIConfigFromFile(indexConfig)
	gufiConfigCache.loaded = true
	gufiConfigCache.path = indexConfig
	gufiConfigCache.values = values
	gufiConfigCache.err = err
	return values, err
}

func readGUFIConfigFromFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}
