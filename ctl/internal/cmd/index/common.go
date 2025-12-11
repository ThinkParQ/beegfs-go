package index

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
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
)

var path string

type gufiConfig struct {
	IndexRoot    string
	QueryPath    string
	Sqlite3Path  string
	StatPath     string
	OutputBuffer int
	Threads      int
}

var (
	configOnce sync.Once
	cfg        *gufiConfig
	cfgErr     error
)

var commonIndexFlags = []bflag.FlagWrapper{
	bflag.Flag("fs-path", "F",
		"File system path for which index will be created.", "-F", ""),
	bflag.Flag("index-path", "I",
		"File system path at which the index will be stored.", "-I", ""),
	bflag.Flag("summary", "s", "Create tree summary table along with other tables", "-s", false),
	bflag.Flag("only-summary", "S", "Create only tree summary table", "-S", false),
	bflag.GlobalFlag(config.NumWorkersKey, "-n"),
	bflag.Flag("min-level", "", "Minimum level to go down", "--min-level", ""),
	bflag.Flag("max-level", "", "Maximum level to go down", "--max-level", ""),
	bflag.Flag("path-list", "", "File containing paths at a single level to index (not including starting path). Must also use --min-level.", "--path-list", ""),
	bflag.Flag("index-xattrs", "x", "Index xattrs", "-x", false),
	bflag.Flag("skip-file", "", "File containing directory names to skip", "--skip-file", ""),
	bflag.Flag("validate-external-dbs", "q", "Check that external databases are valid before tracking during indexing", "-q", false),
	bflag.Flag("plugin", "", "Plugin library for modifying database entries", "--plugin", ""),
	bflag.Flag("target-memory", "", "Target memory utilization (soft limit)", "--target-memory", ""),
	bflag.Flag("swap-prefix", "", "File name prefix for swap files", "--swap-prefix", ""),
	bflag.Flag("subdir-limit", "", "Number of subdirectories allowed to be enqueued for parallel processing. Any remainders will be processed serially", "--subdir-limit", ""),
	bflag.Flag("compress", "", "Compress work items", "--compress", false),
	bflag.Flag("version", "v", "BeeGFS Hive Index version", "-v", false),
	bflag.GlobalFlag(config.DebugKey, "--debug"),
}

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		return fmt.Errorf("BeeGFS Hive Index mode requires the 'beegfs-hive-index' package to be installed")
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		return fmt.Errorf("error: required configuration file %s is"+
			" missing. Verify that beegfs-hive-index is properly installed and configured", indexConfig)
	}

	return nil
}

func loadIndexConfig() (*gufiConfig, error) {
	configOnce.Do(func() {
		cfg, cfgErr = parseIndexConfigFile(indexConfig)
		if cfg == nil {
			return
		}
		if cfg.Sqlite3Path != "" {
			sqlite3Binary = cfg.Sqlite3Path
		}
		if cfg.StatPath != "" {
			statBinary = cfg.StatPath
		}
	})

	return cfg, cfgErr
}

func defaultIndexPath() (string, error) {
	cfg, err := loadIndexConfig()
	if err != nil {
		return "", fmt.Errorf("loading index config: %w", err)
	}

	if cfg == nil || cfg.IndexRoot == "" {
		return "", fmt.Errorf("index root is not configured in %s", indexConfig)
	}

	return cfg.IndexRoot, nil
}

func parseIndexConfigFile(configPath string) (*gufiConfig, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", configPath, err)
	}
	defer file.Close()

	out := &gufiConfig{}
	scanner := bufio.NewScanner(file)
	var errs []error
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if val == "" {
			errs = append(errs, fmt.Errorf("%s: empty value for %s on line %d", configPath, key, lineNum))
			continue
		}

		switch key {
		case "IndexRoot":
			if !filepath.IsAbs(val) {
				errs = append(errs, fmt.Errorf("%s: IndexRoot must be an absolute path (line %d): %s", configPath, lineNum, val))
				continue
			}
			info, statErr := os.Stat(val)
			if statErr != nil {
				errs = append(errs, fmt.Errorf("%s: IndexRoot path invalid (line %d): %v", configPath, lineNum, statErr))
				continue
			}
			if !info.IsDir() {
				errs = append(errs, fmt.Errorf("%s: IndexRoot must be a directory (line %d): %s", configPath, lineNum, val))
				continue
			}
			out.IndexRoot = val
		case "Query":
			out.QueryPath = val
		case "Sqlite3":
			out.Sqlite3Path = val
		case "Stat":
			out.StatPath = val
		case "OutputBuffer":
			size, parseErr := strconv.Atoi(val)
			if parseErr != nil || size < 0 {
				errs = append(errs, fmt.Errorf("%s: invalid OutputBuffer on line %d: %s", configPath, lineNum, val))
				continue
			}
			out.OutputBuffer = size
		case "Threads":
			threads, parseErr := strconv.Atoi(val)
			if parseErr != nil || threads < 1 {
				errs = append(errs, fmt.Errorf("%s: invalid Threads on line %d: %s", configPath, lineNum, val))
				continue
			}
			out.Threads = threads
		default:
			continue
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		errs = append(errs, fmt.Errorf("reading %s: %w", configPath, scanErr))
	}

	if len(errs) > 0 {
		return out, errors.Join(errs...)
	}

	return out, nil
}

func applyIndexRootDefault(cmd *cobra.Command) {
	cfg, _ := loadIndexConfig()
	if cfg == nil || cfg.IndexRoot == "" {
		return
	}

	flag := cmd.Flags().Lookup("index-path")
	if flag == nil {
		return
	}

	flag.DefValue = cfg.IndexRoot
	_ = cmd.Flags().Set("index-path", cfg.IndexRoot)
}
