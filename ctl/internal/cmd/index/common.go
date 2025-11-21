package index

import (
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const (
	beeBinary         = "/usr/local/bin/gufi_dir2index"
	treeSummaryBinary = "/usr/local/bin/gufi_treesummary"
	lsBinary          = "/usr/local/bin/gufi_ls"
	statsBinary       = "/usr/local/bin/gufi_stats"
	indexConfig       = "/etc/GUFI/config"
)

var path string

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
