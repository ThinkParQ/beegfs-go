package index

import (
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const (
	beeBinary   = "/opt/beegfs/python/index/bee"
	indexConfig = "/etc/beegfs/index/config"
)

var path string

var commonIndexFlags = []bflag.FlagWrapper{
	bflag.Flag("fs-path", "F",
		"File system path for which index will be created.", "-F", ""),
	bflag.Flag("index-path", "I",
		"File system path at which the index will be stored.", "-I", ""),
	bflag.GlobalFlag(config.BeeGFSMountPointKey, "-M"),
	bflag.Flag("max-memory", "X", "Max memory usage (e.g. 8GB, 1G)", "-X", ""),
	bflag.GlobalFlag(config.NumWorkersKey, "-n"),
	bflag.Flag("summary", "s", "Create tree summary table along with other tables", "-s", false),
	bflag.Flag("only-summary", "S", "Create only tree summary table", "-S", false),
	bflag.Flag("xattrs", "x", "Pull xattrs from source", "-x", false),
	bflag.Flag("max-level", "z", "Max level to go down", "-z", ""),
	bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
	bflag.Flag("version", "v", "BeeGFS Hive Index Version", "-v", false),
	bflag.GlobalFlag(config.DebugKey, "-V=1"),
	bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
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
