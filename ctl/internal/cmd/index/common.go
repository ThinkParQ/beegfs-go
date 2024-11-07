package index

import (
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const (
	beeBinary   = "/usr/bin/bee"
	indexConfig = "/etc/beegfs/index/config"
	indexEnv    = "/etc/beegfs/index/indexEnv.conf"
	updateEnv   = "/etc/beegfs/index/updateEnv.conf"
)

var path string

var commonIndexFlags = []bflag.FlagWrapper{
	bflag.Flag("fs-path", "F",
		"File system path for which index will be created [default: IndexEnv/UpdateEnv.conf]", "-F", ""),
	bflag.Flag("index-path", "I",
		"File system path for which index will be created [default: IndexEnv/UpdateEnv.conf]", "-I", ""),
	bflag.GlobalFlag(config.BeeGFSMountPointKey, "-M"),
	bflag.Flag("max-memory", "X", "Max memory usage (e.g. 8GB, 1G)", "-X", ""),
	bflag.GlobalFlag(config.NumWorkersKey, "-n"),
	bflag.Flag("summary", "s", "Create tree summary table along with other tables", "-s", false),
	bflag.Flag("only-summary", "S", "Create only tree summary table", "-S", false),
	bflag.Flag("xattrs", "x", "Pull xattrs from source", "-x", false),
	bflag.Flag("max-level", "z", "Max level to go down", "-z", 0),
	bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
	bflag.Flag("port", "p", "Port number to connect with client", "-p", 0),
	bflag.Flag("version", "v", "BeeGFS Hive Index Version", "-v", false),
	bflag.GlobalFlag(config.DebugKey, "-V=1"),
	bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
}

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		//nolint:golint // we want to capitalize BeeGFS in the error string
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexConfig)
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		//nolint:golint // we want to capitalize BeeGFS in the error string
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexConfig)
	}

	if _, err := os.Stat(indexEnv); os.IsNotExist(err) {
		//nolint:golint // we want to capitalize BeeGFS in the error string
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexEnv)
	}

	if _, err := os.Stat(updateEnv); os.IsNotExist(err) {
		//nolint:golint // we want to capitalize BeeGFS in the error string
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", updateEnv)
	}

	return nil
}
