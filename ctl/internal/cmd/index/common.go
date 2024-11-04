//lint:file-ignore ST1005 Ignore all staticcheck warnings about error strings
package index

import (
	"fmt"
	"os"
)

const (
	beeBinary   = "/usr/bin/bee"
	indexConfig = "/etc/beegfs/index/config"
	indexEnv    = "/etc/beegfs/index/indexEnv.conf"
)

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		return fmt.Errorf("BeeGFS Hive Index Binary not found at %s", beeBinary)
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexConfig)
	}

	if _, err := os.Stat(indexEnv); os.IsNotExist(err) {
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexEnv)
	}

	return nil
}
