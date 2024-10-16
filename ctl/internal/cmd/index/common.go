package index

import (
	"fmt"
	"os"
)

const (
	beeBinary   = "/usr/bin/bee"
	indexConfig = "/etc/beegfs/index/config"
	indexEnv    = "/etc/beegfs/index/indexEnv.conf"
	updateEnv   = "/etc/beegfs/index/updateEnv.conf"
)

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		return fmt.Errorf("%s: Hive Index Binary not found at %s", "BeeGFS", beeBinary)
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		return fmt.Errorf("%s Hive Index is not configured: %s not found", "BeeGFS", indexConfig)
	}

	if _, err := os.Stat(indexEnv); os.IsNotExist(err) {
		return fmt.Errorf("%s Hive Index is not configured: %s not found", "BeeGFS", indexEnv)
	}

	if _, err := os.Stat(updateEnv); os.IsNotExist(err) {
		return fmt.Errorf("%s Hive Index is not configured: %s not found", "BeeGFS", updateEnv)
	}

	return nil
}
