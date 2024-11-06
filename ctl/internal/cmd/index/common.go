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

var path string

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		//nolint:golint // we want to capitalize BeeGFS in the error string
		return fmt.Errorf("BeeGFS Hive Index is not configured: %s not found", indexConfig)
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
