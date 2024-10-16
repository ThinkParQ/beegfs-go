package index

import (
	"fmt"
	"os"
	"regexp"
)

const (
	beeBinary   = "/usr/bin/bee"
	indexConfig = "/etc/beegfs/index/config"
	indexEnv    = "/etc/beegfs/index/indexEnv.conf"
)

var (
	validPath   = regexp.MustCompile(`^/[\w/.-]+$`)
	validMemory = regexp.MustCompile(`^\d+(MB|GB|TB|G|M|T)$`)
	validPort   = regexp.MustCompile(`^\d{1,5}$`)
)

func checkBeeGFSConfig() error {
	if _, err := os.Stat(beeBinary); os.IsNotExist(err) {
		return fmt.Errorf("hive binary not found at %s", beeBinary)
	}

	if _, err := os.Stat(indexConfig); os.IsNotExist(err) {
		return fmt.Errorf("hive is not configured: %s not found", indexConfig)
	}

	if _, err := os.Stat(indexEnv); os.IsNotExist(err) {
		return fmt.Errorf("hive is not configured: %s not found", indexEnv)
	}

	return nil
}
