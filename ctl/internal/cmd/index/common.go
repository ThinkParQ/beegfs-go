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

var SIZES = []string{
	"K", "KB", "KiB",
	"M", "MB", "MiB",
	"G", "GB", "GiB",
	"T", "TB", "TiB",
	"P", "PB", "PiB",
	"E", "EB", "EiB",
	"Z", "ZB", "ZiB",
	"Y", "YB", "YiB",
}

func validateBlockSize(size string) error {
	for _, s := range SIZES {
		if size == s {
			return nil
		}
	}
	return fmt.Errorf("%s is not a valid block size", size)
}

func getNonNegative(value int) error {
	if value < 0 {
		return fmt.Errorf("%d is not a non-negative integer", value)
	}
	return nil
}

func getChar(value string) error {
	if len(value) != 1 {
		return fmt.Errorf("%s is not a single character", value)
	}
	return nil
}

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
