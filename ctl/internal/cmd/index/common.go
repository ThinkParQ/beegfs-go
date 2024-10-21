//lint:file-ignore ST1005 Ignore all staticcheck warnings about error strings
package index

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
)

const (
	beeBinary   = "/usr/bin/bee"
	indexConfig = "/etc/beegfs/index/config"
	indexEnv    = "/etc/beegfs/index/indexEnv.conf"
)

var filesize = map[string]int{
	"b": 512,
	"c": 1,
	"w": 2,
	"k": 1024,
	"M": 1048576,
	"G": 1073741824,
}

func isValidSize(size string) error {
	re := regexp.MustCompile(`^([+-]?[0-9]*\.?[0-9]+)([bcwkMG])?$`)
	matches := re.FindStringSubmatch(size)

	if matches == nil {
		return fmt.Errorf("%s is not a valid numeric argument", size)
	}

	numericPart := matches[1]
	unit := matches[2]

	if _, err := strconv.ParseFloat(numericPart, 64); err != nil {
		return fmt.Errorf("%s is not a valid numeric argument", numericPart)
	}

	if unit != "" {
		if _, exists := filesize[unit]; !exists {
			return fmt.Errorf("%s is not a valid size unit", unit)
		}
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
