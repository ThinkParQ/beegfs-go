package logger

import (
	"fmt"
	"os"
	"path/filepath"
)

// ensureLogsAreWritable ensures we can write to the directory used for logging
// and creates the directory if needed.
func ensureLogsAreWritable(configFile string) error {

	logsDir := filepath.Dir(configFile)
	if err := ensureLogDirExists(logsDir, 0755); err != nil {
		return err
	}

	// Writing to the provided log file is not sufficient if we want to
	// rotate log files. Make sure we can write to the directory.
	tempFileName := fmt.Sprintf("%s/.%d.tmp", logsDir, os.Getpid())
	tempFile, err := os.OpenFile(tempFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("creating files in the specified logging directory failed, BeeWatch must have write access to this directory to facilitate log rotation (%s): %w", logsDir, err)
	}
	if err = tempFile.Close(); err != nil {
		return err
	}

	if err = os.Remove(tempFile.Name()); err != nil {
		return err
	}
	return nil
}

// ensureLogDirExists will check if the provided directory exists and create it
// with the provided permissions if needed.
func ensureLogDirExists(dirPath string, perm os.FileMode) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return os.MkdirAll(dirPath, perm)
	} else if err != nil {
		return err
	}
	return nil
}
