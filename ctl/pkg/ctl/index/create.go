package index

import (
	"fmt"
	"os/exec"
)

type CreateIndex_Config struct {
	MaxMemory  string
	FsPath     string
	IndexPath  string
	NumThreads uint
	Summary    bool
	Xattrs     bool
	MaxLevel   uint
	ScanDirs   bool
	Port       uint
	Debug      bool
	MntPath    string
	RunUpdate  bool
}

func RunPythonCreateIndex(cfg *CreateIndex_Config) error {
	args := []string{
		"/usr/bin/bee", "index",
	}

	// Append optional arguments based on user input
	if cfg.FsPath != "" {
		args = append(args, "-F", cfg.FsPath)
	}
	if cfg.IndexPath != "" {
		args = append(args, "-I", cfg.IndexPath)
	}
	if cfg.MaxMemory != "" {
		args = append(args, "-X", cfg.MaxMemory)
	}
	if cfg.NumThreads > 0 {
		args = append(args, "-n", fmt.Sprint(cfg.NumThreads))
	}
	if cfg.Summary {
		args = append(args, "-S")
	}
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	if cfg.MaxLevel > 0 {
		args = append(args, "-z", fmt.Sprint(cfg.MaxLevel))
	}
	if cfg.ScanDirs {
		args = append(args, "-C")
	}
	if cfg.Port > 0 {
		args = append(args, "-p", fmt.Sprint(cfg.Port))
	}
	if cfg.MntPath != "" {
		args = append(args, "-M", cfg.MntPath)
	}
	if cfg.Debug {
		args = append(args, "-V", "1")
	}
	if cfg.RunUpdate {
		args = append(args, "-U")
	}
	args = append(args, "-k")

	// Execute the Python command
	cmd := exec.Command("python3", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing command: %v\nOutput: %s", err, string(output))
	}
	return nil
}
