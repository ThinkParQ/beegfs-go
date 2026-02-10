package index

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

func resolvePaths(args []string) ([]string, error) {
	resolved := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.TrimSpace(arg) == "" {
			return nil, fmt.Errorf("path is required")
		}
		cleanPath := filepath.Clean(arg)
		relative, err := resolveBeeGFSRelativePath(cleanPath)
		if err != nil {
			return nil, fmt.Errorf("invalid path %q: %w", arg, err)
		}
		resolved = append(resolved, relative)
	}
	return resolved, nil
}

func resolveIndexPath(path string) (string, error) {
	cleanPath := filepath.Clean(path)
	indexRoot, ok := getGUFIConfigValue("IndexRoot")
	if !ok || indexRoot == "" {
		return "", fmt.Errorf("IndexRoot not found in %s", indexConfig)
	}
	if filepath.IsAbs(cleanPath) {
		within, err := pathWithin(indexRoot, cleanPath)
		if err != nil {
			return "", err
		}
		if within {
			return cleanPath, nil
		}
	}
	relative, err := resolveBeeGFSRelativePath(cleanPath)
	if err != nil {
		return "", err
	}
	return indexPathFromRelativeWithRoot(indexRoot, relative), nil
}

func resolveBeeGFSRelativePath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if indexRoot, ok := getGUFIConfigValue("IndexRoot"); ok && indexRoot != "" {
		within, err := pathWithin(indexRoot, absPath)
		if err != nil {
			return "", err
		}
		if within {
			rel, err := filepath.Rel(indexRoot, absPath)
			if err != nil {
				return "", err
			}
			client, err := config.BeeGFSClient(".")
			if err != nil {
				return "", err
			}
			absPath = filepath.Join(client.GetMountPath(), rel)
			inMountPath, err := client.GetRelativePathWithinMount(absPath)
			if err != nil {
				return "", err
			}
			relative := strings.TrimPrefix(inMountPath, string(filepath.Separator))
			if relative == "" {
				return ".", nil
			}
			return relative, nil
		}
	}
	client, err := config.BeeGFSClient(path)
	if err != nil {
		return "", err
	}
	inMountPath, err := client.GetRelativePathWithinMount(absPath)
	if err != nil {
		return "", err
	}
	relative := strings.TrimPrefix(inMountPath, string(filepath.Separator))
	if relative == "" {
		return ".", nil
	}
	return relative, nil
}

func indexPathFromRelative(relative string) (string, error) {
	indexRoot, ok := getGUFIConfigValue("IndexRoot")
	if !ok || indexRoot == "" {
		return "", fmt.Errorf("IndexRoot not found in %s", indexConfig)
	}
	return indexPathFromRelativeWithRoot(indexRoot, relative), nil
}

func indexPathFromRelativeWithRoot(indexRoot, relative string) string {
	trimmed := strings.TrimPrefix(relative, string(filepath.Separator))
	if trimmed == "" || trimmed == "." {
		return filepath.Clean(indexRoot)
	}
	return filepath.Join(filepath.Clean(indexRoot), trimmed)
}

func pathWithin(base, target string) (bool, error) {
	if base == "" {
		return false, nil
	}
	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return false, err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(baseAbs, targetAbs)
	if err != nil {
		return false, err
	}
	if rel == "." {
		return true, nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false, nil
	}
	return true, nil
}
