package index

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

func resolveIndexRoot(required bool) (string, bool, error) {
	override := strings.TrimSpace(indexRoot)
	if override != "" {
		root := filepath.Clean(override)
		if !filepath.IsAbs(root) {
			return "", false, fmt.Errorf("--index-root must be an absolute path")
		}
		return root, true, nil
	}
	root, ok := getGUFIConfigValue("IndexRoot")
	if !ok || root == "" {
		if required {
			return "", false, fmt.Errorf("IndexRoot not found in %s (set --index-root to override)", indexConfig)
		}
		return "", false, nil
	}
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		if required {
			return "", false, fmt.Errorf("IndexRoot in %s must be an absolute path", indexConfig)
		}
		return "", false, nil
	}
	return root, true, nil
}

func resolvePaths(args []string, resolve func(string) (string, error)) ([]string, error) {
	resolved := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.TrimSpace(arg) == "" {
			return nil, fmt.Errorf("path is required")
		}
		path, err := resolve(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid path %q: %w", arg, err)
		}
		resolved = append(resolved, path)
	}
	return resolved, nil
}

func resolveBeeGFSRelativePaths(args []string) ([]string, error) {
	return resolvePaths(args, resolveBeeGFSRelativePath)
}

func resolveIndexPath(path string) (string, error) {
	cleanPath := filepath.Clean(path)
	indexRoot, ok, err := resolveIndexRoot(false)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(cleanPath) {
		if !ok {
			return cleanPath, nil
		}
		within, err := pathWithin(indexRoot, cleanPath)
		if err != nil {
			return "", err
		}
		if within {
			return cleanPath, nil
		}
		return "", fmt.Errorf("path %q is outside IndexRoot %q", cleanPath, indexRoot)
	}
	if !ok {
		return "", fmt.Errorf("IndexRoot not found in %s (set --index-root to override)", indexConfig)
	}
	return indexPathFromRelativeWithRoot(indexRoot, cleanPath), nil
}

func resolveBeeGFSRelativePath(path string) (string, error) {
	cleanPath := filepath.Clean(path)
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return "", err
	}
	if indexRoot, ok, err := resolveIndexRoot(false); err != nil {
		return "", err
	} else if ok {
		within, err := pathWithin(indexRoot, absPath)
		if err != nil {
			return "", err
		}
		if within {
			client, err := config.BeeGFSClient(".")
			if err != nil {
				return "", err
			}
			return resolveBeeGFSRelativePathWithClient(absPath, indexRoot, client)
		}
	}
	client, err := config.BeeGFSClient(cleanPath)
	if err != nil {
		return "", err
	}
	return resolveBeeGFSRelativePathWithClient(absPath, "", client)
}

type beegfsRelativePathClient interface {
	GetMountPath() string
	GetRelativePathWithinMount(path string) (string, error)
}

func resolveBeeGFSRelativePathWithClient(absPath, indexRoot string, client beegfsRelativePathClient) (string, error) {
	if indexRoot != "" {
		rel, err := filepath.Rel(indexRoot, absPath)
		if err != nil {
			return "", err
		}
		absPath = filepath.Join(client.GetMountPath(), rel)
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

func resolveBeeGFSAbsolutePath(backend indexBackend, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path is required")
	}
	cleanPath := filepath.Clean(path)
	if !backend.isLocal() {
		if !filepath.IsAbs(cleanPath) {
			return "", fmt.Errorf("remote index-addr requires an absolute path: %q", path)
		}
		return cleanPath, nil
	}
	relative, err := resolveBeeGFSRelativePath(cleanPath)
	if err != nil {
		return "", err
	}
	client, err := config.BeeGFSClient(cleanPath)
	if err != nil {
		return "", err
	}
	mountPath := client.GetMountPath()
	if mountPath == "" {
		return "", fmt.Errorf("BeeGFS mount path is required to resolve %q", path)
	}
	return filepath.Join(mountPath, relative), nil
}

func resolveBeeGFSAbsolutePaths(backend indexBackend, args []string) ([]string, error) {
	resolved := make([]string, 0, len(args))
	for _, arg := range args {
		path, err := resolveBeeGFSAbsolutePath(backend, arg)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, path)
	}
	return resolved, nil
}

func resolveIndexPathInput(backend indexBackend, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("index path is required")
	}
	cleanPath := filepath.Clean(path)
	if filepath.IsAbs(cleanPath) {
		return cleanPath, nil
	}
	if !backend.isLocal() {
		indexRoot, _, err := resolveIndexRoot(true)
		if err != nil {
			return "", err
		}
		return indexPathFromRelativeWithRoot(indexRoot, cleanPath), nil
	}
	return resolveIndexPath(cleanPath)
}

func indexPathFromRelative(relative string) (string, error) {
	indexRoot, _, err := resolveIndexRoot(true)
	if err != nil {
		return "", err
	}
	return indexPathFromRelativeWithRoot(indexRoot, relative), nil
}

func indexPathFromRelativeWithRoot(indexRoot, relative string) string {
	trimmed := strings.TrimPrefix(relative, string(filepath.Separator))
	cleanRoot := filepath.Clean(indexRoot)
	if trimmed == "" || trimmed == "." {
		return cleanRoot
	}
	return filepath.Join(cleanRoot, trimmed)
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
