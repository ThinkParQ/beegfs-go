package index

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

func init() {
	indexPkg.LoadGUFIConfig()
}

// newExecutor creates the appropriate Executor based on the --index-addr viper value.
func newExecutor() (indexPkg.Executor, error) {
	return indexPkg.NewExecutor(viper.GetViper())
}

// applyDotIndexDefaults discovers the BeeGFS mount for fsPath, computes the
// relative path, and loads the best-matching .beegfs.index entry as viper
// defaults. Silently skips unmounted or missing cases.
func applyDotIndexDefaults(fsPath string) {
	client, err := config.BeeGFSClient(fsPath)
	if err != nil {
		return
	}
	rel, err := client.GetRelativePathWithinMount(fsPath)
	if err != nil {
		return
	}
	indexPkg.LoadDotIndexFile(client.GetMountPath(), rel)
}

// checkIndexExists returns an error if no GUFI index (db.db) is found at indexPath.
func checkIndexExists(indexPath string) error {
	dbPath := filepath.Join(indexPath, "db.db")
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no GUFI index found at %q (has 'beegfs index create' been run?)", indexPath)
		}
		return fmt.Errorf("checking index at %q: %w", indexPath, err)
	}
	return nil
}

// fsDirFromIndexPath derives the BeeGFS filesystem path corresponding to a GUFI
// index path. It loads .beegfs.index from the mount root to resolve IndexRootKey,
// then strips the index-root + mount-basename prefix and reconstructs the FS path.
func fsDirFromIndexPath(indexPath string) (string, error) {
	client, err := config.BeeGFSClient(".")
	if err != nil {
		return "", fmt.Errorf("finding BeeGFS mount: %w", err)
	}
	mountPath := client.GetMountPath()

	// Load .beegfs.index from mount root so IndexRootKey reflects the configured value.
	indexPkg.LoadDotIndexFile(mountPath, "/")

	indexRoot := viper.GetString(indexPkg.IndexRootKey)
	rel, err := filepath.Rel(indexRoot, indexPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("index path %q is not under index root %q", indexPath, indexRoot)
	}
	// rel = "mountBase" or "mountBase/relative/path"
	parts := strings.SplitN(rel, string(filepath.Separator), 2)
	mountBase := parts[0]
	relWithinMount := ""
	if len(parts) > 1 {
		relWithinMount = parts[1]
	}

	if filepath.Base(mountPath) != mountBase {
		return "", fmt.Errorf("BeeGFS mount %q does not match index mount basename %q", mountPath, mountBase)
	}
	return filepath.Join(mountPath, relWithinMount), nil
}

// resolveIndexPath converts a user-supplied BeeGFS filesystem path to the
// corresponding GUFI index tree path.
//
//   - If no path is given, the current working directory is used.
//   - Path normalization is done via BeeGFSClient.GetRelativePathWithinMount,
//     which works whether BeeGFS is mounted or not.
//   - .beegfs.index defaults are loaded before reading index-root.
//   - The result is joined with the index-root viper key.
func resolveIndexPath(args []string) (string, error) {
	fsPath := "."
	if len(args) > 0 {
		fsPath = args[0]
	} else {
		var err error
		fsPath, err = os.Getwd()
		if err != nil {
			return "", fmt.Errorf("getting working directory: %w", err)
		}
	}

	beegfsClient, err := config.BeeGFSClient(fsPath)
	if err != nil && !errors.Is(err, filesystem.ErrUnmounted) {
		return "", fmt.Errorf("getting BeeGFS client: %w", err)
	}

	rel, err := beegfsClient.GetRelativePathWithinMount(fsPath)
	if err != nil {
		return "", fmt.Errorf("resolving path within BeeGFS mount: %w", err)
	}

	// Load best-matching .beegfs.index entry before reading IndexRootKey.
	indexPkg.LoadDotIndexFile(beegfsClient.GetMountPath(), rel)

	// gufi_dir2index nests the index under <index-root>/<basename(mount)>/,
	// so we include the mount basename when resolving all index paths.
	mountBase := filepath.Base(beegfsClient.GetMountPath())
	return filepath.Join(viper.GetString(indexPkg.IndexRootKey), mountBase, rel), nil
}
