package index

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

	return filepath.Join(viper.GetString(indexPkg.IndexRootKey), rel), nil
}
