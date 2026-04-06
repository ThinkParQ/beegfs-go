package index

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RescanCfg holds backend configuration for the rescan command.
type RescanCfg struct {
	Paths           []string // source filesystem paths to rescan
	Recurse         bool     // false: --max-level 0 (single directory only)
	Threads         int      // 0 → viper ThreadsKey
	SkipTreesummary bool     // skip gufi_treesummary after rescan
	Xattrs          bool     // index extended attributes
}

// Rescan runs gufi_dir2index for each path, then optionally re-runs
// treesummary. Progress lines are streamed into the returned channel.
func Rescan(ctx context.Context, cfg RescanCfg, indexPath string) (<-chan string, func() error, error) {
	log, _ := config.GetLogger()

	threads := cfg.Threads
	if threads <= 0 {
		threads = viper.GetInt(ThreadsKey)
	}

	numWorkers := max(viper.GetInt(config.NumWorkersKey)-1, 1)
	lines := make(chan string, numWorkers*4)

	dir2indexBin := viper.GetString(IndexBinKey)

	// Clean the index path to remove any trailing slash before computing Dir.
	cleanIndex := filepath.Clean(indexPath)

	// Compute treesummary path as the mount-level index root (e.g. /mnt/index/beegfs),
	// so treesummary propagates stats across the full mounted filesystem index rather
	// than only the rescanned subdirectory.
	treesumPath := cleanIndex
	if indexRoot := viper.GetString(IndexRootKey); indexRoot != "" {
		if rel, err := filepath.Rel(indexRoot, cleanIndex); err == nil && !strings.HasPrefix(rel, "..") {
			parts := strings.SplitN(rel, string(filepath.Separator), 2)
			treesumPath = filepath.Join(indexRoot, parts[0])
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(lines)

		for _, fsPath := range cfg.Paths {
			// Pass filepath.Dir(cleanIndex) as the destination: gufi_dir2index will
			// create <parent>/<basename(fsPath)>/, which is cleanIndex itself.
			args := buildRescanArgs(cfg, fsPath, filepath.Dir(cleanIndex), threads)
			log.Debug("running gufi_dir2index",
				zap.String("bin", dir2indexBin),
				zap.Strings("args", args),
			)
			if err := runSubprocess(gCtx, dir2indexBin, args, lines); err != nil {
				return fmt.Errorf("gufi_dir2index (%s): %w", fsPath, err)
			}
		}

		if !cfg.SkipTreesummary {
			treesumBin := viper.GetString(TreesumBinKey)
			treesumArgs := []string{"-n", fmt.Sprint(threads), treesumPath}
			log.Debug("running gufi_treesummary", zap.String("bin", treesumBin), zap.Strings("args", treesumArgs))
			if err := runSubprocess(gCtx, treesumBin, treesumArgs, lines); err != nil {
				return fmt.Errorf("gufi_treesummary: %w", err)
			}
		}

		return nil
	})

	return lines, g.Wait, nil
}

func buildRescanArgs(cfg RescanCfg, fsPath, indexParent string, threads int) []string {
	args := []string{"-n", fmt.Sprint(threads)}
	if !cfg.Recurse {
		args = append(args, "--max-level", "0")
	}
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	args = append(args, "--plugin", IndexPluginPath)
	args = append(args, fsPath, indexParent)
	return args
}
