package index

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type CreateCfg struct {
	GlobalCfg
	FSPath          string
	SkipTreesummary bool
	Xattrs          bool
	NoMetadata      bool
}

func Create(ctx context.Context, cfg CreateCfg) (<-chan string, func() error, error) {
	log, _ := config.GetLogger()

	if cfg.IndexRoot == "" {
		return nil, nil, fmt.Errorf("create: %w", ErrIndexRootNotSet)
	}

	lines := make(chan string, chanBufSize(cfg.Threads))

	ctx, cancel := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(lines)

		args := buildDir2IndexArgs(cfg, cfg.IndexRoot)
		bin, args, err := WrapForRemote(Dir2IndexBin, args, cfg.IndexAddr)
		if err != nil {
			return err
		}
		log.Debug("running gufi_dir2index", zap.String("bin", bin), zap.Strings("args", args))
		if err := runSubprocess(gCtx, bin, args, lines); err != nil {
			return fmt.Errorf("gufi_dir2index: %w", err)
		}

		if !cfg.SkipTreesummary {
			finalPath := filepath.Join(cfg.IndexRoot, filepath.Base(filepath.Clean(cfg.FSPath)))
			if err := runTreesummary(gCtx, cfg.IndexAddr, cfg.Threads, finalPath, lines); err != nil {
				return err
			}
		}

		return nil
	})

	return lines, func() error {
		cancel()
		return g.Wait()
	}, nil
}

// runTreesummary runs gufi_treesummary over path (locally, or wrapped for a
// remote index), streaming its output to lines. Shared by Create and Rescan,
// whose invocations were otherwise byte-identical.
func runTreesummary(ctx context.Context, indexAddr string, threads int, path string, lines chan<- string) error {
	log, _ := config.GetLogger()
	args := appendThreads(nil, threads)
	args = append(args, path)
	bin, args, err := WrapForRemote(TreesumBin, args, indexAddr)
	if err != nil {
		return err
	}
	log.Debug("running gufi_treesummary", zap.String("bin", bin), zap.Strings("args", args))
	if err := runSubprocess(ctx, bin, args, lines); err != nil {
		return fmt.Errorf("gufi_treesummary: %w", err)
	}
	return nil
}

func buildDir2IndexArgs(cfg CreateCfg, destDir string) []string {
	args := appendThreads(nil, cfg.Threads)
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	if !cfg.NoMetadata {
		args = append(args, "--plugin", IndexPluginPath)
	}
	args = append(args, cfg.FSPath, destDir)
	return args
}

func appendThreads(args []string, threads int) []string {
	if threads > 0 {
		args = append(args, "-n", fmt.Sprint(threads))
	}
	return args
}
