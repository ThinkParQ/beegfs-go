package index

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type RescanTarget struct {
	FSPath    string
	IndexPath string
}

type RescanCfg struct {
	GlobalCfg
	Targets         []RescanTarget
	Recurse         bool
	SkipTreesummary bool
	Xattrs          bool
}

func Rescan(ctx context.Context, cfg RescanCfg) (<-chan string, func() error, error) {
	log, _ := config.GetLogger()

	if len(cfg.Targets) == 0 {
		return nil, nil, fmt.Errorf("rescan: no targets")
	}

	lines := make(chan string, chanBufSize(cfg.Threads))

	treesumPath := filepath.Clean(cfg.Targets[0].IndexPath)
	if cfg.IndexRoot != "" {
		if rel, err := filepath.Rel(cfg.IndexRoot, treesumPath); err == nil && !strings.HasPrefix(rel, "..") {
			parts := strings.SplitN(rel, string(filepath.Separator), 2)
			treesumPath = filepath.Join(cfg.IndexRoot, parts[0])
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(lines)

		for _, t := range cfg.Targets {
			cleanIndex := filepath.Clean(t.IndexPath)
			args := buildRescanArgs(cfg, t.FSPath, filepath.Dir(cleanIndex))
			bin, args, err := WrapForRemote(Dir2IndexBin, args, cfg.IndexAddr)
			if err != nil {
				return err
			}
			log.Debug("running gufi_dir2index",
				zap.String("bin", bin),
				zap.Strings("args", args),
			)
			if err := runSubprocess(gCtx, bin, args, lines); err != nil {
				return fmt.Errorf("gufi_dir2index (%s): %w", t.FSPath, err)
			}
		}

		if !cfg.SkipTreesummary {
			if err := runTreesummary(gCtx, cfg.IndexAddr, cfg.Threads, treesumPath, lines); err != nil {
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

func buildRescanArgs(cfg RescanCfg, fsPath, indexParent string) []string {
	args := appendThreads(nil, cfg.Threads)
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
