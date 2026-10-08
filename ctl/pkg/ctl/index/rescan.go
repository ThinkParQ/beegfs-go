package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

const rescanBatch = 512

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
			idx := filepath.Clean(t.IndexPath)

			if cfg.Recurse {
				if _, err := os.Stat(t.FSPath); err != nil {
					return fmt.Errorf("stat %q: %w", t.FSPath, err)
				}
				if err := removeDirs(gCtx, cfg, lines, idx); err != nil {
					log.Warn("removing old index before rebuild", zap.Error(err))
				}
				if err := dir2index(gCtx, cfg, lines, false, filepath.Dir(idx), t.FSPath); err != nil {
					return fmt.Errorf("%w; the index for %s was removed and must be rescanned", err, t.FSPath)
				}
				continue
			}

			if err := dir2index(gCtx, cfg, lines, true, filepath.Dir(idx), t.FSPath); err != nil {
				return err
			}
			src, err := childDirs(t.FSPath)
			if err != nil {
				return err
			}
			have, err := indexChildDirs(gCtx, cfg, idx)
			if err != nil {
				return err
			}
			var stale, fresh []string
			for n := range have {
				if !src[n] {
					stale = append(stale, filepath.Join(idx, n))
				}
			}
			for n := range src {
				if !have[n] {
					fresh = append(fresh, filepath.Join(t.FSPath, n))
				}
			}
			slices.Sort(stale)
			slices.Sort(fresh)
			for batch := range slices.Chunk(stale, rescanBatch) {
				log.Info("removing stale index directories", zap.Strings("paths", batch))
				if err := removeDirs(gCtx, cfg, lines, batch...); err != nil {
					return err
				}
			}
			for batch := range slices.Chunk(fresh, rescanBatch) {
				if err := dir2index(gCtx, cfg, lines, false, idx, batch...); err != nil {
					return err
				}
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

func dir2index(ctx context.Context, cfg RescanCfg, lines chan<- string, shallow bool, indexParent string, fsPaths ...string) error {
	log, _ := config.GetLogger()
	args := buildRescanArgs(cfg, shallow, indexParent, fsPaths...)
	bin, args, err := WrapForRemote(Dir2IndexBin, args, cfg.IndexAddr)
	if err != nil {
		return err
	}
	log.Debug("running gufi_dir2index", zap.String("bin", bin), zap.Strings("args", args))
	if err := runSubprocess(ctx, bin, args, lines); err != nil {
		return fmt.Errorf("gufi_dir2index (%s): %w", strings.Join(fsPaths, " "), err)
	}
	return nil
}

func removeDirs(ctx context.Context, cfg RescanCfg, lines chan<- string, paths ...string) error {
	bin, args, err := WrapForRemote("rm", append([]string{"-rf", "--"}, paths...), cfg.IndexAddr)
	if err != nil {
		return err
	}
	if err := runSubprocess(ctx, bin, args, lines); err != nil {
		return fmt.Errorf("rm -rf %s: %w", strings.Join(paths, " "), err)
	}
	return nil
}

func indexChildDirs(ctx context.Context, cfg RescanCfg, idx string) (map[string]bool, error) {
	if !IsRemoteAddr(cfg.IndexAddr) {
		return childDirs(idx)
	}
	bin, args, err := WrapForRemote("find", []string{idx, "-mindepth", "1", "-maxdepth", "1", "-type", "d", "-printf", "%f\n"}, cfg.IndexAddr)
	if err != nil {
		return nil, err
	}
	out := make(chan string, rowChanBufFactor)
	errc := make(chan error, 1)
	go func() {
		errc <- runSubprocess(ctx, bin, args, out)
		close(out)
	}()
	dirs := make(map[string]bool)
	for name := range out {
		dirs[name] = true
	}
	if err := <-errc; err != nil {
		return nil, fmt.Errorf("listing %s: %w", idx, err)
	}
	return dirs, nil
}

// childDirs returns the names of the directories directly under path.
func childDirs(path string) (map[string]bool, error) {
	ents, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", path, err)
	}
	dirs := make(map[string]bool, len(ents))
	for _, e := range ents {
		if e.IsDir() {
			dirs[e.Name()] = true
		}
	}
	return dirs, nil
}

func buildRescanArgs(cfg RescanCfg, shallow bool, indexParent string, fsPaths ...string) []string {
	args := appendThreads(nil, cfg.Threads)
	if shallow {
		args = append(args, "--max-level", "0")
	}
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	args = append(args, "--plugin", IndexPluginPath)
	args = append(args, fsPaths...)
	return append(args, indexParent)
}
