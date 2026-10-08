package index

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

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

	if cfg.IndexRoot == "" {
		return nil, nil, fmt.Errorf("rescan: %w", ErrIndexRootNotSet)
	}
	if len(cfg.Targets) == 0 {
		return nil, nil, fmt.Errorf("rescan: no targets")
	}

	lines := make(chan string, chanBufSize(cfg.Threads))

	treesumPath := filepath.Clean(cfg.Targets[0].IndexPath)
	if rel, err := filepath.Rel(cfg.IndexRoot, treesumPath); err == nil && !strings.HasPrefix(rel, "..") {
		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		treesumPath = filepath.Join(cfg.IndexRoot, parts[0])
	}

	ctx, cancel := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(lines)

		for _, t := range cfg.Targets {
			st, err := os.Stat(t.FSPath)
			if err != nil {
				return fmt.Errorf("stat %q: %w", t.FSPath, err)
			}
			var stdin io.Reader
			if !cfg.Recurse {
				sys, ok := st.Sys().(*syscall.Stat_t)
				if !ok {
					return fmt.Errorf("stat %q: inode not available", t.FSPath)
				}
				stdin = strings.NewReader(fmt.Sprintf("%d d\n", sys.Ino))
			}
			args := buildRescanArgs(cfg, t)
			bin, args, err := WrapForRemote(IncrementalUpdateBin, args, cfg.IndexAddr)
			if err != nil {
				return err
			}
			var reported bool
			onLine := func(l string) {
				reported = reported || strings.HasPrefix(strings.TrimSpace(l), "Error:")
			}
			log.Debug("running gufi_incremental_update",
				zap.String("bin", bin),
				zap.Strings("args", args),
			)
			if err := runSubprocess(gCtx, bin, args, lines, stdin, onLine); err != nil {
				return fmt.Errorf("gufi_incremental_update (%s): %w", t.FSPath, err)
			}
			if reported {
				return fmt.Errorf("gufi_incremental_update (%s): reported errors, see output", t.FSPath)
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

func buildRescanArgs(cfg RescanCfg, t RescanTarget) []string {
	args := appendThreads(nil, cfg.Threads)
	if cfg.Recurse {
		args = append(args, "--suspect-method", "3", "--suspect-time", "0")
	} else {
		args = append(args, "--suspect-method", "1", "--suspect-file", "/dev/stdin")
	}
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	args = append(args, "--plugin", IndexPluginPath)
	park := filepath.Join(cfg.IndexRoot, fmt.Sprintf(".rescan-park-%d", os.Getpid()))
	return append(args, filepath.Clean(t.IndexPath), t.FSPath, park)
}
