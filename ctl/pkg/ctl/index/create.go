package index

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// CreateCfg holds backend configuration for the create command.
type CreateCfg struct {
	FSPath          string // source filesystem path (-F)
	IndexPath       string // destination index path (-I)
	Threads         int    // -n (0 → viper ThreadsKey)
	SkipTreesummary bool   // skip gufi_treesummary after indexing
	Xattrs          bool   // index extended attributes
	NoMetadata      bool   // skip BeeGFS plugin (--no-metadata)
}

// Create runs the indexing pipeline:
//  1. gufi_dir2index   (always); index lands at cfg.IndexPath/<basename(cfg.FSPath)>/
//  2. gufi_treesummary on the new index root (unless cfg.SkipTreesummary)
//
// Progress lines from each subprocess are streamed into the returned channel.
func Create(ctx context.Context, cfg CreateCfg) (<-chan string, func() error, error) {
	log, _ := config.GetLogger()

	threads := cfg.Threads
	if threads <= 0 {
		threads = viper.GetInt(ThreadsKey)
	}

	numWorkers := max(viper.GetInt(config.NumWorkersKey)-1, 1)
	lines := make(chan string, numWorkers*4)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(lines)

		// Ensure cfg.IndexPath exists before running gufi_dir2index inside it.
		if err := os.MkdirAll(cfg.IndexPath, 0755); err != nil {
			return fmt.Errorf("creating index directory: %w", err)
		}

		// 1. gufi_dir2index into cfg.IndexPath — creates cfg.IndexPath/<basename(FSPath)>/
		dir2indexBin := viper.GetString(IndexBinKey)
		args := buildDir2IndexArgs(cfg, cfg.IndexPath, threads)
		log.Debug("running gufi_dir2index", zap.String("bin", dir2indexBin), zap.Strings("args", args))
		if err := runSubprocess(gCtx, dir2indexBin, args, lines); err != nil {
			return fmt.Errorf("gufi_dir2index: %w", err)
		}

		// 2. treesummary on the new index root (default on; skip with SkipTreesummary)
		finalPath := filepath.Join(cfg.IndexPath, filepath.Base(filepath.Clean(cfg.FSPath)))
		if !cfg.SkipTreesummary {
			treesumBin := viper.GetString(TreesumBinKey)
			treesumArgs := []string{"-n", fmt.Sprint(threads), finalPath}
			log.Debug("running gufi_treesummary", zap.String("bin", treesumBin), zap.Strings("args", treesumArgs))
			if err := runSubprocess(gCtx, treesumBin, treesumArgs, lines); err != nil {
				return fmt.Errorf("gufi_treesummary: %w", err)
			}
		}

		return nil
	})

	return lines, g.Wait, nil
}

func buildDir2IndexArgs(cfg CreateCfg, destDir string, threads int) []string {
	args := []string{"-n", fmt.Sprint(threads)}
	if cfg.Xattrs {
		args = append(args, "-x")
	}
	if !cfg.NoMetadata {
		args = append(args, "--plugin", IndexPluginPath)
	}
	args = append(args, cfg.FSPath, destDir)
	return args
}

// runSubprocess runs bin with args, streaming each output line to lines.
func runSubprocess(ctx context.Context, bin string, args []string, lines chan<- string) error {
	cmd := exec.CommandContext(ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout // merge stderr into stdout pipe
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", bin, err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		cmd.Wait() //nolint:errcheck
		return fmt.Errorf("scanning %s output: %w", bin, err)
	}
	return cmd.Wait()
}
