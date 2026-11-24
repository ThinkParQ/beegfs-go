package index

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const OnIndexHostFlag = "on-index-host"

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
	if len(cfg.Targets) == 0 {
		return nil, nil, fmt.Errorf("rescan: no targets")
	}
	remote := IsRemoteAddr(cfg.IndexAddr)
	if _, err := filesystem.NewFromMountPoint(cfg.MountPath); !remote && err != nil {
		return nil, nil, fmt.Errorf("rescan: %q is not a mounted BeeGFS: %w", cfg.MountPath, err)
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

		if remote {
			return rescanOnIndexHost(gCtx, cfg, lines)
		}

		for _, t := range cfg.Targets {
			idx := filepath.Clean(t.IndexPath)
			if err := dir2index(gCtx, cfg, lines, !cfg.Recurse, filepath.Dir(idx), t.FSPath); err != nil {
				return err
			}
			fresh, err := pruneIndex(gCtx, cfg, t.FSPath, idx)
			if err != nil {
				return err
			}
			for batch := range slices.Chunk(fresh, 128) {
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

func rescanOnIndexHost(ctx context.Context, cfg RescanCfg, lines chan<- string) error {
	args := []string{"index", "rescan", "--" + OnIndexHostFlag, "--index-addr=local", "--index-root=" + cfg.IndexRoot,
		"--recurse=" + strconv.FormatBool(cfg.Recurse), "--skip-treesummary=" + strconv.FormatBool(cfg.SkipTreesummary),
		"--xattrs=" + strconv.FormatBool(cfg.Xattrs)}
	if cfg.Threads > 0 {
		args = append(args, "--"+config.NumWorkersKey+"="+strconv.Itoa(cfg.Threads))
	}
	for _, key := range []string{config.ManagementAddrKey, config.TlsDisableKey, config.TlsCertFile,
		config.TlsDisableVerificationKey, config.AuthDisableKey, config.AuthFileKey, config.ConnTimeoutKey,
		config.UseProxyKey, config.LogLevelKey, config.DisableAlerts} {
		if viper.IsSet(key) {
			args = append(args, "--"+key+"="+viper.GetString(key))
		}
	}
	args = append(args, "--")
	for _, t := range cfg.Targets {
		args = append(args, t.FSPath)
	}
	bin, args, err := WrapForRemote("/opt/beegfs/sbin/beegfs", args, cfg.IndexAddr)
	if err != nil {
		return err
	}
	if err := runSubprocess(ctx, bin, args, lines); err != nil {
		return fmt.Errorf("beegfs index rescan on the index host: %w", err)
	}
	return nil
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
		return fmt.Errorf("gufi_dir2index %q: %w", fsPaths, err)
	}
	return nil
}

func pruneIndex(ctx context.Context, cfg RescanCfg, fsPath, idxPath string) ([]string, error) {
	src, err := openDir(cfg.MountPath, fsPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%q no longer exists, rescan its parent directory to remove its index: %w", fsPath, err)
	}
	if err != nil {
		return nil, err
	}
	defer src.Close()
	idx, err := openDir(cfg.IndexRoot, idxPath)
	if err != nil {
		return nil, err
	}
	defer idx.Close()
	idxDir, err := idx.Open(".")
	if err != nil {
		return nil, err
	}
	defer idxDir.Close()
	var st unix.Statfs_t
	srcInfo, srcErr := src.Stat(".")
	idxInfo, idxErr := idxDir.Stat()
	if err := errors.Join(srcErr, idxErr, unix.Fstatfs(int(idxDir.Fd()), &st)); err != nil {
		return nil, err
	}

	var fresh []string
	switch {
	case os.SameFile(srcInfo, idxInfo):
		dirs, err := subdirs(src)
		if err != nil {
			return nil, err
		}
		for name := range dirs {
			_, dbErr := src.Lstat(filepath.Join(name, "db.db"))
			_, bdmErr := src.Lstat(filepath.Join(name, ".bdm.db"))
			if name != "db.db" && errors.Is(dbErr, fs.ErrNotExist) && errors.Is(bdmErr, fs.ErrNotExist) {
				fresh = append(fresh, name)
			}
		}
	case st.Type == 0x19830326:
		log, _ := config.GetLogger()
		log.Info("not pruning an index on BeeGFS that is not the source directory", zap.String("index", idxPath))
		return nil, nil
	default:
		idxAbs, absErr := filepath.Abs(idxPath)
		srcReal, srcErr := filepath.EvalSymlinks(fsPath)
		idxReal, idxErr := filepath.EvalSymlinks(idxAbs)
		if err := errors.Join(absErr, srcErr, idxErr); err != nil {
			return nil, err
		}
		if sep := string(filepath.Separator); strings.HasPrefix(srcReal+sep, idxReal+sep) {
			return nil, fmt.Errorf("index directory %q contains the source directory %q", idxPath, fsPath)
		}
		if fresh, err = syncDirs(ctx, src, idx, cfg.Recurse); err != nil {
			return nil, err
		}
	}
	slices.Sort(fresh)
	for i, name := range fresh {
		fresh[i] = filepath.Join(fsPath, name)
	}
	return fresh, nil
}

func syncDirs(ctx context.Context, src, idx *os.Root, recurse bool) ([]string, error) {
	log, _ := config.GetLogger()
	want, err := subdirs(src)
	if err != nil {
		return nil, err
	}
	have, err := subdirs(idx)
	if err != nil {
		return nil, err
	}
	for name := range have {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch {
		case !want[name]:
			log.Info("removing stale index directory", zap.String("path", filepath.Join(idx.Name(), name)))
			err = idx.RemoveAll(name)
		case recurse:
			var s, i *os.Root
			if s, err = openChild(src, name); err == nil {
				if i, err = openChild(idx, name); err == nil {
					_, err = syncDirs(ctx, s, i, true)
					i.Close()
				}
				s.Close()
			}
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				err = nil
			}
		}
		if err != nil {
			return nil, err
		}
	}
	var fresh []string
	for name := range want {
		if !have[name] && name != "db.db" {
			if _, err := idx.Lstat(name); errors.Is(err, fs.ErrNotExist) {
				fresh = append(fresh, name)
			}
		}
	}
	return fresh, nil
}

func subdirs(r *os.Root) (map[string]bool, error) {
	f, err := r.Open(".")
	if err != nil {
		return nil, fmt.Errorf("listing %q: %w", r.Name(), err)
	}
	defer f.Close()
	dirs := make(map[string]bool)
	for {
		ents, err := f.ReadDir(1024)
		for _, e := range ents {
			if e.IsDir() {
				dirs[e.Name()] = true
			}
		}
		if err == io.EOF {
			return dirs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("listing %q: %w", r.Name(), err)
		}
	}
}

func openDir(base, path string) (*os.Root, error) {
	rel, err := filepath.Rel(base, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("%q is not below %q", path, base)
	}
	r, err := os.OpenRoot(base)
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Split(rel, string(filepath.Separator)) {
		child, err := openChild(r, name)
		r.Close()
		if err != nil {
			return nil, err
		}
		r = child
	}
	return r, nil
}

func openChild(r *os.Root, name string) (*os.Root, error) {
	path := filepath.Join(r.Name(), name)
	before, err := r.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", path, err)
	}
	child, err := r.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", path, err)
	}
	if after, err := child.Stat("."); err != nil || !os.SameFile(before, after) {
		child.Close()
		return nil, fmt.Errorf("%q: %w", path, syscall.ENOTDIR)
	}
	return child, nil
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
