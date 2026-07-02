package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
)

// drain consumes ch until it closes or ctx is cancelled, invoking handle for
// each item. On cancellation it tears down the producer via errWait and returns
// ctx's error; on normal close it returns errWait's result. It replaces the
// near-identical labeled for/select drain loop the index subcommands repeated.
func drain[T any](ctx context.Context, ch <-chan T, errWait func() error, handle func(T)) error {
	for {
		select {
		case <-ctx.Done():
			_ = errWait()
			return ctx.Err()
		case item, ok := <-ch:
			if !ok {
				return errWait()
			}
			handle(item)
		}
	}
}

func ownerNamesRequested(defaultColumns []string) bool {
	cols := defaultColumns
	if viper.IsSet(config.ColumnsKey) {
		cols = viper.GetStringSlice(config.ColumnsKey)
	}
	return slices.Contains(cols, "user") || slices.Contains(cols, "group") || slices.Contains(cols, "all")
}

func resolveGlobalCfg(ctx context.Context, base indexPkg.GlobalCfg, fsPath string) indexPkg.GlobalCfg {
	cfg := base
	if client, err := config.BeeGFSClient(fsPath); (err == nil || errors.Is(err, filesystem.ErrUnmounted)) && client != nil {
		mp := client.GetMountPath()
		cfg = indexPkg.ApplyDotIndexOverrides(cfg, mp)
		// Retain the mount point so the <mount-name> segment is taken from the
		// mount this index was resolved against, not re-guessed (or, for a remote
		// index, probed off the wrong host) later in resolveFSPathToIndex.
		if mp != "" {
			cfg.MountPath = mp
		}
		return finalizeGlobalCfg(ctx, cfg)
	}
	if mountPath := discoverBeeGFSMountPath(); mountPath != "" {
		cfg = indexPkg.ApplyDotIndexOverrides(cfg, mountPath)
		cfg.MountPath = mountPath
	}
	return finalizeGlobalCfg(ctx, cfg)
}

func finalizeGlobalCfg(ctx context.Context, cfg indexPkg.GlobalCfg) indexPkg.GlobalCfg {
	if cfg.Threads <= 0 {
		cfg.Threads = indexPkg.DefaultThreads(ctx, cfg.IndexAddr)
	}
	return cfg
}

func resolveMountPath(fsPath string) string {
	if client, err := config.BeeGFSClient(fsPath); (err == nil || errors.Is(err, filesystem.ErrUnmounted)) && client != nil {
		if mp := client.GetMountPath(); mp != "" {
			return mp
		}
	}
	return discoverBeeGFSMountPath()
}

// resolveRefFileInMount resolves a find reference-file argument (--samefile,
// --newer, --anewer, --cnewer) to an absolute path inside the BeeGFS mount and
// verifies it lies there. These predicates compare the reference file's inode or
// timestamps against index rows, which GUFI fills by stat'ing the indexed tree
// through a BeeGFS mount; a reference outside the mount carries an unrelated
// local inode (and, for a remote index, an unrelated mtime), so it is rejected
// rather than silently matching nothing. BeeGFS inodes are consistent across
// clients, so resolving inside the mount also makes these predicates work
// against a remote index. A relative path is anchored onto the mount, matching
// defaultPathArgsToMount.
func resolveRefFileInMount(refPath string) (string, error) {
	mount := resolveMountPath(refPath)
	if mount == "" {
		return "", fmt.Errorf("no BeeGFS mount found to resolve reference file %q against", refPath)
	}
	abs := refPath
	if !filepath.IsAbs(abs) {
		if cwd, err := os.Getwd(); err == nil {
			abs = filepath.Join(cwd, abs)
		}
	}
	abs = filepath.Clean(abs)
	mountClean := filepath.Clean(mount)
	if abs != mountClean && !strings.HasPrefix(abs, mountClean+string(filepath.Separator)) {
		return "", fmt.Errorf("reference file %q must be inside the BeeGFS mount %q (its metadata is matched against the index)", refPath, mountClean)
	}
	return abs, nil
}

// statRefFileInMount resolves a find reference-file argument inside the BeeGFS
// mount (see resolveRefFileInMount) and Lstats it.
func statRefFileInMount(refPath string) (os.FileInfo, error) {
	resolved, err := resolveRefFileInMount(refPath)
	if err != nil {
		return nil, err
	}
	return os.Lstat(resolved)
}

var errLegacyIndex = errors.New("legacy index format")

func checkIndexExists(cfg indexPkg.GlobalCfg, indexPath string) error {
	if indexPkg.IsRemoteAddr(cfg.IndexAddr) {
		return nil
	}
	dbPath := filepath.Join(indexPath, "db.db")
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			if _, legacyErr := os.Stat(filepath.Join(indexPath, ".bdm.db")); legacyErr == nil {
				return fmt.Errorf("%w: index at %q was created by an older BeeGFS version (.bdm.db). Run 'beegfs index migrate' to convert it in place, or 'beegfs index rescan' to rebuild it from the filesystem", errLegacyIndex, indexPath)
			} else if !os.IsNotExist(legacyErr) {
				return fmt.Errorf("checking legacy index at %q: %w", indexPath, legacyErr)
			}
			return fmt.Errorf("no index found at %q (has 'beegfs index create' been run?)", indexPath)
		}
		return fmt.Errorf("checking index at %q: %w", indexPath, err)
	}
	return nil
}

// resolveFSPathToIndex maps a user-supplied path to its on-disk GUFI index
// directory. Resolution order:
//  1. An absolute path under the index root is resolved directly against it.
//  2. A path inside the BeeGFS mount maps to <index-root>/<mount>/<rel>.
//  3. Otherwise the path is treated as index-root-relative.
//
// In cases 1 and 3 the mount-name segment is inserted when omitted, so all forms
// converge on the same directory.
func resolveFSPathToIndex(cfg indexPkg.GlobalCfg, fsPath string) (string, error) {
	if cfg.IndexRoot == "" {
		return "", indexPkg.ErrIndexRootNotSet
	}
	remote := indexPkg.IsRemoteAddr(cfg.IndexAddr)
	if filepath.IsAbs(fsPath) {
		// Separator-aware containment: rel "foo" is inside, "../x" escapes, but a
		// sibling literally named "..foo" must not be mistaken for an escape.
		if rel, err := filepath.Rel(cfg.IndexRoot, filepath.Clean(fsPath)); err == nil &&
			rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			// No mount name needed: a remote absolute path returns rel as-is, and a
			// local one keeps its prior behavior (os.Stat probe, else indexMountBase
			// reads the local index root — including its multi-mount guard).
			return rebaseUnderIndexRoot(cfg.IndexRoot, fsPath, "", remote)
		}
	}

	beegfsClient, err := config.BeeGFSClient(fsPath)
	if (err == nil || errors.Is(err, filesystem.ErrUnmounted)) && beegfsClient != nil {
		if mp := beegfsClient.GetMountPath(); mp != "" {
			if rel, relErr := beegfsClient.GetRelativePathWithinMount(fsPath); relErr == nil {
				mountBase := filepath.Base(mp)
				return filepath.Join(cfg.IndexRoot, mountBase, rel), nil
			}
		}
	}

	// Not in a BeeGFS mount reachable from the cwd: resolve against the index root.
	// For a local index keep the prior behavior exactly — discover the local mount
	// and let indexMountBase probe the (local) index root, multi-mount guard
	// included. For a remote index, whose root cannot be probed here, prefer the
	// mount learned during config resolution (authoritative for the target).
	mountPath := discoverBeeGFSMountPath()
	if remote && cfg.MountPath != "" {
		mountPath = cfg.MountPath
	}
	return rebaseUnderIndexRoot(cfg.IndexRoot, fsPath, mountPath, remote)
}

var (
	discoverMountOnce sync.Once
	discoveredMount   string
)

// discoverBeeGFSMountPath returns the mount point of a locally mounted BeeGFS
// file system (the first one when several are mounted), or "" if none. Memoized
// so procfs is walked at most once per process.
func discoverBeeGFSMountPath() string {
	discoverMountOnce.Do(func() {
		log, _ := config.GetLogger()
		timeout := viper.GetDuration(config.ConnTimeoutKey)
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		clients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{}, log)
		if err != nil {
			return
		}
		for _, c := range clients {
			if c.Mount.Path != "" {
				discoveredMount = c.Mount.Path
				return
			}
		}
	})
	return discoveredMount
}

// displayPath renders a raw row path (GUFI rpath output, which is rooted at the
// index root and therefore prefixed by indexPath) for presentation. By default
// the path is made relative to the search root, matching GNU find/ls. When
// absolute is set the index-root prefix is replaced with the BeeGFS search path
// (fsPath) so callers get a usable filesystem path rather than the internal
// index location.
func displayPath(rawPath, indexPath, fsPath string, absolute bool) string {
	rel := strings.TrimPrefix(rawPath, indexPath)
	rel = strings.TrimPrefix(rel, "/")
	if absolute {
		if rel == "" {
			return fsPath
		}
		return filepath.Join(fsPath, rel)
	}
	if rel == "" {
		return "."
	}
	return rel
}

// rebaseUnderIndexRoot resolves a path against the index root: a relative path is
// joined onto it, an absolute path under it is rebased. The mount-name segment
// (<mount> in <index-root>/<mount>/...) is inserted unless the path already
// starts with it; mountPath, when set, supplies that name via its basename.
//
// For a remote index the index root lives on another host and cannot be probed
// locally, so the local-filesystem checks are skipped: an absolute path under
// the index root already encodes the mount segment (rel is the full path beneath
// the root, so it is used as-is), and a relative path takes the mount name from
// mountPath, erroring when none is known rather than reading the wrong host.
func rebaseUnderIndexRoot(indexRoot, fsPath, mountPath string, remote bool) (string, error) {
	clean := filepath.Clean(fsPath)
	abs := filepath.IsAbs(clean)
	var rel string
	if abs {
		r, err := filepath.Rel(indexRoot, clean)
		if err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("path %q is not under index root %q", fsPath, indexRoot)
		}
		rel = r
	} else {
		if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("path %q escapes index root %q", fsPath, indexRoot)
		}
		rel = clean
	}
	if rel == "." {
		rel = ""
	}

	if remote {
		// An absolute path under the index root already includes the mount segment.
		if abs {
			return filepath.Join(indexRoot, rel), nil
		}
		if mountPath == "" || mountPath == "/" {
			return "", fmt.Errorf("cannot resolve %q against remote index root %q without a local BeeGFS mount; pass an absolute path under the index root, or a path beginning with the mount name", fsPath, indexRoot)
		}
		// A relative path may already start with the mount name; detect that by
		// name (the local os.Stat check below is unavailable for a remote root) so
		// the segment is not inserted twice.
		mountBase := filepath.Base(mountPath)
		first := rel
		if i := strings.IndexByte(rel, filepath.Separator); i >= 0 {
			first = rel[:i]
		}
		if first == mountBase {
			return filepath.Join(indexRoot, rel), nil
		}
		return filepath.Join(indexRoot, mountBase, rel), nil
	}

	// Local: the first segment may already name the mount subtree under the index
	// root (a local probe the remote path above deliberately avoids).
	if rel != "" {
		first := rel
		if i := strings.IndexByte(rel, filepath.Separator); i >= 0 {
			first = rel[:i]
		}
		if fi, err := os.Stat(filepath.Join(indexRoot, first)); err == nil && fi.IsDir() {
			return filepath.Join(indexRoot, rel), nil
		}
	}

	mountBase, err := indexMountBase(indexRoot, mountPath)
	if err != nil {
		return "", err
	}
	return filepath.Join(indexRoot, mountBase, rel), nil
}

// indexMountBase returns the mount-name subdirectory under the index root (layout
// is <index-root>/<mount-name>/...): the basename of mountPath when set,
// otherwise the single subdirectory present under the index root.
func indexMountBase(indexRoot, mountPath string) (string, error) {
	if mountPath != "" && mountPath != "/" {
		return filepath.Base(mountPath), nil
	}

	entries, err := os.ReadDir(indexRoot)
	if err != nil {
		return "", fmt.Errorf("reading index root %q to determine the mount name: %w", indexRoot, err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	switch len(dirs) {
	case 0:
		return "", fmt.Errorf("index root %q has no indexed mount; run 'beegfs index create' first", indexRoot)
	case 1:
		return dirs[0], nil
	default:
		return "", fmt.Errorf("index root %q contains multiple mounts %v; include the mount name in the path", indexRoot, dirs)
	}
}

func primeFSPath(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return "."
}

func resolveCfgAndPaths(ctx context.Context, base *indexPkg.GlobalCfg, args []string) (indexPkg.GlobalCfg, []string) {
	cfg := resolveGlobalCfg(ctx, *base, primeFSPath(args))
	if indexPkg.IsRemoteAddr(cfg.IndexAddr) {
		return cfg, args
	}
	anchored := defaultPathArgsToMount(args)
	if slices.Equal(anchored, args) {
		return cfg, args
	}
	return resolveGlobalCfg(ctx, *base, primeFSPath(anchored)), anchored
}

// defaultPathArgsToMount normalizes index path arguments against the BeeGFS
// mount root: a missing path defaults to the mount root, and a relative path
// that resolves outside the mount is re-anchored onto it. Both forms otherwise
// resolve against the cwd, which fails when the cwd is outside the mount since no
// .beegfs.index entry covers it (ErrIndexRootNotSet). A relative path already
// inside the mount, and any absolute path, are left unchanged. It is only invoked
// for a local index (see resolveCfgAndPaths).
func defaultPathArgsToMount(args []string) []string {
	// All-absolute args need no normalization; skip the procfs mount discovery.
	skip := len(args) > 0
	for _, a := range args {
		if !filepath.IsAbs(a) {
			skip = false
			break
		}
	}
	if skip {
		return args
	}
	cwd, _ := os.Getwd()
	return defaultPathArgs(args, discoverBeeGFSMountPath(), cwd)
}

// defaultPathArgs is the pure core of defaultPathArgsToMount, taking mountPath
// and cwd explicitly so the rule is unit testable without procfs or os.Getwd.
// With no mount (mountPath == "") args are returned unchanged.
func defaultPathArgs(args []string, mountPath, cwd string) []string {
	if mountPath == "" {
		return args
	}
	if len(args) == 0 {
		return []string{mountPath}
	}
	mountClean := filepath.Clean(mountPath)
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a
		if filepath.IsAbs(a) {
			continue
		}
		abs := filepath.Join(cwd, a)
		if abs == mountClean || strings.HasPrefix(abs, mountClean+string(filepath.Separator)) {
			continue
		}
		out[i] = filepath.Join(mountClean, a)
	}
	return out
}

func resolveAndCheckIndexPath(cfg indexPkg.GlobalCfg, args []string) (string, error) {
	fsPath := "."
	if len(args) > 0 {
		fsPath = args[0]
	}

	indexPath, err := resolveFSPathToIndex(cfg, fsPath)
	if err != nil {
		return "", err
	}
	if err := checkIndexExists(cfg, indexPath); err != nil {
		return "", err
	}
	return indexPath, nil
}

type resolvedPath struct {
	fsPath    string // BeeGFS search path: display root and find's searchRoot
	indexPath string // index-root prefix stripped when rendering paths / for path predicates
	walkRoot  string // path handed to gufi_query; == indexPath unless a remote glob
	glob      bool   // walkRoot carries a shell glob the remote host expands; metachars left unescaped
}

func resolveIndexPaths(ctx context.Context, cfg indexPkg.GlobalCfg, args []string) ([]resolvedPath, error) {
	if indexPkg.IsRemoteAddr(cfg.IndexAddr) {
		return resolveRemoteIndexPaths(cfg, args)
	}

	if len(args) == 0 {
		idx, err := resolveFSPathToIndex(cfg, ".")
		if err != nil {
			return nil, err
		}
		return []resolvedPath{{fsPath: ".", indexPath: idx, walkRoot: idx}}, nil
	}
	resolveLiteral := func(arg string) (resolvedPath, error) {
		idx, err := resolveFSPathToIndex(cfg, arg)
		if err != nil {
			return resolvedPath{}, err
		}
		return resolvedPath{fsPath: arg, indexPath: idx, walkRoot: idx}, nil
	}
	var result []resolvedPath
	for _, arg := range args {
		// Literal-first: if the path exists on disk treat glob meta-characters as
		// literal bytes; only expand globs when the literal target is absent.
		if _, err := os.Lstat(arg); err != nil && filesystem.IsGlobPattern(arg) {
			expanded, err := expandIndexGlob(ctx, cfg, arg)
			if err != nil {
				return nil, err
			}
			result = append(result, expanded...)
			continue
		}
		rp, err := resolveLiteral(arg)
		if err != nil {
			return nil, err
		}
		result = append(result, rp)
	}
	return result, nil
}

// resolveRemoteIndexPaths resolves path arguments for a remote index. Glob
// expansion cannot run locally, so a single quoted glob is forwarded verbatim to
// the remote executor, which lets the remote login shell expand it against the
// remote index tree (see RemoteExecutor / escapeRemoteShellArgGlob).
//
// A quoted glob is exactly one argument. A glob appearing alongside other
// arguments therefore means the local shell expanded one (or the user mixed a
// glob with literals), which cannot be forwarded unambiguously — that case is
// rejected with guidance to quote the pattern. Multiple plain literal paths are
// resolved per-argument, so e.g. `stat file1 file2` keeps working.
func resolveRemoteIndexPaths(cfg indexPkg.GlobalCfg, args []string) ([]resolvedPath, error) {
	if len(args) == 0 {
		idx, err := resolveFSPathToIndex(cfg, ".")
		if err != nil {
			return nil, err
		}
		return []resolvedPath{{fsPath: ".", indexPath: idx, walkRoot: idx}}, nil
	}

	globs := 0
	for _, a := range args {
		if filesystem.IsGlobPattern(a) {
			globs++
		}
	}
	if globs > 0 && len(args) > 1 {
		return nil, fmt.Errorf("a remote index (--index-addr ssh:<host>) expands a single quoted glob, but got %d arguments with a glob among them; pass one quoted glob (e.g. \"data/*\") — if you left a pattern unquoted, the shell expanded it locally", len(args))
	}

	if globs == 0 {
		// One or more literal paths: resolve each as-is.
		out := make([]resolvedPath, 0, len(args))
		for _, a := range args {
			idx, err := resolveFSPathToIndex(cfg, a)
			if err != nil {
				return nil, err
			}
			out = append(out, resolvedPath{fsPath: a, indexPath: idx, walkRoot: idx})
		}
		return out, nil
	}

	// Exactly one argument, and it is a glob expanded by the remote login shell.
	// Reject '**': the remote shell is POSIX sh (no globstar), and even with it the
	// overlapping ancestor/descendant roots would double-count — the case the local
	// walk handles via pruneNestedDirs but which has no remote equivalent here.
	arg := args[0]
	if strings.Contains(arg, "**") {
		return nil, fmt.Errorf("recursive ** globs are not supported with a remote index; use a single-level glob (e.g. \"data/*\") or an explicit path: %q", arg)
	}
	walkDirFS, tail, indexAbs, err := globBase(cfg, arg)
	if err != nil {
		return nil, err
	}
	// Trailing '/' restricts the remote shell's match to directories, mirroring
	// the local dir-only GlobMatchDirs: the index tree stores files as DB rows,
	// so only directories are real entries, but each index directory also holds a
	// db.db file that a bare '*' would otherwise match (gufi_query warns on a
	// non-directory root). gufi_query normalizes the trailing slash, so rpath
	// output — and thus the displayRoot trimming below — is unaffected.
	return []resolvedPath{{
		fsPath:    walkDirFS,
		indexPath: indexAbs,
		walkRoot:  filepath.Join(indexAbs, tail) + "/",
		glob:      true,
	}}, nil
}

// globBase splits a glob argument into the literal base directory it walks (in
// BeeGFS path terms), the glob tail relative to that base, and the index path of
// that base. It is shared by local expansion (expandIndexGlob) and remote
// forwarding (resolveRemoteIndexPaths) so both agree on where a glob is rooted.
func globBase(cfg indexPkg.GlobalCfg, arg string) (walkDirFS, tail, indexAbs string, err error) {
	cleaned := filepath.Clean(arg)
	stripped := filesystem.StripGlobPattern(cleaned)

	walkDirFS = stripped
	if !strings.HasSuffix(walkDirFS, string(filepath.Separator)) {
		walkDirFS = filepath.Dir(walkDirFS)
	}
	walkDirFS = filepath.Clean(walkDirFS)

	tail, err = filepath.Rel(walkDirFS, cleaned)
	if err != nil {
		return "", "", "", fmt.Errorf("computing glob tail for %q: %w", arg, err)
	}

	indexAbs, err = resolveFSPathToIndex(cfg, walkDirFS)
	if err != nil {
		return "", "", "", fmt.Errorf("resolving glob base %q: %w", walkDirFS, err)
	}
	return walkDirFS, tail, indexAbs, nil
}

// expandIndexGlob expands a glob against a local index tree, returning one search
// root per matched directory. It is only reached for a local index;
// resolveIndexPaths routes a remote index through resolveRemoteIndexPaths before
// this point.
func expandIndexGlob(ctx context.Context, cfg indexPkg.GlobalCfg, arg string) ([]resolvedPath, error) {
	walkDirFS, tail, indexAbs, err := globBase(cfg, arg)
	if err != nil {
		return nil, err
	}

	// Expand the glob against the index tree exactly as a shell would: match the
	// pattern itself (no implicit recursion into matched subtrees), so each match
	// becomes one search root for the command rather than emitting whole
	// subtrees. Only directories are index nodes; file-name globs (e.g. '*.log')
	// match nothing here because the index FS stores files as DB rows, not
	// entries — stat/find a file by its literal path instead.
	matches, err := filesystem.GlobMatchDirs(ctx, indexAbs, tail)
	if err != nil {
		return nil, fmt.Errorf("expanding glob %q: %w", arg, err)
	}
	// A '**' token expands to a matched directory AND every descendant directory.
	// Each match becomes its own search root, so without pruning the descendants a
	// recursive find re-walks the subtree and emits its entries once per overlapping
	// root. Keep only the topmost matches; their walks already cover the rest.
	matches = pruneNestedDirs(matches)

	out := make([]resolvedPath, 0, len(matches))
	for _, rel := range matches {
		idx := filepath.Join(indexAbs, rel)
		out = append(out, resolvedPath{
			fsPath:    filepath.Join(walkDirFS, rel),
			indexPath: idx,
			walkRoot:  idx,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no index entries match %q", arg)
	}
	return out, nil
}

// pruneNestedDirs removes any path that is a descendant of another path in the
// set, keeping only the topmost matches. Paths are GlobMatchDirs output:
// slash-separated and lexicographically sorted, so an ancestor always precedes
// its descendants (a shorter prefix sorts first). A *sibling* can still sort
// between an ancestor and its descendant, though — bytes like '+' (0x2B), '-'
// (0x2D) and '.' (0x2E) sort before '/' (0x2F), so "data-old" lands between
// "data" and "data/sub". Comparing only against the last kept path therefore
// misses such a descendant ("data/sub" survives behind "data-old") and
// re-introduces the duplicate-output a '**' glob is meant to avoid. Each path is
// instead checked against every kept root; the kept set stays small because only
// topmost matches are retained, the rest being pruned as descendants.
func pruneNestedDirs(paths []string) []string {
	for _, p := range paths {
		if p == "." {
			return []string{"."}
		}
	}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		nested := false
		for _, root := range out {
			if p == root || strings.HasPrefix(p, root+"/") {
				nested = true
				break
			}
		}
		if !nested {
			out = append(out, p)
		}
	}
	return out
}
