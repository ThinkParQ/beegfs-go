package index

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

// newIndexRoot creates a temp index root containing the given mount-name
// subdirectories (the <index-root>/<mount>/... layout).
func newIndexRoot(t *testing.T, mounts ...string) string {
	t.Helper()
	root := t.TempDir()
	for _, m := range mounts {
		require.NoError(t, os.MkdirAll(filepath.Join(root, m), 0o755))
	}
	return root
}

func TestResolveFSPathToIndexRequiresIndexRoot(t *testing.T) {
	// With no index root from a flag or .beegfs.index there is no default to
	// fall back on, so resolution fails up front rather than fabricating a path.
	_, err := resolveFSPathToIndex(indexPkg.GlobalCfg{}, "/mnt/beegfs/data")
	require.ErrorIs(t, err, indexPkg.ErrIndexRootNotSet)
}

func TestRebaseUnderIndexRoot(t *testing.T) {
	// These cases all run outside a BeeGFS mount, so indexMountBase falls back to
	// the single subdirectory under the index root.
	t.Run("inserts and rebases", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs")
		tests := []struct {
			name  string
			input string
			want  string
		}{
			{"relative inserts mount name", "prefix", filepath.Join(root, "beegfs", "prefix")},
			{"relative nested", "prefix/sub", filepath.Join(root, "beegfs", "prefix", "sub")},
			{"relative already has mount name", "beegfs/prefix", filepath.Join(root, "beegfs", "prefix")},
			{"absolute under root inserts mount name", filepath.Join(root, "prefix"), filepath.Join(root, "beegfs", "prefix")},
			{"absolute under root with mount name", filepath.Join(root, "beegfs", "prefix"), filepath.Join(root, "beegfs", "prefix")},
			{"empty relative maps to mount root", "", filepath.Join(root, "beegfs")},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := rebaseUnderIndexRoot(root, tc.input, "", false)
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("absolute outside root errors", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs")
		_, err := rebaseUnderIndexRoot(root, "/somewhere/else/prefix", "", false)
		require.Error(t, err)
	})

	t.Run("relative escaping root errors", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs")
		tests := []struct {
			name  string
			input string
		}{
			{"bare dotdot", ".."},
			{"dotdot path", "../../etc"},
			{"cleans to dotdot", "beegfs/../.."},
			{"dotdot then descend", "../sibling/prefix"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := rebaseUnderIndexRoot(root, tc.input, "", false)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "escapes index root")
			})
		}
	})

	t.Run("interior dotdot that stays under root is allowed", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs")
		got, err := rebaseUnderIndexRoot(root, "beegfs/sub/../prefix", "", false)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(root, "beegfs", "prefix"), got)
	})

	t.Run("discovered mount name wins", func(t *testing.T) {
		root := newIndexRoot(t, "other")
		got, err := rebaseUnderIndexRoot(root, "prefix", "/mnt/beegfs", false)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(root, "beegfs", "prefix"), got)
	})
}

func TestRebaseUnderIndexRootRemote(t *testing.T) {
	// A remote index root that does NOT exist on the local host: for a remote
	// index no local filesystem probe may happen, so resolution must still work
	// off a root that is absent here.
	const root = "/no/such/local/index-root"

	t.Run("absolute under root uses rel as-is (no mount-segment doubling)", func(t *testing.T) {
		got, err := rebaseUnderIndexRoot(root, root+"/beegfs/sanity/d_10", "", true)
		require.NoError(t, err)
		assert.Equal(t, root+"/beegfs/sanity/d_10", got)
	})

	t.Run("relative inserts the mount name from mountPath", func(t *testing.T) {
		got, err := rebaseUnderIndexRoot(root, "sanity/d_10", "/mnt/beegfs", true)
		require.NoError(t, err)
		assert.Equal(t, root+"/beegfs/sanity/d_10", got)
	})

	t.Run("relative already prefixed with the mount name is not doubled", func(t *testing.T) {
		got, err := rebaseUnderIndexRoot(root, "beegfs/sanity/d_10", "/mnt/beegfs", true)
		require.NoError(t, err)
		assert.Equal(t, root+"/beegfs/sanity/d_10", got)
	})

	t.Run("dot maps to the mount root", func(t *testing.T) {
		got, err := rebaseUnderIndexRoot(root, ".", "/mnt/beegfs", true)
		require.NoError(t, err)
		assert.Equal(t, root+"/beegfs", got)
	})

	t.Run("relative with no known mount errors actionably", func(t *testing.T) {
		_, err := rebaseUnderIndexRoot(root, "sanity/d_10", "", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "without a local BeeGFS mount")
	})
}

// TestResolveFSPathToIndexLocalMultiMountAbs locks in that a LOCAL index keeps the
// multi-mount disambiguation guard for an absolute index-root path that omits the
// mount segment: cfg.MountPath (now set whenever mounted) must not silently pick a
// mount for a local index, only for a remote one.
func TestResolveFSPathToIndexLocalMultiMountAbs(t *testing.T) {
	root := newIndexRoot(t, "beegfs", "scratch")
	cfg := indexPkg.GlobalCfg{IndexRoot: root, MountPath: "/mnt/beegfs"} // local index; MountPath set
	_, err := resolveFSPathToIndex(cfg, filepath.Join(root, "prefix"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple mounts")
}

func TestCheckIndexExists(t *testing.T) {
	tests := []struct {
		name      string
		files     []string
		indexAddr string
		wantErr   string
		isLegacy  bool
	}{
		{"new format present", []string{"db.db"}, "", "", false},
		{"missing index", nil, "", "no index found", false},
		{"legacy index only", []string{".bdm.db"}, "", "older BeeGFS version", true},
		{"both present uses new", []string{"db.db", ".bdm.db"}, "", "", false},
		{"remote skips local stat", nil, "ssh:somehost", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, f), nil, 0o644))
			}
			err := checkIndexExists(indexPkg.GlobalCfg{IndexAddr: tc.indexAddr}, dir)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, tc.isLegacy, errors.Is(err, errLegacyIndex))
		})
	}
}

func TestCheckIndexExists_LegacyStatError(t *testing.T) {
	// db.db is absent (ENOENT), but the legacy .bdm.db stat fails for a reason
	// other than "not found". A self-referential symlink makes os.Stat return
	// ELOOP; that non-ENOENT error must surface rather than be reported as a
	// plain missing index.
	dir := t.TempDir()
	loop := filepath.Join(dir, ".bdm.db")
	require.NoError(t, os.Symlink(loop, loop))

	err := checkIndexExists(indexPkg.GlobalCfg{}, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checking legacy index")
	assert.False(t, errors.Is(err, errLegacyIndex))
}

func TestResolveRemoteIndexPaths(t *testing.T) {
	root := newIndexRoot(t, "beegfs")
	cfg := indexPkg.GlobalCfg{IndexAddr: "ssh:somehost", IndexRoot: root}

	t.Run("multiple literal paths resolve per-argument", func(t *testing.T) {
		base := filepath.Join(root, "beegfs")
		got, err := resolveRemoteIndexPaths(cfg, []string{base, filepath.Join(base, "sub")})
		require.NoError(t, err)
		require.Len(t, got, 2, "multiple literals must each resolve (e.g. stat file1 file2)")
		for _, rp := range got {
			assert.False(t, rp.glob)
			assert.Equal(t, rp.indexPath, rp.walkRoot)
		}
	})

	t.Run("glob among multiple args rejected with quote guidance", func(t *testing.T) {
		base := filepath.Join(root, "beegfs")
		_, err := resolveRemoteIndexPaths(cfg, []string{filepath.Join(base, "a"), filepath.Join(base, "*")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "quote")
	})

	t.Run("recursive ** rejected", func(t *testing.T) {
		_, err := resolveRemoteIndexPaths(cfg, []string{filepath.Join(root, "beegfs", "**", "x")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "recursive **")
	})

	t.Run("single glob forwarded with trailing-slash walk root", func(t *testing.T) {
		base := filepath.Join(root, "beegfs")
		got, err := resolveRemoteIndexPaths(cfg, []string{filepath.Join(base, "*")})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.True(t, got[0].glob, "a glob must be flagged for remote expansion")
		assert.Equal(t, base, got[0].indexPath, "displayRoot is the literal glob base")
		assert.Equal(t, filepath.Join(base, "*")+"/", got[0].walkRoot,
			"walk root carries the glob and a trailing slash for dir-only matching")
	})

	t.Run("literal single path is not flagged a glob", func(t *testing.T) {
		base := filepath.Join(root, "beegfs")
		got, err := resolveRemoteIndexPaths(cfg, []string{base})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.False(t, got[0].glob)
		assert.Equal(t, got[0].indexPath, got[0].walkRoot, "literal walk root equals the display root")
	})
}

// TestResolveRemoteIndexPathsNoLocalRoot exercises the end-to-end remote path
// resolution against an index root absent from the local host, with the mount
// learned during config resolution (cfg.MountPath). It must resolve by string
// math without touching the local filesystem.
func TestResolveRemoteIndexPathsNoLocalRoot(t *testing.T) {
	cfg := indexPkg.GlobalCfg{
		IndexAddr: "ssh:host",
		IndexRoot: "/no/such/local/index-root",
		MountPath: "/mnt/beegfs",
	}

	t.Run("relative glob resolves via cfg.MountPath", func(t *testing.T) {
		got, err := resolveRemoteIndexPaths(cfg, []string{"sanity/d_10/*"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.True(t, got[0].glob)
		assert.Equal(t, "/no/such/local/index-root/beegfs/sanity/d_10", got[0].indexPath)
		assert.Equal(t, "/no/such/local/index-root/beegfs/sanity/d_10/*/", got[0].walkRoot)
	})

	t.Run("absolute index-root path resolves without doubling", func(t *testing.T) {
		got, err := resolveRemoteIndexPaths(cfg, []string{"/no/such/local/index-root/beegfs/sanity/d_10"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.False(t, got[0].glob)
		assert.Equal(t, "/no/such/local/index-root/beegfs/sanity/d_10", got[0].walkRoot)
	})
}

func TestIndexMountBase(t *testing.T) {
	t.Run("discovered mount path wins", func(t *testing.T) {
		root := newIndexRoot(t, "other")
		got, err := indexMountBase(root, "/mnt/beegfs")
		require.NoError(t, err)
		assert.Equal(t, "beegfs", got)
	})

	t.Run("single subdirectory", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs")
		got, err := indexMountBase(root, "")
		require.NoError(t, err)
		assert.Equal(t, "beegfs", got)
	})

	t.Run("no mount errors", func(t *testing.T) {
		root := newIndexRoot(t)
		_, err := indexMountBase(root, "")
		require.Error(t, err)
	})

	t.Run("multiple mounts require explicit name", func(t *testing.T) {
		root := newIndexRoot(t, "beegfs", "other")
		_, err := indexMountBase(root, "")
		require.Error(t, err)
	})
}

func TestDefaultPathArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		args      []string
		mountPath string
		cwd       string
		want      []string
	}{
		{"no args, mount detected defaults to mount root", nil, "/mnt/beegfs", "/home/u", []string{"/mnt/beegfs"}},
		{"no args, no mount left unchanged for cwd fallback", nil, "", "/home/u", nil},
		{"absolute path never overridden", []string{"/mnt/beegfs/sub"}, "/mnt/beegfs", "/home/u", []string{"/mnt/beegfs/sub"}},
		{"absolute path outside mount left unchanged", []string{"/etc/passwd"}, "/mnt/beegfs", "/home/u", []string{"/etc/passwd"}},
		{"any path with no mount left unchanged", []string{"sub"}, "", "/home/u", []string{"sub"}},
		{"relative path, cwd outside mount re-anchored to mount root", []string{"sanity"}, "/mnt/beegfs", "/home/u", []string{"/mnt/beegfs/sanity"}},
		{"relative path with trailing slash re-anchored", []string{"sanity/"}, "/mnt/beegfs", "/home/u", []string{"/mnt/beegfs/sanity"}},
		{"relative glob re-anchored preserving meta", []string{"sanity/*"}, "/mnt/beegfs", "/home/u", []string{"/mnt/beegfs/sanity/*"}},
		{"relative path, cwd inside mount kept cwd-relative", []string{"bar"}, "/mnt/beegfs", "/mnt/beegfs/foo", []string{"bar"}},
		{"relative path, cwd is mount root kept", []string{"bar"}, "/mnt/beegfs", "/mnt/beegfs", []string{"bar"}},
		{"mixed args, absolute kept and relative re-anchored", []string{"/etc/x", "sanity", "data/file"}, "/mnt/beegfs", "/home/u", []string{"/etc/x", "/mnt/beegfs/sanity", "/mnt/beegfs/data/file"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, defaultPathArgs(tc.args, tc.mountPath, tc.cwd))
		})
	}
}

func TestPruneNestedDirs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			// A '**' match yields a dir plus its descendants; keep only the top.
			name: "drops descendants of a kept match",
			in:   []string{"proj", "proj/sub1", "proj/sub2", "proj/sub1/deep"},
			want: []string{"proj"},
		},
		{
			name: "keeps siblings and prefix-only lookalikes",
			in:   []string{"a", "a/b", "ab", "ab/c", "b"},
			want: []string{"a", "ab", "b"},
		},
		{
			// Regression: a sibling whose name extends the ancestor with a byte
			// below '/' (here '-' in "data-old") sorts between the ancestor and its
			// descendant, so a last-kept-only check leaves "data/sub" as a second,
			// nested search root and the '**' subtree is emitted twice.
			name: "prunes descendant when a sibling sorts between ancestor and descendant",
			in:   []string{"data", "data-old", "data/sub"},
			want: []string{"data", "data-old"},
		},
		{
			// Regression: a "**" tail matches the walk root itself, reported as
			// "." with descendants as bare names ("a", not "./a"). "." subsumes
			// the whole tree, so it must collapse to the sole root even when a
			// sibling (here "-old", 0x2D) sorts before "." (0x2E).
			name: "collapses to root when '.' matches (a '**' tail)",
			in:   []string{"-old", "-old/x", ".", "a", "a/b", "c"},
			want: []string{"."},
		},
		{
			name: "no nesting keeps everything",
			in:   []string{"other", "proj-2024", "proj-2025"},
			want: []string{"other", "proj-2024", "proj-2025"},
		},
		{
			name: "empty input",
			in:   nil,
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := pruneNestedDirs(append([]string(nil), tc.in...))
			if len(got) == 0 {
				got = nil
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
