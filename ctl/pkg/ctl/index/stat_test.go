package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statSpecRecorder captures the QuerySpec passed to Execute so tests can assert
// on branch selection without running gufi_query.
type statSpecRecorder struct {
	called bool
	spec   QuerySpec
}

func (r *statSpecRecorder) Execute(_ context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	r.called = true
	r.spec = spec
	ch := make(chan []string)
	close(ch)
	return ch, func() error { return nil }, nil
}

// newLocalIndexDir creates a temp directory containing a db.db file, i.e. a
// local index directory.
func newLocalIndexDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "db.db"), nil, 0o644))
	return dir
}

func TestIsIndexDir(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T) string
		want  bool
	}{
		{name: "db.db present", setup: newLocalIndexDir, want: true},
		{name: "db.db absent", setup: func(t *testing.T) string { return t.TempDir() }, want: false},
		{
			name: "db.db is a directory",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				require.NoError(t, os.Mkdir(filepath.Join(dir, "db.db"), 0o755))
				return dir
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsIndexDir(tc.setup(t)))
		})
	}
}

func TestStatIndexDir(t *testing.T) {
	t.Parallel()
	t.Run("index dir resolves to itself", func(t *testing.T) {
		t.Parallel()
		dir := newLocalIndexDir(t)
		assert.Equal(t, dir, StatIndexDir(dir))
	})
	t.Run("plain path resolves to its parent", func(t *testing.T) {
		t.Parallel()
		p := filepath.Join(t.TempDir(), "README.md")
		assert.Equal(t, filepath.Dir(p), StatIndexDir(p))
	})
}

// TestStat_LocalBranchSelection: a local stat uses a plain os.Stat to pick the
// dir (summary) query for an index directory and the file (entries) query for
// anything else.
func TestStat_LocalBranchSelection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(t *testing.T) string
		beegfs  bool
		wantDir bool
	}{
		{name: "index dir routes to dir branch", setup: newLocalIndexDir, wantDir: true},
		{name: "index dir with beegfs uses the summary beegfs template", setup: newLocalIndexDir, beegfs: true, wantDir: true},
		{name: "plain path routes to file branch", setup: func(t *testing.T) string {
			return filepath.Join(t.TempDir(), "README.md")
		}, wantDir: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			entryPath := tc.setup(t)
			rec := &statSpecRecorder{}

			rows, errWait, err := Stat(context.Background(), rec, StatCfg{BeeGFS: tc.beegfs}, entryPath, false)
			require.NoError(t, err)
			for range rows {
			}
			require.NoError(t, errWait())
			require.True(t, rec.called)

			if tc.wantDir {
				wantTmpl := StatDirS
				if tc.beegfs {
					wantTmpl = StatBeeGFSDirS
				}
				assert.Equal(t, entryPath, rec.spec.IndexRoot, "dir branch queries the directory's own index")
				assert.Equal(t, wantTmpl, rec.spec.SQLSummary)
				assert.Empty(t, rec.spec.SQLEntries)
			} else {
				assert.Equal(t, filepath.Dir(entryPath), rec.spec.IndexRoot, "file branch queries the parent index")
				assert.Contains(t, rec.spec.SQLEntries, "name = '"+filepath.Base(entryPath)+"'")
				assert.Empty(t, rec.spec.SQLSummary)
			}
		})
	}
}

// fakeStatExecutor returns canned rows depending on whether it was handed the
// dir (summary) or file (entries) stat query, recording every spec it ran.
type fakeStatExecutor struct {
	dirRows    [][]string
	fileRows   [][]string
	dirWaitErr error // when set, the dir (summary) query's wait() reports this
	specs      []QuerySpec
}

func (f *fakeStatExecutor) Execute(_ context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	f.specs = append(f.specs, spec)
	isDir := spec.SQLSummary != ""
	data := f.fileRows
	if isDir {
		data = f.dirRows
	}
	ch := make(chan []string, len(data))
	for _, r := range data {
		ch <- r
	}
	close(ch)
	waitErr := error(nil)
	if isDir {
		waitErr = f.dirWaitErr
	}
	return ch, func() error { return waitErr }, nil
}

func collectRows(rows <-chan []string) [][]string {
	var out [][]string
	for r := range rows {
		out = append(out, r)
	}
	return out
}

// TestStat_RemoteDetection: a remote stat must resolve dir-vs-file through
// gufi_vt (no direct ssh) — the directory summary query first, falling back to
// the parent's entries query only when the summary yields no row.
func TestStat_RemoteDetection(t *testing.T) {
	t.Parallel()
	cfg := StatCfg{GlobalCfg: GlobalCfg{IndexAddr: "ssh:host"}}

	t.Run("directory resolved by the summary query alone", func(t *testing.T) {
		t.Parallel()
		ex := &fakeStatExecutor{dirRows: [][]string{{"data", "d"}}}
		rows, errWait, err := Stat(context.Background(), ex, cfg, "/idx/mnt/data", false)
		require.NoError(t, err)
		got := collectRows(rows)
		require.NoError(t, errWait())
		assert.Equal(t, [][]string{{"data", "d"}}, got)
		require.Len(t, ex.specs, 1, "a directory must not trigger the file fallback")
		assert.Equal(t, "/idx/mnt/data", ex.specs[0].IndexRoot)
		assert.NotEmpty(t, ex.specs[0].SQLSummary)
	})

	t.Run("file falls back to the parent entries query", func(t *testing.T) {
		t.Parallel()
		ex := &fakeStatExecutor{fileRows: [][]string{{"README.md", "f"}}}
		rows, errWait, err := Stat(context.Background(), ex, cfg, "/idx/mnt/data/README.md", false)
		require.NoError(t, err)
		got := collectRows(rows)
		require.NoError(t, errWait())
		assert.Equal(t, [][]string{{"README.md", "f"}}, got)
		require.Len(t, ex.specs, 2, "summary query first, then the entries fallback")
		assert.NotEmpty(t, ex.specs[0].SQLSummary)
		assert.Equal(t, "/idx/mnt/data", ex.specs[1].IndexRoot)
		assert.Contains(t, ex.specs[1].SQLEntries, "name = 'README.md'")
	})

	t.Run("a dir-query failure is surfaced, not masked as not-found", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("ssh: connection refused")
		ex := &fakeStatExecutor{dirWaitErr: wantErr}
		_, _, err := Stat(context.Background(), ex, cfg, "/idx/mnt/data", false)
		require.ErrorIs(t, err, wantErr, "a real failure must surface, not fall through")
		require.Len(t, ex.specs, 1, "must not run the file fallback after a dir-query error")
	})
}
