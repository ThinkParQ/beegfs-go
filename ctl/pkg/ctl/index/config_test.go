package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeDotIndex(t *testing.T, dir, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, DotIndexFileName), []byte(content), 0o644))
}

func TestApplyDotIndexOverrides(t *testing.T) {
	const dotIndex = `
[index]
path = "/mnt/beegfs"
root = "/idx/default"
addr = "local"
`

	tests := []struct {
		name string
		cfg  GlobalCfg
		want GlobalCfg
	}{
		{
			name: "dot file fills root and addr when unset",
			want: GlobalCfg{IndexRoot: "/idx/default", IndexAddr: "local"},
		},
		{
			name: "explicit flags win over dot file",
			cfg:  GlobalCfg{IndexRoot: "/flag/root", IndexAddr: "ssh:flaghost"},
			want: GlobalCfg{IndexRoot: "/flag/root", IndexAddr: "ssh:flaghost"},
		},
		{
			name: "dot file fills only unset fields",
			cfg:  GlobalCfg{IndexAddr: "local"},
			want: GlobalCfg{IndexRoot: "/idx/default", IndexAddr: "local"},
		},
		{
			name: "threads pass through untouched",
			cfg:  GlobalCfg{Threads: 7},
			want: GlobalCfg{IndexRoot: "/idx/default", IndexAddr: "local", Threads: 7},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mount := t.TempDir()
			writeDotIndex(t, mount, dotIndex)
			got := ApplyDotIndexOverrides(tc.cfg, mount)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("missing file leaves cfg unchanged", func(t *testing.T) {
		t.Parallel()
		cfg := GlobalCfg{IndexAddr: "local", Threads: 3}
		got := ApplyDotIndexOverrides(cfg, t.TempDir())
		assert.Equal(t, cfg, got)
	})

	t.Run("malformed file leaves cfg unchanged", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, "not [valid toml")
		cfg := GlobalCfg{IndexRoot: "/flag/root"}
		got := ApplyDotIndexOverrides(cfg, mount)
		assert.Equal(t, cfg, got)
	})

	t.Run("threads taken from the entry when unset", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, "[index]\npath = \"/mnt/beegfs\"\nroot = \"/idx\"\naddr = \"ssh:host\"\nthreads = 16\n")
		got := ApplyDotIndexOverrides(GlobalCfg{}, mount)
		assert.Equal(t, GlobalCfg{IndexRoot: "/idx", IndexAddr: "ssh:host", Threads: 16}, got)
	})

	t.Run("explicit threads win over the dot file", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, "[index]\npath = \"/mnt/beegfs\"\nroot = \"/idx\"\naddr = \"ssh:host\"\nthreads = 16\n")
		got := ApplyDotIndexOverrides(GlobalCfg{Threads: 4}, mount)
		assert.Equal(t, GlobalCfg{IndexRoot: "/idx", IndexAddr: "ssh:host", Threads: 4}, got)
	})
}

func TestDefaultThreads(t *testing.T) {
	t.Parallel()
	// A remote index is never probed over ssh; it defaults to a single worker
	// (raise it via the .beegfs.index "threads" entry).
	assert.Equal(t, 1, DefaultThreads(context.Background(), "ssh:host"))
	// Local falls back to a positive worker count (num-workers, else CPU count).
	assert.Positive(t, DefaultThreads(context.Background(), "local"))
	assert.Positive(t, DefaultThreads(context.Background(), ""))
}

func TestDotIndexPath(t *testing.T) {
	t.Run("returns the entry path", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, `
[index]
path = "/mnt/beegfs"
root = "/idx/default"
`)
		assert.Equal(t, "/mnt/beegfs", DotIndexPath(mount))
	})

	t.Run("empty mount path", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", DotIndexPath(""))
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", DotIndexPath(t.TempDir()))
	})

	t.Run("malformed file", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, "not [valid toml")
		assert.Equal(t, "", DotIndexPath(mount))
	})

	t.Run("no entry configured", func(t *testing.T) {
		t.Parallel()
		mount := t.TempDir()
		writeDotIndex(t, mount, "# only comments, no entry\n")
		assert.Equal(t, "", DotIndexPath(mount))
	})
}
