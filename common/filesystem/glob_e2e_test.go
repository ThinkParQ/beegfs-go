package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func simulateExpandIndexGlob(ctx context.Context, indexBase string, fsArg string) ([]string, error) {
	cleaned := filepath.Clean(fsArg)
	stripped := StripGlobPattern(cleaned)

	walkDirFS := stripped
	if !strings.HasSuffix(walkDirFS, string(filepath.Separator)) {
		walkDirFS = filepath.Dir(walkDirFS)
	}
	walkDirFS = filepath.Clean(walkDirFS)

	tail, err := filepath.Rel(walkDirFS, cleaned)
	if err != nil {
		return nil, err
	}

	// Mirror the production expandIndexGlob: match the pattern itself via
	// GlobMatchDirs (no implicit "/**" descent into matched subtrees).
	matches, err := GlobMatchDirs(ctx, walkDirFS, tail)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, m := range matches {
		out = append(out, filepath.Join(walkDirFS, m))
	}
	return out, nil
}

func TestGlobExpansionEndToEnd(t *testing.T) {
	root := t.TempDir()
	dirs := []string{
		"proj-2024",
		"proj-2024/sub1",
		"proj-2024/sub2",
		"proj-2025",
		"proj-2025/sub1",
		"proj-archive-old",
		"other",
		"deep/a/b/c/leaf",
	}
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(filepath.Join(root, d), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, d, "db.db"), []byte(""), 0o644))
	}

	rel := func(p string) string { return strings.TrimPrefix(p, root+"/") }
	relAll := func(ps []string) []string {
		if ps == nil {
			return nil
		}
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = rel(p)
		}
		return out
	}

	type tc struct {
		name    string
		arg     string
		want    []string
		wantErr bool
	}

	tests := []tc{
		{
			// A single-level glob matches directories at that depth only; their
			// subtrees are NOT expanded (the old "/**"-append behavior duplicated
			// output when the matched dir was then walked recursively).
			name: "single-segment-glob",
			arg:  filepath.Join(root, "proj-*"),
			want: []string{
				"proj-2024",
				"proj-2025",
				"proj-archive-old",
			},
		},
		{
			name: "glob-with-trailing-literal-segment",
			arg:  filepath.Join(root, "proj-*/sub1"),
			want: []string{
				"proj-2024/sub1",
				"proj-2025/sub1",
			},
		},
		{
			name: "doublestar-finds-leaf",
			arg:  filepath.Join(root, "**/sub1"),
			want: []string{
				"proj-2024/sub1",
				"proj-2025/sub1",
			},
		},
		{
			name: "charclass-glob",
			arg:  filepath.Join(root, "[op]*"),
			want: []string{
				"other",
				"proj-2024",
				"proj-2025",
				"proj-archive-old",
			},
		},
		{
			name: "question-mark-glob",
			arg:  filepath.Join(root, "proj-202?"),
			want: []string{
				"proj-2024",
				"proj-2025",
			},
		},
		{
			name: "no-match-glob",
			arg:  filepath.Join(root, "nonexistent-*"),
			want: nil,
		},
		{
			name: "deep-doublestar",
			arg:  filepath.Join(root, "deep/**/leaf"),
			want: []string{"deep/a/b/c/leaf"},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matches, err := simulateExpandIndexGlob(ctx, root, test.arg)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, relAll(matches))
		})
	}
}
