package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beewatch"
)

func TestNewIgnoreMatcher(t *testing.T) {
	t.Run("empty rules returns nil matcher", func(t *testing.T) {
		im, err := newIgnoreMatcher(nil)
		require.NoError(t, err)
		require.Nil(t, im)
	})

	t.Run("invalid event type errors", func(t *testing.T) {
		_, err := newIgnoreMatcher([]IgnoreFileRule{
			{EventTypes: []string{"NOT_A_TYPE"}, GlobPatterns: []string{"*.tmp"}},
		})
		require.Error(t, err)
	})

	t.Run("invalid glob pattern errors", func(t *testing.T) {
		_, err := newIgnoreMatcher([]IgnoreFileRule{
			{EventTypes: []string{"*"}, GlobPatterns: []string{"[unterminated"}},
		})
		require.Error(t, err)
	})
}

func TestIgnoreMatcherMatch(t *testing.T) {
	const lwc = beewatch.V2Event_LAST_WRITER_CLOSED
	const ob = beewatch.V2Event_OPEN_BLOCKED

	tests := []struct {
		name      string
		rules     []IgnoreFileRule
		eventType beewatch.V2Event_Type
		path      string
		want      bool
	}{
		{
			name:      "basename dotfile glob matches in subdir",
			rules:     []IgnoreFileRule{{EventTypes: []string{"LAST_WRITER_CLOSED"}, GlobPatterns: []string{".*"}}},
			eventType: lwc,
			path:      "/dir/.10.swp",
			want:      true,
		},
		{
			name:      "basename extension glob matches",
			rules:     []IgnoreFileRule{{EventTypes: []string{"LAST_WRITER_CLOSED"}, GlobPatterns: []string{"*.tmp"}}},
			eventType: lwc,
			path:      "/a/b/c.tmp",
			want:      true,
		},
		{
			name:      "basename glob does not match regular file",
			rules:     []IgnoreFileRule{{EventTypes: []string{"LAST_WRITER_CLOSED"}, GlobPatterns: []string{"*.tmp", ".*"}}},
			eventType: lwc,
			path:      "/a/b/report.txt",
			want:      false,
		},
		{
			name:      "rule scoped to event type does not leak to other types",
			rules:     []IgnoreFileRule{{EventTypes: []string{"LAST_WRITER_CLOSED"}, GlobPatterns: []string{"*.tmp"}}},
			eventType: ob,
			path:      "/a/b/c.tmp",
			want:      false,
		},
		{
			name:      "wildcard event type applies to all types",
			rules:     []IgnoreFileRule{{EventTypes: []string{"*"}, GlobPatterns: []string{"*.tmp"}}},
			eventType: ob,
			path:      "/a/b/c.tmp",
			want:      true,
		},
		{
			name:      "absent event types applies to all types",
			rules:     []IgnoreFileRule{{GlobPatterns: []string{"*.tmp"}}},
			eventType: lwc,
			path:      "/a/b/c.tmp",
			want:      true,
		},
		{
			name:      "full path glob with ** matches nested path",
			rules:     []IgnoreFileRule{{EventTypes: []string{"*"}, GlobPatterns: []string{"/scratch/**"}}},
			eventType: lwc,
			path:      "/scratch/jobs/run1/out.dat",
			want:      true,
		},
		{
			name:      "full path glob does not match outside prefix",
			rules:     []IgnoreFileRule{{EventTypes: []string{"*"}, GlobPatterns: []string{"/scratch/**"}}},
			eventType: lwc,
			path:      "/data/out.dat",
			want:      false,
		},
		{
			name:      "basename single-star does not cross directory separators",
			rules:     []IgnoreFileRule{{EventTypes: []string{"*"}, GlobPatterns: []string{"*.tmp"}}},
			eventType: lwc,
			path:      "/a/b.tmp/c.dat",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im, err := newIgnoreMatcher(tt.rules)
			require.NoError(t, err)
			require.NotNil(t, im)
			_, got := im.match(tt.eventType, tt.path)
			require.Equal(t, tt.want, got)
		})
	}
}
