package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatStatRow(t *testing.T) {
	// Column order matches StatCoreE / StatBeeGFSE:
	// name, type, inode, size, blocks, mode, uid, user, gid, group, nlink, atime, mtime, ctime [, beegfs...]
	core := []string{
		"1KB", "f", "3923730569762222354", "1024", "2", "33204", "0", "root", "0", "root", "1",
		"1024", "1024", "1778598765",
	}
	beegfs := append(append([]string{}, core...),
		"1", "0-6A034344-1", "1-6A034344-1", "1", "524288", "1")

	tests := []struct {
		name    string
		raw     []string
		beegfs  bool
		wantLen int
	}{
		{"core row", core, false, 12},
		{"beegfs row", beegfs, true, 18},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatStatRow(tc.raw, tc.beegfs)
			require.Len(t, got, tc.wantLen)

			// Display order: name, type, size, blocks, inode, links, mode,
			// uid, gid, atime, mtime, ctime.
			assert.Equal(t, "1KB", got[0])
			assert.Equal(t, "f", got[1])
			assert.Equal(t, "1024", got[2], "size")
			assert.Equal(t, "2", got[3], "blocks")
			assert.Equal(t, "3923730569762222354", got[4], "inode survives as text, no precision loss")
			assert.Equal(t, "1", got[5], "links")
			assert.Equal(t, "0664/-rw-rw-r--", got[6], "mode rendered octal/symbolic like stat(1)")
			assert.Equal(t, "0/root", got[7], "uid rendered num/name")
			assert.Equal(t, "0/root", got[8], "gid rendered num/name")
			for i, label := range map[int]string{9: "atime", 10: "mtime", 11: "ctime"} {
				assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} `, got[i],
					"%s rendered stat(1)-style", label)
			}

			if tc.beegfs {
				assert.Equal(t,
					[]string{"1", "0-6A034344-1", "1-6A034344-1", "1", "524288", "1"},
					got[12:], "BeeGFS columns appended in template order")
			}
		})
	}
}

func TestFormatStatRow_ShortRow(t *testing.T) {
	// Defensive: missing columns must not panic, just render empty.
	got := formatStatRow([]string{"only-name"}, false)
	assert.Equal(t, "only-name", got[0])
	assert.Equal(t, "?---------", got[6], "unparseable mode")
}
