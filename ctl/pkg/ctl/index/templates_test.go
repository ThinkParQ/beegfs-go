package index

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLsBeeGFSE_HardlinksJoinOnName: hardlinks in one directory share an inode
// and the plugin keeps one row per name, so joining on inode alone returned
// every name once per plugin row.
func TestLsBeeGFSE_HardlinksJoinOnName(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}
	db := filepath.Join(t.TempDir(), "db.db")
	sqliteExec(t, db, `
CREATE TABLE entries(name TEXT, type TEXT, inode TEXT, size INT64, mtime INT64, atime INT64,
    ctime INT64, mode INT64, uid INT64, gid INT64, nlink INT64, blocks INT64);
CREATE TABLE beegfs_file_view(name TEXT, type TEXT, inode TEXT, owner_id INT64, parent_entry_id TEXT,
    entry_id TEXT, stripe_pattern_type INT64, stripe_chunk_size INT64, stripe_num_targets INT64);
INSERT INTO entries VALUES
    ('a', 'f', '10', 1, 0, 0, 0, 33188, 0, 0, 2, 0),
    ('a_hl', 'f', '10', 1, 0, 0, 0, 33188, 0, 0, 2, 0),
    ('b', 'f', '11', 1, 0, 0, 0, 33188, 0, 0, 1, 0);
INSERT INTO beegfs_file_view VALUES
    ('a', 'f', '10', 1, 'P', 'A', 1, 524288, 1),
    ('a_hl', 'f', '10', 1, 'P', 'A', 1, 524288, 1),
    ('b', 'f', '11', 1, 'P', 'B', 1, 524288, 1);`)

	sql := fmt.Sprintf(LsBeeGFSE, ownerCols("e.", false), "1") + " ORDER BY e.name"
	got := sqliteQuery(t, db, "SELECT name, entry_id FROM ("+sql+");")
	assert.Equal(t, "a|A\na_hl|A\nb|B", got)
}
