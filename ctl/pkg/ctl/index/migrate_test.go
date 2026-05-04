package index

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// sqliteExec runs a SQL script against a database using the sqlite3 CLI.
func sqliteExec(t *testing.T, dbPath, sql string) {
	t.Helper()
	cmd := exec.Command("sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "sqlite3 error: %s", stderr.String())
}

// sqliteQuery runs a SQL query and returns trimmed stdout.
func sqliteQuery(t *testing.T, dbPath, sql string) string {
	t.Helper()
	cmd := exec.Command("sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "sqlite3 error: %s", stderr.String())
	return strings.TrimSpace(stdout.String())
}

// createOldBDMDB creates an old hive-index .bdm.db with BeeGFS columns
// embedded in entries and summary, plus the old treesummary schema.
func createOldBDMDB(t *testing.T, dir string) string {
	t.Helper()
	dbPath := filepath.Join(dir, ".bdm.db")
	sqliteExec(t, dbPath, `
CREATE TABLE entries(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattrs BLOB, crtime INT64,
    ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64,
    osstext1 TEXT, osstext2 TEXT,
    pinode TEXT,
    ownerID TEXT, entryID TEXT, parentID TEXT, entryType INT64,
    featureFlag INT64, stripe_pattern_type INT64, chunk_size INT64,
    num_targets INT64, target_info TEXT
);

INSERT INTO entries VALUES(
    'testfile.txt', 'f', '12345', 33188, 1,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    0, 0, 0, 0, '', '',
    '99',
    '500', 'ABCD-1234', 'PARENT-5678', 1,
    0, 1, 524288,
    3, '101:202:303:'
);

INSERT INTO entries VALUES(
    'subdir', 'd', '12346', 16877, 2,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    0, 0, 0, 0, '', '',
    '99',
    '500', 'ABCD-5555', 'PARENT-5678', 0,
    0, 0, 0,
    0, ''
);

CREATE TABLE summary(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattrs BLOB, crtime INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64,
    minmtime INT64, maxmtime INT64,
    minatime INT64, maxatime INT64,
    minblocks INT64, maxblocks INT64,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    rectype INT64, pinode TEXT,
    sequencenum INT64,
    ownerID TEXT, entryID TEXT, parentID TEXT
);

INSERT INTO summary VALUES(
    '/testdir', 'd', '99', 16877, 2,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    1, 0,
    1000, 1000, 1000, 1000,
    4096, 4096,
    0, 1,
    0, 1,
    0, 0,
    4096,
    1700000002, 1700000002,
    1700000001, 1700000001,
    1700000000, 1700000000,
    8, 8,
    0, 0,
    0, 0,
    0, 0, 0,
    0, 0, 0,
    0, 0, 0,
    0, 0, 0,
    0, '1',
    0,
    '500', 'DIR-ENTRY-1', 'DIR-PARENT-1'
);

CREATE TABLE treesummary(
    totsubdirs INT64,
    maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64,
    minmtime INT64, maxmtime INT64,
    minatime INT64, maxatime INT64,
    minblocks INT64, maxblocks INT64,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    rectype INT64, uid INT64, gid INT64
);

INSERT INTO treesummary VALUES(
    1, 1, 0, 4096, 1, 0,
    1000, 1000, 1000, 1000,
    4096, 4096,
    0, 1, 0, 1, 0, 0, 4096,
    1700000002, 1700000002,
    1700000001, 1700000001,
    1700000000, 1700000000,
    8, 8, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1000, 1000
);

CREATE VIEW pentries AS SELECT entries.*, summary.inode AS s_inode FROM entries, summary;
CREATE VIEW vsummarydir AS SELECT * FROM summary WHERE rectype = 0;
CREATE VIEW vsummaryuser AS SELECT * FROM summary WHERE rectype = 1;
CREATE VIEW vsummarygroup AS SELECT * FROM summary WHERE rectype = 2;
`)
	return dbPath
}

// createNewBDMDB creates a new-format .bdm.db with the GUFI-compatible schema
// and old-plugin beegfs_* tables (11-column beegfs_entries, 9-column
// beegfs_stripe_targets with node columns, no beegfs_rst_targets). This
// represents databases indexed with an older plugin version that need the
// simple migration to update to the current schema.
func createNewBDMDB(t *testing.T, dir string) string {
	t.Helper()
	dbPath := filepath.Join(dir, ".bdm.db")
	sqliteExec(t, dbPath, `
CREATE TABLE entries(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattr_names BLOB, crtime INT64,
    ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64,
    osstext1 TEXT, osstext2 TEXT
);

INSERT INTO entries VALUES(
    'newfile.txt', 'f', '22222', 33188, 1,
    1000, 1000, 8192, 4096, 16,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    0, 0, 0, 0, '', ''
);

CREATE TABLE summary(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattr_names BLOB, crtime INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64, totzero INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64, totctime INT64,
    minmtime INT64, maxmtime INT64, totmtime INT64,
    minatime INT64, maxatime INT64, totatime INT64,
    minblocks INT64, maxblocks INT64, totblocks INT16,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64, totcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    rectype INT64, pinode TEXT, isroot INT64, rollupscore INT64
);

INSERT INTO summary VALUES(
    '/testdir', 'd', '100', 16877, 2,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    1, 0,
    1000, 1000, 1000, 1000,
    8192, 8192, 0,
    0, 1, 0, 1, 0, 0, 8192,
    1700000002, 1700000002, 0,
    1700000001, 1700000001, 0,
    1700000000, 1700000000, 0,
    16, 16, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, '1', 1, 0
);

CREATE TABLE treesummary(
    inode TEXT, pinode TEXT,
    totsubdirs INT64,
    maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64, totzero INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64, totctime INT64,
    minmtime INT64, maxmtime INT64, totmtime INT64,
    minatime INT64, maxatime INT64, totatime INT64,
    minblocks INT64, maxblocks INT64, totblocks INT64,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64, totcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    totextdbs INT64, rectype INT64, uid INT64, gid INT64
);

CREATE TABLE beegfs_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, type TEXT NOT NULL, inode INTEGER NOT NULL,
    owner_id INTEGER, parent_entry_id TEXT, entry_id TEXT,
    entry_type INTEGER, feature_flags INTEGER,
    stripe_pattern_type INTEGER, stripe_chunk_size INTEGER,
    stripe_num_targets INTEGER
);

CREATE TABLE beegfs_stripe_targets (
    entry_rowid INTEGER NOT NULL, target_index INTEGER NOT NULL,
    target_or_group INTEGER NOT NULL,
    primary_target INTEGER NOT NULL DEFAULT 0,
    secondary_target INTEGER NOT NULL DEFAULT 0,
    primary_node_id INTEGER NOT NULL DEFAULT 0,
    secondary_node_id INTEGER NOT NULL DEFAULT 0,
    primary_node_strid TEXT DEFAULT '',
    secondary_node_strid TEXT DEFAULT '',
    PRIMARY KEY (entry_rowid, target_index)
);

CREATE TABLE pentries_rollup (
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattr_names BLOB, crtime INT64,
    ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64,
    osstext1 TEXT, osstext2 TEXT,
    pinode TEXT, ppinode TEXT
);

CREATE VIEW pentries AS
SELECT entries.*, summary.inode AS pinode, summary.pinode AS ppinode
FROM entries, summary WHERE isroot = 1
UNION
SELECT * FROM pentries_rollup;

CREATE VIEW vrsummary AS SELECT
    REPLACE(summary.name, RTRIM(summary.name, REPLACE(summary.name, '/', '')), '') AS dname,
    summary.name AS sname,
    summary.rollupscore AS sroll,
    (SELECT COUNT(*) FROM summary AS c WHERE c.pinode = summary.inode) AS srollsubdirs,
    summary.*
FROM summary;

CREATE VIEW vsummarydir AS SELECT * FROM summary WHERE rectype = 0;
CREATE VIEW vsummaryuser AS SELECT * FROM summary WHERE rectype = 1;
CREATE VIEW vsummarygroup AS SELECT * FROM summary WHERE rectype = 2;
CREATE VIEW summarylong AS SELECT * FROM summary;
CREATE VIEW vrsummarylong AS SELECT * FROM vrsummary;

CREATE VIEW beegfs_file_view AS
SELECT e.id AS beegfs_rowid, e.name, e.type, e.inode,
    e.owner_id, e.parent_entry_id, e.entry_id, e.entry_type,
    e.feature_flags, e.stripe_pattern_type,
    CASE e.stripe_pattern_type
        WHEN 1 THEN 'RAID0' WHEN 2 THEN 'RAID10'
        WHEN 3 THEN 'BUDDYMIRROR' WHEN 0 THEN 'INVALID'
        ELSE 'UNKNOWN'
    END AS stripe_pattern_name,
    e.stripe_chunk_size, e.stripe_num_targets
FROM beegfs_entries AS e WHERE e.type = 'f';

CREATE VIEW beegfs_file_targets_view AS
SELECT e.id AS beegfs_rowid, e.name, e.inode,
    t.target_index, t.target_or_group,
    t.primary_target, t.secondary_target,
    t.primary_node_id, t.secondary_node_id,
    t.primary_node_strid, t.secondary_node_strid
FROM beegfs_entries AS e
JOIN beegfs_stripe_targets AS t ON t.entry_rowid = e.id
WHERE e.type = 'f';
`)
	return dbPath
}

func TestMigrateIndex_FullMigration(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Total)
	assert.Equal(t, 1, stats.MigratedFull)
	assert.Equal(t, 0, stats.Failed)

	// Verify .bdm.db is gone and db.db exists.
	dbPath := filepath.Join(subdir, "db.db")
	assert.NoFileExists(t, filepath.Join(subdir, ".bdm.db"))
	assert.FileExists(t, dbPath)

	// Verify result type.
	assert.Equal(t, "full", results[0].Type)
	assert.Nil(t, results[0].Error)

	// Verify beegfs_entries was populated.
	count := sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM beegfs_entries;")
	assert.Equal(t, "2", count) // testfile.txt + subdir

	// Verify stripe targets were parsed from "101:202:303:".
	targetCount := sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM beegfs_stripe_targets;")
	assert.Equal(t, "3", targetCount)

	// Verify specific target values.
	targets := sqliteQuery(t, dbPath,
		"SELECT target_or_group FROM beegfs_stripe_targets ORDER BY target_index;")
	assert.Equal(t, "101\n202\n303", targets)

	// Verify entries still retains old BeeGFS columns (harmless; ALTER TABLE leaves them).
	hasOwnerID := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='ownerID';")
	assert.Equal(t, "1", hasOwnerID)

	// Verify entries table has xattr_names (renamed from xattrs).
	hasXattrNames := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='xattr_names';")
	assert.Equal(t, "1", hasXattrNames)

	// Verify summary has new columns.
	hasIsRoot := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('summary') WHERE name='isroot';")
	assert.Equal(t, "1", hasIsRoot)

	// Verify summary still retains old BeeGFS columns (harmless; ALTER TABLE leaves them).
	hasSeqNum := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('summary') WHERE name='sequencenum';")
	assert.Equal(t, "1", hasSeqNum)

	// Verify isroot was set.
	isRootVal := sqliteQuery(t, dbPath, "SELECT isroot FROM summary WHERE rectype = 0;")
	assert.Equal(t, "1", isRootVal)

	// Verify beegfs_entries has new plugin columns.
	hasNumRstIds := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='num_rst_ids';")
	assert.Equal(t, "1", hasNumRstIds)

	hasStoragePoolId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='storage_pool_id';")
	assert.Equal(t, "1", hasStoragePoolId)

	// Verify beegfs_stripe_targets does not have old node columns.
	hasPrimaryNodeId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_stripe_targets') WHERE name='primary_node_id';")
	assert.Equal(t, "0", hasPrimaryNodeId)

	// Verify beegfs_rst_targets table exists.
	hasRstTable := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='beegfs_rst_targets';")
	assert.Equal(t, "1", hasRstTable)

	// Verify views exist.
	for _, view := range []string{"beegfs_file_view", "beegfs_file_targets_view", "pentries", "vrpentries", "vrsummary"} {
		exists := sqliteQuery(t, dbPath,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='"+view+"';")
		assert.Equal(t, "1", exists, "view %s should exist", view)
	}
}

func TestMigrateIndex_SimpleMigration(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createNewBDMDB(t, subdir)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Total)
	assert.Equal(t, 1, stats.MigratedSimple)
	assert.Equal(t, 0, stats.Failed)

	dbPath := filepath.Join(subdir, "db.db")
	assert.FileExists(t, dbPath)

	// Verify vrpentries view was created.
	exists := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='vrpentries';")
	assert.Equal(t, "1", exists)

	// Verify beegfs_entries was updated to the new plugin schema.
	hasNumRstIds := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='num_rst_ids';")
	assert.Equal(t, "1", hasNumRstIds)

	// Verify beegfs_stripe_targets no longer has old node columns.
	hasPrimaryNodeId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_stripe_targets') WHERE name='primary_node_id';")
	assert.Equal(t, "0", hasPrimaryNodeId)

	// Verify beegfs_rst_targets table was created.
	hasRstTable := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='beegfs_rst_targets';")
	assert.Equal(t, "1", hasRstTable)

	// Verify beegfs views were recreated with the updated schema.
	for _, view := range []string{"beegfs_file_view", "beegfs_file_targets_view"} {
		viewExists := sqliteQuery(t, dbPath,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='"+view+"';")
		assert.Equal(t, "1", viewExists, "view %s should exist", view)
	}
}

func TestMigrateIndex_SkipExisting(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

	// Pre-create db.db so migration is skipped.
	require.NoError(t, os.WriteFile(filepath.Join(subdir, "db.db"), []byte("existing"), 0o644))

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Total)
	assert.Equal(t, 1, stats.Skipped)
	assert.Equal(t, 0, stats.MigratedFull)
	assert.Equal(t, "skipped", results[0].Type)
}

func TestMigrateIndex_ForceBackup(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

	// Pre-create db.db.
	existingContent := []byte("old-db-content")
	require.NoError(t, os.WriteFile(filepath.Join(subdir, "db.db"), existingContent, 0o644))

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir, Force: true}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.MigratedFull)
	assert.Equal(t, 0, stats.Skipped)

	// Verify backup was created.
	bakPath := filepath.Join(subdir, "db.db.bak")
	assert.FileExists(t, bakPath)
	bakContent, err := os.ReadFile(bakPath)
	require.NoError(t, err)
	assert.Equal(t, existingContent, bakContent)
}

func TestMigrateIndex_TreesummaryDropped(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	CollectStats(ch)
	require.NoError(t, errWait())

	dbPath := filepath.Join(subdir, "db.db")

	// Treesummary table should NOT exist after migration. gufi_treesummary
	// checks for the table's existence to decide whether to descend into
	// child directories — an empty table would make it skip descending.
	exists := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='treesummary';")
	assert.Equal(t, "0", exists)
}

func TestMigrateIndex_MultipleDBs(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()

	// Create three subdirectories: two old, one new format.
	old1 := filepath.Join(dir, "dir1")
	old2 := filepath.Join(dir, "dir2")
	new1 := filepath.Join(dir, "dir3")
	require.NoError(t, os.MkdirAll(old1, 0o755))
	require.NoError(t, os.MkdirAll(old2, 0o755))
	require.NoError(t, os.MkdirAll(new1, 0o755))

	createOldBDMDB(t, old1)
	createOldBDMDB(t, old2)
	createNewBDMDB(t, new1)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 2, stats.MigratedFull)
	assert.Equal(t, 1, stats.MigratedSimple)
	assert.Equal(t, 0, stats.Failed)
	assert.Equal(t, 0, stats.Skipped)
}

func TestMigrateIndex_EmptyRoot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 0, stats.Total)
}

func TestMigrateIndex_InodeOverflow(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

	// Create an old-format database with an inode that exceeds MAX_INT64.
	// The unsigned value 12022742383970384569 should become -6424001689739167047
	// when stored as signed int64 (two's complement), matching the C plugin.
	dbPath := filepath.Join(subdir, ".bdm.db")
	sqliteExec(t, dbPath, `
CREATE TABLE entries(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattrs BLOB, crtime INT64,
    ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64,
    osstext1 TEXT, osstext2 TEXT,
    pinode TEXT,
    ownerID TEXT, entryID TEXT, parentID TEXT, entryType INT64,
    featureFlag INT64, stripe_pattern_type INT64, chunk_size INT64,
    num_targets INT64, target_info TEXT
);

INSERT INTO entries VALUES(
    'bigfile.txt', 'f', '12022742383970384569', 33188, 1,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    0, 0, 0, 0, '', '',
    '99',
    '500', 'ABCD-9999', 'PARENT-9999', 1,
    0, 1, 524288,
    1, '101:'
);

CREATE TABLE summary(
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattrs BLOB, crtime INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64,
    minmtime INT64, maxmtime INT64,
    minatime INT64, maxatime INT64,
    minblocks INT64, maxblocks INT64,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    rectype INT64, pinode TEXT,
    sequencenum INT64,
    ownerID TEXT, entryID TEXT, parentID TEXT
);

INSERT INTO summary VALUES(
    '/testdir', 'd', '99', 16877, 2,
    1000, 1000, 4096, 4096, 8,
    1700000000, 1700000001, 1700000002,
    '', NULL, 0,
    1, 0,
    1000, 1000, 1000, 1000,
    4096, 4096,
    0, 1, 0, 1, 0, 0, 4096,
    1700000002, 1700000002,
    1700000001, 1700000001,
    1700000000, 1700000000,
    8, 8,
    0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, '1',
    0,
    '500', 'DIR-ENTRY-1', 'DIR-PARENT-1'
);

CREATE TABLE treesummary(
    totsubdirs INT64,
    maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64,
    totfiles INT64, totlinks INT64,
    minuid INT64, maxuid INT64, mingid INT64, maxgid INT64,
    minsize INT64, maxsize INT64,
    totltk INT64, totmtk INT64,
    totltm INT64, totmtm INT64,
    totmtg INT64, totmtt INT64,
    totsize INT64,
    minctime INT64, maxctime INT64,
    minmtime INT64, maxmtime INT64,
    minatime INT64, maxatime INT64,
    minblocks INT64, maxblocks INT64,
    totxattr INT64, depth INT64,
    mincrtime INT64, maxcrtime INT64,
    minossint1 INT64, maxossint1 INT64, totossint1 INT64,
    minossint2 INT64, maxossint2 INT64, totossint2 INT64,
    minossint3 INT64, maxossint3 INT64, totossint3 INT64,
    minossint4 INT64, maxossint4 INT64, totossint4 INT64,
    rectype INT64, uid INT64, gid INT64
);

INSERT INTO treesummary VALUES(
    0, 0, 0, 0, 1, 0,
    1000, 1000, 1000, 1000,
    4096, 4096,
    0, 1, 0, 1, 0, 0, 4096,
    1700000002, 1700000002,
    1700000001, 1700000001,
    1700000000, 1700000000,
    8, 8, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1000, 1000
);

CREATE VIEW pentries AS SELECT entries.*, summary.inode AS s_inode FROM entries, summary;
CREATE VIEW vsummarydir AS SELECT * FROM summary WHERE rectype = 0;
CREATE VIEW vsummaryuser AS SELECT * FROM summary WHERE rectype = 1;
CREATE VIEW vsummarygroup AS SELECT * FROM summary WHERE rectype = 2;
`)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())
	assert.Equal(t, 1, stats.MigratedFull)

	newDB := filepath.Join(subdir, "db.db")

	// entries.inode should still hold the original unsigned TEXT value.
	textInode := sqliteQuery(t, newDB, "SELECT inode FROM entries WHERE name = 'bigfile.txt';")
	assert.Equal(t, "12022742383970384569", textInode)

	// beegfs_entries.inode should be the signed two's complement value,
	// matching what the C plugin produces: (int64)(uint64(12022742383970384569))
	signedInode := sqliteQuery(t, newDB, "SELECT inode FROM beegfs_entries WHERE name = 'bigfile.txt';")
	assert.Equal(t, "-6424001689739167047", signedInode)
}

func TestMigrateIndex_InvalidRoot(t *testing.T) {
	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: "/nonexistent/path"}
	_, _, err := MigrateIndex(context.Background(), log, cfg)
	assert.Error(t, err)
}
