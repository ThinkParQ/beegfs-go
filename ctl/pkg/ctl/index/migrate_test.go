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

func sqliteExec(t *testing.T, dbPath, sql string) {
	t.Helper()
	cmd := exec.Command("sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "sqlite3 error: %s", stderr.String())
}

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

func requireMigrateTools(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(Sqlite3Bin); err != nil {
		t.Skipf("gufi_sqlite3 not installed at %q", Sqlite3Bin)
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not in PATH (needed by the test fixture helpers)")
	}
}

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

-- Old plugin stored inode as signed INTEGER: the second row's st_ino has the
-- high bit set and became negative (-46819532838934289 == 18399924540870617327).
INSERT INTO beegfs_entries (id, name, type, inode, owner_id, entry_id, stripe_num_targets)
VALUES (1, 'newfile.txt', 'f', 22222, 500, 'ABCD-1111', 1),
       (2, 'bigfile.txt', 'f', -46819532838934289, 500, 'ABCD-2222', 1);

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

INSERT INTO beegfs_stripe_targets (entry_rowid, target_index, target_or_group)
VALUES (2, 0, 101);

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

// TestAssertRegularFile guards the CWE-59 mitigation: the root-run sqlite3
// subprocess must never be handed a symlink (which it would follow into an
// arbitrary file) or other non-regular file in place of a DB.
func TestAssertRegularFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	regular := filepath.Join(dir, "regular")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))
	uid, _, err := assertRegularFile(regular)
	assert.NoError(t, err)
	assert.Equal(t, uint32(os.Getuid()), uid, "must report the validated file's owner")

	// A planted symlink must be refused, not followed: O_NOFOLLOW makes the open
	// fail rather than opening the target.
	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(target, []byte("secret"), 0o600))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(target, link))
	_, _, err = assertRegularFile(link)
	assert.Error(t, err)

	_, _, err = assertRegularFile(dir)
	assert.Error(t, err, "a directory is not a regular file")
	_, _, err = assertRegularFile(filepath.Join(dir, "missing"))
	assert.Error(t, err)
}

func TestMigrateIndex_FullMigration(t *testing.T) {
	requireMigrateTools(t)

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

	dbPath := filepath.Join(subdir, "db.db")
	assert.NoFileExists(t, filepath.Join(subdir, ".bdm.db"))
	assert.FileExists(t, dbPath)

	assert.Equal(t, OutcomeFull, results[0].Type)
	assert.Nil(t, results[0].Error)

	count := sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM beegfs_entries;")
	assert.Equal(t, "2", count)

	targetCount := sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM beegfs_stripe_targets;")
	assert.Equal(t, "3", targetCount)

	targets := sqliteQuery(t, dbPath,
		"SELECT target_or_group FROM beegfs_stripe_targets ORDER BY target_index;")
	assert.Equal(t, "101\n202\n303", targets)

	hasOwnerID := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='ownerID';")
	assert.Equal(t, "1", hasOwnerID)

	hasXattrNames := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='xattr_names';")
	assert.Equal(t, "1", hasXattrNames)

	hasIsRoot := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('summary') WHERE name='isroot';")
	assert.Equal(t, "1", hasIsRoot)

	hasSeqNum := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('summary') WHERE name='sequencenum';")
	assert.Equal(t, "1", hasSeqNum)

	isRootVal := sqliteQuery(t, dbPath, "SELECT isroot FROM summary WHERE rectype = 0;")
	assert.Equal(t, "1", isRootVal)

	hasNumRstIds := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='num_rst_ids';")
	assert.Equal(t, "1", hasNumRstIds)

	hasStoragePoolId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='storage_pool_id';")
	assert.Equal(t, "1", hasStoragePoolId)

	hasPrimaryNodeId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_stripe_targets') WHERE name='primary_node_id';")
	assert.Equal(t, "0", hasPrimaryNodeId)

	hasRstTable := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='beegfs_rst_targets';")
	assert.Equal(t, "1", hasRstTable)

	for _, view := range []string{"beegfs_file_view", "beegfs_file_targets_view", "pentries", "vrpentries", "vrsummary"} {
		exists := sqliteQuery(t, dbPath,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='"+view+"';")
		assert.Equal(t, "1", exists, "view %s should exist", view)

		// Existing is not enough: entries keeps legacy BeeGFS columns after
		// migration, and a pentries view built on "entries.*" fails to
		// prepare (UNION column-count mismatch with pentries_rollup),
		// silently breaking every find. Prove each view is queryable.
		sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM "+view+";")
	}

	// vrpentries must serve the migrated rows: it is the source for find.
	vrpCount := sqliteQuery(t, dbPath, "SELECT COUNT(*) FROM vrpentries;")
	assert.Equal(t, "2", vrpCount)
}

func TestMigrateIndex_SimpleMigration(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createNewBDMDB(t, subdir)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	if stats.Failed > 0 {
		for _, r := range results {
			if r.Error != nil {
				t.Logf("migration error in %s: %v", r.Dir, r.Error)
			}
		}
	}

	assert.Equal(t, 1, stats.Total)
	assert.Equal(t, 1, stats.MigratedSimple)
	assert.Equal(t, 0, stats.Failed)

	dbPath := filepath.Join(subdir, "db.db")
	assert.FileExists(t, dbPath)

	exists := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='vrpentries';")
	assert.Equal(t, "1", exists)

	hasNumRstIds := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_entries') WHERE name='num_rst_ids';")
	assert.Equal(t, "1", hasNumRstIds)

	hasPrimaryNodeId := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('beegfs_stripe_targets') WHERE name='primary_node_id';")
	assert.Equal(t, "0", hasPrimaryNodeId)

	hasRstTable := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='beegfs_rst_targets';")
	assert.Equal(t, "1", hasRstTable)

	for _, view := range []string{"beegfs_file_view", "beegfs_file_targets_view"} {
		viewExists := sqliteQuery(t, dbPath,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='"+view+"';")
		assert.Equal(t, "1", viewExists, "view %s should exist", view)
	}

	// inode is rebuilt as TEXT holding the unsigned decimal st_ino so it
	// joins against the core entries.inode (also TEXT). The old signed
	// INTEGER never matched for inodes >= 2^63.
	inodes := sqliteQuery(t, dbPath,
		"SELECT name, typeof(inode), inode FROM beegfs_entries ORDER BY id;")
	assert.Equal(t, "newfile.txt|text|22222\nbigfile.txt|text|18399924540870617327", inodes)

	// id values survive the table rebuild: stripe targets reference them.
	targetInode := sqliteQuery(t, dbPath,
		"SELECT inode, target_or_group FROM beegfs_file_targets_view;")
	assert.Equal(t, "18399924540870617327|101", targetInode)
}

func TestMigrateIndex_SkipExisting(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

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
	assert.Equal(t, OutcomeSkipped, results[0].Type)
}

func TestMigrateIndex_ForceBackup(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))
	createOldBDMDB(t, subdir)

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

	// On success the displaced db.db backup is cleaned up (like the premigrate
	// snapshot): a completed forced migration leaves only the migrated db.db.
	bakPath := filepath.Join(subdir, "db.db.bak")
	assert.NoFileExists(t, bakPath)

	// db.db now holds the freshly migrated database, not the overwritten content.
	dbContent, err := os.ReadFile(filepath.Join(subdir, "db.db"))
	require.NoError(t, err)
	assert.NotEqual(t, existingContent, dbContent)
}

// TestMigrateIndex_ForceRollbackRestoresDB exercises the failure path: a forced
// migration that aborts must restore the displaced db.db from db.db.bak rather
// than strand the directory with no database.
func TestMigrateIndex_ForceRollbackRestoresDB(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

	// An invalid legacy file fails schema detection before the rename, after the
	// existing db.db has already been moved to db.db.bak.
	require.NoError(t, os.WriteFile(filepath.Join(subdir, ".bdm.db"), []byte("not a database"), 0o644))
	existingContent := []byte("good-db-content")
	dbPath := filepath.Join(subdir, "db.db")
	require.NoError(t, os.WriteFile(dbPath, existingContent, 0o644))

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir, Force: true}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Failed)
	require.Len(t, results, 1)
	assert.Equal(t, OutcomeError, results[0].Type)

	// db.db restored from the backup; the backup itself removed.
	restored, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, existingContent, restored, "db.db must be restored from db.db.bak")
	assert.NoFileExists(t, filepath.Join(subdir, "db.db.bak"))
}

// TestMigrateIndex_RollbackRestoresSnapshotAfterSQLFailure exercises the
// .premigrate rollback path: a DB that passes schema detection but fails the
// migration SQL *after* the snapshot + checkpoint + rename. The legacy DB must be
// restored from the snapshot, the half-written db.db removed, and no .premigrate
// orphan left behind.
func TestMigrateIndex_RollbackRestoresSnapshotAfterSQLFailure(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

	// entries has ownerID (so detectOldSchema selects the full migration) but is
	// missing the columns its INSERT...SELECT needs, so the SQL fails after the
	// rename — reaching restoreOriginal().
	oldPath := filepath.Join(subdir, ".bdm.db")
	sqliteExec(t, oldPath, `CREATE TABLE entries(name TEXT, ownerID TEXT);
INSERT INTO entries VALUES('marker', '42');`)

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)
	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Failed)
	require.Len(t, results, 1)
	assert.Equal(t, OutcomeError, results[0].Type)

	assert.FileExists(t, oldPath)
	assert.NoFileExists(t, filepath.Join(subdir, "db.db"))
	assert.NoFileExists(t, oldPath+".premigrate")
	assert.Equal(t, "42", sqliteQuery(t, oldPath, "SELECT ownerID FROM entries WHERE name='marker';"))
}

// TestSnapshotAndRestoreDB covers the snapshot/restore helpers in isolation: the
// main file and every sidecar must be captured verbatim and put back exactly,
// leaving no snapshot files behind. Uses plain files so it needs no sqlite tools.
func TestSnapshotAndRestoreDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".bdm.db")
	files := map[string]string{
		dbPath:          "main-db-bytes",
		dbPath + "-wal": "wal-sidecar-bytes",
		dbPath + "-shm": "shm-sidecar-bytes",
	}
	for p, content := range files {
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}

	snap := dbPath + ".premigrate"
	require.NoError(t, snapshotDB(dbPath, snap))

	// Snapshot captured the main file and each sidecar byte-for-byte.
	for suffix, want := range map[string]string{
		"":     "main-db-bytes",
		"-wal": "wal-sidecar-bytes",
		"-shm": "shm-sidecar-bytes",
	} {
		got, err := os.ReadFile(snap + suffix)
		require.NoError(t, err, "snapshot missing %q", snap+suffix)
		assert.Equal(t, want, string(got))
	}

	// Simulate the destructive migration wiping the original out, then restore.
	removeDBFiles(dbPath)
	for p := range files {
		require.NoFileExists(t, p)
	}
	require.NoError(t, restoreDB(snap, dbPath))

	// Original is back, byte-for-byte, with no snapshot files left behind.
	for p, want := range files {
		got, err := os.ReadFile(p)
		require.NoError(t, err, "restore missing %q", p)
		assert.Equal(t, want, string(got))
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		assert.NoFileExists(t, snap+suffix)
	}
}

// TestMigrateIndex_RollbackRestoresSidecars proves a WAL sidecar present on the
// source is carried through the snapshot and restored when the migration fails,
// so an old WAL-mode index is rolled back to its exact original state.
func TestMigrateIndex_RollbackRestoresSidecars(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

	// Same forced-SQL-failure fixture as above, plus a sentinel -wal sidecar that
	// removeSidecars deletes from the source mid-migration and that the rollback
	// must put back verbatim.
	oldPath := filepath.Join(subdir, ".bdm.db")
	sqliteExec(t, oldPath, `CREATE TABLE entries(name TEXT, ownerID TEXT);
INSERT INTO entries VALUES('marker', '42');`)
	walPath := oldPath + "-wal"
	walSentinel := []byte("pretend-wal-frames")
	require.NoError(t, os.WriteFile(walPath, walSentinel, 0o644))

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)
	stats, results := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 1, stats.Failed)
	require.Len(t, results, 1)
	assert.Equal(t, OutcomeError, results[0].Type)

	// Original DB and its -wal sidecar restored; no db.db, no snapshot orphans.
	assert.FileExists(t, oldPath)
	gotWAL, err := os.ReadFile(walPath)
	require.NoError(t, err, "the -wal sidecar must be restored")
	assert.Equal(t, walSentinel, gotWAL)
	assert.Equal(t, "42", sqliteQuery(t, oldPath, "SELECT ownerID FROM entries WHERE name='marker';"))
	assert.NoFileExists(t, filepath.Join(subdir, "db.db"))
	assert.NoFileExists(t, oldPath+".premigrate")
	assert.NoFileExists(t, oldPath+".premigrate-wal")
}

func TestMigrateIndex_TreesummaryDropped(t *testing.T) {
	requireMigrateTools(t)

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

	exists := sqliteQuery(t, dbPath,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='treesummary';")
	assert.Equal(t, "0", exists)
}

func TestMigrateIndex_MultipleDBs(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()

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
	requireMigrateTools(t)

	dir := t.TempDir()

	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: dir}
	ch, errWait, err := MigrateIndex(context.Background(), log, cfg)
	require.NoError(t, err)

	stats, _ := CollectStats(ch)
	require.NoError(t, errWait())

	assert.Equal(t, 0, stats.Total)
}

// TestMigrateIndex_HighBitInode: a full migration of a file whose st_ino has
// the high bit set must keep the unsigned decimal TEXT representation in
// beegfs_entries so it joins against the core entries.inode.
func TestMigrateIndex_HighBitInode(t *testing.T) {
	requireMigrateTools(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub1")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

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

	textInode := sqliteQuery(t, newDB, "SELECT inode FROM entries WHERE name = 'bigfile.txt';")
	assert.Equal(t, "12022742383970384569", textInode)

	beegfsInode := sqliteQuery(t, newDB,
		"SELECT typeof(inode), inode FROM beegfs_entries WHERE name = 'bigfile.txt';")
	assert.Equal(t, "text|12022742383970384569", beegfsInode)

	joined := sqliteQuery(t, newDB,
		"SELECT COUNT(*) FROM entries e JOIN beegfs_entries b ON b.inode = e.inode WHERE e.name = 'bigfile.txt';")
	assert.Equal(t, "1", joined)
}

func TestMigrateIndex_InvalidRoot(t *testing.T) {
	log := zap.NewNop()
	cfg := MigrateIndexCfg{IndexRoot: "/nonexistent/path"}
	_, _, err := MigrateIndex(context.Background(), log, cfg)
	// Assert on the index-root message so this cannot pass for an unrelated
	// reason (e.g. a missing sqlite3 binary); the root check runs first.
	assert.ErrorContains(t, err, "index root")
}

// TestMigrateIndex_RejectsPathInFilename: --old/new-filename must be a single
// path component so filepath.Join(dir, NewFileName) cannot escape the matched
// index directory.
func TestMigrateIndex_RejectsPathInFilename(t *testing.T) {
	log := zap.NewNop()
	tests := []struct {
		name string
		cfg  MigrateIndexCfg
	}{
		{"new-filename separator", MigrateIndexCfg{IndexRoot: t.TempDir(), NewFileName: "../../escape.db"}},
		{"new-filename dotdot", MigrateIndexCfg{IndexRoot: t.TempDir(), NewFileName: ".."}},
		{"old-filename separator", MigrateIndexCfg{IndexRoot: t.TempDir(), OldFileName: "sub/.bdm.db"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := MigrateIndex(context.Background(), log, tc.cfg)
			assert.ErrorContains(t, err, "single filename")
		})
	}
}

// TestRelocateToNested_MovesFlatTree verifies a migrated flat index (db.db
// rooted directly at the index root, the legacy layout) is moved into the
// nested <indexRoot>/<mountBase>/ layout, preserving each entry's permissions
// and stamping the new root with the old index root's mode.
func TestRelocateToNested_MovesFlatTree(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o750))
	// Flat layout: root db.db plus a subdir with its own db.db and a distinctive mode.
	require.NoError(t, os.WriteFile(filepath.Join(root, "db.db"), []byte("root"), 0o664))
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	require.NoError(t, os.Chmod(sub, 0o705))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "db.db"), []byte("sub"), 0o664))

	relocated, err := RelocateToNested(root, "beegfs", "db.db")
	require.NoError(t, err)
	assert.True(t, relocated)

	nested := filepath.Join(root, "beegfs")
	// Root db.db and the subtree moved under the mount-name segment.
	assertFileContent(t, filepath.Join(nested, "db.db"), "root")
	assertFileContent(t, filepath.Join(nested, "sub", "db.db"), "sub")
	assert.NoFileExists(t, filepath.Join(root, "db.db"), "flat root db.db must be gone")

	// rename preserves the moved subdir's mode; the new nested root inherits the
	// old index-root mode (the mount-root node in the flat layout).
	subInfo, err := os.Stat(filepath.Join(nested, "sub"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o705), subInfo.Mode().Perm(), "moved subdir mode must be preserved")
	nestedInfo, err := os.Stat(nested)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o750), nestedInfo.Mode().Perm(), "nested root must inherit the old index-root mode")
}

// TestRelocateToNested_AlreadyNested is a no-op when no flat root db.db is
// present (the index was created fresh in the nested layout or already migrated).
func TestRelocateToNested_AlreadyNested(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "beegfs")
	require.NoError(t, os.Mkdir(nested, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nested, "db.db"), []byte("x"), 0o664))

	relocated, err := RelocateToNested(root, "beegfs", "db.db")
	require.NoError(t, err)
	assert.False(t, relocated, "no flat root db.db: nothing to relocate")
	assertFileContent(t, filepath.Join(nested, "db.db"), "x")
	assert.NoDirExists(t, filepath.Join(nested, "beegfs"), "must not double-nest")
}

// TestRelocateToNested_MountSubdirNameCollision covers a source mount that
// itself contained a subdir named like the mount (so <indexRoot>/beegfs already
// exists as real index content); the staging-then-rename move must not collide.
func TestRelocateToNested_MountSubdirNameCollision(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "db.db"), []byte("root"), 0o664))
	collide := filepath.Join(root, "beegfs") // a real subdir, not the future nested root
	require.NoError(t, os.Mkdir(collide, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(collide, "db.db"), []byte("collide"), 0o664))

	relocated, err := RelocateToNested(root, "beegfs", "db.db")
	require.NoError(t, err)
	assert.True(t, relocated)

	nested := filepath.Join(root, "beegfs")
	assertFileContent(t, filepath.Join(nested, "db.db"), "root")
	// the colliding source subdir lands one level deeper, intact.
	assertFileContent(t, filepath.Join(nested, "beegfs", "db.db"), "collide")
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err, "reading %s", path)
	assert.Equal(t, want, string(got))
}
