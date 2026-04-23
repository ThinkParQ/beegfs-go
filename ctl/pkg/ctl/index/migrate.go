package index

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	"go.uber.org/zap"
)

// MigrateIndexCfg configures a migration run.
type MigrateIndexCfg struct {
	IndexRoot   string // root directory to walk for old databases
	OldFileName string // old database filename (default: ".bdm.db")
	NewFileName string // new database filename (default: "db.db")
	Force       bool   // backup existing new files before overwriting
	Workers     int    // number of parallel workers (0 → runtime.NumCPU())
	SkipVacuum  bool   // skip VACUUM after full migration
}

// MigrateResult reports the outcome of migrating a single database.
type MigrateResult struct {
	Dir   string // directory containing the database
	Type  string // "full", "simple", "skipped", or "error"
	Error error  // non-nil if Type == "error"
}

// MigrateStats aggregates results from a migration run.
type MigrateStats struct {
	Total          int
	MigratedFull   int
	MigratedSimple int
	Skipped        int
	Failed         int
}

// MigrateIndex walks cfg.IndexRoot finding cfg.OldFileName databases and
// migrates each one to the new GUFI format (cfg.NewFileName). Results are
// streamed into the returned channel as they complete. Up to cfg.Workers
// databases are migrated concurrently; the walk is streamed so no full path
// list is held in memory, keeping the footprint bounded for large index trees.
// The returned func() error blocks until all work is done and returns the first
// fatal walk error. Individual per-database errors are reported via
// MigrateResult.Error.
func MigrateIndex(ctx context.Context, log *zap.Logger, cfg MigrateIndexCfg) (<-chan MigrateResult, func() error, error) {
	if cfg.OldFileName == "" {
		cfg.OldFileName = ".bdm.db"
	}
	if cfg.NewFileName == "" {
		cfg.NewFileName = "db.db"
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	if _, err := exec.LookPath("sqlite3"); err != nil {
		return nil, nil, fmt.Errorf("sqlite3 not found in PATH: %w", err)
	}

	info, err := os.Stat(cfg.IndexRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("index root %q: %w", cfg.IndexRoot, err)
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("index root %q is not a directory", cfg.IndexRoot)
	}

	ch := make(chan MigrateResult, workers*2)
	sem := make(chan struct{}, workers)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(ch)
		}()

		return filepath.WalkDir(cfg.IndexRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // skip unreadable entries; don't abort the walk
			}
			if d.IsDir() || d.Name() != cfg.OldFileName {
				return nil
			}
			if gCtx.Err() != nil {
				return gCtx.Err()
			}

			// Acquire a worker slot; unblock immediately if context is cancelled.
			select {
			case sem <- struct{}{}:
			case <-gCtx.Done():
				return gCtx.Err()
			}

			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				defer func() { <-sem }()

				result := migrateOne(gCtx, log, p, cfg)
				select {
				case ch <- result:
				case <-gCtx.Done():
				}
			}(path)

			return nil
		})
	})

	return ch, g.Wait, nil
}

// migrateOne performs the migration for a single database file.
func migrateOne(ctx context.Context, log *zap.Logger, oldPath string, cfg MigrateIndexCfg) MigrateResult {
	dir := filepath.Dir(oldPath)
	newPath := filepath.Join(dir, cfg.NewFileName)

	// Check if new file already exists.
	if _, err := os.Stat(newPath); err == nil {
		if !cfg.Force {
			log.Debug("skipping (already has new file)", zap.String("dir", dir))
			return MigrateResult{Dir: dir, Type: "skipped"}
		}
		// Backup existing file.
		bakPath := newPath + ".bak"
		if err := os.Rename(newPath, bakPath); err != nil {
			return MigrateResult{Dir: dir, Type: "error",
				Error: fmt.Errorf("backup %s -> %s: %w", newPath, bakPath, err)}
		}
		log.Debug("backed up existing file", zap.String("from", newPath), zap.String("to", bakPath))
	}

	// Detect schema version before renaming.
	isOld, err := detectOldSchema(ctx, oldPath)
	if err != nil {
		return MigrateResult{Dir: dir, Type: "error",
			Error: fmt.Errorf("detect schema: %w", err)}
	}

	// Rename old -> new.
	if err := os.Rename(oldPath, newPath); err != nil {
		return MigrateResult{Dir: dir, Type: "error",
			Error: fmt.Errorf("rename %s -> %s: %w", oldPath, newPath, err)}
	}

	// Apply migration SQL.
	var sql string
	var migrationType string
	if isOld {
		sql = fullMigrationSQL
		migrationType = "full"
	} else {
		sql = simpleMigrationSQL
		migrationType = "simple"
	}

	if err := runSQL(ctx, newPath, sql); err != nil {
		_ = os.Rename(newPath, oldPath)
		return MigrateResult{Dir: dir, Type: "error",
			Error: fmt.Errorf("SQL migration (%s): %w", migrationType, err)}
	}

	// Fix inode overflow for full migrations: SQLite CAST clamps unsigned
	// values > MAX_INT64 to MAX_INT64 instead of two's complement wrap-around.
	if isOld {
		if err := fixInodeOverflow(ctx, log, newPath); err != nil {
			_ = os.Rename(newPath, oldPath)
			return MigrateResult{Dir: dir, Type: "error",
				Error: fmt.Errorf("inode overflow fix: %w", err)}
		}
	}

	// VACUUM to reclaim the space freed by dropped tables (full migrations
	// only, and only when not explicitly skipped). Non-fatal: the database is
	// fully correct without it; it just occupies more disk space.
	if isOld && !cfg.SkipVacuum {
		if err := runSQL(ctx, newPath, "VACUUM;"); err != nil {
			log.Warn("VACUUM failed (non-fatal)", zap.String("dir", dir), zap.Error(err))
		}
	}

	log.Debug("migrated", zap.String("dir", dir), zap.String("type", migrationType))
	return MigrateResult{Dir: dir, Type: migrationType}
}

// CollectStats drains a MigrateResult channel and returns aggregated stats.
func CollectStats(ch <-chan MigrateResult) (MigrateStats, []MigrateResult) {
	var stats MigrateStats
	var results []MigrateResult
	for r := range ch {
		stats.Total++
		results = append(results, r)
		switch r.Type {
		case "full":
			stats.MigratedFull++
		case "simple":
			stats.MigratedSimple++
		case "skipped":
			stats.Skipped++
		case "error":
			stats.Failed++
		}
	}
	return stats, results
}

// detectOldSchema returns true if the database has the old hive-index schema
// (entries table contains an ownerID column).
func detectOldSchema(ctx context.Context, dbPath string) (bool, error) {
	out, err := runSQLOutput(ctx, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='ownerID';")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "1", nil
}

// runSQL executes a SQL script against a database using the sqlite3 CLI.
func runSQL(ctx context.Context, dbPath, sql string) error {
	cmd := exec.CommandContext(ctx, "sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	return nil
}

// runSQLOutput executes a SQL query and returns stdout.
func runSQLOutput(ctx context.Context, dbPath, sql string) (string, error) {
	cmd := exec.CommandContext(ctx, "sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return stdout.String(), nil
}

// fixInodeOverflow corrects beegfs_entries.inode values that were clamped to
// MAX_INT64 by SQLite's CAST(text AS INTEGER). The C plugin stores inodes as
// (sqlite3_int64)(uint64) which gives two's complement wrap-around for values
// exceeding MAX_INT64. This function replicates that behavior in Go.
func fixInodeOverflow(ctx context.Context, log *zap.Logger, dbPath string) error {
	// Find rows where CAST clamped the inode to MAX_INT64 but the source
	// text value is different (i.e., a genuinely large unsigned inode).
	out, err := runSQLOutput(ctx, dbPath, `
SELECT be.id, e.inode
FROM beegfs_entries be
JOIN entries e ON be.name = e.name AND be.type = e.type
WHERE be.inode = 9223372036854775807
  AND e.inode != '9223372036854775807';
`)
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) == "" {
		return nil // no rows to fix
	}

	var sql strings.Builder
	sql.WriteString("BEGIN TRANSACTION;\n")
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		id, inodeStr := parts[0], parts[1]
		unsigned, err := strconv.ParseUint(inodeStr, 10, 64)
		if err != nil {
			log.Warn("skipping unparseable inode", zap.String("inode", inodeStr), zap.Error(err))
			continue
		}
		signed := int64(unsigned) // exact two's complement, matches C plugin behavior
		sql.WriteString(fmt.Sprintf("UPDATE beegfs_entries SET inode = %d WHERE id = %s;\n", signed, id))
	}
	sql.WriteString("COMMIT;\n")

	return runSQL(ctx, dbPath, sql.String())
}

// fullMigrationSQL transforms an old hive-index .bdm.db (with embedded BeeGFS
// columns in entries/summary) to the new GUFI format with separate beegfs_*
// tables and views. entries and summary are updated in-place with ALTER TABLE
// (old BeeGFS-specific columns are left in place; GUFI ignores them).
// Treesummary is dropped and recreated with the new schema.
// VACUUM is intentionally omitted here and run separately so it can be skipped.
const fullMigrationSQL = `
BEGIN TRANSACTION;

-- Drop any existing views from old schema before renaming tables.
DROP VIEW IF EXISTS pentries;
DROP VIEW IF EXISTS vsummarydir;
DROP VIEW IF EXISTS vsummaryuser;
DROP VIEW IF EXISTS vsummarygroup;
DROP VIEW IF EXISTS vtsummarydir;
DROP VIEW IF EXISTS vtsummaryuser;
DROP VIEW IF EXISTS vtsummarygroup;

-- Step 1: Create beegfs_entries from old embedded BeeGFS columns in entries.
CREATE TABLE IF NOT EXISTS beegfs_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    inode INTEGER NOT NULL,
    owner_id INTEGER,
    parent_entry_id TEXT,
    entry_id TEXT,
    entry_type INTEGER,
    feature_flags INTEGER,
    stripe_pattern_type INTEGER,
    stripe_chunk_size INTEGER,
    stripe_num_targets INTEGER
);

INSERT INTO beegfs_entries (name, type, inode, owner_id, parent_entry_id,
    entry_id, entry_type, feature_flags, stripe_pattern_type,
    stripe_chunk_size, stripe_num_targets)
SELECT name, type, CAST(inode AS INTEGER), CAST(ownerID AS INTEGER), parentID,
    entryID, entryType, featureFlag, stripe_pattern_type,
    chunk_size, num_targets
FROM entries;

CREATE INDEX IF NOT EXISTS beegfs_entries_inode_idx ON beegfs_entries(inode);
CREATE INDEX IF NOT EXISTS beegfs_entries_entry_id_idx ON beegfs_entries(entry_id);

-- Step 2: Create beegfs_stripe_targets and parse target_info.
CREATE TABLE IF NOT EXISTS beegfs_stripe_targets (
    entry_rowid INTEGER NOT NULL,
    target_index INTEGER NOT NULL,
    target_or_group INTEGER NOT NULL,
    primary_target INTEGER NOT NULL DEFAULT 0,
    secondary_target INTEGER NOT NULL DEFAULT 0,
    primary_node_id INTEGER NOT NULL DEFAULT 0,
    secondary_node_id INTEGER NOT NULL DEFAULT 0,
    primary_node_strid TEXT DEFAULT '',
    secondary_node_strid TEXT DEFAULT '',
    PRIMARY KEY (entry_rowid, target_index)
);

CREATE INDEX IF NOT EXISTS beegfs_targets_primary_node_idx ON beegfs_stripe_targets(primary_node_id);

-- Parse the colon-separated target_info into individual rows.
-- Example: "101:202:" -> row(target_or_group=101, index=0), row(target_or_group=202, index=1)
WITH RECURSIVE split(entry_rowid, target_index, target_or_group, rest) AS (
    SELECT
        be.id,
        0,
        CAST(SUBSTR(e.target_info, 1, INSTR(e.target_info, ':') - 1) AS INTEGER),
        SUBSTR(e.target_info, INSTR(e.target_info, ':') + 1)
    FROM entries e
    JOIN beegfs_entries be ON be.inode = CAST(e.inode AS INTEGER) AND be.name = e.name
    WHERE e.target_info IS NOT NULL
      AND e.target_info != ''
      AND INSTR(e.target_info, ':') > 0
    UNION ALL
    SELECT
        entry_rowid,
        target_index + 1,
        CAST(SUBSTR(rest, 1, INSTR(rest, ':') - 1) AS INTEGER),
        SUBSTR(rest, INSTR(rest, ':') + 1)
    FROM split
    WHERE rest != ''
      AND INSTR(rest, ':') > 0
)
INSERT INTO beegfs_stripe_targets
    (entry_rowid, target_index, target_or_group, primary_target,
     secondary_target, primary_node_id, secondary_node_id,
     primary_node_strid, secondary_node_strid)
SELECT entry_rowid, target_index, target_or_group, 0, 0, 0, 0, '', ''
FROM split
WHERE target_or_group IS NOT NULL;

-- Step 3: Update entries in-place (rename xattrs -> xattr_names; other columns are already correct).
-- Old BeeGFS-specific columns (ownerID, entryID, etc.) are left in place; GUFI ignores them.
ALTER TABLE entries RENAME COLUMN xattrs TO xattr_names;

-- Step 4: Update summary in-place (rename xattrs + add 8 missing aggregate columns).
-- Old BeeGFS-specific columns (sequencenum, ownerID, etc.) are left in place; GUFI ignores them.
ALTER TABLE summary RENAME COLUMN xattrs TO xattr_names;
ALTER TABLE summary ADD COLUMN totzero     INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totctime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totmtime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totatime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totblocks   INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totcrtime   INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN isroot      INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN rollupscore INT64 DEFAULT 0;

-- Set isroot for the root directory row (rectype=0 is the directory record).
UPDATE summary SET isroot = 1 WHERE rectype = 0;

-- Step 4b: Drop old treesummary table. gufi_treesummary will recreate it
-- with the new schema. We must NOT create an empty table here because
-- gufi_treesummary checks for the table's existence to decide whether to
-- descend into child directories — an empty table makes it skip descending.
DROP TABLE IF EXISTS treesummary;

-- Step 5: Create BeeGFS views.
CREATE VIEW IF NOT EXISTS beegfs_file_view AS
SELECT
    e.id AS beegfs_rowid, e.name, e.type, e.inode,
    e.owner_id, e.parent_entry_id, e.entry_id, e.entry_type,
    e.feature_flags, e.stripe_pattern_type,
    CASE e.stripe_pattern_type
        WHEN 1 THEN 'RAID0'
        WHEN 2 THEN 'RAID10'
        WHEN 3 THEN 'BUDDYMIRROR'
        WHEN 0 THEN 'INVALID'
        ELSE 'UNKNOWN'
    END AS stripe_pattern_name,
    e.stripe_chunk_size, e.stripe_num_targets
FROM beegfs_entries AS e WHERE e.type = 'f';

CREATE VIEW IF NOT EXISTS beegfs_file_targets_view AS
SELECT
    e.id AS beegfs_rowid, e.name, e.inode,
    t.target_index, t.target_or_group,
    t.primary_target, t.secondary_target,
    t.primary_node_id, t.secondary_node_id,
    t.primary_node_strid, t.secondary_node_strid
FROM beegfs_entries AS e
JOIN beegfs_stripe_targets AS t ON t.entry_rowid = e.id
WHERE e.type = 'f';

-- Step 6: Create auxiliary tables and views.
CREATE TABLE IF NOT EXISTS xattrs_pwd (inode TEXT, name TEXT, value TEXT);
CREATE TABLE IF NOT EXISTS xattrs_rollup (inode TEXT, name TEXT, value TEXT);
DROP VIEW IF EXISTS xattrs_avail;
CREATE VIEW xattrs_avail AS SELECT * FROM xattrs_pwd UNION SELECT * FROM xattrs_rollup;

CREATE TABLE IF NOT EXISTS external_dbs_pwd (
    type TEXT, pinode TEXT, filename TEXT, mode INT64, uid INT64, gid INT64,
    PRIMARY KEY(type, pinode, filename)
);
CREATE TABLE IF NOT EXISTS external_dbs_rollup (
    type TEXT, pinode TEXT, filename TEXT, mode INT64, uid INT64, gid INT64,
    PRIMARY KEY(type, pinode, filename)
);
DROP VIEW IF EXISTS external_dbs;
CREATE VIEW external_dbs AS SELECT * FROM external_dbs_pwd UNION SELECT * FROM external_dbs_rollup;

CREATE TABLE IF NOT EXISTS pentries_rollup (
    name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64,
    uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64,
    atime INT64, mtime INT64, ctime INT64,
    linkname TEXT, xattr_names BLOB, crtime INT64,
    ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64,
    osstext1 TEXT, osstext2 TEXT,
    pinode TEXT, ppinode TEXT
);

DROP VIEW IF EXISTS pentries;
CREATE VIEW pentries AS
SELECT entries.*, summary.inode AS pinode, summary.pinode AS ppinode
FROM entries, summary WHERE isroot = 1
UNION
SELECT * FROM pentries_rollup;

DROP VIEW IF EXISTS vrsummary;
CREATE VIEW vrsummary AS SELECT
    REPLACE(summary.name, RTRIM(summary.name, REPLACE(summary.name, '/', '')), '') AS dname,
    summary.name AS sname,
    summary.rollupscore AS sroll,
    (SELECT COUNT(*) FROM summary AS c WHERE c.pinode = summary.inode) AS srollsubdirs,
    summary.*
FROM summary;

DROP VIEW IF EXISTS summarylong;
CREATE VIEW summarylong AS SELECT * FROM summary;

DROP VIEW IF EXISTS vrsummarylong;
CREATE VIEW vrsummarylong AS SELECT * FROM vrsummary;

DROP VIEW IF EXISTS vrpentries;
CREATE VIEW vrpentries AS SELECT
    REPLACE(vrsummary.name, RTRIM(vrsummary.name, REPLACE(vrsummary.name, '/', '')), '') AS dname,
    vrsummary.name AS sname,
    vrsummary.mode AS dmode, vrsummary.nlink AS dnlink, vrsummary.uid AS duid,
    vrsummary.gid AS dgid, vrsummary.size AS dsize, vrsummary.blksize AS dblksize,
    vrsummary.blocks AS dblocks, vrsummary.atime AS datime, vrsummary.mtime AS dmtime,
    vrsummary.ctime AS dctime, vrsummary.linkname AS dlinkname,
    vrsummary.totfiles AS dtotfiles, vrsummary.totlinks AS dtotlinks,
    vrsummary.minuid AS dminuid, vrsummary.maxuid AS dmaxuid,
    vrsummary.mingid AS dmingid, vrsummary.maxgid AS dmaxgid,
    vrsummary.minsize AS dminsize, vrsummary.maxsize AS dmaxsize,
    vrsummary.totzero AS dtotzero,
    vrsummary.totltk AS dtotltk, vrsummary.totmtk AS dtotmtk,
    vrsummary.totltm AS totltm, vrsummary.totmtm AS dtotmtm,
    vrsummary.totmtg AS dtotmtg, vrsummary.totmtt AS dtotmtt,
    vrsummary.totsize AS dtotsize,
    vrsummary.minctime AS dminctime, vrsummary.maxctime AS dmaxctime, vrsummary.totctime AS dtotctime,
    vrsummary.minmtime AS dminmtime, vrsummary.maxmtime AS dmaxmtime, vrsummary.totmtime AS dtotmtime,
    vrsummary.minatime AS dminatime, vrsummary.maxatime AS dmaxatime, vrsummary.totatime AS dtotatime,
    vrsummary.minblocks AS dminblocks, vrsummary.maxblocks AS dmaxblocks, vrsummary.totblocks AS dtotblocks,
    vrsummary.totxattr AS dtotxattr, vrsummary.depth AS ddepth,
    vrsummary.mincrtime AS dmincrtime, vrsummary.maxcrtime AS dmaxcrtime, vrsummary.totcrtime AS dtotcrtime,
    vrsummary.rollupscore AS sroll, vrsummary.isroot AS atroot, vrsummary.srollsubdirs AS srollsubdirs,
    pentries.*
FROM vrsummary, pentries WHERE vrsummary.inode == pentries.pinode;

DROP VIEW IF EXISTS vsummarydir;
CREATE VIEW vsummarydir AS SELECT * FROM summary WHERE rectype = 0;
DROP VIEW IF EXISTS vsummaryuser;
CREATE VIEW vsummaryuser AS SELECT * FROM summary WHERE rectype = 1;
DROP VIEW IF EXISTS vsummarygroup;
CREATE VIEW vsummarygroup AS SELECT * FROM summary WHERE rectype = 2;

-- Drop treesummary views (table no longer exists; gufi_treesummary will recreate).
DROP VIEW IF EXISTS vtsummarydir;
DROP VIEW IF EXISTS vtsummaryuser;
DROP VIEW IF EXISTS vtsummarygroup;

COMMIT;

PRAGMA journal_mode=DELETE;
`

// simpleMigrationSQL is applied to new-format .bdm.db files that already have
// the GUFI-compatible schema (22-column entries, INT64 aggregates, beegfs_*
// tables) but are missing the vrpentries view.
const simpleMigrationSQL = `
BEGIN TRANSACTION;

DROP VIEW IF EXISTS vrpentries;
CREATE VIEW vrpentries AS SELECT
    REPLACE(vrsummary.name, RTRIM(vrsummary.name, REPLACE(vrsummary.name, '/', '')), '') AS dname,
    vrsummary.name AS sname,
    vrsummary.mode AS dmode, vrsummary.nlink AS dnlink, vrsummary.uid AS duid,
    vrsummary.gid AS dgid, vrsummary.size AS dsize, vrsummary.blksize AS dblksize,
    vrsummary.blocks AS dblocks, vrsummary.atime AS datime, vrsummary.mtime AS dmtime,
    vrsummary.ctime AS dctime, vrsummary.linkname AS dlinkname,
    vrsummary.totfiles AS dtotfiles, vrsummary.totlinks AS dtotlinks,
    vrsummary.minuid AS dminuid, vrsummary.maxuid AS dmaxuid,
    vrsummary.mingid AS dmingid, vrsummary.maxgid AS dmaxgid,
    vrsummary.minsize AS dminsize, vrsummary.maxsize AS dmaxsize,
    vrsummary.totzero AS dtotzero,
    vrsummary.totltk AS dtotltk, vrsummary.totmtk AS dtotmtk,
    vrsummary.totltm AS totltm, vrsummary.totmtm AS dtotmtm,
    vrsummary.totmtg AS dtotmtg, vrsummary.totmtt AS dtotmtt,
    vrsummary.totsize AS dtotsize,
    vrsummary.minctime AS dminctime, vrsummary.maxctime AS dmaxctime, vrsummary.totctime AS dtotctime,
    vrsummary.minmtime AS dminmtime, vrsummary.maxmtime AS dmaxmtime, vrsummary.totmtime AS dtotmtime,
    vrsummary.minatime AS dminatime, vrsummary.maxatime AS dmaxatime, vrsummary.totatime AS dtotatime,
    vrsummary.minblocks AS dminblocks, vrsummary.maxblocks AS dmaxblocks, vrsummary.totblocks AS dtotblocks,
    vrsummary.totxattr AS dtotxattr, vrsummary.depth AS ddepth,
    vrsummary.mincrtime AS dmincrtime, vrsummary.maxcrtime AS dmaxcrtime, vrsummary.totcrtime AS dtotcrtime,
    vrsummary.rollupscore AS sroll, vrsummary.isroot AS atroot, vrsummary.srollsubdirs AS srollsubdirs,
    pentries.*
FROM vrsummary, pentries WHERE vrsummary.inode == pentries.pinode;

COMMIT;

PRAGMA journal_mode=DELETE;
`
