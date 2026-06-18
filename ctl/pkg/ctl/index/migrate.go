package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func sqlite3Command(ctx context.Context, dbPath, sql string, uid, gid uint32) *exec.Cmd {
	cmd := exec.CommandContext(ctx, Sqlite3Bin, dbPath, sql)
	if os.Geteuid() == 0 && uid != 0 {
		// Root migrating a non-root-owned DB: drop to the file's owner so the
		// rewritten DB and its sqlite side-car files stay owned by that user.
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Credential: &syscall.Credential{Uid: uid, Gid: gid},
		}
	} else {
		cmd.SysProcAttr = CallerSysProcAttr()
	}
	return cmd
}

func assertRegularFile(path string) (uid, gid uint32, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if !fi.Mode().IsRegular() {
		return 0, 0, fmt.Errorf("%q is not a regular file (mode %s)", path, fi.Mode())
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("%q: cannot read file ownership", path)
	}
	return st.Uid, st.Gid, nil
}
func copyFile(src, dst string) error {
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	// Close error irrelevant for a read-only source.
	defer func() { _ = in.Close() }()

	out, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := out.Name()
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// dbSidecarSuffixes are the SQLite rollback/WAL sidecar files that can accompany
// a database file.
var dbSidecarSuffixes = []string{"-wal", "-shm", "-journal"}

// removeSidecars removes WAL/journal sidecar files for the given DB path.
// Missing files are silently ignored.
func removeSidecars(dbPath string) {
	for _, suffix := range dbSidecarSuffixes {
		_ = os.Remove(dbPath + suffix)
	}
}

// removeDBFiles removes a database file together with its sidecars.
func removeDBFiles(dbPath string) {
	_ = os.Remove(dbPath)
	removeSidecars(dbPath)
}

// snapshotDB copies dbPath and any existing sidecars to snap, snap-wal, ... so a
// failed migration can be rolled back to the database's untouched on-disk state.
// Old indexes (beegfs-index 8.0.1-8.3) are in WAL mode where committed rows can
// still live in the -wal file, so the sidecars are preserved rather than dropped.
// A partially written snapshot is removed before returning an error.
func snapshotDB(dbPath, snap string) error {
	if err := copyFile(dbPath, snap); err != nil {
		return err
	}
	for _, suffix := range dbSidecarSuffixes {
		src := dbPath + suffix
		if _, err := os.Lstat(src); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			removeDBFiles(snap)
			return fmt.Errorf("stat sidecar %s: %w", src, err)
		}
		if err := copyFile(src, snap+suffix); err != nil {
			removeDBFiles(snap)
			return err
		}
	}
	return nil
}

// restoreDB moves a snapshot taken by snapshotDB back to dst. Sidecars are restored
// before the main file so the database is never momentarily visible without the WAL
// that holds its most recent commits.
func restoreDB(snap, dst string) error {
	for _, suffix := range dbSidecarSuffixes {
		src := snap + suffix
		if _, err := os.Lstat(src); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return fmt.Errorf("stat snapshot sidecar %s: %w", src, err)
		}
		if err := os.Rename(src, dst+suffix); err != nil {
			return fmt.Errorf("restore %s: %w", dst+suffix, err)
		}
	}
	if err := os.Rename(snap, dst); err != nil {
		return fmt.Errorf("restore %s: %w", dst, err)
	}
	return nil
}

type MigrateIndexCfg struct {
	IndexRoot   string
	IndexAddr   string
	OldFileName string
	NewFileName string
	Force       bool
	Workers     int
	SkipVacuum  bool
}

type MigrateOutcome string

const (
	OutcomeFull    MigrateOutcome = "full"
	OutcomeSimple  MigrateOutcome = "simple"
	OutcomeSkipped MigrateOutcome = "skipped"
	OutcomeError   MigrateOutcome = "error"
)

type MigrateResult struct {
	Dir   string
	Type  MigrateOutcome
	Error error
}

type MigrateStats struct {
	Total          int
	MigratedFull   int
	MigratedSimple int
	Skipped        int
	Failed         int
}

// MigrateIndex walks cfg.IndexRoot, migrates old databases concurrently,
// and streams results. Workload is bounded by cfg.Workers.
func MigrateIndex(ctx context.Context, log *zap.Logger, cfg MigrateIndexCfg) (<-chan MigrateResult, func() error, error) {
	if IsRemoteAddr(cfg.IndexAddr) {
		return nil, nil, fmt.Errorf("migrate is not supported for remote indexes (--index-addr ssh:<host>); run migrate directly on the index host")
	}
	if cfg.OldFileName == "" {
		cfg.OldFileName = ".bdm.db"
	}
	if cfg.NewFileName == "" {
		cfg.NewFileName = "db.db"
	}
	// migrateOne writes the new DB via filepath.Join(dir, NewFileName), so a value
	// carrying a path separator (or "."/"..") would escape the matched index
	// directory. Require a bare filename for both flags.
	for _, fn := range []struct{ flag, val string }{
		{"--old-filename", cfg.OldFileName},
		{"--new-filename", cfg.NewFileName},
	} {
		if strings.ContainsRune(fn.val, filepath.Separator) || fn.val == "." || fn.val == ".." {
			return nil, nil, fmt.Errorf("%s %q must be a single filename without a path separator", fn.flag, fn.val)
		}
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	info, err := os.Stat(cfg.IndexRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("index root %q: %w", cfg.IndexRoot, err)
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("index root %q is not a directory", cfg.IndexRoot)
	}

	ch := make(chan MigrateResult, workers*2)
	ctx, cancel := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(ctx)

	paths := make(chan string)
	g.Go(func() error {
		defer close(paths)
		return filepath.WalkDir(cfg.IndexRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				select {
				case ch <- MigrateResult{Dir: path, Type: OutcomeError,
					Error: fmt.Errorf("walk: %w", err)}:
				case <-gCtx.Done():
					return gCtx.Err()
				}
				return nil
			}
			if d.IsDir() || d.Name() != cfg.OldFileName {
				return nil
			}
			if !d.Type().IsRegular() {
				log.Warn("skipping non-regular index db entry",
					zap.String("path", path), zap.Stringer("type", d.Type()))
				return nil
			}
			select {
			case paths <- path:
				return nil
			case <-gCtx.Done():
				return gCtx.Err()
			}
		})
	})

	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for p := range paths {
				result := migrateOne(gCtx, log, p, cfg)
				select {
				case ch <- result:
				case <-gCtx.Done():
					return gCtx.Err()
				}
			}
			return nil
		})
	}

	go func() {
		_ = g.Wait()
		close(ch)
	}()

	return ch, func() error {
		cancel()
		return g.Wait()
	}, nil
}

func RelocateToNested(indexRoot, mountBase, newFileName string) (bool, error) {
	if mountBase == "" || mountBase == "." || mountBase == ".." ||
		strings.ContainsRune(mountBase, filepath.Separator) {
		return false, fmt.Errorf("invalid mount name %q for the nested index layout", mountBase)
	}
	if newFileName == "" {
		newFileName = "db.db"
	}

	flatRoot := filepath.Join(indexRoot, newFileName)
	if _, err := os.Lstat(flatRoot); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("checking %s: %w", flatRoot, err)
	}

	rootInfo, err := os.Stat(indexRoot)
	if err != nil {
		return false, fmt.Errorf("stat index root %s: %w", indexRoot, err)
	}
	entries, err := os.ReadDir(indexRoot)
	if err != nil {
		return false, fmt.Errorf("reading index root %s: %w", indexRoot, err)
	}

	staging, err := os.MkdirTemp(indexRoot, ".bdm-migrate-staging-")
	if err != nil {
		return false, fmt.Errorf("creating staging directory under %s: %w", indexRoot, err)
	}
	stagingName := filepath.Base(staging)

	moved := make([]string, 0, len(entries))
	rollback := func(cause error) error {
		for i := len(moved) - 1; i >= 0; i-- {
			if err := os.Rename(filepath.Join(staging, moved[i]), filepath.Join(indexRoot, moved[i])); err != nil {
				cause = errors.Join(cause, fmt.Errorf("rollback: restoring %q under %s failed, manual recovery may be needed: %w", moved[i], indexRoot, err))
			}
		}
		if err := os.Remove(staging); err != nil {
			cause = errors.Join(cause, fmt.Errorf("rollback: removing staging dir %s failed: %w", staging, err))
		}
		return cause
	}
	for _, e := range entries {
		name := e.Name()
		if name == stagingName {
			continue
		}
		if err := os.Rename(filepath.Join(indexRoot, name), filepath.Join(staging, name)); err != nil {
			return false, rollback(fmt.Errorf("relocating %q into the nested index layout: %w", name, err))
		}
		moved = append(moved, name)
	}

	if err := os.Chmod(staging, rootInfo.Mode().Perm()); err != nil {
		return false, rollback(fmt.Errorf("setting permissions for the nested index root: %w", err))
	}
	if st, ok := rootInfo.Sys().(*syscall.Stat_t); ok {
		if err := os.Chown(staging, int(st.Uid), int(st.Gid)); err != nil {
			return false, rollback(fmt.Errorf("setting owner for the nested index root: %w", err))
		}
	}

	nestedRoot := filepath.Join(indexRoot, mountBase)
	if err := os.Rename(staging, nestedRoot); err != nil {
		return false, rollback(fmt.Errorf("creating nested index root %s: %w", nestedRoot, err))
	}
	return true, nil
}

func migrateOne(ctx context.Context, log *zap.Logger, oldPath string, cfg MigrateIndexCfg) MigrateResult {
	dir := filepath.Dir(oldPath)
	newPath := filepath.Join(dir, cfg.NewFileName)

	var bakPath string
	if _, err := os.Lstat(newPath); err == nil {
		if !cfg.Force {
			log.Debug("skipping (already has new file)", zap.String("dir", dir))
			return MigrateResult{Dir: dir, Type: OutcomeSkipped}
		}
		bakPath = newPath + ".bak"
		if err := os.Rename(newPath, bakPath); err != nil {
			return MigrateResult{Dir: dir, Type: OutcomeError,
				Error: fmt.Errorf("backup %s -> %s: %w", newPath, bakPath, err)}
		}
		log.Debug("backed up existing file", zap.String("from", newPath), zap.String("to", bakPath))
	} else if !errors.Is(err, fs.ErrNotExist) {
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("checking destination %s: %w", newPath, err)}
	}

	bakConsumed := false
	defer func() {
		if bakPath == "" {
			return
		}
		if bakConsumed {
			_ = os.Remove(bakPath)
			return
		}
		_ = os.Remove(newPath)
		removeSidecars(newPath)
		if err := os.Rename(bakPath, newPath); err != nil {
			log.Warn("could not restore db.db backup; left in place for manual recovery",
				zap.String("backup", bakPath),
				zap.String("target", newPath),
				zap.Error(err))
		}
	}()

	// Snapshot the original database — including any WAL/journal sidecars — before
	// opening or mutating it, so a failed migration rolls back to the untouched
	// database rather than to a checkpointed copy. Old indexes (beegfs-index
	// 8.0.1-8.3) are in WAL mode where committed rows can still live in the -wal
	// file, and merely opening the DB (e.g. for schema detection) can checkpoint
	// that away.
	premigratePath := oldPath + ".premigrate"
	if err := snapshotDB(oldPath, premigratePath); err != nil {
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("snapshot %s -> %s: %w", oldPath, premigratePath, err)}
	}
	snapshotConsumed := false
	defer func() {
		if snapshotConsumed {
			removeDBFiles(premigratePath)
		}
	}()

	restoreOriginal := func() {
		removeDBFiles(newPath)
		removeDBFiles(oldPath)
		if err := restoreDB(premigratePath, oldPath); err != nil {
			log.Warn("could not restore premigrate snapshot; left in place for manual recovery",
				zap.String("snapshot", premigratePath),
				zap.String("target", oldPath),
				zap.Error(err))
			return
		}
		snapshotConsumed = true
	}

	isOld, err := detectOldSchema(ctx, oldPath)
	if err != nil {
		restoreOriginal()
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("detect schema: %w", err)}
	}

	// Fold any WAL into the main file and switch to rollback journaling so the
	// database is a single self-contained file: copyFile and os.Rename below each
	// act on one file, so the WAL must be checkpointed in first.
	if err := runSQL(ctx, oldPath, "PRAGMA journal_mode=DELETE;"); err != nil {
		restoreOriginal()
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("WAL checkpoint on %s: %w", oldPath, err)}
	}
	removeSidecars(oldPath)

	if err := os.Rename(oldPath, newPath); err != nil {
		restoreOriginal()
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("rename %s -> %s: %w", oldPath, newPath, err)}
	}

	var sql string
	var migrationType MigrateOutcome
	if isOld {
		sql = fullMigrationSQL
		migrationType = OutcomeFull
	} else {
		sql = simpleMigrationSQL
		migrationType = OutcomeSimple
	}

	if err := runSQL(ctx, newPath, sql); err != nil {
		restoreOriginal()
		return MigrateResult{Dir: dir, Type: OutcomeError,
			Error: fmt.Errorf("SQL migration (%s): %w", migrationType, err)}
	}

	if isOld && !cfg.SkipVacuum {
		if err := runSQL(ctx, newPath, "VACUUM;"); err != nil {
			log.Warn("VACUUM failed (non-fatal)", zap.String("dir", dir), zap.Error(err))
		}
	}

	snapshotConsumed = true
	bakConsumed = true
	log.Debug("migrated", zap.String("dir", dir), zap.String("type", string(migrationType)))
	return MigrateResult{Dir: dir, Type: migrationType}
}

func CollectStats(ch <-chan MigrateResult) (MigrateStats, []MigrateResult) {
	var stats MigrateStats
	var results []MigrateResult
	for r := range ch {
		stats.Total++
		results = append(results, r)
		switch r.Type {
		case OutcomeFull:
			stats.MigratedFull++
		case OutcomeSimple:
			stats.MigratedSimple++
		case OutcomeSkipped:
			stats.Skipped++
		case OutcomeError:
			stats.Failed++
		}
	}
	return stats, results
}

// detectOldSchema returns true if entries table contains ownerID column.
func detectOldSchema(ctx context.Context, dbPath string) (bool, error) {
	out, err := runSQLOutput(ctx, dbPath,
		"SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name='ownerID';")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "1", nil
}

const sqlite3StderrCap = 4096

func truncateStderr(b []byte) string {
	s := strings.TrimRight(string(b), "\n")
	if len(s) > sqlite3StderrCap {
		return s[:sqlite3StderrCap] + "...(truncated)"
	}
	return s
}

// runSQL executes SQL against dbPath, aborting on the first failing statement
// (gufi_sqlite3 stops there in the single sqlite3_exec it runs the script
// through). See sqlite3Command for how the SQL is passed.
func runSQL(ctx context.Context, dbPath, sql string) error {
	uid, gid, err := assertRegularFile(dbPath)
	if err != nil {
		return fmt.Errorf("refusing to run sqlite3 on %q: %w", dbPath, err)
	}
	cmd := sqlite3Command(ctx, dbPath, sql, uid, gid)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, truncateStderr(stderr.Bytes()))
	}
	return nil
}

func runSQLOutput(ctx context.Context, dbPath, sql string) (string, error) {
	uid, gid, err := assertRegularFile(dbPath)
	if err != nil {
		return "", fmt.Errorf("refusing to run sqlite3 on %q: %w", dbPath, err)
	}
	cmd := sqlite3Command(ctx, dbPath, sql, uid, gid)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, truncateStderr(stderr.Bytes()))
	}
	return stdout.String(), nil
}

const fullMigrationSQL = `
BEGIN TRANSACTION;

DROP VIEW IF EXISTS pentries;
DROP VIEW IF EXISTS vsummarydir;
DROP VIEW IF EXISTS vsummaryuser;
DROP VIEW IF EXISTS vsummarygroup;
DROP VIEW IF EXISTS vtsummarydir;
DROP VIEW IF EXISTS vtsummaryuser;
DROP VIEW IF EXISTS vtsummarygroup;

-- inode is TEXT holding the unsigned decimal st_ino to match GUFI's
-- entries.inode exactly; INTEGER would store inodes >= 2^63 as negative
-- int64 and break every "inode = inode" join against the core tables.
CREATE TABLE IF NOT EXISTS beegfs_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    inode TEXT NOT NULL,
    owner_id INTEGER,
    parent_entry_id TEXT,
    entry_id TEXT,
    entry_type INTEGER,
    feature_flags INTEGER,
    stripe_pattern_type INTEGER,
    stripe_chunk_size INTEGER,
    stripe_num_targets INTEGER,
    stripe_default_num_targets INTEGER,
    storage_pool_id INTEGER,
    path_info_flags INTEGER,
    orig_parent_uid INTEGER,
    orig_parent_entry_id TEXT,
    file_data_state INTEGER,
    rst_major_version INTEGER,
    rst_minor_version INTEGER,
    rst_cool_down_period INTEGER,
    rst_file_policies INTEGER,
    num_rst_ids INTEGER
);

INSERT INTO beegfs_entries (name, type, inode, owner_id, parent_entry_id,
    entry_id, entry_type, feature_flags, stripe_pattern_type,
    stripe_chunk_size, stripe_num_targets)
SELECT name, type, inode, CAST(ownerID AS INTEGER), parentID,
    entryID, entryType, featureFlag, stripe_pattern_type,
    chunk_size, num_targets
FROM entries;

CREATE INDEX IF NOT EXISTS beegfs_entries_inode_idx ON beegfs_entries(inode);
CREATE INDEX IF NOT EXISTS beegfs_entries_entry_id_idx ON beegfs_entries(entry_id);

CREATE TABLE IF NOT EXISTS beegfs_stripe_targets (
    entry_rowid INTEGER NOT NULL,
    target_index INTEGER NOT NULL,
    target_or_group INTEGER NOT NULL,
    PRIMARY KEY (entry_rowid, target_index)
);

WITH RECURSIVE split(entry_rowid, target_index, target_or_group, rest) AS (
    SELECT
        be.id,
        0,
        CAST(SUBSTR(e.target_info, 1, INSTR(e.target_info, ':') - 1) AS INTEGER),
        SUBSTR(e.target_info, INSTR(e.target_info, ':') + 1)
    FROM entries e
    JOIN beegfs_entries be ON be.inode = e.inode AND be.name = e.name
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
INSERT INTO beegfs_stripe_targets (entry_rowid, target_index, target_or_group)
SELECT entry_rowid, target_index, target_or_group
FROM split
WHERE target_or_group IS NOT NULL;

CREATE TABLE IF NOT EXISTS beegfs_rst_targets (
    entry_rowid INTEGER NOT NULL,
    rst_index INTEGER NOT NULL,
    rst_id INTEGER NOT NULL,
    PRIMARY KEY (entry_rowid, rst_index)
);

ALTER TABLE entries RENAME COLUMN xattrs TO xattr_names;

ALTER TABLE summary RENAME COLUMN xattrs TO xattr_names;
ALTER TABLE summary ADD COLUMN totzero     INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totctime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totmtime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totatime    INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totblocks   INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN totcrtime   INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN isroot      INT64 DEFAULT 0;
ALTER TABLE summary ADD COLUMN rollupscore INT64 DEFAULT 0;

UPDATE summary SET isroot = 1 WHERE rectype = 0;

-- IMPORTANT: treesummary must NOT be pre-created; gufi_treesummary checks existence to decide descent
DROP TABLE IF EXISTS treesummary;
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
    e.stripe_chunk_size, e.stripe_num_targets,
    e.stripe_default_num_targets, e.storage_pool_id,
    e.path_info_flags, e.orig_parent_uid, e.orig_parent_entry_id,
    e.file_data_state,
    e.rst_major_version, e.rst_minor_version, e.rst_cool_down_period,
    e.rst_file_policies, e.num_rst_ids
FROM beegfs_entries AS e WHERE e.type = 'f';

CREATE VIEW IF NOT EXISTS beegfs_file_targets_view AS
SELECT
    e.id AS beegfs_rowid, e.name, e.inode,
    t.target_index, t.target_or_group
FROM beegfs_entries AS e
JOIN beegfs_stripe_targets AS t ON t.entry_rowid = e.id
WHERE e.type = 'f';

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

-- The migrated entries table keeps the legacy BeeGFS columns (ownerID,
-- entryID, ...) for reference, so "entries.*" would not line up with the
-- 24-column pentries_rollup and the UNION would fail to prepare (breaking
-- vrpentries and every find). List the GUFI columns explicitly instead.
DROP VIEW IF EXISTS pentries;
CREATE VIEW pentries AS
SELECT entries.name, entries.type, entries.inode, entries.mode, entries.nlink,
    entries.uid, entries.gid, entries.size, entries.blksize, entries.blocks,
    entries.atime, entries.mtime, entries.ctime, entries.linkname,
    entries.xattr_names, entries.crtime, entries.ossint1, entries.ossint2,
    entries.ossint3, entries.ossint4, entries.osstext1, entries.osstext2,
    summary.inode AS pinode, summary.pinode AS ppinode
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

DROP VIEW IF EXISTS vtsummarydir;
DROP VIEW IF EXISTS vtsummaryuser;
DROP VIEW IF EXISTS vtsummarygroup;

COMMIT;

PRAGMA journal_mode=DELETE;
`

const simpleMigrationSQL = `
BEGIN TRANSACTION;

DROP VIEW IF EXISTS beegfs_file_view;
DROP VIEW IF EXISTS beegfs_file_targets_view;
DROP VIEW IF EXISTS vrpentries;

ALTER TABLE beegfs_entries ADD COLUMN stripe_default_num_targets INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN storage_pool_id INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN path_info_flags INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN orig_parent_uid INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN orig_parent_entry_id TEXT;
ALTER TABLE beegfs_entries ADD COLUMN file_data_state INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN rst_major_version INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN rst_minor_version INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN rst_cool_down_period INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN rst_file_policies INTEGER;
ALTER TABLE beegfs_entries ADD COLUMN num_rst_ids INTEGER;

ALTER TABLE beegfs_stripe_targets DROP COLUMN primary_target;
ALTER TABLE beegfs_stripe_targets DROP COLUMN secondary_target;
ALTER TABLE beegfs_stripe_targets DROP COLUMN primary_node_id;
ALTER TABLE beegfs_stripe_targets DROP COLUMN secondary_node_id;
ALTER TABLE beegfs_stripe_targets DROP COLUMN primary_node_strid;
ALTER TABLE beegfs_stripe_targets DROP COLUMN secondary_node_strid;

-- Rebuild beegfs_entries with inode as TEXT holding the unsigned decimal
-- st_ino (matches GUFI's entries.inode). The old plugin stored inode as
-- signed INTEGER, so inodes >= 2^63 were negative and never joined against
-- the core tables. printf('%llu', ...) reinterprets the signed value as
-- unsigned. id is copied verbatim: beegfs_stripe_targets.entry_rowid
-- references it.
CREATE TABLE beegfs_entries_migrated (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    inode TEXT NOT NULL,
    owner_id INTEGER,
    parent_entry_id TEXT,
    entry_id TEXT,
    entry_type INTEGER,
    feature_flags INTEGER,
    stripe_pattern_type INTEGER,
    stripe_chunk_size INTEGER,
    stripe_num_targets INTEGER,
    stripe_default_num_targets INTEGER,
    storage_pool_id INTEGER,
    path_info_flags INTEGER,
    orig_parent_uid INTEGER,
    orig_parent_entry_id TEXT,
    file_data_state INTEGER,
    rst_major_version INTEGER,
    rst_minor_version INTEGER,
    rst_cool_down_period INTEGER,
    rst_file_policies INTEGER,
    num_rst_ids INTEGER
);

INSERT INTO beegfs_entries_migrated
SELECT id, name, type, printf('%llu', inode), owner_id, parent_entry_id,
    entry_id, entry_type, feature_flags, stripe_pattern_type,
    stripe_chunk_size, stripe_num_targets, stripe_default_num_targets,
    storage_pool_id, path_info_flags, orig_parent_uid, orig_parent_entry_id,
    file_data_state, rst_major_version, rst_minor_version,
    rst_cool_down_period, rst_file_policies, num_rst_ids
FROM beegfs_entries;

DROP TABLE beegfs_entries;
ALTER TABLE beegfs_entries_migrated RENAME TO beegfs_entries;

CREATE INDEX IF NOT EXISTS beegfs_entries_inode_idx ON beegfs_entries(inode);
CREATE INDEX IF NOT EXISTS beegfs_entries_entry_id_idx ON beegfs_entries(entry_id);

CREATE TABLE IF NOT EXISTS beegfs_rst_targets (
    entry_rowid INTEGER NOT NULL,
    rst_index INTEGER NOT NULL,
    rst_id INTEGER NOT NULL,
    PRIMARY KEY (entry_rowid, rst_index)
);
DROP VIEW IF EXISTS beegfs_file_view;
CREATE VIEW beegfs_file_view AS
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
    e.stripe_chunk_size, e.stripe_num_targets,
    e.stripe_default_num_targets, e.storage_pool_id,
    e.path_info_flags, e.orig_parent_uid, e.orig_parent_entry_id,
    e.file_data_state,
    e.rst_major_version, e.rst_minor_version, e.rst_cool_down_period,
    e.rst_file_policies, e.num_rst_ids
FROM beegfs_entries AS e WHERE e.type = 'f';

DROP VIEW IF EXISTS beegfs_file_targets_view;
CREATE VIEW beegfs_file_targets_view AS
SELECT
    e.id AS beegfs_rowid, e.name, e.inode,
    t.target_index, t.target_or_group
FROM beegfs_entries AS e
JOIN beegfs_stripe_targets AS t ON t.entry_rowid = e.id
WHERE e.type = 'f';

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
