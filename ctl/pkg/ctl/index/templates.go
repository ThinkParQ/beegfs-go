package index

// SQL templates for each command tier. Each contains exactly one %s placeholder
// that receives the WHERE clause produced by PredicateSet.WhereClause().
// ORDER BY and LIMIT are appended dynamically by the calling function.
//
// Tier selection:
//   - Core      — vrpentries (sname/sroll available for rpath())
//   - BeeGFS    — vrpentries/entries + LEFT JOIN beegfs_file_view (for entry-id / owner-id / BeeGFS cols)
//   - Targets   — vrpentries + INNER JOIN beegfs_file_targets_view (for target-id filtering)
//
// rpath(sname, sroll, name) reconstructs the full path from directory path
// and entry name — it is a UDF registered by gufi_query at startup.

// ── find ────────────────────────────────────────────────────────────────────

const FindCoreE = `` +
	`SELECT rpath(sname, sroll, name) AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrpentries WHERE %s`

const FindBeeGFSE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, v.uid, v.gid, v.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v LEFT JOIN beegfs_file_view AS b ON b.inode = v.inode`

const FindTargetsE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, v.uid, v.gid, v.nlink, ` +
	`t.target_or_group ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v INNER JOIN beegfs_file_targets_view AS t ON t.inode = v.inode`

// FindDirS queries directories from vrsummary (isroot=1 gives each db.db's
// own directory row). sname IS the full path — no rpath() call needed.
const FindDirS = `` +
	`SELECT sname AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrsummary WHERE isroot=1 AND %s`

// LsRecursiveDirS is FindDirS reused for recursive ls --type d.
const LsRecursiveDirS = FindDirS

// ── ls ──────────────────────────────────────────────────────────────────────
// Non-recursive: query entries in the given directory only (--max-level 0)
// for files/symlinks, and summary at --min-level 1 --max-level 1 for
// immediate subdirectories (which live in their own db.db, not in entries).
// Recursive: query vrpentries across all subdirectories (no level constraint).
// ORDER BY and LIMIT are appended dynamically in Ls().

const LsCoreE = `` +
	`SELECT name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM entries WHERE %s`

// LsDirS queries the summary table for directory entries.
// Used alongside LsCoreE to list immediate subdirectories in non-recursive ls;
// run with --min-level 1 --max-level 1 so only child directories are visited.
const LsDirS = `` +
	`SELECT name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM summary WHERE %s`

const LsCoreRecursiveE = `` +
	`SELECT rpath(sname, sroll, name) AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrpentries WHERE %s`

const LsBeeGFSE = `` +
	`SELECT e.name, e.type, e.inode, e.size, e.mtime, e.atime, e.ctime, e.mode, e.uid, e.gid, e.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets ` +
	`FROM (SELECT * FROM entries WHERE %s) AS e LEFT JOIN beegfs_file_view AS b ON b.inode = e.inode`

const LsBeeGFSDirS = `` +
	`SELECT s.name, s.type, s.inode, s.size, s.mtime, s.atime, s.ctime, s.mode, s.uid, s.gid, s.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets ` +
	`FROM (SELECT * FROM summary WHERE %s) AS s LEFT JOIN beegfs_file_view AS b ON b.inode = s.inode`

const LsBeeGFSRecursiveE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, v.uid, v.gid, v.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v LEFT JOIN beegfs_file_view AS b ON b.inode = v.inode`

// ── stat ─────────────────────────────────────────────────────────────────────
// --max-level 0 + WHERE name='<basename>' gives single-entry stat.

const StatCoreE = `` +
	`SELECT name, type, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM entries WHERE %s LIMIT 1`

const StatBeeGFSE = `` +
	`SELECT e.name, e.type, e.size, e.mtime, e.atime, e.ctime, e.mode, e.uid, e.gid, e.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, ` +
	`b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM entries WHERE %s) AS e LEFT JOIN beegfs_file_view AS b ON b.inode = e.inode LIMIT 1`

// ── stats ────────────────────────────────────────────────────────────────────
// Each stat uses gufi_query's aggregation pipeline:
//   -I  create aggregate output table (once, in-memory aggregate DB)
//   -K  create per-thread working table
//   -J  insert this directory's contribution into working table
//   -G  merge working table into aggregate
//   -F  final SELECT from aggregate

const statsIntermedTable = `t`
const statsAggTable = `agg`

// total-filecount: count all regular files across the tree.
// The %s placeholder receives an optional " AND uid=N" suffix.
const StatsTotalFilecountI = `CREATE TABLE ` + statsAggTable + `(count INT64)`
const StatsTotalFilecountK = `CREATE TABLE ` + statsIntermedTable + `(count INT64)`
const StatsTotalFilecountJ = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM entries WHERE type='f'%s`
const StatsTotalFilecountG = `INSERT INTO ` + statsAggTable + ` SELECT SUM(count) FROM ` + statsIntermedTable
const StatsTotalFilecountF = `SELECT count FROM ` + statsAggTable

// total-dircount: count all directories across the tree.
const StatsTotalDircountI = `CREATE TABLE ` + statsAggTable + `(count INT64)`
const StatsTotalDircountK = `CREATE TABLE ` + statsIntermedTable + `(count INT64)`
const StatsTotalDircountJ = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM summary%s`
const StatsTotalDircountG = `INSERT INTO ` + statsAggTable + ` SELECT SUM(count) FROM ` + statsIntermedTable
const StatsTotalDircountF = `SELECT count FROM ` + statsAggTable

// total-filesize: sum of all file sizes.
const StatsTotalFilesizeI = `CREATE TABLE ` + statsAggTable + `(size INT64)`
const StatsTotalFilesizeK = `CREATE TABLE ` + statsIntermedTable + `(size INT64)`
const StatsTotalFilesizeJ = `INSERT INTO ` + statsIntermedTable + ` SELECT SUM(size) FROM entries WHERE type='f'%s`
const StatsTotalFilesizeG = `INSERT INTO ` + statsAggTable + ` SELECT SUM(size) FROM ` + statsIntermedTable
const StatsTotalFilesizeF = `SELECT size FROM ` + statsAggTable

// total-linkcount: count all symlinks across the tree.
const StatsTotalLinkcountI = `CREATE TABLE ` + statsAggTable + `(count INT64)`
const StatsTotalLinkcountK = `CREATE TABLE ` + statsIntermedTable + `(count INT64)`
const StatsTotalLinkcountJ = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM entries WHERE type='l'%s`
const StatsTotalLinkcountG = `INSERT INTO ` + statsAggTable + ` SELECT SUM(count) FROM ` + statsIntermedTable
const StatsTotalLinkcountF = `SELECT count FROM ` + statsAggTable

// filecount: per-directory file count (output: path, count).
// The %s placeholder receives optional " ORDER BY ... LIMIT N".
const StatsFilecountS = `` +
	`SELECT sname AS path, totfiles AS count FROM vrsummary WHERE isroot=1%s`

// filesize: per-directory total file size (output: path, size).
const StatsFilesizeS = `` +
	`SELECT sname AS path, totsize AS size FROM vrsummary WHERE isroot=1%s`

// depth: directory depth distribution (output: path, depth).
const StatsDepthS = `` +
	`SELECT sname AS path, depth FROM vrsummary WHERE isroot=1%s`
