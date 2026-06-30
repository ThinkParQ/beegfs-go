package index

func ownerCols(qual string, resolve bool) string {
	if resolve {
		return "uidtouser(" + qual + "uid), gidtogroup(" + qual + "gid)"
	}
	return qual + "uid, " + qual + "gid"
}

const FindCoreE = `` +
	`SELECT rpath(sname, sroll, name) AS path, name, type, inode, size, mtime, atime, ctime, mode, %s, nlink ` +
	`FROM vrpentries WHERE %s`

// The leading %s in these templates is the uid/gid SELECT fragment (ownerCols).
// FindBeeGFSE and FindTargetsE then take two WHERE positions: the first holds
// POSIX predicates (columns of vrpentries, evaluated inside the subquery),
// the second holds BeeGFS predicates (b.* / t.*, only valid after the join).
// Both default to "1" when their bucket is empty.
const FindBeeGFSE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, %s, v.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v LEFT JOIN beegfs_file_view AS b ON b.inode = v.inode ` +
	`WHERE %s`

// FindTargetsE additionally LEFT JOINs beegfs_file_view so b.* predicates
// (--entryID/--ownerID) can be combined with --targetID. It always selects the
// four BeeGFS columns ahead of t.target_or_group (17 columns total), so a
// --targetID query combined with --beegfs can display both; the parse layout in
// ParseFindRow keys off which template ran (targets), not the requested flags.
const FindTargetsE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, %s, v.nlink, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_num_targets, t.target_or_group ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v INNER JOIN beegfs_file_targets_view AS t ON t.inode = v.inode ` +
	`LEFT JOIN beegfs_file_view AS b ON b.inode = v.inode ` +
	`WHERE %s`

const FindDirS = `` +
	`SELECT rpath(sname, sroll) AS path, name, type, inode, size, mtime, atime, ctime, mode, %s, nlink ` +
	`FROM vrsummary WHERE isroot=1 AND %s`

const findDirSNumeric = `` +
	`SELECT rpath(sname, sroll) AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrsummary WHERE isroot=1 AND %s`

const LsRecursiveDirS = `` +
	`SELECT rpath(sname, sroll) AS path, name, type, inode, size, mtime, atime, ctime, mode, %s, nlink, blocks ` +
	`FROM vrsummary WHERE isroot=1 AND level() > 0 AND %s`

const findAggCols = `path TEXT, name TEXT, type TEXT, inode TEXT, size INT64, ` +
	`mtime INT64, atime INT64, ctime INT64, mode INT64, uid INT64, gid INT64, nlink INT64`

const FindAggI = `CREATE TABLE ` + statsIntermedTable + `(` + findAggCols + `)`

const FindAggESelect = `INSERT INTO ` + statsIntermedTable + ` ` +
	`SELECT rpath(sname, sroll, name) AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrpentries`

const FindAggSSelect = `INSERT INTO ` + statsIntermedTable + ` ` +
	`SELECT rpath(sname, sroll) AS path, name, type, inode, size, mtime, atime, ctime, mode, uid, gid, nlink ` +
	`FROM vrsummary`

const FindAggDirSSelect = `INSERT INTO ` + statsIntermedTable + ` ` + findDirSNumeric

const FindAggK = `CREATE TABLE ` + statsAggTable + `(` + findAggCols + `)`
const FindAggJ = `INSERT INTO ` + statsAggTable + ` SELECT * FROM ` + statsIntermedTable

const FindAggGSelect = `SELECT path, name, type, inode, size, mtime, atime, ctime, mode, %s, nlink ` +
	`FROM ` + statsAggTable

const LsCoreE = `` +
	`SELECT name, type, inode, size, mtime, atime, ctime, mode, %s, nlink, blocks ` +
	`FROM entries WHERE %s`

const LsDirS = `` +
	`SELECT name, type, inode, size, mtime, atime, ctime, mode, %s, nlink, blocks ` +
	`FROM summary WHERE isroot=1 AND %s`

const LsCoreRecursiveE = `` +
	`SELECT rpath(sname, sroll, name) AS path, name, type, inode, size, mtime, atime, ctime, mode, %s, nlink, blocks ` +
	`FROM vrpentries WHERE %s`

const LsBeeGFSE = `` +
	`SELECT e.name, e.type, e.inode, e.size, e.mtime, e.atime, e.ctime, e.mode, %s, e.nlink, e.blocks, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM entries WHERE %s) AS e LEFT JOIN beegfs_file_view AS b ON b.inode = e.inode`

const LsBeeGFSDirS = `` +
	`SELECT s.name, s.type, s.inode, s.size, s.mtime, s.atime, s.ctime, s.mode, %s, s.nlink, s.blocks, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM summary WHERE isroot=1 AND %s) AS s LEFT JOIN beegfs_file_view AS b ON b.inode = s.inode`

const LsBeeGFSRecursiveE = `` +
	`SELECT rpath(v.sname, v.sroll, v.name) AS path, v.name, v.type, v.inode, v.size, v.mtime, v.atime, v.ctime, v.mode, %s, v.nlink, v.blocks, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM vrpentries WHERE %s) AS v LEFT JOIN beegfs_file_view AS b ON b.inode = v.inode`

const LsBeeGFSRecursiveDirS = `` +
	`SELECT rpath(s.sname, s.sroll) AS path, s.name, s.type, s.inode, s.size, s.mtime, s.atime, s.ctime, s.mode, %s, s.nlink, s.blocks, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM vrsummary WHERE isroot=1 AND level() > 0 AND %s) AS s LEFT JOIN beegfs_file_view AS b ON b.inode = s.inode`

const StatCoreE = `` +
	`SELECT name, type, inode, size, blocks, mode, uid, uidtouser(uid), gid, gidtogroup(gid), nlink, atime, mtime, ctime ` +
	`FROM entries WHERE %s LIMIT 1`

const StatBeeGFSE = `` +
	`SELECT e.name, e.type, e.inode, e.size, e.blocks, e.mode, e.uid, uidtouser(e.uid), e.gid, gidtogroup(e.gid), e.nlink, e.atime, e.mtime, e.ctime, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, ` +
	`b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM entries WHERE %s) AS e LEFT JOIN beegfs_file_view AS b ON b.inode = e.inode LIMIT 1`

const StatDirS = `` +
	`SELECT name, type, inode, size, blocks, mode, uid, uidtouser(uid), gid, gidtogroup(gid), nlink, atime, mtime, ctime ` +
	`FROM summary WHERE isroot=1 LIMIT 1`

const StatBeeGFSDirS = `` +
	`SELECT s.name, s.type, s.inode, s.size, s.blocks, s.mode, s.uid, uidtouser(s.uid), s.gid, gidtogroup(s.gid), s.nlink, s.atime, s.mtime, s.ctime, ` +
	`b.owner_id, b.parent_entry_id, b.entry_id, ` +
	`b.stripe_pattern_type, b.stripe_chunk_size, b.stripe_num_targets ` +
	`FROM (SELECT * FROM summary WHERE isroot=1) AS s LEFT JOIN beegfs_file_view AS b ON b.inode = s.inode LIMIT 1`

const statsIntermedTable = `intermediate`
const statsAggTable = `aggregate`

// Shared aggregation-pipeline fragments. Many stat families differ only in their
// per-entry/-directory SELECT (E/S) and final aggregate (G); the create/copy
// steps (I/K/J) are byte-identical, so those families alias these constants
// instead of repeating the literals.
const (
	statsCountIntermedI = `CREATE TABLE ` + statsIntermedTable + `(count INT64)`
	statsCountAggK      = `CREATE TABLE ` + statsAggTable + `(count INT64)`
	statsCountCopyJ     = `INSERT INTO ` + statsAggTable + ` SELECT count FROM ` + statsIntermedTable
	statsCountSumG      = `SELECT SUM(count) FROM ` + statsAggTable

	statsLevelIntermedI = `CREATE TABLE ` + statsIntermedTable + `(level INT64, count INT64)`
	statsLevelAggK      = `CREATE TABLE ` + statsAggTable + `(level INT64, count INT64)`
	statsLevelCopyJ     = `INSERT INTO ` + statsAggTable + ` SELECT level, count FROM ` + statsIntermedTable

	statsLeafValIntermedI = `CREATE TABLE ` + statsIntermedTable + `(val REAL, is_leaf INT64)`
	statsLeafValAggK      = `CREATE TABLE ` + statsAggTable + `(val REAL, is_leaf INT64)`
	statsLeafValCopyJ     = `INSERT INTO ` + statsAggTable + ` SELECT val, is_leaf FROM ` + statsIntermedTable

	statsExpIntermedI = `CREATE TABLE ` + statsIntermedTable + `(exponent INT64)`
	statsExpAggK      = `CREATE TABLE ` + statsAggTable + `(exponent INT64, count INT64)`
	statsExpCopyJ     = `INSERT INTO ` + statsAggTable + ` SELECT exponent, COUNT(*) FROM ` + statsIntermedTable + ` GROUP BY exponent`
)

const StatsTotalFilecountI = statsCountIntermedI
const StatsTotalFilecountE = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM entries WHERE type='f'%s`
const StatsTotalFilecountK = statsCountAggK
const StatsTotalFilecountJ = statsCountCopyJ
const StatsTotalFilecountG = statsCountSumG

const StatsTotalDircountI = statsCountIntermedI
const StatsTotalDircountS = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM summary WHERE isroot=1%s`
const StatsTotalDircountK = statsCountAggK
const StatsTotalDircountJ = statsCountCopyJ
const StatsTotalDircountG = statsCountSumG

const StatsTotalFilesizeI = `CREATE TABLE ` + statsIntermedTable + `(size INT64)`
const StatsTotalFilesizeE = `INSERT INTO ` + statsIntermedTable + ` SELECT SUM(size) FROM entries WHERE type='f'%s`
const StatsTotalFilesizeK = `CREATE TABLE ` + statsAggTable + `(size INT64)`
const StatsTotalFilesizeJ = `INSERT INTO ` + statsAggTable + ` SELECT size FROM ` + statsIntermedTable
const StatsTotalFilesizeG = `SELECT SUM(size) FROM ` + statsAggTable

const StatsTotalLinkcountI = statsCountIntermedI
const StatsTotalLinkcountE = `INSERT INTO ` + statsIntermedTable + ` SELECT COUNT(*) FROM entries WHERE type='l'%s`
const StatsTotalLinkcountK = statsCountAggK
const StatsTotalLinkcountJ = statsCountCopyJ
const StatsTotalLinkcountG = statsCountSumG

const StatsFilecountS = `` +
	`SELECT sname AS path, totfiles AS count FROM vrsummary WHERE isroot=1%s`

const StatsFilesizeS = `` +
	`SELECT sname AS path, totsize AS size FROM vrsummary WHERE isroot=1%s`

const StatsDepthS = `` +
	`SELECT sname AS path, depth FROM vrsummary WHERE isroot=1%s`

const StatsLinkcountS = `` +
	`SELECT sname AS path, totlinks AS count FROM vrsummary WHERE isroot=1%s`

const StatsDircountS = `` +
	`SELECT sname AS path, nlink-2 AS count FROM vrsummary WHERE isroot=1%s`

const StatsLeafDirsS = `SELECT sname FROM vrsummary WHERE isroot=1 AND nlink=2`

const StatsLeafDepthS = `SELECT sname, level() FROM vrsummary WHERE isroot=1 AND nlink=2`

const StatsLeafFilesS = `` +
	`SELECT sname, (SELECT COUNT(*) FROM entries WHERE type='f') FROM vrsummary WHERE isroot=1 AND nlink=2`

const StatsLeafLinksS = `` +
	`SELECT sname, (SELECT COUNT(*) FROM entries WHERE type='l') FROM vrsummary WHERE isroot=1 AND nlink=2`

const StatsTotalLeafFilesI = statsCountIntermedI
const StatsTotalLeafFilesS = `INSERT INTO ` + statsIntermedTable + ` SELECT totfiles FROM summary WHERE isroot=1 AND nlink=2`
const StatsTotalLeafFilesK = statsCountAggK
const StatsTotalLeafFilesJ = statsCountCopyJ
const StatsTotalLeafFilesG = statsCountSumG

const StatsTotalLeafLinksI = statsCountIntermedI
const StatsTotalLeafLinksS = `INSERT INTO ` + statsIntermedTable + ` SELECT totlinks FROM summary WHERE isroot=1 AND nlink=2`
const StatsTotalLeafLinksK = statsCountAggK
const StatsTotalLeafLinksJ = statsCountCopyJ
const StatsTotalLeafLinksG = statsCountSumG

const StatsFilesPerLevelI = statsLevelIntermedI
const StatsFilesPerLevelS = `INSERT INTO ` + statsIntermedTable + ` SELECT level(), COUNT(*) FROM entries WHERE type='f' GROUP BY 1`
const StatsFilesPerLevelK = statsLevelAggK
const StatsFilesPerLevelJ = statsLevelCopyJ
const StatsFilesPerLevelG = `SELECT level, SUM(count) FROM ` + statsAggTable + ` GROUP BY level ORDER BY level ASC`

const StatsLinksPerLevelI = statsLevelIntermedI
const StatsLinksPerLevelS = `INSERT INTO ` + statsIntermedTable + ` SELECT level(), COUNT(*) FROM entries WHERE type='l' GROUP BY 1`
const StatsLinksPerLevelK = statsLevelAggK
const StatsLinksPerLevelJ = statsLevelCopyJ
const StatsLinksPerLevelG = `SELECT level, SUM(count) FROM ` + statsAggTable + ` GROUP BY level ORDER BY level ASC`

const StatsDirsPerLevelI = statsLevelIntermedI
const StatsDirsPerLevelS = `INSERT INTO ` + statsIntermedTable + ` SELECT level(), 1 FROM summary WHERE isroot=1`
const StatsDirsPerLevelK = statsLevelAggK
const StatsDirsPerLevelJ = statsLevelCopyJ
const StatsDirsPerLevelG = `SELECT level, SUM(count) FROM ` + statsAggTable + ` GROUP BY level ORDER BY level ASC`

const StatsAverageLeafFilesI = statsLeafValIntermedI
const StatsAverageLeafFilesS = `INSERT INTO ` + statsIntermedTable + ` SELECT totfiles, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsAverageLeafFilesK = statsLeafValAggK
const StatsAverageLeafFilesJ = statsLeafValCopyJ
const StatsAverageLeafFilesG = `SELECT AVG(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsAverageLeafLinksI = statsLeafValIntermedI
const StatsAverageLeafLinksS = `INSERT INTO ` + statsIntermedTable + ` SELECT totlinks, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsAverageLeafLinksK = statsLeafValAggK
const StatsAverageLeafLinksJ = statsLeafValCopyJ
const StatsAverageLeafLinksG = `SELECT AVG(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsAverageLeafSizeI = statsLeafValIntermedI
const StatsAverageLeafSizeS = `INSERT INTO ` + statsIntermedTable + ` SELECT totsize, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsAverageLeafSizeK = statsLeafValAggK
const StatsAverageLeafSizeJ = statsLeafValCopyJ
const StatsAverageLeafSizeG = `SELECT AVG(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsMedianLeafFilesI = statsLeafValIntermedI
const StatsMedianLeafFilesS = `INSERT INTO ` + statsIntermedTable + ` SELECT totfiles, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsMedianLeafFilesK = statsLeafValAggK
const StatsMedianLeafFilesJ = statsLeafValCopyJ
const StatsMedianLeafFilesG = `` +
	`SELECT median(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsMedianLeafLinksI = statsLeafValIntermedI
const StatsMedianLeafLinksS = `INSERT INTO ` + statsIntermedTable + ` SELECT totlinks, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsMedianLeafLinksK = statsLeafValAggK
const StatsMedianLeafLinksJ = statsLeafValCopyJ
const StatsMedianLeafLinksG = `` +
	`SELECT median(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsMedianLeafSizeI = statsLeafValIntermedI
const StatsMedianLeafSizeS = `INSERT INTO ` + statsIntermedTable + ` SELECT totsize, (nlink=2) FROM vrsummary WHERE isroot=1`
const StatsMedianLeafSizeK = statsLeafValAggK
const StatsMedianLeafSizeJ = statsLeafValCopyJ
const StatsMedianLeafSizeG = `` +
	`SELECT median(val) FROM ` + statsAggTable + ` WHERE is_leaf=1`

const StatsDuplicateNamesI = `CREATE TABLE ` + statsIntermedTable + `(name TEXT)`
const StatsDuplicateNamesE = `INSERT INTO ` + statsIntermedTable + ` SELECT name FROM entries`
const StatsDuplicateNamesK = `CREATE TABLE ` + statsAggTable + `(name TEXT, count INT64)`
const StatsDuplicateNamesJ = `INSERT INTO ` + statsAggTable + ` SELECT name, COUNT(*) FROM ` + statsIntermedTable + ` GROUP BY name`
const StatsDuplicateNamesG = `SELECT a.name FROM ` + statsAggTable + ` AS a INNER JOIN ` + statsAggTable + ` AS b ON a.name=b.name WHERE a.count > 1 GROUP BY a.name ORDER BY a.name ASC`

const StatsUidSizeI = `CREATE TABLE ` + statsIntermedTable + `(uid INT64, size INT64, name TEXT)`
const StatsUidSizeE = `INSERT INTO ` + statsIntermedTable + ` SELECT uid, size, rpath(sname, sroll, name) FROM vrpentries WHERE type='f'`
const StatsUidSizeK = `CREATE TABLE ` + statsAggTable + `(uid INT64, size INT64, name TEXT)`
const StatsUidSizeJ = `INSERT INTO ` + statsAggTable + ` SELECT uid, size, name FROM ` + statsIntermedTable

const StatsGidSizeI = `CREATE TABLE ` + statsIntermedTable + `(gid INT64, size INT64, name TEXT)`
const StatsGidSizeE = `INSERT INTO ` + statsIntermedTable + ` SELECT gid, size, rpath(sname, sroll, name) FROM vrpentries WHERE type='f'`
const StatsGidSizeK = `CREATE TABLE ` + statsAggTable + `(gid INT64, size INT64, name TEXT)`
const StatsGidSizeJ = `INSERT INTO ` + statsAggTable + ` SELECT gid, size, name FROM ` + statsIntermedTable

const StatsExtensionsI = `CREATE TABLE ` + statsIntermedTable + `(extension TEXT, freq INT64)`
const StatsExtensionsE = `INSERT INTO ` + statsIntermedTable + ` SELECT ext(name), COUNT(*) FROM vrpentries WHERE type='f' GROUP BY ext(name)`
const StatsExtensionsK = `CREATE TABLE ` + statsAggTable + `(extension TEXT, freq INT64)`
const StatsExtensionsJ = `INSERT INTO ` + statsAggTable + ` SELECT extension, SUM(freq) FROM ` + statsIntermedTable + ` GROUP BY extension`
const StatsExtensionsG = `SELECT extension, SUM(freq) FROM ` + statsAggTable + ` GROUP BY extension ORDER BY SUM(freq) DESC, extension ASC`

const StatsFilesizeLog2BinsI = statsExpIntermedI
const StatsFilesizeLog2BinsE = `INSERT INTO ` + statsIntermedTable + ` SELECT CASE WHEN size==0 THEN -1 ELSE CAST(FLOOR(LOG(2, size)) AS INTEGER) END FROM vrpentries WHERE type='f'`
const StatsFilesizeLog2BinsK = statsExpAggK
const StatsFilesizeLog2BinsJ = statsExpCopyJ
const StatsFilesizeLog2BinsG = `SELECT CASE WHEN exponent==-1 THEN '[0,1)' ELSE '[' || CAST(pow(2, exponent) AS INTEGER) || ',' || CAST(pow(2, exponent+1) AS INTEGER) || ')' END, SUM(count) FROM ` + statsAggTable + ` GROUP BY exponent ORDER BY exponent ASC`

const StatsFilesizeLog1024BinsI = statsExpIntermedI
const StatsFilesizeLog1024BinsE = `INSERT INTO ` + statsIntermedTable + ` SELECT CASE WHEN size==0 THEN -1 ELSE CAST(FLOOR(LOG(1024, size)) AS INTEGER) END FROM vrpentries WHERE type='f'`
const StatsFilesizeLog1024BinsK = statsExpAggK
const StatsFilesizeLog1024BinsJ = statsExpCopyJ
const StatsFilesizeLog1024BinsG = `SELECT CASE WHEN exponent==-1 THEN '[0,1)' ELSE '[' || CAST(pow(1024, exponent) AS INTEGER) || ',' || CAST(pow(1024, exponent+1) AS INTEGER) || ')' END, SUM(count) FROM ` + statsAggTable + ` GROUP BY exponent ORDER BY exponent ASC`

const StatsDirfilecountLog2BinsI = statsExpIntermedI
const StatsDirfilecountLog2BinsS = `INSERT INTO ` + statsIntermedTable + ` SELECT CASE WHEN totfiles==0 THEN -1 ELSE CAST(FLOOR(LOG(2, totfiles)) AS INTEGER) END FROM vrsummary WHERE isroot=1`
const StatsDirfilecountLog2BinsK = statsExpAggK
const StatsDirfilecountLog2BinsJ = statsExpCopyJ
const StatsDirfilecountLog2BinsG = `SELECT CASE WHEN exponent==-1 THEN '[0,1)' ELSE '[' || CAST(pow(2, exponent) AS INTEGER) || ',' || CAST(pow(2, exponent+1) AS INTEGER) || ')' END, SUM(count) FROM ` + statsAggTable + ` GROUP BY exponent ORDER BY exponent ASC`

const StatsDirfilecountLog1024BinsI = statsExpIntermedI
const StatsDirfilecountLog1024BinsS = `INSERT INTO ` + statsIntermedTable + ` SELECT CASE WHEN totfiles==0 THEN -1 ELSE CAST(FLOOR(LOG(1024, totfiles)) AS INTEGER) END FROM vrsummary WHERE isroot=1`
const StatsDirfilecountLog1024BinsK = statsExpAggK
const StatsDirfilecountLog1024BinsJ = statsExpCopyJ
const StatsDirfilecountLog1024BinsG = `SELECT CASE WHEN exponent==-1 THEN '[0,1)' ELSE '[' || CAST(pow(1024, exponent) AS INTEGER) || ',' || CAST(pow(1024, exponent+1) AS INTEGER) || ')' END, SUM(count) FROM ` + statsAggTable + ` GROUP BY exponent ORDER BY exponent ASC`

func AgeBucketCaseSQL(reftime int64, timeCol string) string {
	return `CASE` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 0 THEN 0` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 1 THEN 1` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 60 THEN 2` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 3600 THEN 3` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 86400 THEN 4` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 604800 THEN 5` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 2419200 THEN 6` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 31536000 THEN 7` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 63072000 THEN 8` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 157680000 THEN 9` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 315360000 THEN 10` +
		` WHEN (` + i2s(reftime) + ` - ` + timeCol + `) < 630720000 THEN 11` +
		` ELSE 12 END`
}

const AgeBucketLabelSQL = `CASE bucket` +
	` WHEN 0 THEN '[future, 0 sec ago)'` +
	` WHEN 1 THEN '[0 sec ago, 1 sec ago)'` +
	` WHEN 2 THEN '[1 sec ago, 1 minute ago)'` +
	` WHEN 3 THEN '[1 minute ago, 1 hour ago)'` +
	` WHEN 4 THEN '[1 hour ago, 1 day ago)'` +
	` WHEN 5 THEN '[1 day ago, 1 week ago)'` +
	` WHEN 6 THEN '[1 week ago, 4 weeks ago)'` +
	` WHEN 7 THEN '[4 weeks ago, 1 year ago)'` +
	` WHEN 8 THEN '[1 year ago, 2 years ago)'` +
	` WHEN 9 THEN '[2 year ago, 5 years ago)'` +
	` WHEN 10 THEN '[5 year ago, 10 years ago)'` +
	` WHEN 11 THEN '[10 year ago, 20 years ago)'` +
	` WHEN 12 THEN '[20 year ago, older)' END`
