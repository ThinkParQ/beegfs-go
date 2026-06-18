package index

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func i2s(v int64) string { return strconv.FormatInt(v, 10) }

// StatName is the type of aggregate statistic to compute.
type StatName string

const (
	StatTotalFilecount          StatName = "total-filecount"
	StatTotalDircount           StatName = "total-dircount"
	StatTotalFilesize           StatName = "total-filesize"
	StatTotalLinkcount          StatName = "total-linkcount"
	StatFilecount               StatName = "filecount"
	StatFilesize                StatName = "filesize"
	StatDepth                   StatName = "depth"
	StatLinkcount               StatName = "linkcount"
	StatDircount                StatName = "dircount"
	StatLeafDirs                StatName = "leaf-dirs"
	StatLeafDepth               StatName = "leaf-depth"
	StatLeafFiles               StatName = "leaf-files"
	StatLeafLinks               StatName = "leaf-links"
	StatTotalLeafFiles          StatName = "total-leaf-files"
	StatTotalLeafLinks          StatName = "total-leaf-links"
	StatFilesPerLevel           StatName = "files-per-level"
	StatLinksPerLevel           StatName = "links-per-level"
	StatDirsPerLevel            StatName = "dirs-per-level"
	StatAverageLeafFiles        StatName = "average-leaf-files"
	StatAverageLeafLinks        StatName = "average-leaf-links"
	StatAverageLeafSize         StatName = "average-leaf-size"
	StatMedianLeafFiles         StatName = "median-leaf-files"
	StatMedianLeafLinks         StatName = "median-leaf-links"
	StatMedianLeafSize          StatName = "median-leaf-size"
	StatDuplicateNames          StatName = "duplicate-names"
	StatUidSize                 StatName = "uid-size"
	StatGidSize                 StatName = "gid-size"
	StatExtensions              StatName = "extensions"
	StatFilesizeLog2Bins        StatName = "filesize-log2-bins"
	StatFilesizeLog1024Bins     StatName = "filesize-log1024-bins"
	StatDirfilecountLog2Bins    StatName = "dirfilecount-log2-bins"
	StatDirfilecountLog1024Bins StatName = "dirfilecount-log1024-bins"

	StatFileAgeAtime StatName = "file-age-atime"
	StatFileAgeMtime StatName = "file-age-mtime"
	StatFileAgeCtime StatName = "file-age-ctime"
	StatDirAgeAtime  StatName = "dir-age-atime"
	StatDirAgeMtime  StatName = "dir-age-mtime"
	StatDirAgeCtime  StatName = "dir-age-ctime"
)

// ValidStatNames enumerates every supported StatName in the order surfaced
// in user-facing error messages and help text. Keep beside the const block
// so adding a new stat without listing it here is a visible omission.
var ValidStatNames = []StatName{
	StatTotalFilecount, StatTotalDircount, StatTotalFilesize, StatTotalLinkcount,
	StatFilecount, StatFilesize, StatDepth, StatLinkcount, StatDircount,
	StatLeafDirs, StatLeafDepth, StatLeafFiles, StatLeafLinks,
	StatTotalLeafFiles, StatTotalLeafLinks,
	StatFilesPerLevel, StatLinksPerLevel, StatDirsPerLevel,
	StatAverageLeafFiles, StatAverageLeafLinks, StatAverageLeafSize,
	StatMedianLeafFiles, StatMedianLeafLinks, StatMedianLeafSize,
	StatDuplicateNames, StatUidSize, StatGidSize, StatExtensions,
	StatFilesizeLog2Bins, StatFilesizeLog1024Bins,
	StatDirfilecountLog2Bins, StatDirfilecountLog1024Bins,
	StatFileAgeAtime, StatFileAgeMtime, StatFileAgeCtime,
	StatDirAgeAtime, StatDirAgeMtime, StatDirAgeCtime,
}

// statOutputColumns maps each StatName to its output column names for display.
var statOutputColumns = map[StatName][]string{
	StatTotalFilecount:          {"count"},
	StatTotalDircount:           {"count"},
	StatTotalFilesize:           {"size"},
	StatTotalLinkcount:          {"count"},
	StatFilecount:               {"path", "count"},
	StatFilesize:                {"path", "size"},
	StatDepth:                   {"path", "depth"},
	StatLinkcount:               {"path", "count"},
	StatDircount:                {"path", "count"},
	StatLeafDirs:                {"path"},
	StatLeafDepth:               {"path", "level"},
	StatLeafFiles:               {"path", "count"},
	StatLeafLinks:               {"path", "count"},
	StatTotalLeafFiles:          {"count"},
	StatTotalLeafLinks:          {"count"},
	StatFilesPerLevel:           {"level", "count"},
	StatLinksPerLevel:           {"level", "count"},
	StatDirsPerLevel:            {"level", "count"},
	StatAverageLeafFiles:        {"value"},
	StatAverageLeafLinks:        {"value"},
	StatAverageLeafSize:         {"value"},
	StatMedianLeafFiles:         {"value"},
	StatMedianLeafLinks:         {"value"},
	StatMedianLeafSize:          {"value"},
	StatDuplicateNames:          {"name"},
	StatUidSize:                 {"uid", "size", "name"},
	StatGidSize:                 {"gid", "size", "name"},
	StatExtensions:              {"extension", "count"},
	StatFilesizeLog2Bins:        {"bin", "count"},
	StatFilesizeLog1024Bins:     {"bin", "count"},
	StatDirfilecountLog2Bins:    {"bin", "count"},
	StatDirfilecountLog1024Bins: {"bin", "count"},
	StatFileAgeAtime:            {"bucket", "count"},
	StatFileAgeMtime:            {"bucket", "count"},
	StatFileAgeCtime:            {"bucket", "count"},
	StatDirAgeAtime:             {"bucket", "count"},
	StatDirAgeMtime:             {"bucket", "count"},
	StatDirAgeCtime:             {"bucket", "count"},
}

var validStatNamesHelp = func() string {
	parts := make([]string, len(ValidStatNames))
	for i, s := range ValidStatNames {
		parts[i] = string(s)
	}
	return strings.Join(parts, ", ")
}()

type StatsCfg struct {
	GlobalCfg
	Stat       StatName
	Uid        int
	Recursive  bool
	Cumulative bool
	NumResults int
	Order      string
	Reftime    int64
}

func Stats(ctx context.Context, exec Executor, cfg StatsCfg, indexPath string) ([]string, <-chan []string, func() error, error) {
	spec, err := buildStatsSpec(cfg, indexPath)
	if err != nil {
		return nil, nil, nil, err
	}
	rows, wait, err := exec.Execute(ctx, spec)
	return spec.Columns, rows, wait, err
}

func buildStatsSpec(cfg StatsCfg, indexPath string) (QuerySpec, error) {
	base := QuerySpec{
		IndexRoot:  indexPath,
		Threads:    cfg.Threads,
		PluginPath: QueryPluginPath,
		MaxLevel:   MaxLevelUnlimited,
	}

	uidWhere := ""
	if cfg.Uid >= 0 {
		uidWhere = fmt.Sprintf(" AND uid=%d", cfg.Uid)
	}

	perDirSuffix := buildOrderLimitSuffix(cfg, 2)

	applyEntriesPipeline := func(I, E, K, J, G string) {
		base.SQLInit = I
		base.SQLEntries = E
		base.SQLAggInit = K
		base.SQLIntermed = J
		base.SQLAggregate = G
	}
	applySummaryPipeline := func(I, S, K, J, G string) {
		base.SQLInit = I
		base.SQLSummary = S
		base.SQLAggInit = K
		base.SQLIntermed = J
		base.SQLAggregate = G
	}
	applyPerDirSummary := func(template string) {
		if !cfg.Recursive {
			base.MaxLevel = 0
		}
		base.SQLSummary = fmt.Sprintf(template, perDirSuffix)
	}
	applyEntriesPipelineLevel0 := func(I, E, K, J, G string) {
		if !cfg.Recursive && !cfg.Cumulative {
			base.MaxLevel = 0
		}
		applyEntriesPipeline(I, E, K, J, G)
	}
	applySummaryPipelineLevel0 := func(I, S, K, J, G string) {
		if !cfg.Recursive && !cfg.Cumulative {
			base.MaxLevel = 0
		}
		applySummaryPipeline(I, S, K, J, G)
	}

	switch cfg.Stat {
	case StatTotalFilecount:
		applyEntriesPipeline(StatsTotalFilecountI, fmt.Sprintf(StatsTotalFilecountE, uidWhere),
			StatsTotalFilecountK, StatsTotalFilecountJ, StatsTotalFilecountG)

	case StatTotalDircount:
		applySummaryPipeline(StatsTotalDircountI, fmt.Sprintf(StatsTotalDircountS, uidWhere),
			StatsTotalDircountK, StatsTotalDircountJ, StatsTotalDircountG)

	case StatTotalFilesize:
		applyEntriesPipeline(StatsTotalFilesizeI, fmt.Sprintf(StatsTotalFilesizeE, uidWhere),
			StatsTotalFilesizeK, StatsTotalFilesizeJ, StatsTotalFilesizeG)

	case StatTotalLinkcount:
		applyEntriesPipeline(StatsTotalLinkcountI, fmt.Sprintf(StatsTotalLinkcountE, uidWhere),
			StatsTotalLinkcountK, StatsTotalLinkcountJ, StatsTotalLinkcountG)

	case StatFilecount:
		applyPerDirSummary(StatsFilecountS)
	case StatFilesize:
		applyPerDirSummary(StatsFilesizeS)
	case StatDepth:
		applyPerDirSummary(StatsDepthS)
	case StatLinkcount:
		applyPerDirSummary(StatsLinkcountS)
	case StatDircount:
		applyPerDirSummary(StatsDircountS)

	case StatLeafDirs:
		base.SQLSummary = StatsLeafDirsS + buildOrderLimitSuffix(cfg, 1)
	case StatLeafDepth:
		base.SQLSummary = StatsLeafDepthS + buildOrderLimitSuffix(cfg, 2)
	case StatLeafFiles:
		base.SQLSummary = StatsLeafFilesS + buildOrderLimitSuffix(cfg, 2)
	case StatLeafLinks:
		base.SQLSummary = StatsLeafLinksS + buildOrderLimitSuffix(cfg, 2)

	case StatTotalLeafFiles:
		applySummaryPipeline(StatsTotalLeafFilesI, StatsTotalLeafFilesS,
			StatsTotalLeafFilesK, StatsTotalLeafFilesJ, StatsTotalLeafFilesG)
	case StatTotalLeafLinks:
		applySummaryPipeline(StatsTotalLeafLinksI, StatsTotalLeafLinksS,
			StatsTotalLeafLinksK, StatsTotalLeafLinksJ, StatsTotalLeafLinksG)

	case StatFilesPerLevel:
		applySummaryPipeline(StatsFilesPerLevelI, StatsFilesPerLevelS,
			StatsFilesPerLevelK, StatsFilesPerLevelJ, StatsFilesPerLevelG)
	case StatLinksPerLevel:
		applySummaryPipeline(StatsLinksPerLevelI, StatsLinksPerLevelS,
			StatsLinksPerLevelK, StatsLinksPerLevelJ, StatsLinksPerLevelG)
	case StatDirsPerLevel:
		applySummaryPipeline(StatsDirsPerLevelI, StatsDirsPerLevelS,
			StatsDirsPerLevelK, StatsDirsPerLevelJ, StatsDirsPerLevelG)

	case StatAverageLeafFiles:
		applySummaryPipeline(StatsAverageLeafFilesI, StatsAverageLeafFilesS,
			StatsAverageLeafFilesK, StatsAverageLeafFilesJ, StatsAverageLeafFilesG)
	case StatAverageLeafLinks:
		applySummaryPipeline(StatsAverageLeafLinksI, StatsAverageLeafLinksS,
			StatsAverageLeafLinksK, StatsAverageLeafLinksJ, StatsAverageLeafLinksG)
	case StatAverageLeafSize:
		applySummaryPipeline(StatsAverageLeafSizeI, StatsAverageLeafSizeS,
			StatsAverageLeafSizeK, StatsAverageLeafSizeJ, StatsAverageLeafSizeG)

	case StatMedianLeafFiles:
		applySummaryPipeline(StatsMedianLeafFilesI, StatsMedianLeafFilesS,
			StatsMedianLeafFilesK, StatsMedianLeafFilesJ, StatsMedianLeafFilesG)
	case StatMedianLeafLinks:
		applySummaryPipeline(StatsMedianLeafLinksI, StatsMedianLeafLinksS,
			StatsMedianLeafLinksK, StatsMedianLeafLinksJ, StatsMedianLeafLinksG)
	case StatMedianLeafSize:
		applySummaryPipeline(StatsMedianLeafSizeI, StatsMedianLeafSizeS,
			StatsMedianLeafSizeK, StatsMedianLeafSizeJ, StatsMedianLeafSizeG)

	case StatDuplicateNames:
		applyEntriesPipeline(StatsDuplicateNamesI, StatsDuplicateNamesE,
			StatsDuplicateNamesK, StatsDuplicateNamesJ, StatsDuplicateNamesG)
		base.AddUp = 1
		base.Threads = 1

	case StatUidSize:
		order := normalizeOrder(cfg.Order)
		applyEntriesPipeline(StatsUidSizeI, StatsUidSizeE, StatsUidSizeK, StatsUidSizeJ,
			buildIdSizeG("uid", order, cfg.NumResults))
		base.AddUp = 1

	case StatGidSize:
		order := normalizeOrder(cfg.Order)
		applyEntriesPipeline(StatsGidSizeI, StatsGidSizeE, StatsGidSizeK, StatsGidSizeJ,
			buildIdSizeG("gid", order, cfg.NumResults))
		base.AddUp = 1

	case StatExtensions:
		applyEntriesPipelineLevel0(StatsExtensionsI, StatsExtensionsE,
			StatsExtensionsK, StatsExtensionsJ, StatsExtensionsG)

	case StatFilesizeLog2Bins:
		applyEntriesPipelineLevel0(StatsFilesizeLog2BinsI, StatsFilesizeLog2BinsE,
			StatsFilesizeLog2BinsK, StatsFilesizeLog2BinsJ, StatsFilesizeLog2BinsG)
	case StatFilesizeLog1024Bins:
		applyEntriesPipelineLevel0(StatsFilesizeLog1024BinsI, StatsFilesizeLog1024BinsE,
			StatsFilesizeLog1024BinsK, StatsFilesizeLog1024BinsJ, StatsFilesizeLog1024BinsG)
	case StatDirfilecountLog2Bins:
		applySummaryPipelineLevel0(StatsDirfilecountLog2BinsI, StatsDirfilecountLog2BinsS,
			StatsDirfilecountLog2BinsK, StatsDirfilecountLog2BinsJ, StatsDirfilecountLog2BinsG)
	case StatDirfilecountLog1024Bins:
		applySummaryPipelineLevel0(StatsDirfilecountLog1024BinsI, StatsDirfilecountLog1024BinsS,
			StatsDirfilecountLog1024BinsK, StatsDirfilecountLog1024BinsJ, StatsDirfilecountLog1024BinsG)

	case StatFileAgeAtime:
		applyAgeBins(&base, cfg, "atime", false)
	case StatFileAgeMtime:
		applyAgeBins(&base, cfg, "mtime", false)
	case StatFileAgeCtime:
		applyAgeBins(&base, cfg, "ctime", false)
	case StatDirAgeAtime:
		applyAgeBins(&base, cfg, "atime", true)
	case StatDirAgeMtime:
		applyAgeBins(&base, cfg, "mtime", true)
	case StatDirAgeCtime:
		applyAgeBins(&base, cfg, "ctime", true)

	default:
		return QuerySpec{}, fmt.Errorf("unknown stat %q: valid values are %s", cfg.Stat, validStatNamesHelp)
	}

	base.Columns = statOutputColumns[cfg.Stat]
	return base, nil
}

func normalizeOrder(s string) string {
	o := strings.ToUpper(s)
	if o == "ASC" || o == "DESC" {
		return o
	}
	return "ASC"
}

func buildIdSizeG(field, order string, numResults int) string {
	if numResults > 0 {
		return fmt.Sprintf(
			`SELECT %[1]s, size, name FROM (SELECT %[1]s, size, name, row_number() OVER (PARTITION BY %[1]s ORDER BY size %[2]s, name %[2]s) AS rn FROM %[3]s) WHERE rn <= %[4]d ORDER BY %[1]s %[2]s, size %[2]s, name %[2]s`,
			field, order, statsAggTable, numResults)
	}
	return fmt.Sprintf(
		`SELECT %[1]s, size, name FROM %[2]s ORDER BY %[1]s %[3]s, size %[3]s, name %[3]s`,
		field, statsAggTable, order)
}

func applyAgeBins(base *QuerySpec, cfg StatsCfg, timeCol string, isDir bool) {
	reftime := cfg.Reftime
	if reftime == 0 {
		reftime = time.Now().Unix()
	}
	bucketCase := AgeBucketCaseSQL(reftime, timeCol)

	if !cfg.Recursive && !cfg.Cumulative {
		base.MaxLevel = 0
	}

	base.SQLInit = `CREATE TABLE ` + statsIntermedTable + `(bucket INT64)`
	base.SQLAggInit = `CREATE TABLE ` + statsAggTable + `(bucket INT64, count INT64)`
	base.SQLIntermed = `INSERT INTO ` + statsAggTable +
		` SELECT bucket, COUNT(*) FROM ` + statsIntermedTable + ` GROUP BY bucket`
	base.SQLAggregate = `SELECT ` + AgeBucketLabelSQL + `, SUM(count) FROM ` +
		statsAggTable + ` GROUP BY bucket ORDER BY bucket ASC`

	if isDir {
		base.SQLSummary = `INSERT INTO ` + statsIntermedTable +
			` SELECT ` + bucketCase + ` FROM vrsummary WHERE isroot=1`
	} else {
		base.SQLEntries = `INSERT INTO ` + statsIntermedTable +
			` SELECT ` + bucketCase + ` FROM vrpentries WHERE type='f'`
	}
}

// buildOrderLimitSuffix renders the optional "ORDER BY <orderCol> / LIMIT" tail.
// orderCol is the 1-based result column to sort on: per-directory and most leaf
// stats sort on the value (column 2), but leaf-dirs is name-only, so it sorts on
// column 1.
func buildOrderLimitSuffix(cfg StatsCfg, orderCol int) string {
	var parts []string
	if cfg.Order != "" {
		order := strings.ToUpper(cfg.Order)
		if order == "ASC" || order == "DESC" {
			parts = append(parts, fmt.Sprintf(" ORDER BY %d %s", orderCol, order))
		}
	}
	if cfg.NumResults > 0 {
		parts = append(parts, fmt.Sprintf(" LIMIT %d", cfg.NumResults))
	}
	return strings.Join(parts, "")
}
