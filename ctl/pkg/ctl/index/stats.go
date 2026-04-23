package index

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// StatName is the type of aggregate statistic to compute.
type StatName string

const (
	StatTotalFilecount StatName = "total-filecount"
	StatTotalDircount  StatName = "total-dircount"
	StatTotalFilesize  StatName = "total-filesize"
	StatTotalLinkcount StatName = "total-linkcount"
	StatFilecount      StatName = "filecount"
	StatFilesize       StatName = "filesize"
	StatDepth          StatName = "depth"
)

// StatsCfg holds backend configuration for the stats command.
type StatsCfg struct {
	Stat       StatName
	Uid        int    // filter by uid (-1 = no filter)
	Recursive  bool   // for per-dir stats: traverse all subdirectories
	NumResults int    // limit results (0 = no limit)
	Order      string // sort order for per-dir stats: "ASC" or "DESC"
}

// Stats computes an aggregate statistic over the GUFI index tree.
// The stat name selects which SQL aggregation pipeline to run.
func Stats(ctx context.Context, exec Executor, cfg StatsCfg, indexPath string) (<-chan []string, func() error, error) {
	spec, err := buildStatsSpec(cfg, indexPath)
	if err != nil {
		return nil, nil, err
	}
	return exec.Execute(ctx, spec)
}

func buildStatsSpec(cfg StatsCfg, indexPath string) (QuerySpec, error) {
	base := QuerySpec{
		IndexRoot:  indexPath,
		Threads:    viper.GetInt(ThreadsKey),
		PluginPath: QueryPluginPath,
		Delimiter:  "|",
		MaxLevel:   MaxLevelUnlimited,
	}

	// Build optional uid WHERE suffix for aggregation J steps.
	uidWhere := ""
	if cfg.Uid >= 0 {
		uidWhere = fmt.Sprintf(" AND uid=%d", cfg.Uid)
	}

	// Build optional ORDER BY / LIMIT suffix for per-directory summary queries.
	perDirSuffix := buildPerDirSuffix(cfg)

	switch cfg.Stat {
	case StatTotalFilecount:
		base.SQLInit = StatsTotalFilecountI
		base.SQLAggInit = StatsTotalFilecountK
		base.SQLIntermed = fmt.Sprintf(StatsTotalFilecountJ, uidWhere)
		base.SQLAggregate = StatsTotalFilecountG
		base.SQLFinal = StatsTotalFilecountF

	case StatTotalDircount:
		// uid filter on summary table is less meaningful for dircount,
		// but honour it for consistency (summary.uid = directory owner).
		dirWhere := ""
		if cfg.Uid >= 0 {
			dirWhere = fmt.Sprintf(" WHERE uid=%d", cfg.Uid)
		}
		base.SQLInit = StatsTotalDircountI
		base.SQLAggInit = StatsTotalDircountK
		base.SQLIntermed = fmt.Sprintf(StatsTotalDircountJ, dirWhere)
		base.SQLAggregate = StatsTotalDircountG
		base.SQLFinal = StatsTotalDircountF

	case StatTotalFilesize:
		base.SQLInit = StatsTotalFilesizeI
		base.SQLAggInit = StatsTotalFilesizeK
		base.SQLIntermed = fmt.Sprintf(StatsTotalFilesizeJ, uidWhere)
		base.SQLAggregate = StatsTotalFilesizeG
		base.SQLFinal = StatsTotalFilesizeF

	case StatTotalLinkcount:
		base.SQLInit = StatsTotalLinkcountI
		base.SQLAggInit = StatsTotalLinkcountK
		base.SQLIntermed = fmt.Sprintf(StatsTotalLinkcountJ, uidWhere)
		base.SQLAggregate = StatsTotalLinkcountG
		base.SQLFinal = StatsTotalLinkcountF

	case StatFilecount:
		if !cfg.Recursive {
			base.MaxLevel = 0
		}
		base.SQLSummary = fmt.Sprintf(StatsFilecountS, perDirSuffix)

	case StatFilesize:
		if !cfg.Recursive {
			base.MaxLevel = 0
		}
		base.SQLSummary = fmt.Sprintf(StatsFilesizeS, perDirSuffix)

	case StatDepth:
		if !cfg.Recursive {
			base.MaxLevel = 0
		}
		base.SQLSummary = fmt.Sprintf(StatsDepthS, perDirSuffix)

	default:
		return QuerySpec{}, fmt.Errorf(
			"unknown stat %q: valid values are total-filecount, total-dircount, total-filesize, "+
				"total-linkcount, filecount, filesize, depth",
			cfg.Stat,
		)
	}

	return base, nil
}

// buildPerDirSuffix returns an optional ORDER BY / LIMIT clause for per-directory stats.
func buildPerDirSuffix(cfg StatsCfg) string {
	var parts []string
	if cfg.Order != "" {
		order := strings.ToUpper(cfg.Order)
		if order == "ASC" || order == "DESC" {
			parts = append(parts, fmt.Sprintf(" ORDER BY 2 %s", order)) // column 2 is the value
		}
	}
	if cfg.NumResults > 0 {
		parts = append(parts, fmt.Sprintf(" LIMIT %d", cfg.NumResults))
	}
	return strings.Join(parts, "")
}
