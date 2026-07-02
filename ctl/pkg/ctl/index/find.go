package index

import (
	"context"
	"fmt"
)

func Find(ctx context.Context, exec Executor, cfg FindCfg, walkRoot, displayRoot, fsPath string, glob bool) (<-chan []string, func() error, error) {
	preds := BuildFindPredicates(cfg)
	addPathPredicates(&preds, cfg, displayRoot, fsPath)
	if err := preds.Err(); err != nil {
		return nil, nil, err
	}
	if preds.NeedsSummary && (preds.NeedsBeeGFS || preds.NeedsTargets || cfg.BeeGFS) {
		return nil, nil, fmt.Errorf("--type d cannot be combined with --beegfs/--entryID/--ownerID/--targetID: BeeGFS metadata is stored per file, not per directory")
	}
	if preds.Empty && (preds.NeedsBeeGFS || preds.NeedsTargets || cfg.BeeGFS) {
		return nil, nil, fmt.Errorf("--empty cannot be combined with --beegfs/--entryID/--ownerID/--targetID: the aggregation emits only core columns, so the BeeGFS columns would be blank")
	}
	if preds.Empty && (cfg.Smallest || cfg.Largest) {
		return nil, nil, fmt.Errorf("--empty cannot be combined with --smallest/--largest")
	}
	// --smallest/--largest order globally across all entries; the BeeGFS/targets
	// paths sort per directory instead, so reject the combination rather than
	// silently degrading to a per-directory sort.
	if (cfg.Smallest || cfg.Largest) && (preds.NeedsBeeGFS || cfg.BeeGFS || preds.NeedsTargets) {
		return nil, nil, fmt.Errorf("--smallest/--largest cannot be combined with --beegfs/--entryID/--ownerID/--targetID: global size ordering is not supported alongside BeeGFS metadata columns")
	}
	if preds.Empty && cfg.Type == "l" {
		return staticRows(nil)
	}

	spec := QuerySpec{
		IndexRoot:     walkRoot,
		IndexRootGlob: glob,
		Threads:       cfg.Threads,
		PluginPath:    QueryPluginPath,
		MaxLevel:      MaxLevelUnlimited,
	}
	if cfg.MaxDepth != nil {
		spec.MaxLevel = *cfg.MaxDepth
	}
	if cfg.MinDepth != nil {
		spec.MinLevel = *cfg.MinDepth
	}

	aggGSel := fmt.Sprintf(FindAggGSelect, ownerCols("", cfg.ResolveOwnerNames))

	if preds.Empty {
		spec.SQLInit = FindAggI
		spec.SQLAggInit = FindAggK
		spec.SQLIntermed = FindAggJ
		spec.SQLAggregate = appendLimit(aggGSel, cfg.NumResults)
		if cfg.Type != "d" {
			spec.SQLEntries = FindAggESelect + " WHERE " + preds.entriesWhere() + " AND type='f' AND size=0"
		}
		if cfg.Type != "f" {
			spec.SQLSummary = FindAggSSelect + " WHERE isroot=1 AND " + preds.summaryWhere() + " AND (totfiles+totlinks)=0 AND nlink=2"
		}
		if spec.SQLEntries != "" && spec.SQLSummary != "" {
			spec.AddUp = 1
		}
		// FindAggGSelect (and the FindDirS summary feeding it) emit the 12 core
		// columns; declare them so the remote executor validates row arity.
		spec.Columns = findCoreColumns
	} else if preds.NeedsSummary {
		if cfg.Smallest || cfg.Largest {
			order := "DESC"
			if cfg.Smallest {
				order = "ASC"
			}
			dirSQL := fmt.Sprintf(FindAggDirSSelect, preds.summaryWhere()) + " ORDER BY size " + order
			spec.SQLInit = FindAggI
			spec.SQLSummary = appendLimit(dirSQL, cfg.NumResults)
			spec.SQLAggInit = FindAggK
			spec.SQLIntermed = FindAggJ
			spec.SQLAggregate = appendLimit(aggGSel+" ORDER BY size "+order, cfg.NumResults)
		} else {
			spec.SQLSummary = appendLimit(fmt.Sprintf(FindDirS, ownerCols("", cfg.ResolveOwnerNames), preds.summaryWhere()), cfg.NumResults)
		}
		// Both NeedsSummary paths emit the 12 core columns (FindAggGSelect /
		// FindDirS); declare them for remote arity validation.
		spec.Columns = findCoreColumns
	} else if cfg.Smallest || cfg.Largest {
		// Reaching here with --smallest/--largest implies no BeeGFS/targets columns:
		// that combination is rejected at the top of Find, and the Empty/NeedsSummary
		// branches above handle their own ordering.
		order := "DESC"
		if cfg.Smallest {
			order = "ASC"
		}
		entriesSQL := FindAggESelect + " WHERE " + preds.entriesWhere() + " ORDER BY size " + order
		entriesSQL = appendLimit(entriesSQL, cfg.NumResults)
		aggSQL := aggGSel + " ORDER BY size " + order
		aggSQL = appendLimit(aggSQL, cfg.NumResults)
		spec.SQLInit = FindAggI
		spec.SQLEntries = entriesSQL
		spec.SQLAggInit = FindAggK
		spec.SQLIntermed = FindAggJ
		spec.SQLAggregate = aggSQL
		// Aggregate output is the 12 core columns (FindAggGSelect).
		spec.Columns = findCoreColumns
	} else {
		// --smallest/--largest with BeeGFS/targets is rejected above, so no sort
		// is appended here.
		spec.SQLEntries = appendLimit(findEntriesSQL(cfg, preds), cfg.NumResults)
		spec.Columns = findEntriesColumns(cfg, preds)
	}

	return exec.Execute(ctx, spec)
}

// findEntriesColumns returns the declared output columns for the entries
// template findEntriesSQL selects, in SELECT order. Set as QuerySpec.Columns so
// the remote executor can validate row arity (catching a path value with an
// embedded separator/newline even in a single-row result).
func findEntriesColumns(cfg FindCfg, preds PredicateSet) []string {
	switch {
	case preds.NeedsTargets:
		return findTargetsColumns
	case preds.NeedsBeeGFS || cfg.BeeGFS:
		return findBeeGFSColumns
	default:
		return findCoreColumns
	}
}

// Raw output columns of the find entries templates, in SELECT order (see
// FindCoreE/FindBeeGFSE/FindTargetsE in templates.go). ParseFindRow reads the
// same positions.
var (
	findCoreColumns    = []string{"path", "name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink"}
	findBeeGFSColumns  = []string{"path", "name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "owner_id", "parent_entry_id", "entry_id", "stripe_num_targets"}
	findTargetsColumns = []string{"path", "name", "type", "inode", "size", "mtime", "atime", "ctime", "mode", "uid", "gid", "nlink", "owner_id", "parent_entry_id", "entry_id", "stripe_num_targets", "target_or_group"}
)

// findEntriesSQL renders the per-entry SQL for a find. POSIX predicates go
// into the vrpentries subquery; BeeGFS predicates (b.* / t.*) go into the
// outer WHERE after the plugin-view join, where those aliases are visible.
func findEntriesSQL(cfg FindCfg, preds PredicateSet) string {
	// The path matchers reference rpath(sname, sroll, name), whose columns live in
	// the vrpentries subquery, so they go into the first %s (the vrpentries WHERE),
	// never the outer b.*/t.* position.
	r := cfg.ResolveOwnerNames
	switch {
	case preds.NeedsTargets:
		return fmt.Sprintf(FindTargetsE, ownerCols("v.", r), preds.entriesWhere(), preds.BeeGFSWhereClause())
	case preds.NeedsBeeGFS || cfg.BeeGFS:
		return fmt.Sprintf(FindBeeGFSE, ownerCols("v.", r), preds.entriesWhere(), preds.BeeGFSWhereClause())
	default:
		return fmt.Sprintf(FindCoreE, ownerCols("", r), preds.entriesWhere())
	}
}
