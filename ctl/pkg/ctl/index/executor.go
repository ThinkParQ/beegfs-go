package index

import "context"

// MaxLevelUnlimited is the sentinel value for QuerySpec.MaxLevel meaning
// "no --max-level flag" (traverse the entire index tree). Any value >= 0
// maps to an explicit --max-level N flag.
const MaxLevelUnlimited = -1

// QuerySpec describes a GUFI query. The same spec drives all execution modes —
// local gufi_query and remote gufi_vt.
//
// MaxLevel semantics: MaxLevelUnlimited (-1) = no --max-level flag (unlimited);
// 0 = --max-level 0 (root directory only); N > 0 = --max-level N.
// All other integer fields use the zero value as "omit the flag".
type QuerySpec struct {
	IndexRoot    string // positional arg / index= param
	SQLEntries   string // -E: per-entry SQL (runs against each entries table)
	SQLSummary   string // -S: per-directory SQL (runs against each summary table)
	SQLTreeSum   string // -T: tree-summary SQL
	SQLInit      string // -I: initialise the in-memory aggregate DB
	SQLAggInit   string // -K: initialise per-thread aggregate table
	SQLIntermed  string // -J: insert directory result into per-thread aggregate
	SQLAggregate string // -G: insert per-thread aggregate into global aggregate
	SQLFinal     string // -F: final SELECT from aggregate DB
	Threads      int
	MinLevel     int
	MaxLevel     int
	PluginPath   string
	Delimiter    string
}

// Executor runs GUFI queries. All implementations return the repo-standard
// (<-chan ResultT, func() error, error) triplet — same as entry.GetEntries,
// rst.GetStatus, etc.
//
// Callers consume the channel until it is closed, then call errWait() to
// collect any processing error. If Execute returns a non-nil error the
// channel and errWait are both nil.
type Executor interface {
	Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error)
}
