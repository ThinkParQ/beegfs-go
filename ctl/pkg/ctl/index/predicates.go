package index

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PredicateSet accumulates SQL WHERE clauses from command flags and tracks
// which join tier the query requires.
type PredicateSet struct {
	Clauses      []string
	NeedsBeeGFS  bool
	NeedsTargets bool
	NeedsSummary bool // type='d': directories live in summary, not entries
}

// WhereClause joins all accumulated clauses with AND.
// Returns "1" when no clauses have been added, matching everything.
func (p PredicateSet) WhereClause() string {
	if len(p.Clauses) == 0 {
		return "1"
	}
	return strings.Join(p.Clauses, " AND ")
}

func (p *PredicateSet) add(clause string) {
	p.Clauses = append(p.Clauses, clause)
}

// ── Find predicates ──────────────────────────────────────────────────────────

// FindCfg holds backend configuration for the find command.
// Fields map 1:1 to cobra flags — no translation needed in the frontend.
type FindCfg struct {
	// Core POSIX predicates
	Name string
	Size string
	Type string
	Uid  int // exact match; -1 = unset
	Gid  int // exact match; -1 = unset

	// BeeGFS-specific predicates
	EntryID  string
	OwnerID  string
	TargetID string
	BeeGFS   bool

	// Depth control (-1 = unset for MaxDepth; 0 = no minimum for MinDepth)
	MaxDepth int
	MinDepth int

	// Time predicates — day-based (same +/-N convention as GNU find)
	Mtime string // mtime N*24 hours ago
	Atime string // atime N*24 hours ago
	Ctime string // ctime N*24 hours ago

	// Time predicates — minute-based
	Mmin string // mtime N minutes ago
	Amin string // atime N minutes ago
	Cmin string // ctime N minutes ago

	// Newer-than predicates (Unix timestamps; 0 = unset; set by frontend after os.Lstat)
	NewerThan int64 // mtime > this timestamp
	AnewThan  int64 // atime > this timestamp
	CnewThan  int64 // ctime > this timestamp

	// Name / path predicates
	Iname  string // case-insensitive name match (REGEXP with (?i))
	Path   string // full path GLOB (rpath(sname,sroll,name))
	Regex  string // full path regex
	Iregex string // case-insensitive full path regex
	Lname  string // symlink target GLOB (implies type='l')

	// Identity predicates (+/-N notation)
	Inum  string // inode number
	Links string // nlink count

	// Permission predicates
	Executable bool // (mode & 64) = 64  (0100 octal)
	Readable   bool // (mode & 256) = 256 (0400 octal)
	Writable   bool // (mode & 128) = 128 (0200 octal)

	// State predicates
	Empty bool // type='f' AND size=0

	// Output control
	NumResults int  // LIMIT N per directory (0 = no limit)
	Smallest   bool // ORDER BY size ASC
	Largest    bool // ORDER BY size DESC
}

// BuildFindPredicates converts FindCfg flags to SQL WHERE clauses.
func BuildFindPredicates(cfg FindCfg) PredicateSet {
	var p PredicateSet

	// Core predicates
	if cfg.Name != "" {
		p.add(fmt.Sprintf("name GLOB %s", sqlQuote(cfg.Name)))
	}
	if cfg.Size != "" {
		if clause, err := parseSizeClause("size", cfg.Size); err == nil {
			p.add(clause)
		}
	}
	if cfg.Type == "d" {
		p.NeedsSummary = true // directories live in summary, not entries
	} else if cfg.Type != "" {
		p.add(fmt.Sprintf("type = %s", sqlQuote(cfg.Type)))
	}
	if cfg.Uid >= 0 {
		p.add(fmt.Sprintf("uid = %d", cfg.Uid))
	}
	if cfg.Gid >= 0 {
		p.add(fmt.Sprintf("gid = %d", cfg.Gid))
	}

	// BeeGFS predicates
	if cfg.EntryID != "" {
		p.add(fmt.Sprintf("b.entry_id GLOB %s", sqlQuote(cfg.EntryID)))
		p.NeedsBeeGFS = true
	}
	if cfg.OwnerID != "" {
		if id, err := strconv.ParseInt(cfg.OwnerID, 10, 64); err == nil {
			p.add(fmt.Sprintf("b.owner_id = %d", id))
		} else {
			p.add(fmt.Sprintf("b.owner_id GLOB %s", sqlQuote(cfg.OwnerID)))
		}
		p.NeedsBeeGFS = true
	}
	if cfg.TargetID != "" {
		if id, err := strconv.ParseInt(cfg.TargetID, 10, 64); err == nil {
			p.add(fmt.Sprintf("t.target_or_group = %d", id))
		} else {
			p.add(fmt.Sprintf("t.target_or_group GLOB %s", sqlQuote(cfg.TargetID)))
		}
		p.NeedsTargets = true
	}

	// Day-based time predicates
	if cfg.Mtime != "" {
		if clause, err := parseDayClause("mtime", cfg.Mtime); err == nil {
			p.add(clause)
		}
	}
	if cfg.Atime != "" {
		if clause, err := parseDayClause("atime", cfg.Atime); err == nil {
			p.add(clause)
		}
	}
	if cfg.Ctime != "" {
		if clause, err := parseDayClause("ctime", cfg.Ctime); err == nil {
			p.add(clause)
		}
	}

	// Minute-based time predicates
	if cfg.Mmin != "" {
		if clause, err := parseMinuteClause("mtime", cfg.Mmin); err == nil {
			p.add(clause)
		}
	}
	if cfg.Amin != "" {
		if clause, err := parseMinuteClause("atime", cfg.Amin); err == nil {
			p.add(clause)
		}
	}
	if cfg.Cmin != "" {
		if clause, err := parseMinuteClause("ctime", cfg.Cmin); err == nil {
			p.add(clause)
		}
	}

	// Newer-than predicates
	if cfg.NewerThan > 0 {
		p.add(fmt.Sprintf("mtime > %d", cfg.NewerThan))
	}
	if cfg.AnewThan > 0 {
		p.add(fmt.Sprintf("atime > %d", cfg.AnewThan))
	}
	if cfg.CnewThan > 0 {
		p.add(fmt.Sprintf("ctime > %d", cfg.CnewThan))
	}

	// Name / path predicates
	if cfg.Iname != "" {
		p.add(fmt.Sprintf("name REGEXP %s", sqlQuote("(?i)"+cfg.Iname)))
	}
	if cfg.Path != "" {
		p.add(fmt.Sprintf("rpath(sname, sroll, name) GLOB %s", sqlQuote(cfg.Path)))
	}
	if cfg.Regex != "" {
		p.add(fmt.Sprintf("rpath(sname, sroll, name) REGEXP %s", sqlQuote(cfg.Regex)))
	}
	if cfg.Iregex != "" {
		p.add(fmt.Sprintf("rpath(sname, sroll, name) REGEXP %s", sqlQuote("(?i)"+cfg.Iregex)))
	}
	if cfg.Lname != "" {
		p.add(fmt.Sprintf("type = 'l' AND linkname GLOB %s", sqlQuote(cfg.Lname)))
	}

	// Identity predicates
	if cfg.Inum != "" {
		if clause, err := parseNumericClause("inode", cfg.Inum); err == nil {
			p.add(clause)
		}
	}
	if cfg.Links != "" {
		if clause, err := parseNumericClause("nlink", cfg.Links); err == nil {
			p.add(clause)
		}
	}

	// Permission predicates (decimal equivalents of octal constants)
	if cfg.Executable {
		p.add("(mode & 64) = 64")
	}
	if cfg.Readable {
		p.add("(mode & 256) = 256")
	}
	if cfg.Writable {
		p.add("(mode & 128) = 128")
	}

	// State predicates
	if cfg.Empty {
		p.add("type = 'f' AND size = 0")
	}

	return p
}

// ── Ls predicates ────────────────────────────────────────────────────────────

// LsCfg holds backend configuration for the ls command.
type LsCfg struct {
	// Filter predicates
	Name         string
	Type         string
	All          bool // show hidden files (override default dotfile filter)
	AlmostAll    bool // equivalent to All in GUFI context (. and .. are not indexed)
	IgnoreBackups bool // ignore entries ending with ~

	// Display options
	BeeGFS    bool
	Long      bool // long listing (show all columns as defaults)
	ShowInode bool // include inode column

	// Traversal
	Recursive bool // recursive listing (no MaxLevel constraint)

	// Sorting
	Reverse     bool // reverse sort order
	SortBySize  bool // sort by file size, largest first
	SortByMtime bool // sort by modification time, newest first

	// Output control
	NumResults int // LIMIT N (0 = no limit)
}

// BuildLsPredicates converts LsCfg flags to SQL WHERE clauses.
func BuildLsPredicates(cfg LsCfg) PredicateSet {
	var p PredicateSet

	// Hidden file filtering: hide dotfiles by default unless --all or --almost-all.
	if !cfg.All && !cfg.AlmostAll {
		p.add("name NOT LIKE '.%'")
	}
	if cfg.IgnoreBackups {
		p.add("name NOT LIKE '%~'")
	}
	if cfg.Name != "" {
		p.add(fmt.Sprintf("name GLOB %s", sqlQuote(cfg.Name)))
	}
	if cfg.Type != "" {
		p.add(fmt.Sprintf("type = %s", sqlQuote(cfg.Type)))
	}

	return p
}

// BuildLsDirPredicates builds WHERE clauses for the summary table in non-recursive ls.
// Identical to BuildLsPredicates but omits the type filter: summary only holds
// directories, so filtering by type would either be redundant or produce no rows.
func BuildLsDirPredicates(cfg LsCfg) PredicateSet {
	var p PredicateSet
	if !cfg.All && !cfg.AlmostAll {
		p.add("name NOT LIKE '.%'")
	}
	if cfg.IgnoreBackups {
		p.add("name NOT LIKE '%~'")
	}
	if cfg.Name != "" {
		p.add(fmt.Sprintf("name GLOB %s", sqlQuote(cfg.Name)))
	}
	return p
}

// ── Stat predicates ──────────────────────────────────────────────────────────

// StatCfg holds backend configuration for the stat command.
type StatCfg struct {
	Filename string // basename of the entry to stat
	BeeGFS   bool
}

// BuildStatPredicates converts StatCfg to a WHERE clause (name = 'basename').
func BuildStatPredicates(cfg StatCfg) PredicateSet {
	var p PredicateSet
	if cfg.Filename != "" {
		p.add(fmt.Sprintf("name = %s", sqlQuote(cfg.Filename)))
	}
	return p
}

// ── helpers ──────────────────────────────────────────────────────────────────

// sqlQuote wraps s in single quotes, escaping any embedded single quotes.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// parseDayClause converts a find-style day-based expression to a SQL clause.
//
//	-7  → "col > <7 days ago>"  (changed less than 7 days ago)
//	+7  → "col < <7 days ago>"  (changed more than 7 days ago)
//	7   → "col = <7 days ago>"  (changed exactly 7*24h ago)
func parseDayClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = "<"
		s = s[1:]
	case '-':
		op = ">"
		s = s[1:]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid day expression %q: %w", expr, err)
	}

	ts := time.Now().AddDate(0, 0, -int(n)).Unix()
	return fmt.Sprintf("%s %s %d", col, op, ts), nil
}

// parseSizeClause converts a find-style size expression to a SQL clause.
//
//	+1G  → "size > 1073741824"
//	-100k → "size < 102400"
//	5M   → "size = 5242880"
func parseSizeClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty size expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = ">"
		s = s[1:]
	case '-':
		op = "<"
		s = s[1:]
	}

	if s == "" {
		return "", fmt.Errorf("empty size value after operator")
	}

	// Determine unit suffix.
	var mult int64 = 1
	last := s[len(s)-1]
	switch last {
	case 'c', 'C':
		s = s[:len(s)-1] // bytes — mult stays 1
	case 'k', 'K':
		mult = 1024
		s = s[:len(s)-1]
	case 'M':
		mult = 1024 * 1024
		s = s[:len(s)-1]
	case 'G':
		mult = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case 'T':
		mult = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid size %q: %w", expr, err)
	}

	return fmt.Sprintf("%s %s %d", col, op, n*mult), nil
}

// parseMinuteClause converts a find-style minute-based expression to a SQL clause.
//
//	-10  → "col > <10 minutes ago>"  (changed less than 10 minutes ago)
//	+10  → "col < <10 minutes ago>"  (changed more than 10 minutes ago)
//	10   → "col = <10 minutes ago>"
func parseMinuteClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = "<"
		s = s[1:]
	case '-':
		op = ">"
		s = s[1:]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid minute expression %q: %w", expr, err)
	}

	ts := time.Now().Add(-time.Duration(n) * time.Minute).Unix()
	return fmt.Sprintf("%s %s %d", col, op, ts), nil
}

// parseNumericClause converts a find-style numeric expression to a SQL clause.
//
//	+5  → "col > 5"
//	-5  → "col < 5"
//	5   → "col = 5"
func parseNumericClause(col, expr string) (string, error) {
	if expr == "" {
		return "", fmt.Errorf("empty expression")
	}

	op := "="
	s := expr
	switch s[0] {
	case '+':
		op = ">"
		s = s[1:]
	case '-':
		op = "<"
		s = s[1:]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid numeric expression %q: %w", expr, err)
	}

	return fmt.Sprintf("%s %s %d", col, op, n), nil
}
