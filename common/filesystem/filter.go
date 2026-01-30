package filesystem

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/expr-lang/expr"
)

type FileInfo struct {
	Path  string    // Full file path
	Name  string    // Base name of the file
	Size  int64     // File size in bytes
	Mode  uint32    // raw mode bits from syscall.Stat_t (type + permissions)
	Perm  uint32    // just the permission bits (mode & 0777)
	Mtime time.Time // Modification time
	Atime time.Time // Access time
	Ctime time.Time // Change time
	Uid   uint32    // User ID
	Gid   uint32    // Group ID
}

// File type filter expressions and definitions
var (
	fileTypeMask  = 0o170000
	fileTypes     = map[string]uint32{"file": 0o100000, "directory": 0o040000, "symlink": 0o120000, "block": 0o060000, "char": 0o020000, "fifo": 0o010000, "socket": 0o140000}
	fileTypeNames = func() []string {
		names := make([]string, 0, len(fileTypes))
		for name := range fileTypes {
			names = append(names, name)
		}
		return names
	}()
	fileTypeGroupRe = "(?:" + strings.Join(fileTypeNames, "|") + ")"
	fileTypeRe      = regexp.MustCompile(`\b(?i)type\s*(==|!=)\s*(` + fileTypeGroupRe + `(?:\s*,\s*` + fileTypeGroupRe + `)*)\b`)
)

var (
	// modeOctRe insists on a leading 0 with 5-6 octal digits. This targets classic stat-style
	// literals like 0100644 without touching plain decimal values like 33188. The rewrite is scoped
	// to mode to avoid clobbering other fields that might also match.
	modeOctRe = regexp.MustCompile(`\b(?i)(mode)\s*(==|!=|<=|>=|<|>)\s*(0[0-7]{5,6})\b`)
	// permOctRe allows 3-4 digits with an optional leading zero. This matches the chmod style
	// notation users expect for permissions like 644 or 01644. Because the rewrite is scoped to
	// perm, relaxing the prefix doesn't risk clobbering arbitrary decimal limits (like sizes).
	permOctRe = regexp.MustCompile(`\b(?i)(perm)\s*(==|!=|<=|>=|<|>)\s*(0?[0-7]{3,4})\b`)
	timeRe    = regexp.MustCompile(`\b(?i)(mtime|atime|ctime)\s*(<=|>=|<|>)\s*([0-9]+(?:\.[0-9]+)?[smhdMyw]+)\b`)
	sizeRe    = regexp.MustCompile(`\b(?i)(size)\s*(<=|>=|<|>|!=|=)\s*([0-9]+(?:\.[0-9]+)?(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB))\b`)
	identRe   = regexp.MustCompile(`\b(?i)(mtime|atime|ctime|size|name|uid|gid|path|mode|perm)\b`)
	fieldMap  = map[string]string{
		"mtime": "Mtime", "atime": "Atime", "ctime": "Ctime",
		"size": "Size", "name": "Name", "uid": "Uid", "gid": "Gid",
		"path": "Path", "mode": "Mode", "perm": "Perm",
	}
	unitFactors = map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
	}
)

const FilterFilesHelp = "Filter files by expression: fields(name/path <string>, uid/gid <int>, " +
	"mode <octal[like 0100644, 0o0100644] | decimal[like 33188]>, perm <octal[like 644, 0644, 0o0644]>, " +
	"type <file|directory|symlink|block|char|fifo|socket>, " +
	"mtime/atime/ctime <duration[like 1s, 2m, 3h, 4d, 5M, 10y]>, size <bytes[like 1B, 2KB, 3MiB, 4GiB]>); " +
	"operators(==,!=,<,>,<=,>=); helpers(glob([name|path], pattern), regex([name|path], pattern)); " +
	"logic(and|or|not); Example: --filter-files=\"mtime > 365d and type == file,symlink and glob(name, '*.txt')\""

type FileInfoFilter func(FileInfo) (bool, error)

// CompileFilter turns a DSL expression into a filter function.
func CompileFilter(query string) (FileInfoFilter, error) {
	// Preprocess DSL (includes octal normalization now)
	q := preprocessDSL(query)

	prog, err := expr.Compile(q,
		expr.Env(FileInfo{}),
		expr.Function("ago", func(params ...any) (any, error) { return ago(params[0].(string)) }),
		expr.Function("bytes", func(params ...any) (any, error) { return parseBytes(params[0].(string)) }),
		expr.Function("glob", func(params ...any) (any, error) { return globMatch(params[0].(string), params[1].(string)) }),
		expr.Function("regex", func(params ...any) (any, error) { return regexMatch(params[0].(string), params[1].(string)) }),
		expr.Function("now", func(params ...any) (any, error) { return time.Now(), nil }),
	)
	if err != nil {
		return nil, err
	}

	return func(fi FileInfo) (bool, error) {
		out, err := expr.Run(prog, fi)
		if err != nil {
			return false, fmt.Errorf("filter eval %q on %s: %w", query, fi.Path, err)
		}
		result, ok := out.(bool)
		if !ok {
			return false, fmt.Errorf("filter expression resulted in a non-boolean value of type %T. Make sure your filter is a valid comparison (e.g., 'size>100MB')", out)
		}

		return result, nil
	}, nil
}

// preprocessDSL applies all DSL→Go rewrites, including octal normalization.
func preprocessDSL(q string) string {
	// octal to decimal
	q = normalizeOctal(q, permOctRe)
	q = normalizeOctal(q, modeOctRe)
	// file types
	q = setFileType(q)
	// time shifts
	q = timeRe.ReplaceAllStringFunc(q, func(m string) string {
		parts := timeRe.FindStringSubmatch(m)
		f, op, val := strings.ToLower(parts[1]), parts[2], parts[3]
		if goF, ok := fieldMap[f]; ok {
			switch op {
			case ">":
				op = "<"
			case "<":
				op = ">"
			case ">=":
				op = "<="
			case "<=":
				op = ">="
			}
			return fmt.Sprintf("%s %s ago(%q)", goF, op, val)
		}
		return m
	})
	// size units
	q = sizeRe.ReplaceAllString(q, `$1 $2 bytes("$3")`)
	// identifiers
	q = identRe.ReplaceAllStringFunc(q, func(s string) string {
		if goF, ok := fieldMap[strings.ToLower(s)]; ok {
			return goF
		}
		return s
	})
	return q
}

// setFileType transform a type expressions one or more into mode filters to include or exclude
// specific file types.
func setFileType(q string) string {
	return fileTypeRe.ReplaceAllStringFunc(q, func(expr string) string {
		parts := fileTypeRe.FindStringSubmatch(expr)
		op, types := parts[1], parts[2]

		used := make(map[string]struct{})
		clauses := []string{}
		for name := range strings.SplitSeq(types, ",") {
			name = strings.TrimSpace(strings.ToLower(name))
			if _, ok := used[name]; ok {
				continue
			}
			if _, ok := fileTypes[name]; !ok {
				return expr
			}
			used[name] = struct{}{}
			clauses = append(clauses, fmt.Sprintf("bitand(int(Mode), %d) == %d", fileTypeMask, fileTypes[name]))
		}

		expr = "(" + strings.Join(clauses, " or ") + ")"
		if op == "!=" {
			return fmt.Sprintf("not %s", expr)
		}
		return expr
	})
}

// normalizeOctal takes a string and parses it to a base8 integer after stripping an optional 0o
// prefix or leading 0. For example 644, 0644, and 0o644 are all converted to 420.
func normalizeOctal(q string, re *regexp.Regexp) string {
	return re.ReplaceAllStringFunc(q, func(expr string) string {
		sub := re.FindStringSubmatch(expr)
		if len(sub) != 4 {
			return expr
		}
		field, op, lit := sub[1], sub[2], sub[3]
		if strings.HasPrefix(lit, "0o") || strings.HasPrefix(lit, "0O") {
			lit = lit[2:]
		} else if strings.HasPrefix(lit, "0") {
			lit = lit[1:]
		}
		val, err := strconv.ParseInt(lit, 8, 64)
		if err != nil {
			return expr
		}
		return fmt.Sprintf("%s %s %d", field, op, val)
	})
}

func StatToFileInfo(path string, st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Path:  path,
		Name:  filepath.Base(path),
		Size:  st.Size,
		Mode:  st.Mode,
		Perm:  st.Mode & 0o7777, // special permissions bit + os.ModePerm
		Atime: time.Unix(st.Atim.Sec, st.Atim.Nsec),
		Mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Ctime: time.Unix(st.Ctim.Sec, st.Ctim.Nsec),
		Uid:   st.Uid,
		Gid:   st.Gid,
	}
}

// ago returns time.Now() minus parsed duration.
func ago(durationStr string) (time.Time, error) {
	d, err := parseExtendedDuration(durationStr)
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(-d), nil
}

// parseExtendedDuration supports standard and custom units (d, M, y).
func parseExtendedDuration(s string) (time.Duration, error) {
	// fast path for Go durations
	sfx := s[len(s)-1]
	if strings.IndexByte("nsmh", sfx) != -1 {
		return time.ParseDuration(s)
	}
	var factor time.Duration
	num, unit := s[:len(s)-1], s[len(s)-1:]
	switch unit {
	case "d":
		factor = 24 * time.Hour
	case "M":
		factor = 30 * 24 * time.Hour
	case "y":
		factor = 365 * 24 * time.Hour
	default:
		return time.ParseDuration(s)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return time.Duration(f * float64(factor)), nil
}

// parseBytes converts size strings into byte counts.
func parseBytes(sizeStr string) (int64, error) {
	i := len(sizeStr)
	for i > 0 && (sizeStr[i-1] < '0' || sizeStr[i-1] > '9') {
		i--
	}
	num, unit := sizeStr[:i], strings.TrimSpace(sizeStr[i:])
	if unit == "" {
		unit = "B"
	}
	mul, ok := unitFactors[unit]
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q", unit)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", sizeStr, err)
	}
	return int64(f * mul), nil
}

// globMatch uses filepath.Match
func globMatch(s, pattern string) (bool, error) {
	return filepath.Match(pattern, s)
}

// regexMatch uses precompiled regex
func regexMatch(s, pattern string) (bool, error) {
	return regexp.MatchString(pattern, s)
}

// ApplyFilter returns whether the file should be kept. If filter==nil then (true, nil) will be
// returned.
//
// Use ApplyFilterByStatT instead of ApplyFilter when the in-mount path and file
// stat information is already known to avoid making duplicate stat calls.
func ApplyFilter(inMountPath string, filter FileInfoFilter, client Provider) (keep bool, err error) {
	if filter == nil {
		return true, nil
	}

	info, err := client.Lstat(inMountPath)
	if err != nil {
		return false, fmt.Errorf("unable to filter file: %w", err)
	}

	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unable to retrieve stat information: unsupported platform")
	}

	if keep, err = filter(StatToFileInfo(inMountPath, statT)); err != nil {
		return false, fmt.Errorf("unable to apply filter: %w", err)
	}
	return
}

// ApplyFilterByStatT returns whether the file should be kept. If filter==nil then (true, nil) will
// be returned.
//
// ApplyFilterByStatT should be used instead of ApplyFilter when the in-mount path and stat are
// already known to avoid duplicate stat calls.
func ApplyFilterByStatT(inMountPath string, statT *syscall.Stat_t, filter FileInfoFilter) (keep bool, err error) {
	if filter == nil {
		return true, nil
	}

	if keep, err = filter(StatToFileInfo(inMountPath, statT)); err != nil {
		return false, fmt.Errorf("unable to apply filter: %w", err)
	}
	return
}
