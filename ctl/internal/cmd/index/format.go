package index

import (
	"fmt"
	"os/user"
	"strconv"
	"time"
)

// modeString converts a raw Unix mode integer (decimal string) and GUFI type char
// to a 10-character ls-style permission string (e.g., "-rw-r--r--").
func modeString(modeStr, typeChar string) string {
	mode, err := strconv.ParseInt(modeStr, 10, 64)
	if err != nil {
		return "?---------"
	}
	buf := [10]byte{'-', '-', '-', '-', '-', '-', '-', '-', '-', '-'}
	switch typeChar {
	case "d":
		buf[0] = 'd'
	case "l":
		buf[0] = 'l'
	}
	if mode&0400 != 0 {
		buf[1] = 'r'
	}
	if mode&0200 != 0 {
		buf[2] = 'w'
	}
	if mode&0100 != 0 {
		buf[3] = 'x'
	}
	if mode&0040 != 0 {
		buf[4] = 'r'
	}
	if mode&0020 != 0 {
		buf[5] = 'w'
	}
	if mode&0010 != 0 {
		buf[6] = 'x'
	}
	if mode&0004 != 0 {
		buf[7] = 'r'
	}
	if mode&0002 != 0 {
		buf[8] = 'w'
	}
	if mode&0001 != 0 {
		buf[9] = 'x'
	}
	if mode&04000 != 0 {
		if buf[3] == 'x' {
			buf[3] = 's'
		} else {
			buf[3] = 'S'
		}
	}
	if mode&02000 != 0 {
		if buf[6] == 'x' {
			buf[6] = 's'
		} else {
			buf[6] = 'S'
		}
	}
	if mode&01000 != 0 {
		if buf[9] == 'x' {
			buf[9] = 't'
		} else {
			buf[9] = 'T'
		}
	}
	return string(buf[:])
}

// fmtTimestamp formats a Unix timestamp string in ls-style:
//   - within 180 days: "Jan  2 15:04"
//   - older:           "Jan  2  2006"
func fmtTimestamp(tsStr string) string {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return tsStr
	}
	t := time.Unix(ts, 0).Local()
	if !t.After(time.Now()) && time.Since(t) < 180*24*time.Hour {
		return t.Format("Jan _2 15:04")
	}
	return t.Format("Jan _2  2006")
}

// fmtStatTime formats a Unix timestamp for stat-style output: "2006-01-02 15:04:05 -0700 MST"
func fmtStatTime(tsStr string) string {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return tsStr
	}
	return time.Unix(ts, 0).Local().Format("2006-01-02 15:04:05 -0700 MST")
}

// fmtSizeHuman formats a byte-count string as a human-readable size (e.g., 1.5K, 2.3M).
func fmtSizeHuman(sizeStr string) string {
	n, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return sizeStr
	}
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case n < kb:
		return fmt.Sprintf("%d", n)
	case n < mb:
		return fmt.Sprintf("%.1fK", float64(n)/kb)
	case n < gb:
		return fmt.Sprintf("%.1fM", float64(n)/mb)
	case n < tb:
		return fmt.Sprintf("%.1fG", float64(n)/gb)
	default:
		return fmt.Sprintf("%.1fT", float64(n)/tb)
	}
}

// lookupUID resolves a numeric UID string to a username; falls back to the raw UID on error.
func lookupUID(uidStr string) string {
	u, err := user.LookupId(uidStr)
	if err != nil {
		return uidStr
	}
	return u.Username
}

// lookupGID resolves a numeric GID string to a group name; falls back to the raw GID on error.
func lookupGID(gidStr string) string {
	g, err := user.LookupGroupId(gidStr)
	if err != nil {
		return gidStr
	}
	return g.Name
}

// typeDesc maps a GUFI type character to a human-readable description.
func typeDesc(typeChar string) string {
	switch typeChar {
	case "f":
		return "regular file"
	case "d":
		return "directory"
	case "l":
		return "symbolic link"
	default:
		return typeChar
	}
}

// formatInfoRow formats an info row for display.
// Row positions: index_root(0), index_addr(1), last_updated(2),
// total_files(3), total_dirs(4), total_links(5), total_size(6), depth(7),
// min_uid(8), max_uid(9), min_gid(10), max_gid(11),
// min_size(12), max_size(13), min_mtime(14), max_mtime(15),
// zero_files(16), total_blocks(17).
// Without raw, sizes (6, 12, 13) are human-readable and mtimes (14, 15) are formatted.
func formatInfoRow(row []string, raw bool) []string {
	out := make([]string, len(row))
	copy(out, row)
	if !raw {
		for _, i := range []int{6, 12, 13} {
			if i < len(out) && out[i] != "" {
				out[i] = fmtSizeHuman(out[i])
			}
		}
		for _, i := range []int{14, 15} {
			if i < len(out) && out[i] != "" {
				out[i] = fmtStatTime(out[i])
			}
		}
	}
	return out
}

// safeGet returns raw[i] or "" if i is out of bounds.
func safeGet(raw []string, i int) string {
	if i >= 0 && i < len(raw) {
		return raw[i]
	}
	return ""
}
