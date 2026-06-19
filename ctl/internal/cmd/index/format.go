package index

import (
	"fmt"
	"math"
	"os/user"
	"strconv"
	"time"
)

func genericColumns(n int) []string {
	cols := make([]string, n)
	for i := range cols {
		cols[i] = fmt.Sprintf("col%d", i+1)
	}
	return cols
}

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

func fmtStatTime(tsStr string) string {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return tsStr
	}
	return time.Unix(ts, 0).Local().Format("2006-01-02 15:04:05 -0700 MST")
}

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

func lookupUID(uidStr string) string {
	u, err := user.LookupId(uidStr)
	if err != nil {
		return uidStr
	}
	return u.Username
}

func lookupGID(gidStr string) string {
	g, err := user.LookupGroupId(gidStr)
	if err != nil {
		return gidStr
	}
	return g.Name
}

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

func padRow(row []string, n int) []string {
	if len(row) == n {
		return row
	}
	out := make([]string, n)
	copy(out, row)
	return out
}

func fmtBlockSize(blocksStr, unit string) string {
	n, err := strconv.ParseInt(blocksStr, 10, 64)
	if err != nil {
		return blocksStr
	}
	if n < 0 || n > math.MaxInt64/512 {
		return blocksStr
	}
	return scaleBytes(n*512, unit, blocksStr)
}

func fmtSizeWithBlockSize(sizeStr, unit string) string {
	n, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return sizeStr
	}
	if n < 0 {
		return sizeStr
	}
	return scaleBytes(n, unit, sizeStr)
}

func scaleBytes(bytes int64, unit, raw string) string {
	switch unit {
	case "K":
		return fmt.Sprintf("%d", ceilDiv(bytes, 1024))
	case "M":
		return fmt.Sprintf("%d", ceilDiv(bytes, 1024*1024))
	case "G":
		return fmt.Sprintf("%d", ceilDiv(bytes, 1024*1024*1024))
	default:
		return raw
	}
}

func ceilDiv(n, divisor int64) int64 {
	q := n / divisor
	if n%divisor != 0 {
		q++
	}
	return q
}
