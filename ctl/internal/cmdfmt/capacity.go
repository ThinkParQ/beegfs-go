package cmdfmt

import (
	"fmt"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

// NotAvailable is printed for a capacity field the management service did not report, or reported
// inconsistently (more free than total). Capacity is modelled with pointers throughout so a target
// (or an aggregate of targets) that reported nothing is distinguishable from one that genuinely
// reported zero.
const NotAvailable = "-"

// FormatSpace formats a number of bytes as an IEC value suffixed with "B", or as the raw byte count
// when --raw is set.
func FormatSpace(bytes *uint64) string {
	if bytes == nil {
		return NotAvailable
	}
	if viper.GetBool(config.RawKey) {
		return fmt.Sprintf("%d", *bytes)
	}
	return fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(*bytes), unitconv.IEC, 1))
}

// FormatSpaceUsed formats the used space (total minus free) followed by the percentage of the total
// that is used. The percentage is omitted when the total is zero because it would be undefined.
func FormatSpaceUsed(totalBytes *uint64, freeBytes *uint64) string {
	if totalBytes == nil || freeBytes == nil || *freeBytes > *totalBytes {
		return NotAvailable
	}
	var used string
	if viper.GetBool(config.RawKey) {
		used = fmt.Sprintf("%d", *totalBytes-*freeBytes)
	} else {
		used = fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(*totalBytes)-float64(*freeBytes), unitconv.IEC, 1))
	}
	return used + formatPercentUsed(*totalBytes, *freeBytes)
}

// FormatInodes formats a number of inodes as an SI value, or as the raw count when --raw is set.
// Inodes are counted, not sized, so unlike FormatSpace there is no unit suffix.
func FormatInodes(inodes *uint64) string {
	if inodes == nil {
		return NotAvailable
	}
	if viper.GetBool(config.RawKey) {
		return fmt.Sprintf("%d", *inodes)
	}
	return unitconv.FormatPrefix(float64(*inodes), unitconv.SI, 1)
}

// FormatInodesUsed formats the used inodes (total minus free) followed by the percentage of the
// total that is used. The percentage is omitted when the total is zero because it would be undefined.
func FormatInodesUsed(totalInodes *uint64, freeInodes *uint64) string {
	if totalInodes == nil || freeInodes == nil || *freeInodes > *totalInodes {
		return NotAvailable
	}
	var used string
	if viper.GetBool(config.RawKey) {
		used = fmt.Sprintf("%d", *totalInodes-*freeInodes)
	} else {
		used = unitconv.FormatPrefix(float64(*totalInodes)-float64(*freeInodes), unitconv.SI, 1)
	}
	return used + formatPercentUsed(*totalInodes, *freeInodes)
}

// formatPercentUsed returns the used percentage as a parenthesized suffix, or an empty string when
// the total is zero (which would otherwise print as "NaN%").
func formatPercentUsed(total uint64, free uint64) string {
	if total == 0 {
		return ""
	}
	return fmt.Sprintf(" (%.2f%%)", 100-(float64(free)/float64(total))*100)
}
