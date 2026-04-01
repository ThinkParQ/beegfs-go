package strfmt

import (
	"fmt"
	"time"
)

// ExpirationString returns a human-readable string describing how much time remains until or has
// elapsed since an expiration point. The remaining parameter should be the duration until expiry
// (positive means not yet expired, negative means already expired). This is intended for consistent
// formatting of expiration messages across license checks, TLS certificate checks, etc.
func ExpirationString(remaining time.Duration) string {
	switch {
	case remaining > 0 && remaining < time.Hour:
		return "expires in less than an hour"
	case remaining > 0:
		return fmt.Sprintf("expires in %s", RoundToHoursOrDays(remaining))
	case remaining <= 0 && remaining > -time.Hour:
		return "expired less than an hour ago"
	default:
		return fmt.Sprintf("expired %s ago", RoundToHoursOrDays(-remaining))
	}
}

// RoundToHoursOrDays formats a positive duration as a human-readable string rounded to the nearest
// day (if >= 24h) or hour. For example: "3 days", "1 day", "12 hours", "1 hour".
func RoundToHoursOrDays(d time.Duration) string {
	if d >= 24*time.Hour {
		count := int(d.Round(24*time.Hour) / (24 * time.Hour))
		unit := "days"
		if count == 1 {
			unit = "day"
		}
		return fmt.Sprintf("%d %s", count, unit)
	}
	count := int(d.Round(time.Hour) / time.Hour)
	unit := "hours"
	if count == 1 {
		unit = "hour"
	}
	return fmt.Sprintf("%d %s", count, unit)
}
