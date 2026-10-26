package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

// The enum-valued flags must accept the UpperCamelCase form the CLI prints, that form's kebab-case
// spelling, and the tokens they accepted before the enums standardized on UpperCamelCase — the last
// so existing scripts and user habits keep working.

func TestStripePatternFlagAcceptedForms(t *testing.T) {
	accepted := map[string]beegfs.StripePatternType{
		// Display form and its separator spellings.
		"RAID0":        beegfs.StripePatternRaid0,
		"raid0":        beegfs.StripePatternRaid0,
		"BuddyMirror":  beegfs.StripePatternBuddyMirror,
		"buddymirror":  beegfs.StripePatternBuddyMirror,
		"buddy-mirror": beegfs.StripePatternBuddyMirror,
		"buddy_mirror": beegfs.StripePatternBuddyMirror,
		// Legacy token.
		"mirrored": beegfs.StripePatternBuddyMirror,
	}
	for in, want := range accepted {
		t.Run(in, func(t *testing.T) {
			var got *beegfs.StripePatternType
			require.NoError(t, newStripePatternFlag(&got).Set(in))
			require.NotNil(t, got)
			assert.Equal(t, want, *got)
		})
	}

	for _, in := range []string{"", "nonsense", "RAID10"} {
		var got *beegfs.StripePatternType
		assert.Error(t, newStripePatternFlag(&got).Set(in), "input %q must be rejected", in)
	}

	// String() reports the display form so the value echoed back matches the value printed elsewhere.
	pattern := beegfs.StripePatternBuddyMirror
	set := &pattern
	assert.Equal(t, "buddy-mirror", newStripePatternFlag(&set).String())

	var unset *beegfs.StripePatternType
	assert.Equal(t, "unchanged", newStripePatternFlag(&unset).String())
}

func TestAccessControlFlagAcceptedForms(t *testing.T) {
	accepted := map[string]beegfs.AccessFlags{
		"Unlocked":          beegfs.AccessFlagUnlocked,
		"unlocked":          beegfs.AccessFlagUnlocked,
		"none":              beegfs.AccessFlagUnlocked,
		"LockedRead":        beegfs.AccessFlagReadLock,
		"locked-read":       beegfs.AccessFlagReadLock,
		"read-lock":         beegfs.AccessFlagReadLock,
		"LockedWrite":       beegfs.AccessFlagWriteLock,
		"locked-write":      beegfs.AccessFlagWriteLock,
		"write-lock":        beegfs.AccessFlagWriteLock,
		"LockedReadWrite":   beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock,
		"locked-read-write": beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock,
		"read-write-lock":   beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock,
	}
	for in, want := range accepted {
		t.Run(in, func(t *testing.T) {
			var got *beegfs.AccessFlags
			require.NoError(t, newAccessControlFlag(&got).Set(in))
			require.NotNil(t, got)
			assert.Equal(t, want, *got)
		})
	}

	for _, in := range []string{"", "nonsense", "locked"} {
		var got *beegfs.AccessFlags
		assert.Error(t, newAccessControlFlag(&got).Set(in), "input %q must be rejected", in)
	}
}

func TestDataStateFlagAcceptedForms(t *testing.T) {
	accepted := map[string]beegfs.DataState{
		"Available":       beegfs.DataStateAvailable,
		"available":       beegfs.DataStateAvailable,
		"ManualRestore":   beegfs.DataStateManualRestore,
		"manual-restore":  beegfs.DataStateManualRestore,
		"AutoRestore":     beegfs.DataStateAutoRestore,
		"auto-restore":    beegfs.DataStateAutoRestore,
		"DelayedRestore":  beegfs.DataStateDelayedRestore,
		"delayed_restore": beegfs.DataStateDelayedRestore,
		"Unavailable":     beegfs.DataStateUnavailable,
		// The raw numeric form this support-only flag has always taken, including a reserved state
		// that has no display name and so is only reachable this way.
		"0":    beegfs.DataStateAvailable,
		"2":    beegfs.DataStateAutoRestore,
		"7":    beegfs.DataState(7),
		"none": beegfs.DataStateAvailable,
	}
	for in, want := range accepted {
		t.Run(in, func(t *testing.T) {
			var got *beegfs.DataState
			require.NoError(t, newDataStateFlag(&got).Set(in))
			require.NotNil(t, got)
			assert.Equal(t, want, *got)
		})
	}

	for _, in := range []string{"", "nonsense", "8", "-1"} {
		var got *beegfs.DataState
		assert.Error(t, newDataStateFlag(&got).Set(in), "input %q must be rejected", in)
	}
}
