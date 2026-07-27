package entry

import (
	"testing"

	"github.com/thinkparq/beegfs-go/common/beegfs"
)

// TestNewEntryFilterEmpty verifies an empty expression yields no filter (fetch-everything).
func TestNewEntryFilterEmpty(t *testing.T) {
	t.Parallel()
	f, err := NewEntryFilter("", nil)
	if err != nil {
		t.Fatalf("NewEntryFilter(\"\") error: %v", err)
	}
	if f != nil {
		t.Fatalf("NewEntryFilter(\"\") = %v, want nil", f)
	}
}

func TestNewEntryFilterInvalid(t *testing.T) {
	t.Parallel()
	if _, err := NewEntryFilter("this is not valid !!", nil); err == nil {
		t.Fatalf("NewEntryFilter(invalid) = nil error, want error")
	}
}

// TestPatternTokenDriftCanary pins the DSL pattern tokens to the beegfs stripe pattern enum. If a
// new StripePatternType is added or renamed this fails, forcing the DSL token map to be updated
// rather than silently returning "invalid" (which would make pattern filters match nothing).
func TestPatternTokenDriftCanary(t *testing.T) {
	t.Parallel()
	cases := map[beegfs.StripePatternType]string{
		beegfs.StripePatternRaid0:       "raid0",
		beegfs.StripePatternRaid10:      "raid10",
		beegfs.StripePatternBuddyMirror: "buddymirror",
	}
	for pt, want := range cases {
		if got := patternTokens[pt]; got != want {
			t.Errorf("patternTokens[%v] = %q, want %q", pt, got, want)
		}
	}
	// Every valid enum value (excluding the invalid sentinel) must have a DSL token.
	for pt := beegfs.StripePatternRaid0; pt <= beegfs.StripePatternBuddyMirror; pt++ {
		if patternTokens[pt] == "" {
			t.Errorf("patternTokens[%v] is empty; a known stripe pattern type is missing a DSL token", pt)
		}
	}
}

// TestDataStateTokenDriftCanary pins the DSL data-state tokens to the beegfs data-state enum and
// checks the offloaded convenience stays consistent with IsDataStateOffloaded.
func TestDataStateTokenDriftCanary(t *testing.T) {
	t.Parallel()
	cases := map[beegfs.DataState]string{
		beegfs.DataStateAvailable:      "available",
		beegfs.DataStateManualRestore:  "manualrestore",
		beegfs.DataStateAutoRestore:    "autorestore",
		beegfs.DataStateDelayedRestore: "delayedrestore",
		beegfs.DataStateUnavailable:    "unavailable",
	}
	for ds, want := range cases {
		if got := dataStateTokens[ds]; got != want {
			t.Errorf("dataStateTokens[%v] = %q, want %q", ds, got, want)
		}
		// offloaded == (state != available)
		wantOffloaded := ds != beegfs.DataStateAvailable
		if beegfs.IsDataStateOffloaded(ds) != wantOffloaded {
			t.Errorf("IsDataStateOffloaded(%v) = %v, want %v", ds, beegfs.IsDataStateOffloaded(ds), wantOffloaded)
		}
	}
}

// TestAccessTokenDriftCanary pins the DSL access tokens to the beegfs access-flag enum.
func TestAccessTokenDriftCanary(t *testing.T) {
	t.Parallel()
	cases := map[beegfs.AccessFlags]string{
		beegfs.AccessFlagUnlocked:                              "unlocked",
		beegfs.AccessFlagReadLock:                              "readlock",
		beegfs.AccessFlagWriteLock:                             "writelock",
		beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock: "readwritelock",
	}
	for af, want := range cases {
		if got := accessTokens[af]; got != want {
			t.Errorf("accessTokens[%v] = %q, want %q", af, got, want)
		}
	}
}
