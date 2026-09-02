package beegfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNormalizeEnumInput pins which spellings collapse to the same value: the UpperCamelCase display
// form and its kebab-case and snake_case equivalents, but not a value with an interior space.
func TestNormalizeEnumInput(t *testing.T) {
	for _, in := range []string{"NeedsResync", "needsresync", "needs-resync", "needs_resync", "NEEDS_RESYNC", "  needs-resync  "} {
		assert.Equal(t, "needsresync", NormalizeEnumInput(in), "input %q", in)
	}
	// An interior space is not a separator a user types in place of a CamelCase boundary, so it must
	// not be folded away — otherwise typos like "cli ent" would start matching.
	assert.NotEqual(t, "needsresync", NormalizeEnumInput("needs resync"))
	assert.Equal(t, "", NormalizeEnumInput(""))
}

// TestMatchEnumInput verifies a variant is selected by its own display form or a kebab-case spelling
// of it, and that an unrecognized value reports no match rather than a wrong one.
func TestMatchEnumInput(t *testing.T) {
	states := []ConsistencyState{Good, NeedsResync, Bad}

	for _, in := range []string{"NeedsResync", "needs-resync", "needs_resync", "needsresync"} {
		got, ok := MatchEnumInput(in, states...)
		assert.True(t, ok, "input %q", in)
		assert.Equal(t, NeedsResync, got, "input %q", in)
	}

	got, ok := MatchEnumInput("Good", states...)
	assert.True(t, ok)
	assert.Equal(t, Good, got)

	_, ok = MatchEnumInput("nonsense", states...)
	assert.False(t, ok)
	_, ok = MatchEnumInput("", states...)
	assert.False(t, ok)
}

// TestEnumInputMatches verifies the display form, its kebab-case spelling and any extra alias all
// match, which is how flags keep the tokens they accepted before enums standardized on
// UpperCamelCase working.
func TestEnumInputMatches(t *testing.T) {
	// AccessFlagReadLock displays as "LockedRead"; "read-lock" is the legacy token, which names what
	// is blocked rather than what is locked and so cannot be derived from the display form.
	for _, in := range []string{"LockedRead", "lockedread", "locked-read", "read-lock", "READ_LOCK"} {
		assert.True(t, EnumInputMatches(in, AccessFlagReadLock, "read-lock"), "input %q", in)
	}
	assert.False(t, EnumInputMatches("LockedWrite", AccessFlagReadLock, "read-lock"))
	assert.False(t, EnumInputMatches("nonsense", AccessFlagReadLock, "read-lock"))

	// With no aliases only the display form and its separator spellings match.
	assert.True(t, EnumInputMatches("BuddyMirror", StripePatternBuddyMirror))
	assert.True(t, EnumInputMatches("buddy-mirror", StripePatternBuddyMirror))
	assert.False(t, EnumInputMatches("mirrored", StripePatternBuddyMirror))
}

// TestConsistencyStateFromStringAcceptedForms pins that the form String() prints round-trips, and
// that the historically documented "needs_resync" spelling still parses.
func TestConsistencyStateFromStringAcceptedForms(t *testing.T) {
	for _, state := range []ConsistencyState{Good, NeedsResync, Bad} {
		assert.Equal(t, state, ConsistencyStateFromString(state.String()), "display form of %s", state)
	}
	assert.Equal(t, NeedsResync, ConsistencyStateFromString("needs_resync"))
	assert.Equal(t, NeedsResync, ConsistencyStateFromString("needs-resync"))
	assert.Equal(t, Good, ConsistencyStateFromString("good"))
	assert.Equal(t, ConsistencyStateUnspecified, ConsistencyStateFromString(""))
	assert.Equal(t, ConsistencyStateUnspecified, ConsistencyStateFromString("nonsense"))
}

// TestNodeTypeFromStringAcceptedForms pins that NodeType's display form parses back. NodeType has no
// multi-word variants so it needs only case folding, and must stay strict about separator noise.
func TestNodeTypeFromStringAcceptedForms(t *testing.T) {
	for _, nt := range []NodeType{Client, Meta, Storage, Management} {
		assert.Equal(t, nt, NodeTypeFromString(nt.String()), "display form of %s", nt)
	}
	assert.Equal(t, InvalidNodeType, NodeTypeFromString("me_"))
	assert.Equal(t, InvalidNodeType, NodeTypeFromString("cli ent"))
}

// emptyDisplay is a variant whose String() is empty. No real enum has one, but the matchers must
// keep it unreachable rather than letting it absorb every separator-only value a user could type.
type emptyDisplay string

func (e emptyDisplay) String() string { return string(e) }

// separatorOnlyInput is everything a user can type that NormalizeEnumInput reduces to "": nothing at
// all, whitespace, and the separators the normalizer folds away.
var separatorOnlyInput = []string{"", " ", "   ", "-", "--", "_", "__", "-_-", " _ "}

// TestEnumInputMatchesRejectsEmptyInput pins that none of those inputs names a variant, including
// through an alias a caller had no value for. Alias tables are positional — common/rst/flags.go
// passes a scalar legacyAlias to every restore policy unconditionally — so a variant added with no
// legacy token to keep leaves an empty string in the alias list, and without the guard that would
// silently make `--restore-policy=`, `--restore-policy=-` and `--restore-policy=__` select it.
func TestEnumInputMatchesRejectsEmptyInput(t *testing.T) {
	for _, in := range separatorOnlyInput {
		assert.False(t, EnumInputMatches(in, AccessFlagReadLock), "no aliases, input %q", in)
		assert.False(t, EnumInputMatches(in, AccessFlagReadLock, "read-lock"), "real alias, input %q", in)
		assert.False(t, EnumInputMatches(in, AccessFlagReadLock, ""), "unset alias, input %q", in)
		assert.False(t, EnumInputMatches(in, emptyDisplay(""), ""), "empty display form, input %q", in)
	}

	// The guard must not cost a real value its match when an unset alias sits beside it.
	assert.True(t, EnumInputMatches("locked-read", AccessFlagReadLock, "", "read-lock"))
	assert.True(t, EnumInputMatches("read-lock", AccessFlagReadLock, "", "read-lock"))
}

// TestMatchEnumInputRejectsEmptyInput pins the same guard for the multi-variant matcher, where the
// empty form can only come from a variant's own String() rather than from an alias.
func TestMatchEnumInputRejectsEmptyInput(t *testing.T) {
	for _, in := range separatorOnlyInput {
		_, ok := MatchEnumInput(in, Good, NeedsResync, Bad)
		assert.False(t, ok, "input %q", in)

		_, ok = MatchEnumInput(in, emptyDisplay(""), emptyDisplay("good"))
		assert.False(t, ok, "empty display form, input %q", in)
	}

	// A variant listed after one with an empty display form is still reachable.
	got, ok := MatchEnumInput("good", emptyDisplay(""), emptyDisplay("good"))
	assert.True(t, ok)
	assert.Equal(t, emptyDisplay("good"), got)
}
