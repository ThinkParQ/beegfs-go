package beegfs

import (
	"fmt"
	"strings"
)

// This file is the single chokepoint for turning a user supplied string back into an enum variant.
// Enum String() methods print lowercase kebab-case, and a user must be able to type that exact form
// as well as its snake_case and no-separator spellings, in any case. Parsers get that by comparing
// against the variant's own String() through these helpers rather than against hardcoded tokens, so
// the accepted input follows the display form automatically and the two cannot drift.

// NormalizeEnumInput prepares a user supplied enum value for comparison. It trims surrounding
// whitespace, lowercases, and drops the "-" and "_" separators that distinguish the kebab-case
// display form from its snake_case and no-separator spellings, so "needs-resync", "needs_resync",
// "needsresync" and "NeedsResync" all normalize to "needsresync".
//
// Interior spaces are deliberately preserved so a value like "needs resync" stays invalid; only
// separators a user would plausibly type between words are removed.
//
// Note this is lossy for a value that is empty or made only of whitespace and separators: "", "  ",
// "-" and "__" all normalize to "". The matchers below treat that result as naming nothing, so a
// caller comparing normalized forms by hand must do the same.
func NormalizeEnumInput(input string) string {
	return strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(strings.TrimSpace(input)))
}

// MatchEnumInput returns the first variant the input names, comparing with NormalizeEnumInput. Use
// it where the input selects one of several variants; the bool reports whether anything matched so
// the caller decides what an unrecognized value means (a sentinel variant or an error).
//
// Input that normalizes to "" never matches, so a variant whose String() is empty is unreachable
// rather than being selected by every separator-only value a user could type.
func MatchEnumInput[T fmt.Stringer](input string, variants ...T) (T, bool) {
	var zero T
	normalized := NormalizeEnumInput(input)
	if normalized == "" {
		return zero, false
	}
	for _, variant := range variants {
		if NormalizeEnumInput(variant.String()) == normalized {
			return variant, true
		}
	}
	return zero, false
}

// EnumInputMatches reports whether the input names the given value, accepting its display form, any
// separator spelling of it, or any of the extra aliases. Pass tokens a flag accepted before the enums
// were standardized as aliases so existing scripts keep working.
func EnumInputMatches(input string, value fmt.Stringer, aliases ...string) bool {
	normalized := NormalizeEnumInput(input)
	if normalized == "" {
		return false
	}
	if NormalizeEnumInput(value.String()) == normalized {
		return true
	}
	for _, alias := range aliases {
		if NormalizeEnumInput(alias) == normalized {
			return true
		}
	}
	return false
}
