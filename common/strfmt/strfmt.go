// Package strfmt provides reusable functions for producing human-readable, opinionated string
// representations used across outputs in BeeGFS Go. It always outputs strings, however the input
// might be strings or other types that should be formatted as a string.
package strfmt

import (
	"unicode"
	"unicode/utf8"
)

func CapitalizeFirst(s string) string {
	if s == "" {
		return s
	}

	r, size := utf8.DecodeRuneInString(s)
	return string(unicode.ToUpper(r)) + s[size:]
}
