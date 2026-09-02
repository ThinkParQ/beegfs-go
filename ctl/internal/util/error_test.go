package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCtlExitCodeStrings pins the display form of every named exit code, and that an unnamed one
// keeps its number. This enum declares no sentinel and deliberately skips 3 and 4 (see
// NodesUnreachable), so without the number every reserved code would read as the same word.
func TestCtlExitCodeStrings(t *testing.T) {
	for _, c := range []struct {
		val  CtlExitCode
		want string
	}{
		{Success, "success"},
		{GeneralError, "general-error"},
		{PartialSuccess, "partial-success"},
		{NodesUnreachable, "nodes-unreachable"},
		{CtlExitCode(3), "unknown(3)"}, // reserved, intentionally unused
		{CtlExitCode(4), "unknown(4)"}, // reserved, intentionally unused
		{CtlExitCode(99), "unknown(99)"},
	} {
		assert.Equal(t, c.want, c.val.String())
	}
}
