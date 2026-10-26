package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPathInputTypeSentinelAndUnmatched pins that the uninitialized sentinel and a value matching no
// variant render differently, so a PathInputMethod that was never run through
// DeterminePathInputMethod is distinguishable from one holding a value the enum does not cover.
func TestPathInputTypeSentinelAndUnmatched(t *testing.T) {
	assert.Equal(t, "stdin", PathInputStdin.String())
	assert.Equal(t, "invalid", PathInputInvalid.String())
	assert.Equal(t, "unknown(9)", PathInputType(9).String())
}
