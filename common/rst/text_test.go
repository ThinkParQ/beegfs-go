package rst

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckText(t *testing.T) {
	assert.NoError(t, CheckText("name", "archive-2024 ü", MaxNameLength))
	assert.NoError(t, CheckText("reason", "", MaxReasonLength), "an empty reason is allowed")
	assert.ErrorContains(t, CheckText("name", strings.Repeat("a", MaxNameLength+1), MaxNameLength), "at most 256")
	assert.ErrorContains(t, CheckText("name", "two\nlines", MaxNameLength), "U+000A")
	assert.ErrorContains(t, CheckText("reason", "\x1b[2Jcleared", MaxReasonLength), "U+001B")
	assert.ErrorContains(t, CheckText("name", "bad\xffbyte", MaxNameLength), "not valid UTF-8")
}

func TestCheckName(t *testing.T) {
	assert.NoError(t, CheckName("remote storage target", "archive"))
	assert.ErrorContains(t, CheckName("remote storage target parent", ""), "a remote storage target parent needs a name")
	assert.ErrorContains(t, CheckName("remote storage target", "two\nlines"), "invalid remote storage target name")
}
