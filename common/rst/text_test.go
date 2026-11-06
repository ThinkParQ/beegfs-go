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
	for _, name := range []string{"archive", "ab", "site-2.eu_west", GenerateTargetName(^uint32(0)), "a" + strings.Repeat("b", MaxNameLength-1)} {
		assert.NoError(t, CheckName("remote storage target", name), name)
	}
	assert.ErrorContains(t, CheckName("remote storage backend", ""), "a remote storage backend needs a name")
	assert.ErrorContains(t, CheckName("remote storage target", "a"+strings.Repeat("b", MaxNameLength)), "at most 256")
	for _, name := range []string{"a", "--force", "-site", "7", "7up", "_site", ".site", "my site", "two\nlines", "site/eu", "sité", "\x1b[2J"} {
		assert.ErrorContains(t, CheckName("remote storage target", name), "must start with a letter", "%q", name)
	}
	assert.ErrorContains(t, CheckName("remote storage backend", "--force"), "was the value of --name left out?")
	assert.NotContains(t, CheckName("remote storage backend", "my site").Error(), "--name")
}
