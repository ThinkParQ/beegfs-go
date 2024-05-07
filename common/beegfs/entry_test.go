package beegfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFile(t *testing.T) {
	assert.False(t, EntryDirectory.IsFile())
	assert.True(t, EntryRegularFile.IsFile())
	assert.True(t, EntrySOCKET.IsFile())
}

func TestFeatureFlags(t *testing.T) {
	var flag EntryFeatureFlags
	assert.False(t, flag.IsBuddyMirrored())
	assert.False(t, flag.IsInlined())

	flag.SetBuddyMirrored()
	assert.True(t, flag.IsBuddyMirrored())
	assert.False(t, flag.IsInlined())

	flag.SetInlined()
	assert.True(t, flag.IsBuddyMirrored())
	assert.True(t, flag.IsInlined())
}
