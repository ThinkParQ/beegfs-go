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
	assert.Equal(t, int32(0), flag.IsBuddyMirroredI32())
	assert.False(t, flag.IsInlined())

	flag.SetBuddyMirrored()
	assert.True(t, flag.IsBuddyMirrored())
	assert.Equal(t, int32(1), flag.IsBuddyMirroredI32())
	assert.False(t, flag.IsInlined())

	flag.SetInlined()
	assert.True(t, flag.IsBuddyMirrored())
	assert.Equal(t, int32(1), flag.IsBuddyMirroredI32())
	assert.True(t, flag.IsInlined())
}

func TestAccessState(t *testing.T) {
	var flag FileState

	flag = flag.WithAccessState(AccessFlagWriteLock)
	assert.Equal(t, FileState(0b10), flag)

	flag = flag.WithAccessState(AccessFlagReadLock | AccessFlagWriteLock)
	assert.Equal(t, FileState(0b11), flag)

	flag = flag.WithoutAccessState(AccessFlagReadLock)
	assert.Equal(t, FileState(0b10), flag)

	flag = flag.WithAccessState(AccessFlagReadLock | AccessFlagWriteLock)
	assert.Equal(t, FileState(0b11), flag)

	flag = flag.WithoutAccessState(AccessFlagReadLock | AccessFlagWriteLock)
	assert.Equal(t, FileState(0b00), flag)
}
