package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

func TestMapperLen(t *testing.T) {
	testMapper := Mapper[int]{
		byAlias:    make(map[beegfs.Alias]int),
		byLegacyID: make(map[beegfs.LegacyId]int),
		byUID:      make(map[beegfs.Uid]int),
	}

	testMapper.byAlias[beegfs.Alias("test")] = 1
	testMapper.byLegacyID[beegfs.LegacyId{NumId: beegfs.NumId(1)}] = 1
	assert.Equal(t, -1, testMapper.Len(), "should return -1 if the lengths don't match")

	testMapper.byUID[beegfs.Uid(1)] = 1
	assert.Equal(t, 1, testMapper.Len(), "should return the lengths if they match")
}
