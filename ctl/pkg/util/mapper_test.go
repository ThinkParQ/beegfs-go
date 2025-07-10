package util

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGetCachedMappings_FirstCall(t *testing.T) {
	cachedMappings = nil
	cachedMappingsErr = nil
	cachedMappingsLastModified = time.Time{}
	mockedMappings := &Mappings{}
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		return mockedMappings, nil
	}

	// Verify cache is immediately updated and returned
	mappings, err := GetCachedMappings(context.Background())
	require.Nil(t, err)
	require.Same(t, mappings, mockedMappings)
	require.False(t, MappingsForceUpdate)
}

func TestGetCachedMappings_ErrorCase_RefreshOnError(t *testing.T) {
	cachedMappings = &Mappings{}
	cachedMappingsErr = errors.New("error retrieving mappings")
	cachedMappingsLastModified = time.Time{}
	MappingsForceUpdate = false
	mockedMappings := &Mappings{}
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		return mockedMappings, nil
	}

	// Verify GetCachedMappings blocks until cache is updated
	mappings, err := GetCachedMappings(context.Background())
	require.Nil(t, err)
	require.Same(t, mappings, mockedMappings)

}

func TestGetCachedMappings_ForceUpdate(t *testing.T) {
	cachedMappings = &Mappings{}
	cachedMappingsErr = nil
	cachedMappingsLastModified = time.Time{}
	MappingsForceUpdate = true
	mockedMappings := &Mappings{}
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		return mockedMappings, nil
	}

	// Verify cache is immediately updated and returned
	mappings, err := GetCachedMappings(context.Background())
	require.Nil(t, err)
	require.Same(t, mappings, mockedMappings)
	require.False(t, MappingsForceUpdate)
}

func TestGetCachedMappings_CacheHit_NoBackgroundUpdate(t *testing.T) {
	originalCachedMappings := &Mappings{}
	cachedMappings = originalCachedMappings
	cachedMappingsErr = nil
	cachedMappingsLastModified = time.Now()
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		t.Fatal("getMappingsFunc should not be called on cache hit when no force-update and cache is fresh")
		return nil, nil
	}

	// Verify cache is immediately returned
	mappings, err := GetCachedMappings(context.Background())
	require.Nil(t, err)
	require.Same(t, mappings, originalCachedMappings)
	require.False(t, MappingsForceUpdate)
}

func TestGetCachedMappings_CacheHit_BackgroundUpdate(t *testing.T) {
	cachedMappings = &Mappings{}
	cachedMappingsErr = nil
	cachedMappingsLastModified = time.Now().Add(-cachedMappingsUpdateDelay * time.Second)
	mockedMappings := &Mappings{}
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		if cachedMappings != mockedMappings {
			return mockedMappings, nil
		}
		t.Fatal("getMappingsFunc should only be called once")
		return nil, nil
	}

	// Verify original cached value is returned
	mappings, err := GetCachedMappings(context.Background())
	require.True(t, activeCachedMappingsUpdate)
	require.Nil(t, err)
	require.NotSame(t, mappings, mockedMappings)

	// Wait for background update and verify cache is updated
	time.Sleep(100 * time.Microsecond)
	mappings, err = GetCachedMappings(context.Background())
	require.False(t, activeCachedMappingsUpdate)
	require.Nil(t, err)
	require.Same(t, mappings, mockedMappings)

	// Subsequent call should not change the cache
	mappings, err = GetCachedMappings(context.Background())
	require.False(t, activeCachedMappingsUpdate)
	require.Nil(t, err)
	require.Same(t, mappings, mockedMappings)
}

func TestUpdateCachedMappingsInBackground_NoUpdateWhenAlreadyActive(t *testing.T) {
	activeCachedMappingsUpdate = true
	cachedMappingsLastModified = time.Now().Add(-cachedMappingsUpdateDelay * time.Second)
	cachedMappings = &Mappings{}
	cachedMappingsErr = nil
	MappingsForceUpdate = false
	getMappingsFunc = func(context.Context) (*Mappings, error) {
		t.Fatal("getMappingsFunc should not be called when cache is actively being updated")
		return nil, nil
	}

	// Verify GetMappings is never called though cachedMappingsUpdateDelaySec has been exceeded
	GetCachedMappings(context.Background())
}
