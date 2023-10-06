package kvstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndGetEntry(t *testing.T) {

	ms, closeDB, err := newMapStoreForTesting[int]("/tmp", 10)
	assert.NoError(t, err)
	defer closeDB()

	entry, release, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)
	entry.Value = map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}
	assert.NoError(t, release())

	// Verify if the key already exists in the cache we get the correct error:
	_, _, err = ms.CreateAndLockEntry("k1")
	assert.ErrorIs(t, err, ErrEntryAlreadyExistsInCache)

	// Verify we can get the entry:
	entry, release, err = ms.GetAndLockEntry("k1")
	assert.NoError(t, err)
	expectedMap := map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}
	assert.Equal(t, expectedMap, entry.Value)
	assert.NoError(t, release())

	// Verify we can delete the entry:
	err = ms.DeleteEntry("k1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(ms.cache))
	assert.Equal(t, true, entry.isDeleted)

	// Verify the entry was fully deleted from the cache+DB and we get the correct error:
	_, _, err = ms.GetAndLockEntry("k1")
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)

	// Repeated calls to delete an already deleted key are idempotent (no error):
	err = ms.DeleteEntry("k1")
	assert.NoError(t, err)
}

func TestAutomaticCacheEviction(t *testing.T) {
	ms, closeDB, err := newMapStoreForTesting[int]("/tmp", 3)
	assert.NoError(t, err)
	defer closeDB()

	for i := 0; i <= 5; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(t, err)
		entry.Value = map[string]int{
			fmt.Sprintf("innerKey%d_1", i): 1,
			fmt.Sprintf("innerKey%d_2", i): 2,
		}
		assert.NoError(t, release())
	}

	// After releasing all items the cache size should be at 3:
	assert.Len(t, ms.cache, 3)

	// The first three items are cached according to our eviction policy:
	for i := 0; i < 3; i++ {
		entry, ok := ms.cache[fmt.Sprint(i)]
		assert.True(t, ok)
		assert.Equal(t, map[string]int{fmt.Sprintf("innerKey%d_1", i): 1, fmt.Sprintf("innerKey%d_2", i): 2}, entry.Value)
	}
}

func TestGetAndLockEntry(t *testing.T) {
	ms, closeDB, err := newMapStoreForTesting[int]("/tmp", 1)
	assert.NoError(t, err)
	defer closeDB()

	// First fill up the database:
	_, release, err := ms.CreateAndLockEntry("foo")
	assert.NoError(t, err)
	assert.NoError(t, release())

	// Then create and lock an entry (but don't release it):
	ptr1, rel1, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)

	// In another goroutine attempt to get the same entry. Note these tests will
	// complete after the first goroutine chronologically.
	go func() {
		ptr2, rel2, err := ms.GetAndLockEntry("k1")
		assert.NoError(t, err)
		// Verify once we got the entry keepCached was decremented:
		assert.Equal(t, int32(0), ptr2.keepCached.Load())
		// And that we see the changes made by the first goroutine:
		assert.Equal(t, map[string]int{"1": 1, "2": 2}, ptr2.Value)
		// Release the entry:
		assert.NoError(t, rel2())
		// And there is only one item in the cache:
		assert.Len(t, ms.cache, 1)
	}()

	// Sleep a bit to ensure the other goroutine gets a chance to run:
	time.Sleep(1 * time.Second)

	// Verify the first goroutine sees it should keep the entry cached:
	assert.Equal(t, int32(1), ptr1.keepCached.Load())

	// Modify the entry in the first goroutine:
	ptr1.Value = map[string]int{"1": 1, "2": 2}

	// Then release the entry:
	assert.NoError(t, rel1())

	// Sleep a bit to ensure the other goroutine gets a chance to finish:
	time.Sleep(1 * time.Second)
}
