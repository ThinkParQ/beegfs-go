package kvstore

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkCacheSize = 1
	badgerTestDir      = "/tmp"
)

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for BadgerDB and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func tempPathForTesting(path string) (string, func(t require.TestingT), error) {
	tempDBPath, err := os.MkdirTemp(path, "mapStoreTestMode")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(t require.TestingT) {
		require.NoError(t, os.RemoveAll(tempDBPath), "error cleaning up after test")
	}

	return tempDBPath, cleanup, nil

}

func TestReservedKeyPrefix(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	_, _, err = ms.CreateAndLockEntry(ReservedKeyPrefix + "hello")
	assert.ErrorIs(t, err, ErrEntryIllegalKey)

	_, _, err = ms.GetAndLockEntry(ReservedKeyPrefix + "_world")
	assert.ErrorIs(t, err, ErrEntryIllegalKey)

	_, err = ms.GetEntry(ReservedKeyPrefix + "from")
	assert.ErrorIs(t, err, ErrEntryIllegalKey)

	err = ms.DeleteEntry(ReservedKeyPrefix + "beegfs")
	assert.ErrorIs(t, err, ErrEntryIllegalKey)

}

func TestCreateAndGetEntry(t *testing.T) {

	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	entry, release, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)
	entry.Metadata = map[string]string{
		"path": "/foo/bar",
	}
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
	expectedMetaMap := map[string]string{
		"path": "/foo/bar",
	}
	expectedValueMap := map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}
	assert.Equal(t, expectedMetaMap, entry.Metadata)
	assert.Equal(t, expectedValueMap, entry.Value)
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

	// Simulate deleting an entry that existed when we tried to take a lock,
	// but the last lock holder already deleted it.
	ms.cache["k1"] = &CacheEntry[map[string]int]{
		Metadata: map[string]string{
			"path": "/foo/bar",
		},
		Value: map[string]int{
			"innerKey1": 1,
			"innerKey2": 2,
		},
		mu:        sync.Mutex{},
		isDeleted: true,
		isCached:  false,
	}
	err = ms.DeleteEntry("k1")
	assert.NoError(t, err)

	// Simulate deleting an entry that was cached when we tried to take a lock,
	// but the last lock holder evicted it from the cache.
	ms.cache["k1"] = &CacheEntry[map[string]int]{
		Metadata: map[string]string{
			"path": "/foo/bar",
		},
		Value: map[string]int{
			"innerKey1": 1,
			"innerKey2": 2,
		},
		mu:        sync.Mutex{},
		isDeleted: false,
		isCached:  false,
	}
	err = ms.DeleteEntry("k1")
	assert.NoError(t, err)
}

func TestCreateAndGetEntryAutoGenKey(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	for i := 0; i <= 10; i++ {
		entry, release, err := ms.CreateAndLockEntry("")
		require.NoError(t, err)
		entry.Value = i
		require.NoError(t, release())
	}

	getNext, cleanupIterator, err := ms.GetEntries()
	require.NoError(t, err)
	defer cleanupIterator()

	for i := 0; i <= 10; i++ {
		entry, err := getNext()
		require.NoError(t, err)
		expectedKey := fmt.Sprintf("%013s", strconv.FormatInt(int64(i), 36))
		assert.Equal(t, expectedKey, entry.Key)
		assert.Equal(t, i, entry.Entry.Value)
		assert.Equal(t, expectedKey, entry.Entry.Metadata["mapstore_generated_pk"])
	}

}

func TestGetEntryAndUpdateFlag(t *testing.T) {

	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	entry, release, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)
	entry.Metadata = map[string]string{
		"path": "/foo/bar",
	}
	entry.Value = map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}

	assert.NoError(t, release(UpdateOnly))
	// If we call release with the UpdateOnly flag we should still hold the lock
	// on the entry (so trying the lock should fail):
	assert.False(t, entry.mu.TryLock())
	// A subsequent release should not return an error:
	assert.NoError(t, release())
	// And now the entry is unlocked:
	assert.True(t, entry.mu.TryLock())
}

func TestGetEntryAndDeleteFlag(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	entry, release, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)
	entry.Metadata = map[string]string{
		"path": "/foo/bar",
	}
	entry.Value = map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}

	assert.NoError(t, release())

	// Get the entry but set the delete flag when releasing:
	_, release, err = ms.GetAndLockEntry("k1")
	assert.NoError(t, err)
	assert.NoError(t, release([]CommitFlag{DeleteEntry}...))

	// Verify the entry was fully deleted from the cache+DB and we get the correct error:
	_, _, err = ms.GetAndLockEntry("k1")
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestGetEntry(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	entry, release, err := ms.CreateAndLockEntry("k1")
	assert.NoError(t, err)
	entry.Metadata = map[string]string{
		"path": "/foo/bar",
	}
	entry.Value = map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}
	assert.NoError(t, release())

	// If we call release a second time we should get an error.
	assert.ErrorIs(t, ErrEntryLockAlreadyReleased, release())

	readOnlyEntry, err := ms.GetEntry("k1")
	assert.NoError(t, err)
	expectedMetaMap := map[string]string{
		"path": "/foo/bar",
	}
	expectedValueMap := map[string]int{
		"innerKey1": 1,
		"innerKey2": 2,
	}
	assert.Equal(t, expectedMetaMap, readOnlyEntry.Metadata)
	assert.Equal(t, expectedValueMap, readOnlyEntry.Value)
}

func TestGetEntries(t *testing.T) {

	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[int](badger.DefaultOptions(path), 10)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	// Verify configuration checks
	nextItem, cleanupIterator, err := ms.GetEntries(WithKeyPrefix("/foo"), WithStartingKey("/bar"))
	cleanupIterator()
	require.Error(t, err)
	require.Nil(t, nextItem)

	// If there are no entries to return, there should not be an error and nextItem() should not be
	// nil but return nil even if called multiple times.
	nextItem, cleanupIterator, err = ms.GetEntries()
	require.NoError(t, err)
	require.NotNil(t, nextItem)
	item, err := nextItem()
	require.Nil(t, item)
	require.NoError(t, err)
	cleanupIterator()

	nextItem, cleanupIterator, err = ms.GetEntries()
	require.NoError(t, err)
	require.NotNil(t, nextItem)
	item, err = nextItem()
	require.Nil(t, item)
	require.NoError(t, err)
	cleanupIterator()

	// Return all items if no prefix is specified:
	var expectedSeqOrder []string
	for expectedVal := 0; expectedVal <= 100; expectedVal++ {
		expectedKey := fmt.Sprintf("%03d", expectedVal)
		entry, release, err := ms.CreateAndLockEntry(expectedKey)
		assert.NoError(t, err)
		entry.Value = expectedVal
		assert.NoError(t, release())
		expectedSeqOrder = append(expectedSeqOrder, expectedKey)
	}

	nextItem, cleanupIterator, err = ms.GetEntries()
	require.NoError(t, err)
	defer cleanupIterator() // No error to test.

	for i, expectation := range expectedSeqOrder {
		n, err := nextItem()
		require.NoError(t, err)
		require.Equal(t, expectation, n.Key)
		require.Equal(t, i, n.Entry.Value)
	}

	// When we've iterated over all items, return nil and no error.
	n, err := nextItem()
	require.Nil(t, n)
	require.NoError(t, err)
	cleanupIterator()

	// Return only items with the specified prefix:
	var expectedFileOrder []string
	for i := 0; i <= 100; i++ {
		key := fmt.Sprintf("/foo/%d", i)
		entry, release, err := ms.CreateAndLockEntry(key)
		assert.NoError(t, err)
		entry.Metadata = map[string]string{
			"path": key,
		}
		entry.Value = i
		assert.NoError(t, release())
		expectedFileOrder = append(expectedFileOrder, key)
	}

	// Add additional entries that don't match the key prefix but would come before the specified key prefix:
	entry, release, err := ms.CreateAndLockEntry("/baz/0")
	require.NoError(t, err)
	entry.Value = 999
	require.NoError(t, release())

	entry, release, err = ms.CreateAndLockEntry("/baz/2")
	require.NoError(t, err)
	entry.Value = 9999
	require.NoError(t, release())

	// Put everything into byte-wise lexicographical sorting order.
	// i.e., /foo/1, /foo/10, /foo/11, /foo/2, ...
	sort.Strings(expectedFileOrder)

	nextItem, cleanupIterator, err = ms.GetEntries(WithKeyPrefix("/foo"))
	require.NoError(t, err)
	defer cleanupIterator() // No error to test.

	for _, expectation := range expectedFileOrder {
		n, err := nextItem()
		require.NoError(t, err)
		require.Equal(t, expectation, n.Key)
		val, err := strconv.Atoi(strings.Split(n.Key, "/foo/")[1])
		require.NoError(t, err)
		require.Equal(t, val, n.Entry.Value)
		require.Equal(t, expectation, n.Entry.Metadata["path"])
	}

	// When we've iterated over all items, return nil and no error.
	n, err = nextItem()
	require.Nil(t, n)
	require.NoError(t, err)
	cleanupIterator()

	// Verify if a starting key is specified that doesn't exist, we start at the next closest match:
	nextItem, cleanupIterator, err = ms.GetEntries(WithStartingKey("/baz/1"))
	require.NoError(t, err)
	defer cleanupIterator() // No error to test.
	n, err = nextItem()
	require.NoError(t, err)
	require.Equal(t, "/baz/2", n.Key)
	require.Equal(t, 9999, n.Entry.Value)
	cleanupIterator()

	// Verify when both a starting key and prefix are specified
	nextItem, cleanupIterator, err = ms.GetEntries(WithStartingKey("/baz/0"), WithKeyPrefix("/baz"))
	require.NoError(t, err)
	defer cleanupIterator() // No error to test.
	n, err = nextItem()
	require.NoError(t, err)
	require.Equal(t, "/baz/0", n.Key)
	require.Equal(t, 999, n.Entry.Value)
	cleanupIterator()
}

func TestAutomaticCacheEviction(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 3)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

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

	// Verify we can get an entry we know isn't cached:
	entry, release, err := ms.GetAndLockEntry("4")
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"innerKey4_1": 1, "innerKey4_2": 2}, entry.Value)
	assert.NoError(t, release())
}

func TestGetAndLockEntry(t *testing.T) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 1)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

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

type TestWorkResult struct {
	RequestID  string
	Status     int32
	Message    string
	AssignedTo string
}

var testWorkResult = TestWorkResult{
	RequestID:  "1",
	Status:     1,
	Message:    "the quick brown fox jumped over the lazy dog",
	AssignedTo: "node-xxx",
}

func BenchmarkCreateAndLockEntry(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[int](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	requestID := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		entry.Metadata["path"] = "/foo/bar"
		entry.Value = requestID
		err = release()
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkCreateAndGetEntryAutoGenKey(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[int](badger.DefaultOptions(path), 10)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	requestID := 0

	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry("")
		require.NoError(b, err)
		requestID += 1
		entry.Metadata["path"] = "/foo/bar"
		entry.Value = requestID
		require.NoError(b, release())
	}
	b.StopTimer()

	// Verify entries were created as expected.
	getNext, cleanupIterator, err := ms.GetEntries()
	require.NoError(b, err)
	defer cleanupIterator()

	for i := 0; i <= b.N; i++ {
		entry, err := getNext()
		require.NoError(b, err)
		expectedKey := fmt.Sprintf("%013s", strconv.FormatInt(int64(i), 36))
		assert.Equal(b, expectedKey, entry.Key)
		assert.Equal(b, i+1, entry.Entry.Value)
		assert.Equal(b, expectedKey, entry.Entry.Metadata["mapstore_generated_pk"])
	}

}

func BenchmarkGetAndLockEntry(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Metadata["path"] = "/foo/bar"
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, release, err := ms.GetAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Metadata["path"] = "/foo/bar"
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkDeleteEntry(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Metadata["path"] = "/foo/bar"
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ms.DeleteEntry(fmt.Sprint(i))
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkConcurrent2GetEntry(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	doneCh := make(chan bool, 2)
	errCh := make(chan error, 2)
	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Metadata["path"] = "/foo/bar"
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}

	getFunc := func() {
		for i := 0; i < b.N; i++ {
			entry, release, err := ms.GetAndLockEntry(fmt.Sprint(i))
			if err != nil {
				errCh <- fmt.Errorf("unable to GetAndLockEntry: %w", err)
				return
			}
			requestID += 1
			testWorkResult.RequestID = fmt.Sprint(requestID)
			entry.Metadata["path"] = "/foo/bar"
			entry.Value[testWorkResult.RequestID] = testWorkResult
			err = release()
			if err != nil {
				errCh <- fmt.Errorf("unable to release entry: %w", err)
				return
			}
		}
		doneCh <- true
	}

	b.ResetTimer()
	go getFunc()
	go getFunc()
	completed := 0
	for completed < 2 {
		select {
		case err := <-errCh:
			b.Fatalf("Received error: %v", err)
		case <-doneCh:
			completed++
		}
	}
	b.StopTimer()
}

func BenchmarkConcurrentCreateGetDelete(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	doneCh := make(chan bool, 2)
	errCh := make(chan error, 2)

	// First we have a leader function going ahead and creating new entries:
	createFunc := func() {
		// We'll modify the request ID to ensure entries are actually updated.
		requestID := 0

		var testWorkResult = TestWorkResult{
			RequestID:  "1",
			Status:     1,
			Message:    "the quick brown fox jumped over the lazy dog",
			AssignedTo: "node-xxx",
		}

		for i := 0; i < b.N; i++ {
			entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
			if err != nil {
				errCh <- fmt.Errorf("unable to CreateAndLockEntry: %w", err)
				return
			}
			requestID += 1
			testWorkResult.RequestID = fmt.Sprint(requestID)
			entry.Metadata["path"] = "/foo/bar"
			entry.Value[testWorkResult.RequestID] = testWorkResult
			err = release()
			if err != nil {
				errCh <- err
				return
			}
		}
		doneCh <- true
	}

	// Then we have a follower function trailing behind and deleting those entries.
	// We first get the entry so we can verify it actually exists before deleting it.
	// This is because delete doesn't return an error if the entry doesn't exist.
	deleteFunc := func() {
		for i := 0; i < b.N; i++ {
			// If the entry doesn't exist yet sleep a bit to give create time to get ahead.
		retryLoop:
			for {
				_, release, err := ms.GetAndLockEntry(fmt.Sprint(i))
				if err == badger.ErrKeyNotFound || err == ErrEntryAlreadyDeleted {
					time.Sleep(10 * time.Millisecond)
					continue retryLoop
				}
				if err != nil {
					errCh <- fmt.Errorf("unable to GetAndLockEntry: %w", err)
					return
				}
				err = release()
				if err != nil {
					errCh <- err
					return
				}
				err = ms.DeleteEntry(fmt.Sprint(i))
				if err != nil {
					errCh <- fmt.Errorf("unable to delete entry: %w", err)
					return
				}
				break
			}
		}
		doneCh <- true
	}

	b.ResetTimer()
	go createFunc()
	go deleteFunc()
	completed := 0
	for completed < 2 {
		select {
		case err := <-errCh:
			b.Fatalf("Received error: %v", err)
		case <-doneCh:
			completed++
		}
	}
	b.StopTimer()
}

func BenchmarkConcurrentCreateGetDeleteWithTwoDBs(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms1, closeDB, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	path2, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms2, closeDB2, err := NewMapStore[map[string]TestWorkResult](badger.DefaultOptions(path2), benchmarkCacheSize)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB2())
	}()

	doneCh := make(chan bool, 4)
	errCh := make(chan error, 4)

	// First we have a leader function going ahead and creating new entries:
	createFunc := func(db *MapStore[map[string]TestWorkResult]) {
		// We'll modify the request ID to ensure entries are actually updated.
		requestID := 0

		var testWorkResult = TestWorkResult{
			RequestID:  "1",
			Status:     1,
			Message:    "the quick brown fox jumped over the lazy dog",
			AssignedTo: "node-xxx",
		}

		for i := 0; i < b.N; i++ {
			entry, release, err := db.CreateAndLockEntry(fmt.Sprint(i))
			if err != nil {
				errCh <- fmt.Errorf("unable to CreateAndLockEntry: %w", err)
				return
			}
			requestID += 1
			testWorkResult.RequestID = fmt.Sprint(requestID)
			entry.Metadata["path"] = "/foo/bar"
			entry.Value[testWorkResult.RequestID] = testWorkResult
			err = release()
			if err != nil {
				errCh <- err
				return
			}
		}
		doneCh <- true
	}

	// Then we have a follower function trailing behind and deleting those entries.
	// We first get the entry so we can verify it actually exists before deleting it.
	// This is because delete doesn't return an error if the entry doesn't exist.
	deleteFunc := func(db *MapStore[map[string]TestWorkResult]) {
		for i := 0; i < b.N; i++ {
			// If the entry doesn't exist yet sleep a bit to give create time to get ahead.
		retryLoop:
			for {
				_, release, err := db.GetAndLockEntry(fmt.Sprint(i))
				if err == badger.ErrKeyNotFound || err == ErrEntryAlreadyDeleted {
					time.Sleep(10 * time.Millisecond)
					continue retryLoop
				}
				if err != nil {
					errCh <- fmt.Errorf("unable to GetAndLockEntry: %w", err)
					return
				}
				err = release()
				if err != nil {
					errCh <- err
					return
				}
				err = db.DeleteEntry(fmt.Sprint(i))
				if err != nil {
					errCh <- fmt.Errorf("unable to delete entry: %w", err)
					return
				}
				break
			}
		}
		doneCh <- true
	}

	b.ResetTimer()
	go createFunc(ms1)
	go deleteFunc(ms1)
	go createFunc(ms2)
	go deleteFunc(ms2)
	completed := 0
	for completed < 4 {
		select {
		case err := <-errCh:
			b.Fatalf("Received error: %v", err)
		case <-doneCh:
			completed++
		}
	}
	b.StopTimer()
}

func BenchmarkGetEntry(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprintf("/foo/%d", i))
		assert.NoError(b, err)
		entry.Metadata = map[string]string{
			"path": fmt.Sprintf("/foo/%d", i),
		}
		entry.Value = map[string]int{
			"innerKey1": i,
		}
		assert.NoError(b, release())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, err := ms.GetEntry("/foo/" + strconv.Itoa(i))
		assert.NoError(b, err)
		assert.Equal(b, i, entry.Value["innerKey1"])
	}
	b.StopTimer()
}

func BenchmarkGetEntries(b *testing.B) {
	path, cleanup, err := tempPathForTesting(badgerTestDir)
	require.NoError(b, err, "error during test setup")
	defer cleanup(b)

	ms, closeDB, err := NewMapStore[map[string]int](badger.DefaultOptions(path), 10)
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, closeDB())
	}()

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprintf("/foo/%d", i))
		assert.NoError(b, err)
		entry.Metadata = map[string]string{
			"path": fmt.Sprintf("/foo/%d", i),
		}
		entry.Value = map[string]int{
			"innerKey1": i,
		}
		assert.NoError(b, release())
	}

	nextEntry, cleanupEntries, err := ms.GetEntries()
	require.NoError(b, err)
	defer cleanupEntries()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry, err := nextEntry()
		require.NoError(b, err)
		require.NotNil(b, entry)
	}
	b.StopTimer()
	entry, err := nextEntry()
	require.NoError(b, err)
	require.Nil(b, entry)
}
