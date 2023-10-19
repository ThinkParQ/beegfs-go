package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

const (
	benchmarkCacheSize = 1
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

	// Simulate deleting an entry that existed when we tried to take a lock,
	// but the last lock holder already deleted it.
	ms.cache["k1"] = &MSEntry[int]{
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
	ms.cache["k1"] = &MSEntry[int]{
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

	ms, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

	requestID := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkGetAndLockEntry(b *testing.B) {

	ms, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
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
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkDeleteEntry(b *testing.B) {

	ms, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
		entry.Value[testWorkResult.RequestID] = testWorkResult
		err = release()
		assert.NoError(b, err)
	}

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.GetAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
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
	ms, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

	doneCh := make(chan bool, 2)
	errCh := make(chan error, 2)
	// We'll modify the request ID to ensure entries are actually updated.
	requestID := 0

	for i := 0; i < b.N; i++ {
		entry, release, err := ms.CreateAndLockEntry(fmt.Sprint(i))
		assert.NoError(b, err)
		requestID += 1
		testWorkResult.RequestID = fmt.Sprint(requestID)
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
	ms, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

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
	ms1, closeDB, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB()

	ms2, closeDB2, err := newMapStoreForTesting[TestWorkResult]("/tmp", benchmarkCacheSize)
	assert.NoError(b, err)
	defer closeDB2()

	doneCh := make(chan bool, 4)
	errCh := make(chan error, 4)

	// First we have a leader function going ahead and creating new entries:
	createFunc := func(db *MapStore[TestWorkResult]) {
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
	deleteFunc := func(db *MapStore[TestWorkResult]) {
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
