package kvstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// CacheEntry represents an in-memory entry in the MapStore.
type CacheEntry[T any] struct {
	// Metadata can optionally store arbitrary details about the entry. One use
	// case is storing additional information needed to look up details about
	// this entry in another MapStore. For example if one MapStore tracks
	// map[fsPath]Job and other tracks map[jobID]jobResults the latter could
	// store fsPath as metadata allowing reverse lookups of a Job based on the
	// jobID.
	Metadata map[string]string
	// Store a map of strings to generic types in BadgerDB. This provides
	// callers flexibility when using the MapStore. For example an entry with
	// the key "/path" may want value to store a map[jobID]Job indicating all
	// jobs running for that /path.
	Value map[string]T
	// The mutex should be locked when reading or writing to a particular entry.
	mu sync.Mutex
	// The isDeleted flag should be checked immediately after a goroutine locks an
	// entry. It indicates the entry has been isDeleted from the cache map and DB
	// and should no longer be used. This is used to coordinate thread-safe
	// deletions from the map without requiring the entire map to be locked.
	isDeleted bool
	// keepCached should be incremented by goroutines intending to access the
	// entry before attempting to lock the entry. This signals to the goroutine
	// currently holding the entry lock it should not be evicted from the cache
	// when we've exceeded the cacheCapacity (to attempt avoiding expensive
	// reads from disk). Note this does not guarantee the entry will be kept in
	// the cache and callers should still check isCached after acquiring the
	// entry lock.
	keepCached atomic.Int32
	// The isCached flag should be checked immediately after a goroutine locks
	// an entry. It indicates the entry is no longer in the cache but still
	// present in the database. It is a safety mechanism in case there is a race
	// incrementing and checking keepCached.
	isCached bool
}

// BadgerItem represents a key/value pair that is stored on-disk using BadgerDB.
// It is typically used when directly returning multiple Badger entries.
type BadgerItem[T any] struct {
	Key   string
	Entry *BadgerEntry[T]
}

// BadgerEntry is what is stored on-disk using BadgerDB for a particular entry.
// It is the on-disk equivalent of a cache entry. It is used when
// encoding/decoding the contents of an entry from BadgerDB.
type BadgerEntry[T any] struct {
	Metadata map[string]string
	Value    map[string]T
}

// MapStore is an in memory representation of a map[string]map[string]T that
// handles automatically loading and unloading entries as needed from BadgerDB
// into memory to keep overall memory utilization in check.
type MapStore[T any] struct {
	// Global lock on the cache. A write lock is required to add or remove
	// entries from the cache map. A read lock is required when getting an
	// existing entry in the map. Once the entry is retrieved the caller should
	// release the read lock and instead attempt to take a write lock on the
	// entry. IMPORTANT: Don't hold the global lock on the cache while also
	// waiting to lock an entry or a deadlock can occur when trying to release
	// an entry if the entry needs to be evicted from the cache or deleted.
	mu sync.RWMutex
	// An in memory cache of MapStore entries that also exist in the DB. The
	// cache size is kept in check by immediately evicting new entries if the
	// maximum cache size is exceeded. This logic is because the oldest or least
	// recently used items in the cache are actually most likely to be needed
	// sooner since they represent the longest running jobs. IMPORTANT: The
	// cache is less about speeding up performance and mostly about providing
	// additional ordering guarantees not provided directly by Badger. Notably
	// we ensure multiple goroutines can update the same entry and the results
	// from both goroutines will be reflected in the DB. The read only
	// "GetEntry" methods provide the fastest access to the MapStore by avoiding
	// locking at the cost of returning slightly outdated results.
	cache map[string]*CacheEntry[T]
	// cacheCapacity indicates the maximum number of entries in the cache.
	// Note the actual cache size can be slightly higher than this additional
	// cache space is allocated as needed for entries actively being accessed.
	cacheCapacity int
	// We don't do any additional locking around the database since badger
	// already gives us transactional guarantees. Developers should be
	// familiar with what guarantees this provides in the context of Badger:
	// https://dgraph.io/docs/badger/get-started/#transactions
	db *badger.DB
}

// NewMapStore attempts to initialize a MapStore backed by a database at dbPath.
// It returns an pointer to the MapStore and a function that should be called
// to close the database when the MapStore is no longer needed.
func NewMapStore[T any](opts badger.Options, cacheCapacity int) (*MapStore[T], func(), error) {

	db, err := badger.Open(opts)
	if err != nil {
		return nil, nil, err
	}

	return &MapStore[T]{
		mu:            sync.RWMutex{},
		cache:         make(map[string]*CacheEntry[T]),
		cacheCapacity: cacheCapacity,
		db:            db,
	}, func() { db.Close() }, nil
}

// newMapStoreForTesting creates a new MapStore with a database at
// tempDBPathBase (e.g., `/tmp`) under a unique directory allowing it to be
// called simultaneously from multiple goroutines. It returns a pointer to the
// MapStore and a function that should be called to both close the database and
// cleanup the temporary directory structure when the test completes.
func newMapStoreForTesting[T any](tempDBPathBase string, cacheCapacity int) (*MapStore[T], func(), error) {

	tempDBPath, err := os.MkdirTemp(tempDBPathBase, "mapstoretests")
	if err != nil {
		return nil, nil, err
	}

	opts := badger.DefaultOptions(tempDBPath)
	opts.Logger = nil

	mapStore, closeMapStore, err := NewMapStore[T](opts, cacheCapacity)
	if err != nil {
		return nil, nil, err
	}

	return mapStore, func() { closeMapStore(); os.RemoveAll(tempDBPath) }, nil
}

// CreateAndLockEntry() creates a new entry in the MapStore for the provided
// key. If the provided key already has an entry it returns an error. Otherwise
// it creates an entry, locks it, adds it to the in-memory cache then returns
// the pointer to the new entry and a function that must be used to commit
// changes to the database and release the lock when the caller is finished
// using it. Once the lock is released the caller SHOULD NOT reuse the pointer
// to the entry and best practice would be to immediately set it to nil to
// prevent reuse and ensure it can be garbage collected.
func (s *MapStore[T]) CreateAndLockEntry(key string) (*CacheEntry[T], func() error, error) {

	// First verify the in-memory MapStore and database don't already
	// have an entry for key. If they do refuse to create a new entry.
	_, commitAndReleaseEntry, err := s.lockAndLoadEntryFromDB(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, nil, err // An unknown error occurred.
	} else if err == nil {
		commitAndReleaseEntry() // If we got a valid entry we must release it.
		return nil, nil, ErrEntryAlreadyExistsInDB
	}

	// If the entry isn't in the database more than likely we'll be able to
	// create it. First lock the in-memory MapStore so we can add an entry.
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cache[key]; ok {
		// This indicates something else raced with us and was able to create a
		// conflicting entry. For now lets not try to add a bunch of logic to
		// prevent this from ever happening. With the way we use the MapStore
		// this shouldn't ever be a problem.
		return nil, nil, fmt.Errorf("%w: (probably the caller is not setup to prevent race conditions)", ErrEntryAlreadyExistsInDB)
	}

	newEntry := &CacheEntry[T]{
		mu:        sync.Mutex{},
		Metadata:  make(map[string]string),
		Value:     make(map[string]T),
		isDeleted: false,
		isCached:  true,
	}

	// Lock the new entry entry before adding to the cache.
	// That way another goroutine can't race and get it.
	newEntry.mu.Lock()
	s.cache[key] = newEntry

	return newEntry, s.commitAndReleaseEntry(key, newEntry), nil
}

// GetAndLockEntry is used to get access to the MapStore entry for key. It first
// checks if the entry is already loaded in the in-memory cache and
// automatically attempts to load it from the database if needed. It returns a
// pointer to the entry and a function that must be used to release it when
// access is no longer required. If the entry doesn't exist a
// badger.ErrKeyNotFound error will be returned. Once the lock is released the
// caller SHOULD NOT reuse the pointer to the entry and best practice would be
// to immediately set it to nil to prevent reuse and ensure it can be garbage
// collected.
func (s *MapStore[T]) GetAndLockEntry(key string) (*CacheEntry[T], func() error, error) {

	// First see if there if there is already an entry in the cache for key:
	var entry *CacheEntry[T]
	for {
		s.mu.RLock()
		var ok bool
		entry, ok = s.cache[key]
		s.mu.RUnlock()
		if !ok {
			// If its not in the cache, see if we can load it from the database:
			entry, commitAndReleaseEntry, err := s.lockAndLoadEntryFromDB(key)
			if err == ErrEntryAlreadyExistsInCache {
				// It is possible another goroutine was trying to access this
				// entry and loaded it into the cache before we could. Check
				// cache again.
				continue
			} else if err != nil {
				return nil, nil, err
			}
			return entry, commitAndReleaseEntry, nil
		}
		break
	}

	// If another goroutine is accessing the entry, when it calls commitAndReleaseEntry()
	// the entry may be evicted from the cache if we're over the cacheCapacity. To try and
	// avoid having to reload the entry from disk before trying to lock, we can signal our
	// intent to other goroutines by incrementing keepCached.
	entry.keepCached.Add(1)
	defer entry.keepCached.Add(-1)

	// Important we lock the entry before checking to see it was deleted. This
	// allows us to only take a read lock above to get the entry from the map,
	// then release the global lock and only wait for the entry lock thus
	// minimizing lock contention.
	entry.mu.Lock()
	if entry.isDeleted {
		entry.mu.Unlock()
		return nil, nil, ErrEntryAlreadyDeleted
	} else if !entry.isCached {
		entry.mu.Unlock()
		// TODO: Is it safe to use recursion here to retry getting the entry if there was a
		// race condition and the entry was evicted from the cache before we could get at it?
		// Likely under normal use this should never cause a problem unless there were two or
		// more goroutines constantly trying to get the same entry (which probably indicates
		// an upstream bug anyway) and the cache is at capacity.
		return s.GetAndLockEntry(key)
	}
	return entry, s.commitAndReleaseEntry(key, entry), nil
}

// GetEntry is used for read only access to the database. It returns a copy of
// the entry specified by key, or a badger.ErrKeyNotFound if it does not exist.
// It avoids lock contention by bypassing the cache and always reading directly
// from the database. It is thread-safe because Badger provides transactional
// guarantees, however it can return out-of-date results if writes happens after
// the transaction to get the entry was already created.
func (s *MapStore[T]) GetEntry(key string) (*BadgerEntry[T], error) {

	var entryToGet = &BadgerEntry[T]{}

	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			dec := gob.NewDecoder(bytes.NewReader(val))
			err := dec.Decode(&entryToGet)
			return err
		})
		return err
	}); err != nil {
		return nil, err
	}
	return entryToGet, nil
}

// GetEntries is used for read only access to the database. It returns a copy of
// all entries whose keys match the specified prefix. Entries are returned in
// byte-wise lexicographical sorted order using a slice of BadgerItems. Each
// item contain the key and contents of a single entry. If no entries match it
// will return an empty slice. An error will only returned if there was a
// problem reading from the database. It is thread-safe because Badger provides
// transactional guarantees, however it can return out-of-date results if writes
// happens after the transaction to get the entry was already created.
func (s *MapStore[T]) GetEntries(keyPrefix string) ([]*BadgerItem[T], error) {

	badgerItems := make([]*BadgerItem[T], 0)

	if err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var entryToGet = &BadgerEntry[T]{}
				dec := gob.NewDecoder(bytes.NewReader(val))
				err := dec.Decode(&entryToGet)
				if err != nil {
					return err
				}
				badgerItem := &BadgerItem[T]{
					Key:   string(item.KeyCopy(nil)),
					Entry: entryToGet,
				}
				badgerItems = append(badgerItems, badgerItem)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return badgerItems, nil
}

// DeleteEntry removes the entry for the specified key from the map. To ensure
// deletions are thread-safe without needing to lock the entire map it does not
// guarantee memory associated with the entry is immediately freed, but just
// deletes the key from the cache map and underlying BadgerDB. It then sets the
// deleted flag on the entry informing other goroutines that may be trying to
// lock the entry that it is no longer valid. It is idempotent so if delete is
// called against an entry that was already deleted it will not return an error.
func (s *MapStore[T]) DeleteEntry(key string) error {

	// Note we can't reuse GetAndLockEntry() or lockAndLoadEntryFromDB() here
	// otherwise the entry would just be recreated when commitAndReleaseEntry()
	// was called.
	deleteEntryFromDBFunc := func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			err := txn.Delete([]byte(key))
			return err
		})
	}

	s.mu.RLock()
	entry, ok := s.cache[key]
	if !ok {
		// If the entry isn't cached just delete the entry from the database
		// without reloading it into the cache. This is safe to do with only
		// a read lock because it prevents anything else from trying to reload
		// this entry into the cache (but we don't need to write to the cache).
		defer s.mu.RUnlock()
		return deleteEntryFromDBFunc()
	}
	// Otherwise if the entry was cached we also have to clean up the cached
	// entry. First we need to release our global lock until we actually lock
	// the entry. Otherwise we could end up a deadlock if another goroutine is
	// trying to evict this entry from the cache.
	s.mu.RUnlock()
	entry.keepCached.Add(1)
	defer entry.keepCached.Add(-1)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.isDeleted {
		return nil // Entry was already deleted.
	}

	err := deleteEntryFromDBFunc()
	if err != nil {
		return err
	}
	entry.isDeleted = true

	// This is unlikely, but it is possible whoever held the lock last on the
	// entry evicted it from cache already. If this is the case we get a slight
	// performance gain by not having to take a global lock and delete the entry
	// from cache.
	if !entry.isCached {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, key)
	entry.isCached = false
	return nil
}

// lockAndLoadEntryFromDB attempts to get an entry from the DB and add it to the
// in-memory cache. If there is already an entry in the cache for the specified
// key it will always return an error. If it gets the entry from the DB it locks
// the entry before adding it to the cache then returns a pointer to the entry
// and a function that should be called to commit the entry to the database and
// release the lock. This ensures the caller is always granted exclusive access
// to the newly loaded entry. If the entry is not found in the database then an
// badger.ErrKeyNotFound error will be returned.
func (s *MapStore[T]) lockAndLoadEntryFromDB(key string) (*CacheEntry[T], func() error, error) {

	for {
		// First lock the MapStore then double check the entry isn't already cached
		// so we don't accidentally overwrite an existing entry. We will only unlock
		// if we get a valid entry so we can check if the entry was deleted.
		// Otherwise keep holding the lock so we can add and lock the new entry.
		s.mu.Lock()
		entry, ok := s.cache[key]
		if ok {
			s.mu.Unlock()
			entry.mu.Lock()
			isDeleted := entry.isDeleted
			entry.mu.Unlock()
			if isDeleted {
				// Possibly there was a race and something deleted this entry
				// after we looked it up. In this case the entry doesn't
				// actually exist in the cache, but we should keep checking
				// the cache to see if someone else recreated it.
				continue
			}
			return nil, nil, ErrEntryAlreadyExistsInCache
		}
		break
	}
	newEntry := &CacheEntry[T]{
		mu:        sync.Mutex{},
		isDeleted: false,
		isCached:  true,
	}
	newEntry.mu.Lock()
	s.cache[key] = newEntry
	// Safe to unlock here because we now have an exclusive lock on this specific entry.
	// Reading from the DB could be expensive, we don't want to leave the mutex locked.
	s.mu.Unlock()

	var entryToGet = &BadgerEntry[T]{}

	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			dec := gob.NewDecoder(bytes.NewReader(val))
			err := dec.Decode(&entryToGet)
			return err
		})
		return err
	}); err != nil {
		// If we can't load value from the database ensure not to leave an
		// invalid entry in the cache. Currently we don't allow overwriting
		// existing entries in the cache so there is no other way to correct
		// this scenario (for example a dangling entry currently breaks
		// CreateAndLockEntry()). To ensure the deletion is thread-safe we don't
		// delete the new entry from memory as we would panic if another
		// goroutine is trying to get a lock on this entry. Instead we just
		// remove it from the cache map and set the deleted flag so any
		// outstanding goroutines trying to lock this particular entry will know
		// it is no longer valid.
		newEntry.isDeleted = true
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.cache, key)
		newEntry.mu.Unlock()
		return nil, nil, err
	}

	newEntry.Metadata = entryToGet.Metadata
	newEntry.Value = entryToGet.Value
	return newEntry, s.commitAndReleaseEntry(key, newEntry), nil
}

// commitAndReleaseEntry is used to commit the value of an already locked
// in-memory entry to the database then release the lock on the entry. If the
// cache is at capacity and no other goroutines are waiting on this entry it
// will automatically evict the entry from the cache once the DB is updated.
func (s *MapStore[T]) commitAndReleaseEntry(key string, entry *CacheEntry[T]) func() error {

	return func() error {
		defer entry.mu.Unlock()

		var valueBuf bytes.Buffer
		enc := gob.NewEncoder(&valueBuf)

		entryToStore := BadgerEntry[T]{
			Metadata: entry.Metadata,
			Value:    entry.Value,
		}

		if err := enc.Encode(entryToStore); err != nil {
			return err
		}

		if err := s.db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(key), valueBuf.Bytes())
			return err
		}); err != nil {
			return err
		}

		if len(s.cache) > s.cacheCapacity && entry.keepCached.Load() == 0 {
			entry.isCached = false
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.cache, key)
		}
		return nil
	}
}
