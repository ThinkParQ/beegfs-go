package kvstore

import (
	"errors"
)

var (
	ErrEntryAlreadyExistsInDB    = errors.New("an entry already exists in the database for the specified key")
	ErrEntryAlreadyExistsInCache = errors.New("an entry already exists in the cache for the specified key")
	ErrEntryAlreadyDeleted       = errors.New("the entry was deleted before a lock could be acquired (probably there is a race condition)")
	// TODO: https://github.com/ThinkParQ/gobee/issues/10
	// Consider if we should prevent this from ever happening.
	//
	// To avoid copying the CacheEntry maps with every GetEntry, we return
	// pointers to entries inside the cache. This means even after commit() is
	// called, there is nothing stopping the user from reusing their pointer to
	// modify the cache entry. Presuming they eventually try to commit the
	// updates to BadgerDB. If a caller receives this error it is likely they
	// already overwrote the cached entry. At this point we may have a mismatch
	// between what is in BadgerDB and the cache but no way to automatically
	// resolve the conflict. If nothing else has the entry locked we could
	// overwrite the cache from BadgerDB, but if something else had the entry
	// locked we may have overwritten their changes. As this should never happen
	// when the MapStore is used correctly, we just return an error so the bug
	// in the caller can be addressed.
	ErrEntryLockAlreadyReleased = errors.New("commit was called a against an entry whose lock was already released (this indicates a bug in the caller)")
	// In addition to these errors, badger.ErrKeyNotFound is used to indicate when a key is not found in cache or database.
	ErrEntryIllegalKey = errors.New("illegal key for entry: may not start with '" + ReservedKeyPrefix + "'")
)
