package kvstore

import (
	"errors"
)

var (
	ErrEntryAlreadyExistsInDB    = errors.New("an entry already exists in the database for the specified key")
	ErrEntryAlreadyExistsInCache = errors.New("an entry already exists in the cache for the specified key")
	ErrEntryAlreadyDeleted       = errors.New("the entry was deleted before a lock could be acquired (probably there is a race condition)")
	// In addition to these errors, badger.ErrKeyNotFound is used to indicate when a key is not found in cache or database.
)
