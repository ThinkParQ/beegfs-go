package kvstore

import (
	"errors"
)

var (
	ErrEntryAlreadyExistsInDB   = errors.New("an entry already exists in the database for the specified key")
	ErrEntryNotInDB             = errors.New("the entry does not exist in the database for the specified key")
	ErrEntryLockAlreadyExists   = errors.New("an entry lock already exists for the specified key")
	ErrEntryAlreadyDeleted      = errors.New("the entry was deleted before a lock could be acquired (probably there is a race condition)")
	ErrEntryLockAlreadyReleased = errors.New("commit was called a against an entry whose lock was already released (this indicates a bug in the caller)")
	ErrEntryIllegalKey          = errors.New("illegal key for entry: may not start with '" + reservedKeyPrefix + "'")
)
