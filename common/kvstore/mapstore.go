package kvstore

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/beegfs-go/v8/common/types"
)

const (
	// Errors will be returned when trying to create or get entries that match `reservedKeyPrefix`.
	// IMPORTANT: Changing this is not backwards compatible.
	reservedKeyPrefix = "mapstore_"
)

// EntryLock is used to manage access to database entries.
type EntryLock struct {
	// The mutex should be locked when reading or writing to a particular entry.
	mu sync.Mutex
	// The isEntryDeleted flag should be checked immediately after a go routine locks an entry. This flag indicates that
	// the entry has been deleted from the database and should no longer be used. It helps coordinate thread-safe
	// deletions from the MapStore without locking the entire MapStore.
	isEntryDeleted bool
	// keepLock is a hold counter for go routines intending to access the entry before acquiring the lock. Note that this
	// does not guarantee the entry will be retained, so the caller should still check isEntryDeleted after successfully
	// acquiring the lock.
	keepLock atomic.Int32
	// The isEntryLockDeleted flag should be checked immediately after a go routine locks an entry.
	// This flag indicates that the entry's lock has been deleted, but the entry itself may still exist.
	// It acts as a safety mechanism to handle potential race conditions when incrementing and checking keepLock.
	isEntryLockDeleted bool
}

// Create and return a new EntryLock with its mutex already locked.
func newLockedEntryLock() *EntryLock {
	lock := &EntryLock{
		mu:                 sync.Mutex{},
		isEntryDeleted:     false,
		isEntryLockDeleted: false,
	}
	lock.mu.Lock()
	return lock
}

// badgerEntry is used to encode and decode entries from the database.
type badgerEntry[T any] struct {
	Value T
}

// Update BadgerEntry with an encoded entry.
func (e *badgerEntry[T]) updateWithEncodedEntry(entry []byte) error {
	decoder := gob.NewDecoder(bytes.NewReader(entry))
	err := decoder.Decode(&e)
	return err
}

// encode the BadgerEntry as []byte.
func (e *badgerEntry[T]) encode() (encodedEntry []byte, err error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err = encoder.Encode(e); err != nil {
		return nil, err
	}
	encodedEntry = buffer.Bytes()
	return
}

// BadgerItem represents a key/value pair that is stored on-disk using BadgerDB.
// It is typically used when directly returning multiple Badger entries.
type BadgerItem[T any] struct {
	Key   string
	Entry *badgerEntry[T]
}

// MapStore provides thread-safe management of key-value entries stored in a Badger database, using a map[string]T
// representation.
type MapStore[T any] struct {
	// Global MapStore lock is used to synchronize read and write operations on the EntryLocks map. IMPORTANT: After
	// looking up an entryLock in the EntryLocks map, release the global MapStore lock BEFORE attempting to lock the
	// entryLock or deadlocks will occur (because deletions require the entry and global EntryLocks map be locked).
	mu sync.RWMutex
	// The entryLocks map maintains mappings for all active entry locks. These locks are used to manage access to
	// database entries.
	entryLocks map[string]*EntryLock
	// No additional locking is implemented around the database, as Badger already provides transactional guarantees.
	// Developers should understand the guarantees provided by Badger in this context:
	// https://dgraph.io/docs/badger/get-started/#transactions
	db *badger.DB
	// If the caller desires when creating DB entries can generate unique monotonically increasing uint64 keys. As keys
	// are strings in BadgerDB, by default we encode strings using base36 with a fixed width allowing the caller to
	// iterate over entries in the order they were written. This behavior can be customized through the `WithPK`
	// options.
	pkSeqGenerator *badger.Sequence
	// Configuration options for the MapStore.
	config *mapStoreConfig
	// Manages badger database garbage collection.
	gc *badgerGarbageCollection
}

type mapStoreConfig struct {
	pkSeqBandwidth uint64
	pkSeqWidth     int
	pkSeqBase      int
}
type mapStoreOpt func(*mapStoreConfig)

// See the BadgerDB documentation on Monotonically increasing integers.
// https://dgraph.io/docs/badger/get-started/#monotonically-increasing-integers
func WithPKBandwidth(bw uint64) mapStoreOpt {
	return func(cfg *mapStoreConfig) {
		cfg.pkSeqBandwidth = bw
	}
}

// Automatically generated keys are zero-padded to this many characters. This allows the caller to
// use GetEntries to iterate over entries in the order they were created. Don't set the width
// smaller than what is needed to store the maximum key that may be generated, otherwise entries
// cannot be returned in the order they were created. Setting this too large will use unnecessary
// space. The default width of 13 allows a uint64 key to be stored in base36.
func WithPKWidth(width int) mapStoreOpt {
	return func(cfg *mapStoreConfig) {
		cfg.pkSeqWidth = width
	}
}

// By default automatically generated keys are stored in base36 to reduce the amount of space they
// consume on-disk. This could be set to any base (i.e., 10, 64), but if returning entries in the
// order they were created is important use a base that naturally maintains byte-wise
// lexicographical sorted order such as base10 or base36.
func WithPKBase(base int) mapStoreOpt {
	return func(cfg *mapStoreConfig) {
		cfg.pkSeqWidth = base
	}
}

// NewMapStore initializes a MapStore where key-value entries are stored in a database. It returns a pointer to the
// MapStore and a function that should be called to close the database when the MapStore is no longer needed.
func NewMapStore[T any](opts badger.Options, msOpts ...mapStoreOpt) (*MapStore[T], func() error, error) {

	cfg := &mapStoreConfig{
		pkSeqBandwidth: 1000,
		pkSeqWidth:     13,
		pkSeqBase:      36,
	}

	for _, opt := range msOpts {
		opt(cfg)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, nil, err
	}

	// Badger will actually create an entry in the DB that matches this key. At best this is
	// confusing to users and at worst breaks methods like GetEntries. To avoid having to use a
	// separate DB for generating sequence we have established a ReservedKeyPrefix used to create
	// entries for internal use that are ignored by the facing methods.
	seq, err := db.GetSequence([]byte(reservedKeyPrefix+"pk_seq_gen"), cfg.pkSeqBandwidth)
	if err != nil {
		return nil, nil, err
	}

	gc := NewBadgerGarbageCollection(db)
	gc.StartRunner()

	closeFunc := func() error {
		multiErr := types.MultiError{}
		if err := seq.Release(); err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
		}
		if err := db.Close(); err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
		}
		if len(multiErr.Errors) != 0 {
			return &multiErr
		}
		return nil
	}

	return &MapStore[T]{
		mu:             sync.RWMutex{},
		entryLocks:     make(map[string]*EntryLock),
		db:             db,
		pkSeqGenerator: seq,
		config:         cfg,
		gc:             gc,
	}, closeFunc, nil
}

func (s *MapStore[T]) generateNextPK() (string, error) {
	seq, err := s.pkSeqGenerator.Next()
	if err != nil {
		return "", fmt.Errorf("unable to generate next PK sequence: %w", err)
	}
	return fmt.Sprintf("%0*s", s.config.pkSeqWidth, strconv.FormatInt(int64(seq), s.config.pkSeqBase)), nil
}

type createAndLockEntryConfig struct {
	allowExistingEntry bool
	value              any
}

type createAndLockEntryOpt func(*createAndLockEntryConfig)

// Including the WithAllowExisting option permits existing database entries to be returned. However, the
// ErrEntryAlreadyExistsInDB error will still be returned when existing entries are encountered.
func WithAllowExisting(allowExistingEntry bool) createAndLockEntryOpt {
	return func(cfg *createAndLockEntryConfig) {
		cfg.allowExistingEntry = allowExistingEntry
	}
}

// Including the WithValue option forces the CreateAndLockEntry call to initialize the database entry with the provided value.
// This option is ignored if WithAllowExisting and the entry always exists (i.e., the existing value will not be overwritten).
func WithValue(value any) createAndLockEntryOpt {
	return func(cfg *createAndLockEntryConfig) {
		cfg.value = value
	}
}

// CreateAndLockEntry() creates a new entry and returns it along with a commit function. By default, if an entry already
// exists for the specified key, the function returns an ErrEntryAlreadyExistsInDB error. This behavior can be modified
// by using the WithAllowExistingEntry(true) option. When this option is enabled, if an entry already exists, a copy of
// the existing entry is returned along with a commit function and the ErrEntryAlreadyExistsInDB error. In this case,
// the WithValue() option will be ignored.
//
// If no key is provided (i.e., key == ""), a unique, monotonically increasing primary key (PK) is automatically
// generated, with a 13-character numerical string in base-36 format. These generated keys are guaranteed to be unique
// and sorted in byte-wise lexicographical order. While gaps in the key sequence may occur due to crashes (depending on
// the WithPKBandwidth settings), the uniqueness and ordering of keys are preserved. Callers can mix generated and
// user-specified keys within the same database. If a collision occurs when attempting to create an entry with an
// existing key, an error will be returned.
//
// IMPORTANT: All keys prefixed with "mapstore_" are reserved for internal use ONLY. Attempts to create or access
// entries with this prefix will result in an error.
func (s *MapStore[T]) CreateAndLockEntry(key string, opts ...createAndLockEntryOpt) (string, *badgerEntry[T], func(...commitEntryConfigOpts) error, error) {
	if strings.HasPrefix(key, reservedKeyPrefix) {
		return key, nil, nil, ErrEntryIllegalKey
	}

	if key == "" {
		generatedKey, err := s.generateNextPK()
		if err != nil {
			return key, nil, nil, err
		}
		key = generatedKey
	}

	cfg := &createAndLockEntryConfig{
		allowExistingEntry: false,
		value:              nil,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	entryLock, err := s.getEntryLock(key)
	if err != nil {
		return key, nil, nil, err
	}

	entry, err := s.getEntry(key)
	if err == nil { // Entry unexpectedly exists
		if cfg.allowExistingEntry {
			return key, entry, s.commitEntry(key, entry, entryLock), ErrEntryAlreadyExistsInDB
		}
		s.deleteEntryLockUnlocked(key, entryLock)
		entryLock.mu.Unlock()
		return key, nil, nil, ErrEntryAlreadyExistsInDB
	} else if !errors.Is(err, ErrEntryNotInDB) { // Unknown error
		return key, nil, nil, err
	}

	newEntry := &badgerEntry[T]{}
	if cfg.value != nil {
		newEntry.Value = cfg.value.(T)
	}
	return key, newEntry, s.commitEntry(key, newEntry, entryLock), nil
}

// GetAndLockEntry provides access to a MapStore entry, returning a read-only copy of the entry and a release function.
// The release function must be called to apply any changes to the database. If the entry does not exist, an
// ErrEntryNotInDB error is returned.
func (s *MapStore[T]) GetAndLockEntry(key string) (*badgerEntry[T], func(...commitEntryConfigOpts) error, error) {
	if strings.HasPrefix(key, reservedKeyPrefix) {
		return nil, nil, ErrEntryIllegalKey
	}

	entryLock, err := s.getEntryLock(key)
	if err != nil {
		return nil, nil, err
	}

	entry, err := s.getEntry(key)
	if err != nil {
		// Delete entryLock since an entry was not obtained from the database.
		s.deleteEntryLockUnlocked(key, entryLock)
		entryLock.mu.Unlock()
		return nil, nil, fmt.Errorf("failed to retrieve database entry: %w", err)
	}

	return entry, s.commitEntry(key, entry, entryLock), nil
}

// GetEntry provides read-only access to the database. It returns a copy of the entry specified by the key, or
// ErrEntryNotInDB if the entry does not exist. Although thread-safe due to Badger's transactional guarantees,
// this method may return out-of-date results if writes occur after the transaction to retrieve the entry has been initiated.
func (s *MapStore[T]) GetEntry(key string) (*badgerEntry[T], error) {
	if strings.HasPrefix(key, reservedKeyPrefix) {
		return nil, ErrEntryIllegalKey
	}

	return s.getEntry(key)
}

// Remove the entry from the database for the specified key. This operation is idempotent, meaning that if delete
// is called on an entry that has already been deleted, no error will be returned.
//
// Note: For workflows that use GetAndLockEntry() to inspect and verify the entry before deletion, ensure that the entry
// is released before calling DeleteEntry() to avoid a deadlock. A better approach is to call commitEntry() with the
// DeleteEntry CommitFlag set, which deletes the entry before releasing the lock, thereby preventing any further
// modifications that could lead to data loss.
func (s *MapStore[T]) DeleteEntry(key string) error {
	if strings.HasPrefix(key, reservedKeyPrefix) {
		return ErrEntryIllegalKey
	}

	entryLock, err := s.getEntryLock(key)
	if errors.Is(err, ErrEntryAlreadyDeleted) {
		return nil
	}

	if err := s.deleteEntry(key, entryLock); err != nil && !errors.Is(err, ErrEntryNotInDB) {
		return err
	}

	s.deleteEntryLockUnlocked(key, entryLock)
	entryLock.mu.Unlock()
	return nil
}

type getEntriesConfig struct {
	keyPrefix    string
	startFromKey string
	prefetchSize int
}
type getEntriesOpt func(*getEntriesConfig)

// Including the WithKeyPrefix option forces GetEntries to start from the first key matching the prefix and return only
// keys that match the prefix.
func WithKeyPrefix(s string) getEntriesOpt {
	return func(cfg *getEntriesConfig) {
		cfg.keyPrefix = s
	}
}

// Including the WithStartingKey option forces GetEntries to start from this key if it exists. Otherwise, it starts from
// the next smallest key greater than the specified key. Note that when a starting key is specified, a key prefix must
// also be provided.
func WithStartingKey(s string) getEntriesOpt {
	return func(cfg *getEntriesConfig) {
		cfg.startFromKey = s
	}
}

// Including the WithPrefetchSize option forces GetEntries to prefetch the values of the next N items (default is 100).
func WithPrefetchSize(i int) getEntriesOpt {
	return func(cfg *getEntriesConfig) {
		cfg.prefetchSize = i
	}
}

// GetEntries is used for read only access to the database. It is thread-safe because Badger provides transactional
// guarantees, however it can return out-of-date results if writes happens after GetEntries() is first called.
//
// It returns a function used iterate over items in BadgerDB in byte-wise lexicographical sorted order until there are
// no more items in the DB, or no more items matching a KeyPrefix, then it will return nil. If for some reason there is
// an error getting an item (such as failing to deserialize the item) the function will return an error for the item and
// move to the next one. The caller can choose to cleanup immediately or attempt to continue iterating over items. As
// long as GetEntries() does not return an error it is always safe to call this function, it will simply continue to
// return nil if there are no more items.
//
// If the caller is done iterating over items in the database before all matching items have been returned, the cleanup
// function MUST be called to cleanup properly. The cleanup function is always safe to call and will never return an
// error, best practice is to always call it even if GetEntries() may have called it automatically because there were no
// remaining entries.
//
// Options:
//
//   - WithKeyPrefix: Start from the first key matching the prefix, and only return keys matching the prefix.
//   - WithStartingKey: Start from this key if present. Otherwise start from the next smallest key greater than this key.
//   - WithPrefetchSize: Prefetch the value of the next N items (default: 100).
//
// Both WithKeyPrefix and WithStartingKey can be set, but then the starting key must also contain the key prefix.
//
// Note: If your keys are sequence numbers (for example to return items in chronological order of when they were
// created), ensure keys are zero padded to a fixed width to ensure the string representations of the sequence numbers
// are return in order when sorted lexicographically.
func (s *MapStore[T]) GetEntries(opts ...getEntriesOpt) (func() (*BadgerItem[T], error), func(), error) {

	cfg := &getEntriesConfig{
		keyPrefix:    "",
		startFromKey: "",
		prefetchSize: 100,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	txn := s.db.NewTransaction(false)
	badgerOpts := badger.DefaultIteratorOptions
	badgerOpts.PrefetchSize = cfg.prefetchSize
	it := txn.NewIterator(badgerOpts)

	cleanup := func() {
		it.Close()
		txn.Discard()
	}

	// If StartFromKey is specified, then we use this as the seekToPrefix to initially seek to.
	// If KeyPrefix was also specified, make sure StartFromKey has that prefix.
	seekToPrefix := ""
	if cfg.startFromKey != "" {
		if !strings.HasPrefix(cfg.startFromKey, cfg.keyPrefix) {
			return nil, cleanup, fmt.Errorf("invalid options: WithStartingKey does not have the prefix WithKeyPrefix")
		}
		seekToPrefix = cfg.startFromKey
	} else if cfg.keyPrefix != "" {
		seekToPrefix = cfg.keyPrefix
	} else {
		// Otherwise start from the zero-th position.
		it.Rewind()
	}

	bytePrefix := []byte(cfg.keyPrefix)
	if seekToPrefix != "" {
		it.Seek([]byte(seekToPrefix))
		// Seek returns the next smallest key greater than the prefix (if there were no matches). If we should only
		// return keys matching a prefix, check the key we found matches.
		if !it.ValidForPrefix([]byte(cfg.keyPrefix)) {
			cleanup()
			return func() (*BadgerItem[T], error) { return nil, nil }, cleanup, nil
		}
	}

	getNext := func() (*BadgerItem[T], error) {

		// Checks the iterate is still valid and has the keyPrefix (if specified).
		if !it.ValidForPrefix(bytePrefix) {
			return nil, nil
		} else if it.ValidForPrefix([]byte(reservedKeyPrefix)) {
			// Automatically skip over internal entries.
			for {
				it.Next()
				// Here we care if ValidForPrefix is false because the iterator is no longer valid, or because it
				// doesn't match the prefix. If its no longer valid we must return nil, otherwise there are remaining
				// items to iterate over.
				if !it.Valid() {
					return nil, nil
				} else if !it.ValidForPrefix([]byte(reservedKeyPrefix)) {
					break
				}
			}
		}

		nextResult := &BadgerItem[T]{
			Key:   "",
			Entry: &badgerEntry[T]{},
		}

		item := it.Item()
		err := item.Value(func(encodedEntry []byte) error {
			nextResult.Key = string(item.Key())
			if err := nextResult.Entry.updateWithEncodedEntry(encodedEntry); err != nil {
				return fmt.Errorf("error decoding entry: %w", err)
			}
			return nil
		})

		// We move to the next item even if decoding fails or some other error happened getting the item. The caller can
		// decide if they get an error getting an item if they want to be done by calling the cleanup func, or keep
		// trying to read items.
		it.Next()
		if err != nil {
			return nil, err
		}
		return nextResult, nil
	}

	if !it.Valid() {
		cleanup()
		return func() (*BadgerItem[T], error) { return nil, nil }, cleanup, nil
	}
	return getNext, cleanup, nil
}

// Update the database entry for the specified key.
func (s *MapStore[T]) updateEntry(key string, entry *badgerEntry[T]) error {
	entryToStore, err := entry.encode()
	if err != nil {
		return err
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), entryToStore)
		return err
	}); err != nil {
		return err
	}

	return nil
}

// Remove the entry from the database for the specified key.
func (s *MapStore[T]) deleteEntry(key string, entryLock *EntryLock) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrEntryNotInDB
	}

	if err != nil {
		return fmt.Errorf("unable to delete entry from DB, the cached entry may no longer match the database entry: %w", err)
	}

	entryLock.isEntryDeleted = true
	return nil
}

// getEntry provides read-only access to the database. It returns a copy of the entry specified by the key, or
// ErrEntryNotInDB when the entry does not exist. Although thread-safe due to Badger's transactional guarantees,
// this method may return out-of-date results if writes occur after the transaction to retrieve the entry has been initiated.
func (s *MapStore[T]) getEntry(key string) (*badgerEntry[T], error) {
	var entry = &badgerEntry[T]{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(encodedEntry []byte) error {
			return entry.updateWithEncodedEntry(encodedEntry)
		})
		return err
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrEntryNotInDB
		}
		return nil, err
	}

	return entry, nil
}

// Add the entryLock to the MapStore's EntryLocks map under the specified key. The calling goroutine MUST already hold
// the entryLock, as addEntryLockUnlocked does not acquire it.
func (s *MapStore[T]) addEntryLockUnlocked(key string, entryLock *EntryLock) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entryLocks[key]; ok {
		// This indicates that a race condition occurred, allowing a conflicting entryLock to be created. For now, we
		// won’t add complex logic to prevent this, as it shouldn’t pose an issue given our current usage of the
		// MapStore. The caller will probably just need to retry.
		return fmt.Errorf("%w: (probably the caller is not setup to prevent race conditions)", ErrEntryLockAlreadyExists)
	}

	s.entryLocks[key] = entryLock
	return nil
}

// Removes the entryLock from the EntryLocks map for the specified key if no other goroutines are waiting for it.
// The calling goroutine MUST already hold the entryLock, as deleteEntryLockUnlocked does not acquire it.
func (s *MapStore[T]) deleteEntryLockUnlocked(key string, entryLock *EntryLock) {
	if entryLock.isEntryLockDeleted || entryLock.keepLock.Load() > 0 {
		return
	}
	entryLock.isEntryLockDeleted = true

	s.mu.Lock()
	delete(s.entryLocks, key)
	s.mu.Unlock()
}

// Retrieve an existing entryLock or create a new one for the specified key. If an entryLock already exists, this will
// block until the entry becomes available. If the caller acquires the lock, then they MUST release it by calling
// entryLock.mu.Unlock()
func (s *MapStore[T]) getEntryLock(key string) (*EntryLock, error) {
	s.mu.RLock()
	entryLock, entryLockExists := s.entryLocks[key]
	s.mu.RUnlock()

	if !entryLockExists {
		entryLock := newLockedEntryLock()
		if err := s.addEntryLockUnlocked(key, entryLock); err != nil {
			// If another entryLock was created for the same key and added first then recall getEntryLock().
			if errors.Is(err, ErrEntryLockAlreadyExists) {
				return s.getEntryLock(key)
			}
			return nil, err
		}
		return entryLock, nil
	}

	s.waitForEntryLock(entryLock)

	// Check if the entry has been deleted. If it has, confirm that it's entryLock has also been removed from the
	// EntryLocks map, and then return ErrEntryAlreadyDeleted.
	if entryLock.isEntryDeleted {
		s.deleteEntryLockUnlocked(key, entryLock)
		entryLock.mu.Unlock()
		return nil, ErrEntryAlreadyDeleted
	}

	// Since the entry has not been deleted, ensure that the entryLock has not been removed. If it has, call
	// getEntryLock() again to obtain a new entryLock. This scenario occurs when one go routine deletes the entryLock
	// without deleting the entry itself, after another go routine has acquired the lock but before keepLock has been
	// incremented.
	if entryLock.isEntryLockDeleted {
		entryLock.mu.Unlock()
		return s.getEntryLock(key)
	}

	return entryLock, nil
}

// Acquire a hold on an existing entryLock and block until it becomes available. Note that this does not prevent the
// entry from being deleted; it only ensures that the entryLock itself is not removed from the MapStore's EntryLocks
// map.
func (s *MapStore[T]) waitForEntryLock(entryLock *EntryLock) {
	entryLock.keepLock.Add(1)
	entryLock.mu.Lock()
	entryLock.keepLock.Add(-1)
}

type commitEntryConfig struct {
	deleteEntry       bool
	updateOnly        bool
	holdLockOnFailure bool
}

type commitEntryConfigOpts func(*commitEntryConfig)

// Including the WithDeleteEntry option in a commitEntry call will delete the entry from the database before the lock is
// released.
func WithDeleteEntry(deleteEntry bool) commitEntryConfigOpts {
	return func(cfg *commitEntryConfig) {
		cfg.deleteEntry = deleteEntry
	}
}

// Including the WithUpdateOnly option in a commitEntry call will update the entry in BadgerDB without releasing the lock.
// This allows the caller to commit incremental updates to stable storage while maintaining exclusive access to the entry.
// This is particularly useful when other processes may be using the read-only GetEntry or GetEntries methods.
func WithUpdateOnly(updateOnly bool) commitEntryConfigOpts {
	return func(cfg *commitEntryConfig) {
		cfg.updateOnly = updateOnly
	}
}

// Including the WithHoldLockOnFailure option in a commitEntry call will prevent the entryLock from being released
// if the update or delete operation fails.
func WithHoldLockOnFailure(holdLockOnFailure bool) func(*commitEntryConfig) {
	return func(cfg *commitEntryConfig) {
		cfg.holdLockOnFailure = holdLockOnFailure
	}
}

// Commit the entry to the database.
//
// The behavior of commitEntry can be modified using CommitFlags. For instance, passing UpdateOnly allows the database
// to be updated without releasing the entryLock, enabling further changes. By default, commitEntry updates the database
// and then releases the entryLock regardless of whether the commit succeeds. To guarantee the success of the commit use
// ReleaseLockOnSuccess
//
// IMPORTANT: Always handle the returned error instead of silently discarding it. Avoid using defer without proper error
// handling. A common pattern for handling this is:
//
//	defer func() {
//	    if err := commitEntry(); err != nil {
//	        log.Error("Failed to release entry", zap.Error(err))
//	    }
//	}()
func (s *MapStore[T]) commitEntry(key string, entry *badgerEntry[T], entryLock *EntryLock) func(opts ...commitEntryConfigOpts) error {

	// lockReleased flag is maintained be cause this function can be repeatedly called. For example this closure is
	// called once, release(UpdateOnly) then the closure would still be valid to call again.
	lockReleased := false

	return func(opts ...commitEntryConfigOpts) error {

		cfg := &commitEntryConfig{
			deleteEntry:       false,
			updateOnly:        false,
			holdLockOnFailure: false,
		}
		for _, opt := range opts {
			opt(cfg)
		}
		// The err variable is used to manage the deferred release of the entryLock. When the ReleaseLockOnSuccess
		// CommitFlag is passed, the entryLock will only be released if err == nil.
		var err error

		if lockReleased {
			return ErrEntryLockAlreadyReleased
		}

		if cfg.deleteEntry && cfg.updateOnly {
			return fmt.Errorf("invalid combination of commit flags specified, retry operation with corrected flags (cannot both delete and only update an entry)")
		}

		// Unless the UpdateOnly CommitFlag is passed, create a deferred closure to release the entry's entryLock.
		if !cfg.updateOnly {
			defer func() {
				if !cfg.holdLockOnFailure || err == nil {
					lockReleased = true
					s.deleteEntryLockUnlocked(key, entryLock)
					entryLock.mu.Unlock()
				}
			}()
		}

		if cfg.deleteEntry {
			err = s.deleteEntry(key, entryLock)
			return err
		}

		err = s.updateEntry(key, entry)
		return err
	}
}

type garbageCollector interface {
	// runDatabaseGC() run the database's garbage collection.
	RunValueLogGC(discardRatio float64) error
	// isDatabaseClosed() indicates whether the database has been closed.
	IsClosed() bool
}

type systemLoadDetector interface {
	isSystemLoadHigh() (bool, float64, error)
}

var _ systemLoadDetector = &systemLoad{}

type systemLoad struct {
	// Garbage collection runs at regular intervals but may be deferred if the system load is high. The
	// loadAverageThreshold is a normalized value representing the system load as a percentage, calculated as
	// loadAverage/numberOfCpus. A value greater than 1.0 indicates that, on average, there are processes waiting for CPU
	// time per available CPU.
	loadAverageThreshold float64
	readFile             func(string) ([]byte, error)
}

type badgerGarbageCollection struct {
	garbageCollector
	systemLoadDetector

	// Channel is used to ensure there's one and only one garbage collection start started.
	start chan struct{}
	// Database logger
	logger badger.Logger
	// initialSleepDelaySec is the sleep interval between garbage collection runs.
	initialSleepDelaySec int
	// forcedGCThresholdSec is the minimum sleep interval till garbage collection can no longer be deferred.
	forcedGCThresholdSec int
	// sleepDelayNoiseSec defines the range of randomness added to each garbage collection sleep interval. This helps prevent
	// overlapping garbage collection runs across multiple MapStore instances.
	sleepDelayNoiseSec int
	// Context for terminating the garbage collection runner.
	runnerCtx context.Context
	// Context cancel function for terminating the garbage collection runner.
	runnerCtxCancel context.CancelFunc
}

type badgerGarbageCollectionOpt func(*badgerGarbageCollection)

func NewBadgerGarbageCollection(db *badger.DB, opts ...badgerGarbageCollectionOpt) *badgerGarbageCollection {
	runnerCtx, runnerCtxCancel := context.WithCancel(context.Background())
	gc := &badgerGarbageCollection{
		start:                make(chan struct{}, 1),
		logger:               db.Opts().Logger,
		initialSleepDelaySec: 6 * 60 * 60,
		forcedGCThresholdSec: 15 * 60,
		sleepDelayNoiseSec:   10 * 60,
		runnerCtx:            runnerCtx,
		runnerCtxCancel:      runnerCtxCancel,
	}
	gc.garbageCollector = db
	gc.systemLoadDetector = &systemLoad{loadAverageThreshold: 1.0, readFile: os.ReadFile}

	for _, opt := range opts {
		opt(gc)
	}
	return gc
}

// Set system load threshold value.
func WithSystemLoadThreshold(threshold float64) badgerGarbageCollectionOpt {
	return func(gc *badgerGarbageCollection) {
		if threshold <= 0.0 {
			gc.logger.Warningf("invalid system load threshold! It must be greater than 0.0.")
			return
		}
		gc.systemLoadDetector = &systemLoad{loadAverageThreshold: threshold}
	}
}

// Set initial sleep delay time.
func WithInitialSleepDelaySec(delay int) badgerGarbageCollectionOpt {
	return func(gc *badgerGarbageCollection) {
		if delay <= 0 {
			gc.logger.Warningf("invalid initialSleepDelaySec time! It must be greater than 0.")
			return
		}
		gc.initialSleepDelaySec = delay

	}
}

// Set sleep delay minimum value.
func WithForcedGCThresholdSec(delay int) badgerGarbageCollectionOpt {
	return func(gc *badgerGarbageCollection) {
		if delay <= 0 {
			gc.logger.Warningf("invalid forcedGCThresholdSec time! It must be greater than 0.")
			return
		}
		gc.forcedGCThresholdSec = delay

	}
}

// Set sleep delay noise time.
func WithSleepDelayNoiseSec(delay int) badgerGarbageCollectionOpt {
	return func(gc *badgerGarbageCollection) {
		if delay <= 0 {
			gc.logger.Warningf("invalid sleepDelayNoiseSec value! It must be greater than 0.")
			return
		}
		gc.sleepDelayNoiseSec = delay

	}
}

// StartRunner() initiates the garbage collection process if it is not already running. It ensure one and only one
// instance runs at a time. The process executes in a separate go routine. Clean up will be triggered when the database
// is closed. Returns true when a runner has started.
func (gc *badgerGarbageCollection) StartRunner() bool {
	select {
	case gc.start <- struct{}{}:
		go func() {
			defer func() { <-gc.start }()
			gc.runner()
		}()
		return true
	default:
		return false
	}
}

// runner() executes garbage collection at regular intervals with an added random time to prevent multiple instances of
// badgerDB running at the same time. The interval between runs can be deferred based on high system load averages; each time
// garbage collection is deferred, the interval is reduced. If the sleep time is less than GCSleepIntervalMinimum then
// garbage collection cannot be deferred.
func (gc *badgerGarbageCollection) runner() {
	delay := gc.initialSleepDelaySec
	for {
		if gc.IsClosed() {
			return
		}

		select {
		case <-gc.runnerCtx.Done():
			return
		case <-time.After(gc.getSleepInterval(delay)):
		}

		delay = gc.attemptGarbageCollection(delay)
	}
}

// Attempt to execute garbage collection. Garbage collection will be deferred when the system load is high unless delay
// is less than the minimum. If the delay is less than the minimum, garbage collection will be forced to run. Should the
// database have been closed or is unavailable then the minimum delay time will be returned.
func (gc *badgerGarbageCollection) attemptGarbageCollection(delay int) int {
	isSystemLoadHigh, _, err := gc.isSystemLoadHigh()
	if err != nil {
		gc.logger.Warningf("failed to determine system load: %v", err)
	}
	if !isSystemLoadHigh || delay <= gc.forcedGCThresholdSec {
		if success := gc.runGarbageCollection(); success {
			delay = gc.initialSleepDelaySec
		} else {
			delay = gc.forcedGCThresholdSec
			gc.logger.Infof("database garbage collection failed to complete. Retry in %d seconds.", delay)
		}
	} else {
		delay /= 2.0
		gc.logger.Infof("database garbage collection was deferred. Retry in %d seconds", delay)
	}

	return delay
}

// runDatabaseGC() will be run repeatedly until there is nothing more to do unless either an error or there is a high
// system load detected.
func (gc *badgerGarbageCollection) runGarbageCollection() bool {
	start := time.Now()
	for {
		if err := gc.RunValueLogGC(0.5); err != nil {
			if errors.Is(err, badger.ErrNoRewrite) {
				gc.logger.Debugf("database garbage collection complete (total time: %s)", time.Since(start).String())
				return true
			}
			gc.logger.Warningf("database garbage collection failed to complete: %v.", err)
			return false
		}

		isSystemLoadHigh, load, err := gc.isSystemLoadHigh()
		if err != nil {
			gc.logger.Warningf("failed to determine system load: %v", err)
		}
		if isSystemLoadHigh {
			gc.logger.Infof("paused garbage collection since system load average is high. (system load %.2f).", load)
			return false
		}
	}
}

// getSleepInterval() adds random noise to the provided interval.
func (gc *badgerGarbageCollection) getSleepInterval(seconds int) time.Duration {
	noise := rand.Intn(gc.sleepDelayNoiseSec) - (gc.sleepDelayNoiseSec / 2)
	seconds += noise
	return time.Duration(seconds * int(time.Second))
}

// isSystemLoadHigh() determines whether there is a high system load. The pass 1 and 5 minutes are considered.
func (l *systemLoad) isSystemLoadHigh() (bool, float64, error) {
	data, err := l.readFile("/proc/loadavg")
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return false, 0.0, fmt.Errorf("failed to retrieve system load information: %v", err)
	}
	load1Min, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return false, 0.0, fmt.Errorf("failed to retrieve system load information: %v", err)
	}

	cpuCount := float64(runtime.NumCPU())
	load := load1Min / cpuCount
	return load >= l.loadAverageThreshold, load, nil
}

var _ garbageCollector = &mockGarbageCollection{}

type mockGarbageCollection struct {
	mock.Mock
}

func (m *mockGarbageCollection) RunValueLogGC(discardRatio float64) error {
	args := m.Called()
	return args.Error(0)
}
func (m *mockGarbageCollection) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

var _ systemLoadDetector = &mockSystemLoad{}

type mockSystemLoad struct {
	mock.Mock
}

func (m *mockSystemLoad) isSystemLoadHigh() (bool, float64, error) {
	args := m.Called()
	return args.Bool(0), args.Get(1).(float64), args.Error(2)
}

func newBadgerGarbageCollectionForTesting(db *badger.DB, opts ...badgerGarbageCollectionOpt) *badgerGarbageCollection {
	runnerCtx, runnerCtxCancel := context.WithCancel(context.Background())
	gc := &badgerGarbageCollection{
		start:                make(chan struct{}, 1),
		logger:               db.Opts().Logger,
		initialSleepDelaySec: 6 * 60 * 60,
		forcedGCThresholdSec: 15 * 60,
		sleepDelayNoiseSec:   10 * 60,
		runnerCtx:            runnerCtx,
		runnerCtxCancel:      runnerCtxCancel,
	}
	gc.garbageCollector = &mockGarbageCollection{}
	gc.systemLoadDetector = &mockSystemLoad{}

	for _, opt := range opts {
		opt(gc)
	}
	return gc
}
