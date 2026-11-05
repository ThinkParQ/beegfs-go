// Package rstdb stores the remote storage targets (RSTs) BeeGFS Remote serves. It is the source of
// truth for them: Remote builds its providers from it at startup and writes every change an admin
// makes to it before it applies the change.
//
// The whole set of targets is one MapStore entry under a single key. MapStore has no transaction
// across entries, so one entry is what makes each change atomic: a crash cannot leave half of an
// import or half of a delete behind. The set holds a few small records, and Remote already
// serializes every change, so per-target entries would buy nothing.
//
// The set keeps three kinds of records:
//   - Live targets, which Remote serves. A target with a parent is stored as its definition: only
//     the values it sets itself (see common/rst/inherit.go). Remote builds its provider from the
//     effective configuration, which Effective returns.
//   - Parents, keyed by name. A parent is a partial configuration that targets inherit from. It
//     names no parent. It is not a target, so nothing outside this database names it.
//   - Deleted targets. Stubs and file metadata may still name a deleted target's ID, so Remote
//     keeps its configuration to undo jobs that were still running (see the retired providers in
//     workermgr), and never gives its ID to another target. A deleted target is stored with its
//     effective configuration and no parent, so its retired provider does not change when a
//     parent changes or is deleted later.
package rstdb

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// setKey is the key of the one entry that holds the whole set.
const setKey = "rsts"

// Set is the decoded set of targets.
type Set struct {
	Live    map[uint32]*flex.RemoteStorageTarget
	Parents map[string]*flex.RemoteStorageTarget
	Deleted map[uint32]Deleted
}

// Deleted is the record of a deleted target.
type Deleted struct {
	// Config is the target's configuration when it was deleted. Remote builds a retired provider
	// from it, which it uses only to undo jobs that were still running.
	Config    *flex.RemoteStorageTarget
	Reason    string
	DeletedAt time.Time
}

func newSet() Set {
	return Set{
		Live:    make(map[uint32]*flex.RemoteStorageTarget),
		Parents: make(map[string]*flex.RemoteStorageTarget),
		Deleted: make(map[uint32]Deleted),
	}
}

// IsEmpty reports whether the set has never held a target, live or deleted, or a parent.
func (s Set) IsEmpty() bool {
	return len(s.Live) == 0 && len(s.Parents) == 0 && len(s.Deleted) == 0
}

// Effective returns the effective configuration of the live target id: its definition with every
// value it inherits from its parents filled in.
func (s Set) Effective(id uint32) (*flex.RemoteStorageTarget, error) {
	config, ok := s.Live[id]
	if !ok {
		return nil, fmt.Errorf("remote storage target %d does not exist", id)
	}
	effective, err := rst.Resolve(config, s.Parents)
	if err != nil {
		return nil, fmt.Errorf("remote storage target %d: %w", id, err)
	}
	return effective, nil
}

// EffectiveLive returns the effective configuration of every live target, sorted by ID. Remote
// builds its providers from these and sends them to worker nodes.
func (s Set) EffectiveLive() ([]*flex.RemoteStorageTarget, error) {
	effective := make([]*flex.RemoteStorageTarget, 0, len(s.Live))
	for _, id := range slices.Sorted(maps.Keys(s.Live)) {
		config, err := s.Effective(id)
		if err != nil {
			return nil, err
		}
		effective = append(effective, config)
	}
	return effective, nil
}

// Children returns the live targets that name the parent called name, sorted. A parent names no
// parent, so these are every target the parent's values reach.
func (s Set) Children(name string) []uint32 {
	ids := []uint32{}
	for id, config := range s.Live {
		if config.GetParent() == name {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// NextID returns the ID for a new target when the admin gives none, or false when every ID is
// taken. A deleted target's ID is never assigned on its own, because stubs may still name it:
//   - Normally it is one more than the highest ID ever used, counting deleted targets.
//   - When the highest possible ID is taken, for example by an explicit --id, adding one would wrap
//     to the job builder's reserved ID. It is then the lowest ID that is neither live nor deleted.
//     Every ID ever used is one or the other, so that ID was never used.
func (s Set) NextID() (uint32, bool) {
	highest := uint32(rst.JobBuilderRstId)
	for id := range s.Live {
		highest = max(highest, id)
	}
	for id := range s.Deleted {
		highest = max(highest, id)
	}
	if highest < math.MaxUint32 {
		return highest + 1, true
	}
	for id := uint32(rst.JobBuilderRstId) + 1; id < math.MaxUint32; id++ {
		_, live := s.Live[id]
		_, deleted := s.Deleted[id]
		if !live && !deleted {
			return id, true
		}
	}
	return 0, false
}

// storedSet is the form the set takes on disk. MapStore encodes values with gob, and gob cannot
// encode a protobuf message that has a oneof, so each configuration is kept as its proto.Marshal
// bytes.
//
// Parents was added after the first release of this format. gob decodes an entry written without
// it as a nil map, which decode treats as no parents.
type storedSet struct {
	Live    map[uint32][]byte
	Parents map[string][]byte
	Deleted map[uint32]storedDeleted
}

type storedDeleted struct {
	Config    []byte
	Reason    string
	DeletedAt time.Time
}

// Store is the database of targets.
type Store struct {
	db *kvstore.MapStore[storedSet]
}

// Open opens the database at path, creating it when it does not exist. The returned function closes
// it.
func Open(path string, log *zap.Logger) (*Store, func() error, error) {
	opts := badger.DefaultOptions(path).WithLogger(logger.NewBadgerLoggerBridge("rstDB", log))
	db, closeDB, err := kvstore.NewMapStore[storedSet](opts)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to open the remote storage target database: %w", err)
	}
	return &Store{db: db}, closeDB, nil
}

// Load returns the set as stored. A database that was never written returns an empty set.
func (s *Store) Load() (Set, error) {
	entry, err := s.db.GetEntry(setKey)
	if errors.Is(err, kvstore.ErrEntryNotInDB) {
		return newSet(), nil
	} else if err != nil {
		return Set{}, fmt.Errorf("unable to read the remote storage targets: %w", err)
	}
	return decode(entry.Value)
}

// Update reads the set, passes it to change, and stores the result when change returns no error.
// It returns the set as stored after the call. When change fails, nothing is stored and the error
// is returned. change may modify the set it is given.
func (s *Store) Update(change func(*Set) error) (Set, error) {
	_, entry, commit, err := s.db.CreateAndLockEntry(setKey, kvstore.WithAllowExisting(true))
	if err != nil && !errors.Is(err, kvstore.ErrEntryAlreadyExistsInDB) {
		return Set{}, fmt.Errorf("unable to lock the remote storage targets: %w", err)
	}

	// MapStore offers no way to release an entry without writing it, so a failed change commits
	// the value it read. The entry's value is only replaced once the new set is encoded.
	before := entry.Value
	set, err := decode(before)
	if err == nil {
		err = change(&set)
	}
	var encoded storedSet
	if err == nil {
		encoded, err = encode(set)
	}
	if err != nil {
		if commitErr := commit(); commitErr != nil {
			return Set{}, errors.Join(err, fmt.Errorf("unable to release the remote storage targets: %w", commitErr))
		}
		return Set{}, err
	}

	entry.Value = encoded
	if err := commit(); err != nil {
		return Set{}, fmt.Errorf("unable to store the remote storage targets: %w", err)
	}
	return set, nil
}

func decode(stored storedSet) (Set, error) {
	set := newSet()
	for id, b := range stored.Live {
		config := &flex.RemoteStorageTarget{}
		if err := proto.Unmarshal(b, config); err != nil {
			return Set{}, fmt.Errorf("unable to decode remote storage target %d: %w", id, err)
		}
		set.Live[id] = config
	}
	for name, b := range stored.Parents {
		config := &flex.RemoteStorageTarget{}
		if err := proto.Unmarshal(b, config); err != nil {
			return Set{}, fmt.Errorf("unable to decode remote storage target parent %q: %w", name, err)
		}
		set.Parents[name] = config
	}
	for id, d := range stored.Deleted {
		config := &flex.RemoteStorageTarget{}
		if err := proto.Unmarshal(d.Config, config); err != nil {
			return Set{}, fmt.Errorf("unable to decode deleted remote storage target %d: %w", id, err)
		}
		set.Deleted[id] = Deleted{Config: config, Reason: d.Reason, DeletedAt: d.DeletedAt}
	}
	return set, nil
}

// encode returns the stored form of set. It refuses a set that breaks an invariant the rest of
// Remote relies on, so a bug in a change cannot store a set Remote cannot start from:
//   - Every live target is stored under its own ID.
//   - Every parent is stored under its own name, has no ID, and names no parent.
//   - The parent of every live target exists and has the same type.
//   - A deleted target has no parent, and is not also live.
func encode(set Set) (storedSet, error) {
	stored := storedSet{
		Live:    make(map[uint32][]byte, len(set.Live)),
		Parents: make(map[string][]byte, len(set.Parents)),
		Deleted: make(map[uint32]storedDeleted, len(set.Deleted)),
	}
	for name, config := range set.Parents {
		if config.GetName() != name {
			return storedSet{}, fmt.Errorf("remote storage target parent stored under name %q has name %q (this is likely a bug)", name, config.GetName())
		}
		if config.GetId() != 0 {
			return storedSet{}, fmt.Errorf("remote storage target parent %q has ID %d, but parents have no ID (this is likely a bug)", name, config.GetId())
		}
		if config.GetParent() != "" {
			return storedSet{}, fmt.Errorf("remote storage target parent %q names parent %q, but a parent cannot have a parent (this is likely a bug)", name, config.GetParent())
		}
		b, err := proto.Marshal(config)
		if err != nil {
			return storedSet{}, fmt.Errorf("unable to encode remote storage target parent %q: %w", name, err)
		}
		stored.Parents[name] = b
	}
	for id, config := range set.Live {
		if config.GetId() != id {
			return storedSet{}, fmt.Errorf("remote storage target stored under ID %d has ID %d (this is likely a bug)", id, config.GetId())
		}
		if _, err := set.Effective(id); err != nil {
			return storedSet{}, err
		}
		b, err := proto.Marshal(config)
		if err != nil {
			return storedSet{}, fmt.Errorf("unable to encode remote storage target %d: %w", id, err)
		}
		stored.Live[id] = b
	}
	for id, d := range set.Deleted {
		if _, ok := set.Live[id]; ok {
			return storedSet{}, fmt.Errorf("remote storage target %d is both live and deleted (this is likely a bug)", id)
		}
		if d.Config.GetParent() != "" {
			return storedSet{}, fmt.Errorf("deleted remote storage target %d names parent %q, but a deleted target keeps its effective configuration (this is likely a bug)", id, d.Config.GetParent())
		}
		b, err := proto.Marshal(d.Config)
		if err != nil {
			return storedSet{}, fmt.Errorf("unable to encode deleted remote storage target %d: %w", id, err)
		}
		stored.Deleted[id] = storedDeleted{Config: b, Reason: d.Reason, DeletedAt: d.DeletedAt}
	}
	return stored, nil
}

// LoadOrImport returns the stored set. Remote calls it once at startup with the targets from its
// TOML file:
//   - When the database has never held a target, the TOML targets are checked, stored and returned.
//     This is the one time they are read.
//   - Otherwise the database wins. TOML targets are ignored with a warning, because an admin may
//     have changed or deleted them since the import, and the file must not undo that.
//
// validate is the check Remote runs on every target it stores, rst.ValidateTarget with Remote's
// mount point and state root. The import runs it on each target's effective configuration, so a
// TOML target meets the same rules as one created with the CLI. The check runs before anything is
// stored. So an invalid file stores nothing, and the next start, with the file fixed, imports again.
// Storing first would leave the invalid set in the database, and Remote could never start again.
//
// Two rules apply only to the import. A TOML target must give its ID, because files may already
// name it, and two targets cannot share one.
func (s *Store) LoadOrImport(fromConfig []*flex.RemoteStorageTarget, validate func(effective *flex.RemoteStorageTarget) error, log *zap.Logger) (Set, error) {
	if validate == nil {
		return Set{}, fmt.Errorf("no validation was given for the import (this is likely a bug)")
	}
	set, err := s.Load()
	if err != nil {
		return Set{}, err
	}
	if !set.IsEmpty() {
		if len(fromConfig) > 0 {
			log.Warn("ignoring the remote storage targets in the configuration file because they were already imported into the database (hint: manage them with 'beegfs remote target' and remove them from the file)", zap.Int("ignored", len(fromConfig)))
		}
		return set, nil
	}
	if len(fromConfig) == 0 {
		return set, nil
	}
	imported, err := s.Update(func(set *Set) error {
		for _, config := range fromConfig {
			id := config.GetId()
			if id == rst.JobBuilderRstId {
				return fmt.Errorf("remote storage target ID %d is reserved for the job builder", id)
			}
			if _, ok := set.Live[id]; ok {
				return fmt.Errorf("found multiple remote storage targets with the same ID: %d", id)
			}
			set.Live[id] = rst.NormalizeConfig(config)
		}
		for _, id := range slices.Sorted(maps.Keys(set.Live)) {
			effective, err := set.Effective(id)
			if err == nil {
				err = validate(effective)
			}
			if err != nil {
				return fmt.Errorf("remote storage target %d (%s) in the configuration file is not valid, so no target was imported (hint: fix its [[remote-storage-target]] section and restart): %w", id, set.Live[id].GetName(), err)
			}
		}
		return nil
	})
	if err != nil {
		return Set{}, err
	}
	for _, id := range slices.Sorted(maps.Keys(imported.Live)) {
		log.Info("imported remote storage target from the configuration file", zap.Uint32("rstID", id), zap.String("name", imported.Live[id].GetName()))
	}
	return imported, nil
}
