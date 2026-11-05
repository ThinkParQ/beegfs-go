package rstdb

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func s3Target(id uint32, bucket string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{
		Id:   id,
		Name: "t",
		Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: bucket, EndpointUrl: "https://url"}},
	}
}

func xtreemstoreTarget(id uint32) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{
		Id: id,
		Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{
			S3: &flex.RemoteStorageTarget_S3{Bucket: "tape", EndpointUrl: "https://xts"},
			BulkOperations: []*flex.RemoteStorageTarget_XtreemStore_BulkOperation{
				{Operation: flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE, RetryDelay: proto.String("1h")},
			},
		}},
	}
}

func TestRoundTripAndReopen(t *testing.T) {
	path := t.TempDir()
	store, closeDB, err := Open(path, zap.NewNop())
	require.NoError(t, err)

	set, err := store.Load()
	require.NoError(t, err)
	assert.True(t, set.IsEmpty())
	next, ok := set.NextID()
	assert.True(t, ok)
	assert.Equal(t, uint32(1), next)

	deletedAt := time.Date(2026, 11, 3, 9, 12, 0, 0, time.UTC)
	_, err = store.Update(func(s *Set) error {
		s.Live[1] = s3Target(1, "a")
		s.Live[10] = xtreemstoreTarget(10)
		s.Deleted[7] = Deleted{Config: s3Target(7, "old"), Reason: "gone", DeletedAt: deletedAt}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, closeDB())

	store, closeDB, err = Open(path, zap.NewNop())
	require.NoError(t, err)
	defer closeDB()
	set, err = store.Load()
	require.NoError(t, err)
	assert.True(t, proto.Equal(s3Target(1, "a"), set.Live[1]))
	assert.True(t, proto.Equal(xtreemstoreTarget(10), set.Live[10]), "a oneof with nested repeated fields must survive the round trip")
	assert.True(t, proto.Equal(s3Target(7, "old"), set.Deleted[7].Config))
	assert.Equal(t, "gone", set.Deleted[7].Reason)
	assert.True(t, deletedAt.Equal(set.Deleted[7].DeletedAt))
	next, ok = set.NextID()
	assert.True(t, ok)
	assert.Equal(t, uint32(11), next)
}

func TestNextIDCountsDeletedTargets(t *testing.T) {
	set := newSet()
	set.Live[3] = s3Target(3, "a")
	set.Deleted[9] = Deleted{Config: s3Target(9, "b")}
	next, ok := set.NextID()
	assert.True(t, ok)
	assert.Equal(t, uint32(10), next)
}

// TestNextIDDoesNotWrap checks a set whose highest ID is the highest possible one, for example after
// create --id 4294967295. One more would wrap to the job builder's reserved ID 0, so the lowest ID
// never used is assigned instead.
func TestNextIDDoesNotWrap(t *testing.T) {
	set := newSet()
	set.Live[math.MaxUint32] = s3Target(math.MaxUint32, "max")
	set.Live[1] = s3Target(1, "a")
	set.Deleted[2] = Deleted{Config: s3Target(2, "b")}
	next, ok := set.NextID()
	assert.True(t, ok)
	assert.Equal(t, uint32(3), next, "neither live nor deleted, so never used")
}

func TestFailedUpdateStoresNothing(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	_, err = store.Update(func(s *Set) error {
		s.Live[1] = s3Target(1, "a")
		return nil
	})
	require.NoError(t, err)

	boom := errors.New("boom")
	_, err = store.Update(func(s *Set) error {
		s.Live[2] = s3Target(2, "b")
		delete(s.Live, 1)
		return boom
	})
	assert.ErrorIs(t, err, boom)

	set, err := store.Load()
	require.NoError(t, err)
	assert.Len(t, set.Live, 1)
	assert.Contains(t, set.Live, uint32(1))

	// The entry lock must have been released, so a later update still works.
	_, err = store.Update(func(s *Set) error { return nil })
	assert.NoError(t, err)
}

func TestInvalidSetIsRefused(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	_, err = store.Update(func(s *Set) error {
		s.Live[1] = s3Target(2, "a")
		return nil
	})
	assert.Error(t, err, "a target stored under another ID")

	_, err = store.Update(func(s *Set) error {
		s.Live[1] = s3Target(1, "a")
		s.Deleted[1] = Deleted{Config: s3Target(1, "a")}
		return nil
	})
	assert.Error(t, err, "a target both live and deleted")

	set, err := store.Load()
	require.NoError(t, err)
	assert.True(t, set.IsEmpty())
}

// validTarget is the check Remote passes to LoadOrImport: the one it runs on every target it stores.
func validTarget(effective *flex.RemoteStorageTarget) error {
	return rst.ValidateTarget(context.Background(), effective, filesystem.NewMockFS(), rst.DefaultStateRoot)
}

func TestLoadOrImport(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	set, err := store.LoadOrImport(nil, validTarget, zap.NewNop())
	require.NoError(t, err)
	assert.True(t, set.IsEmpty(), "nothing to import leaves the database empty")

	set, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(1, "a"), s3Target(2, "b")}, validTarget, zap.NewNop())
	require.NoError(t, err)
	assert.Len(t, set.Live, 2)
	assert.NotNil(t, set.Live[1].GetPolicies(), "imported configs are normalized")

	_, err = store.Update(func(s *Set) error {
		s.Deleted[2] = Deleted{Config: s.Live[2], Reason: "gone"}
		delete(s.Live, 2)
		return nil
	})
	require.NoError(t, err)

	// The file still lists both targets, but the database wins once it holds any.
	set, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(1, "a"), s3Target(2, "b"), s3Target(3, "c")}, validTarget, zap.NewNop())
	require.NoError(t, err)
	assert.Len(t, set.Live, 1)
	assert.Contains(t, set.Deleted, uint32(2))
}

func TestLoadOrImportRefusesAnInvalidFile(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	_, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(1, "a"), s3Target(1, "b")}, validTarget, zap.NewNop())
	assert.Error(t, err)
	_, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(0, "a")}, validTarget, zap.NewNop())
	assert.Error(t, err)
	set, err := store.Load()
	require.NoError(t, err)
	assert.True(t, set.IsEmpty())
}

// TestLoadOrImportChecksEveryTarget checks that a TOML target meets the rules of a CLI-created
// one, and that a file that fails them stores nothing. Storing first would keep the invalid set in
// the database, and Remote would fail on every start, with the fixed file ignored.
func TestLoadOrImportChecksEveryTarget(t *testing.T) {
	noBucket := s3Target(2, "")
	noName := s3Target(3, "c")
	noName.SetName("")
	withParent := s3Target(4, "d")
	withParent.SetParent("site")
	badClass := s3Target(5, "e")
	badClass.GetS3().SetStorageClass([]*flex.RemoteStorageTarget_S3_StorageClass{{Name: "GLACIER", Archival: &flex.RemoteStorageTarget_S3_StorageClass_Archival{RetrievalTier: "Bulk"}}})

	for name, tc := range map[string]struct {
		target *flex.RemoteStorageTarget
		want   string
	}{
		"a required setting is missing": {noBucket, "no bucket is set"},
		"the name is missing":           {noName, "needs a name"},
		"a parent is named":             {withParent, `parent "site" does not exist`},
		"the provider refuses an entry": {badClass, "must specify check-time"},
	} {
		t.Run(name, func(t *testing.T) {
			store, closeDB, err := Open(t.TempDir(), zap.NewNop())
			require.NoError(t, err)
			defer closeDB()

			_, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(1, "a"), tc.target}, validTarget, zap.NewNop())
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, err, "no target was imported")
			set, err := store.Load()
			require.NoError(t, err)
			assert.True(t, set.IsEmpty(), "an invalid file stores nothing, not even its valid targets")

			// The admin fixes the file and restarts: the import runs again.
			set, err = store.LoadOrImport([]*flex.RemoteStorageTarget{s3Target(1, "a")}, validTarget, zap.NewNop())
			require.NoError(t, err)
			assert.Len(t, set.Live, 1)
		})
	}
}

func s3Parent(name, parent string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{
		Name:   name,
		Parent: parent,
		Type:   &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Region: "r-" + name}},
	}
}

func TestParentsRoundTripAndResolve(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	child := s3Target(1, "a")
	child.SetParent("site")
	child.GetS3().SetEndpointUrl("")
	site := s3Parent("site", "")
	site.GetS3().SetEndpointUrl("https://site")
	_, err = store.Update(func(s *Set) error {
		s.Parents["site"] = site
		s.Parents["unused"] = s3Parent("unused", "")
		s.Live[1] = child
		s.Live[2] = s3Target(2, "b")
		return nil
	})
	require.NoError(t, err)

	set, err := store.Load()
	require.NoError(t, err)
	assert.True(t, proto.Equal(child, set.Live[1]), "a target is stored as its definition")
	effective, err := set.Effective(1)
	require.NoError(t, err)
	assert.Equal(t, "https://site", effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "r-site", effective.GetS3().GetRegion())

	assert.Equal(t, []uint32{1}, set.Children("site"))
	assert.Empty(t, set.Children("unused"))

	live, err := set.EffectiveLive()
	require.NoError(t, err)
	require.Len(t, live, 2)
	assert.Equal(t, uint32(1), live[0].GetId(), "sorted by ID")
}

func TestInvalidParentsAreRefused(t *testing.T) {
	store, closeDB, err := Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	defer closeDB()

	for name, change := range map[string]func(s *Set){
		"a parent stored under another name": func(s *Set) { s.Parents["a"] = s3Parent("b", "") },
		"a parent with an ID": func(s *Set) {
			p := s3Parent("a", "")
			p.SetId(3)
			s.Parents["a"] = p
		},
		"a target naming a missing parent": func(s *Set) {
			c := s3Target(1, "a")
			c.SetParent("missing")
			s.Live[1] = c
		},
		"a parent of another type": func(s *Set) {
			s.Parents["a"] = s3Parent("a", "")
			c := xtreemstoreTarget(1)
			c.SetParent("a")
			s.Live[1] = c
		},
		"a parent naming a parent": func(s *Set) {
			s.Parents["a"] = s3Parent("a", "b")
			s.Parents["b"] = s3Parent("b", "")
		},
		"a deleted target naming a parent": func(s *Set) {
			s.Parents["a"] = s3Parent("a", "")
			c := s3Target(1, "a")
			c.SetParent("a")
			s.Deleted[1] = Deleted{Config: c}
		},
	} {
		_, err := store.Update(func(s *Set) error { change(s); return nil })
		assert.Error(t, err, name)
	}

	set, err := store.Load()
	require.NoError(t, err)
	assert.True(t, set.IsEmpty())
}

// TestDecodeSetWithoutParents checks that an entry written before parents existed still decodes.
// gob leaves a field the encoded value lacks at its zero value.
func TestDecodeSetWithoutParents(t *testing.T) {
	type storedSetBeforeParents struct {
		Live    map[uint32][]byte
		Deleted map[uint32]storedDeleted
	}
	b, err := proto.Marshal(s3Target(1, "a"))
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(storedSetBeforeParents{Live: map[uint32][]byte{1: b}}))

	var stored storedSet
	require.NoError(t, gob.NewDecoder(&buf).Decode(&stored))
	set, err := decode(stored)
	require.NoError(t, err)
	assert.Len(t, set.Live, 1)
	assert.NotNil(t, set.Parents, "decode makes the map, so a change can add a parent")
	assert.Empty(t, set.Parents)
}
