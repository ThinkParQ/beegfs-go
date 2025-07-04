package util

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrMappingRSTs          = errors.New("unable to download remote storage targets")
	ErrMapperNotInitialized = errors.New("mapper not initialized (this is likely a bug)")
	ErrMapperNotFound       = errors.New("requested entity ID not found in the mapper")
)

// Mappings combines all common Mappers into a single instance that can be initialized once and
// easily passed around code that does not have/need a context or the ability to return errors.
// Usually this is initialized using GetMappings().
type Mappings struct {
	NodeToTargets               Mapper[[]beegfs.EntityIdSet]
	TargetToNode                Mapper[beegfs.EntityIdSet]
	TargetToEntityIdSet         Mapper[beegfs.EntityIdSet]
	StorageBuddyToEntityIdSet   Mapper[beegfs.EntityIdSet]
	StoragePoolToConfig         Mapper[pool.GetStoragePools_Result]
	MetaBuddyGroupToPrimaryNode Mapper[beegfs.EntityIdSet]
	StorageTargetsToBuddyGroup  Mapper[beegfs.EntityIdSet]
	MirroredTargetToPrimary     Mapper[beegfs.EntityIdSet]
	RstIdToConfig               map[uint32]*flex.RemoteStorageTarget
}

// GetMappings() is helpful if mappings need to be initialized as part of setup code that has a
// context and makes decisions about when to return an error, then later used from code that cannot
// return an error and does not have/need a context. It returns what mapping it was able to
// initialize along with an error, if assembling any mappings fails. Notably callers may wish to
// ignore when RST mappings which returns the error ErrMappingRSTs.
func GetMappings(ctx context.Context) (*Mappings, error) {
	mappings := &Mappings{}

	// Target mappings:
	targets, err := target.GetTargets(ctx)
	if err != nil {
		return mappings, fmt.Errorf("unable to get target list from management: %w", err)
	}
	mappings.NodeToTargets = MapNodeToTargets(targets)
	mappings.TargetToNode = MapTargetToNode(targets)
	mappings.TargetToEntityIdSet = MapTargetToEntityIdSet(targets)

	// Storage pool mappings:
	storagePools, err := pool.GetStoragePools(ctx, pool.GetStoragePools_Config{})
	if err != nil {
		return mappings, fmt.Errorf("unable to get pools from management: %w", err)
	}
	mappings.StoragePoolToConfig = MapStoragePoolToConfig(storagePools)

	// Buddy group mappings:
	store, err := config.NodeStore(ctx)
	if err != nil {
		return mappings, fmt.Errorf("unable to get node store: %w", err)
	}
	buddyMirrors, err := buddygroup.GetBuddyGroups(ctx)
	if err != nil {
		return mappings, fmt.Errorf("unable to get buddy groups from management: %w", err)
	}
	mappings.StorageBuddyToEntityIdSet = MapStorageBuddyToEntityIdSet(buddyMirrors)
	mappings.MetaBuddyGroupToPrimaryNode, err = MapMetaBuddyGroupToPrimaryNode(buddyMirrors, store)
	if err != nil {
		return mappings, err
	}
	mappings.StorageTargetsToBuddyGroup, err = MapStorageTargetsToBuddyGroup(buddyMirrors)
	if err != nil {
		return mappings, err
	}

	mappings.MirroredTargetToPrimary = MapMirroredTargetToPrimary(buddyMirrors)

	// RST Mappings. Initialized last so if there is an error the caller can choose to ignore it and
	// still use the other mappings.
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return mappings, fmt.Errorf("%w: %w", ErrMappingRSTs, err)
	}
	// Don't call the RST package because that calls GetEntry which would cause an import cycle.
	rsts, err := beeRemote.GetRSTConfig(ctx, &beeremote.GetRSTConfigRequest{})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unavailable {
				return mappings, fmt.Errorf("%w: %w (hint: did you configure the right --%s=address:port?)", ErrMappingRSTs, err, config.BeeRemoteAddrKey)
			}
		}
		return mappings, fmt.Errorf("%w: %w", ErrMappingRSTs, err)
	}
	mappings.RstIdToConfig = MapRstIdToConfig(rsts)

	return mappings, nil

}

// A generic Mapper implementation for getting different types of BeeGFS entities based on their
// entity ID, or iterating over mappings based on different entity ID types. Callers can implement
// their own Mappers or use the MapX functions to get instances of common Mappers, or use
// GetMappings() to initialize and get all common Mappings.
type Mapper[T any] struct {
	byUID      map[beegfs.Uid]T
	byAlias    map[beegfs.Alias]T
	byLegacyID map[beegfs.LegacyId]T
}

// Get an item from the mapper by its entity ID. Returns ErrMapperNotFound if the item does not
// exist or ErrMapperNotInitialized if the caller did not first initialize the mapper (this should
// never happen when the mapper is initialized using one of the MapX functions).
func (m *Mapper[T]) Get(id beegfs.EntityId) (t T, err error) {

	if m.byUID == nil ||
		m.byAlias == nil ||
		m.byLegacyID == nil {
		return t, ErrMapperNotInitialized
	}

	var ok bool
	switch v := id.(type) {
	case beegfs.Uid:
		t, ok = m.byUID[v]
		if !ok {
			return t, fmt.Errorf("%w: %s", ErrMapperNotFound, v.String())
		}
	case beegfs.Alias:
		t, ok = m.byAlias[v]
		if !ok {
			return t, fmt.Errorf("%w: %s", ErrMapperNotFound, v.String())
		}
	case beegfs.LegacyId:
		t, ok = m.byLegacyID[v]
		if !ok {
			return t, fmt.Errorf("%w: %s", ErrMapperNotFound, v.String())
		}
	default:
		return t, fmt.Errorf("invalid entity ID")
	}

	return t, nil
}

// Returns the number of UIDs, Legacy IDs, and Aliases stored in the mapper. Returns -1 if the
// lengths are not all equal (this can happen if the mapper was not properly initialized).
func (m *Mapper[T]) Len() int {
	uidLen := len(m.byUID)
	lenLegId := len(m.byLegacyID)
	lenAlias := len(m.byAlias)
	if uidLen == lenLegId {
		if lenLegId == lenAlias {
			return len(m.byAlias)
		}
	}
	return -1
}

// Iterate over items in the mapper by their UIDs.
//
//	for k, v := range mapper.UIDs() {
//		// Do something with k and v.
//	}
func (m *Mapper[T]) UIDs() iter.Seq2[beegfs.Uid, T] {
	return func(yield func(beegfs.Uid, T) bool) {
		for k, v := range m.byUID {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Iterate over items in the mapper by their Aliases.
//
//	for k, v := range mapper.Aliases() {
//		// Do something with k and v.
//	}
func (m *Mapper[T]) Aliases() iter.Seq2[beegfs.Alias, T] {
	return func(yield func(beegfs.Alias, T) bool) {
		for k, v := range m.byAlias {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Iterate over items in the mapper by their Legacy IDs.
//
//	for k, v := range mapper.LegacyIDs() {
//		// Do something with k and v.
//	}
func (m *Mapper[T]) LegacyIDs() iter.Seq2[beegfs.LegacyId, T] {
	return func(yield func(beegfs.LegacyId, T) bool) {
		for k, v := range m.byLegacyID {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Map nodes to a slice of their targets entity ID sets. Available nodes are determined by the
// provided target list, so it will not include nodes that do not have targets mapped.
func MapNodeToTargets(targets []target.GetTargets_Result) Mapper[[]beegfs.EntityIdSet] {
	var nodeToTargetsMapper = Mapper[[]beegfs.EntityIdSet]{}
	nodeToTargetsMapper.byUID = make(map[beegfs.Uid][]beegfs.EntityIdSet)
	nodeToTargetsMapper.byAlias = make(map[beegfs.Alias][]beegfs.EntityIdSet)
	nodeToTargetsMapper.byLegacyID = make(map[beegfs.LegacyId][]beegfs.EntityIdSet)
	for _, target := range targets {
		nodeToTargetsMapper.byUID[target.Node.Uid] = append(nodeToTargetsMapper.byUID[target.Node.Uid], target.Target)
		nodeToTargetsMapper.byAlias[target.Node.Alias] = append(nodeToTargetsMapper.byAlias[target.Node.Alias], target.Target)
		nodeToTargetsMapper.byLegacyID[target.Node.LegacyId] = append(nodeToTargetsMapper.byLegacyID[target.Node.LegacyId], target.Target)
	}
	return nodeToTargetsMapper
}

// Map targets to the entity ID set of their owning node.
func MapTargetToNode(targets []target.GetTargets_Result) Mapper[beegfs.EntityIdSet] {
	var targetToNodeMapper = Mapper[beegfs.EntityIdSet]{}
	targetToNodeMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	targetToNodeMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	targetToNodeMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
	for _, tgt := range targets {
		targetToNodeMapper.byUID[tgt.Target.Uid] = tgt.Node
		targetToNodeMapper.byAlias[tgt.Target.Alias] = tgt.Node
		targetToNodeMapper.byLegacyID[tgt.Target.LegacyId] = tgt.Node
	}
	return targetToNodeMapper
}

// Map targets to their full entity ID set.
func MapTargetToEntityIdSet(targets []target.GetTargets_Result) Mapper[beegfs.EntityIdSet] {
	var targetMapper = Mapper[beegfs.EntityIdSet]{}
	targetMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	targetMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	targetMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
	for _, t := range targets {
		targetMapper.byUID[t.Target.Uid] = t.Target
		targetMapper.byAlias[t.Target.Alias] = t.Target
		targetMapper.byLegacyID[t.Target.LegacyId] = t.Target
	}
	return targetMapper
}

// Map storage buddy groups to their full entity ID set.
func MapStorageBuddyToEntityIdSet(buddyMirrors []buddygroup.GetBuddyGroups_Result) Mapper[beegfs.EntityIdSet] {
	var buddyMapper = Mapper[beegfs.EntityIdSet]{}
	buddyMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	buddyMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	buddyMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
	for _, m := range buddyMirrors {
		if m.NodeType != beegfs.Storage {
			continue
		}
		buddyMapper.byUID[m.BuddyGroup.Uid] = m.BuddyGroup
		buddyMapper.byAlias[m.BuddyGroup.Alias] = m.BuddyGroup
		buddyMapper.byLegacyID[m.BuddyGroup.LegacyId] = m.BuddyGroup
	}
	return buddyMapper
}

// Map storage pool entity IDs to the full configuration details of that storage pool.
func MapStoragePoolToConfig(pools []pool.GetStoragePools_Result) Mapper[pool.GetStoragePools_Result] {
	var storagePoolMapper = Mapper[pool.GetStoragePools_Result]{}
	storagePoolMapper.byUID = make(map[beegfs.Uid]pool.GetStoragePools_Result)
	storagePoolMapper.byAlias = make(map[beegfs.Alias]pool.GetStoragePools_Result)
	storagePoolMapper.byLegacyID = make(map[beegfs.LegacyId]pool.GetStoragePools_Result)
	for _, p := range pools {
		storagePoolMapper.byUID[p.Pool.Uid] = p
		storagePoolMapper.byAlias[p.Pool.Alias] = p
		storagePoolMapper.byLegacyID[p.Pool.LegacyId] = p
	}
	return storagePoolMapper
}

// Map metadata buddy mirror group entity IDs to the entity ID set of the primary node. Returns an
// error if the primary node for any buddy mirror does not exist in the provided node store.
func MapMetaBuddyGroupToPrimaryNode(buddyMirrors []buddygroup.GetBuddyGroups_Result, store *beemsg.NodeStore) (Mapper[beegfs.EntityIdSet], error) {
	var buddyMapper = Mapper[beegfs.EntityIdSet]{}
	buddyMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	buddyMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	buddyMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
	for _, m := range buddyMirrors {
		// Note this approach only works with metadata buddy groups, since metadata nodes only have
		// one target which shares the same legacy ID as the node.
		if m.NodeType != beegfs.Meta {
			continue
		}
		node, err := store.GetNode(m.PrimaryTarget.LegacyId)
		if err != nil {
			return Mapper[beegfs.EntityIdSet]{}, fmt.Errorf("unable to map metadata buddy mirror group %s to a primary metadata node: %w", m.BuddyGroup, err)
		}
		entityIdSet := beegfs.EntityIdSet{
			Uid: node.Uid,
			LegacyId: beegfs.LegacyId{
				NodeType: node.Id.NodeType,
				NumId:    node.Id.NumId,
			},
			Alias: node.Alias,
		}
		buddyMapper.byUID[m.BuddyGroup.Uid] = entityIdSet
		buddyMapper.byAlias[m.BuddyGroup.Alias] = entityIdSet
		buddyMapper.byLegacyID[m.BuddyGroup.LegacyId] = entityIdSet
	}
	return buddyMapper, nil
}

// Map primary and secondary storage target entity IDs to the entity ID set of the buddy group they
// are in (provided they are in a buddy group).
func MapStorageTargetsToBuddyGroup(buddyMirrors []buddygroup.GetBuddyGroups_Result) (Mapper[beegfs.EntityIdSet], error) {
	var buddyMapper = Mapper[beegfs.EntityIdSet]{}
	buddyMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	buddyMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	buddyMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
	for _, m := range buddyMirrors {
		if m.NodeType != beegfs.Storage {
			continue
		}
		buddyMapper.byUID[m.PrimaryTarget.Uid] = m.BuddyGroup
		buddyMapper.byAlias[m.PrimaryTarget.Alias] = m.BuddyGroup
		buddyMapper.byLegacyID[m.PrimaryTarget.LegacyId] = m.BuddyGroup
		buddyMapper.byUID[m.SecondaryTarget.Uid] = m.BuddyGroup
		buddyMapper.byAlias[m.SecondaryTarget.Alias] = m.BuddyGroup
		buddyMapper.byLegacyID[m.SecondaryTarget.LegacyId] = m.BuddyGroup
	}
	return buddyMapper, nil
}

func MapMirroredTargetToPrimary(buddyMirrors []buddygroup.GetBuddyGroups_Result) Mapper[beegfs.EntityIdSet] {
	var buddyMapper = Mapper[beegfs.EntityIdSet]{}
	buddyMapper.byUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
	buddyMapper.byAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
	buddyMapper.byLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)

	for _, m := range buddyMirrors {
		primary := beegfs.EntityIdSet{
			Uid:      m.PrimaryTarget.Uid,
			LegacyId: m.PrimaryTarget.LegacyId,
			Alias:    m.PrimaryTarget.Alias,
		}
		buddyMapper.byUID[m.PrimaryTarget.Uid] = primary
		buddyMapper.byAlias[m.PrimaryTarget.Alias] = primary
		buddyMapper.byLegacyID[m.PrimaryTarget.LegacyId] = primary
		buddyMapper.byUID[m.SecondaryTarget.Uid] = primary
		buddyMapper.byAlias[m.SecondaryTarget.Alias] = primary
		buddyMapper.byLegacyID[m.SecondaryTarget.LegacyId] = primary
	}
	return buddyMapper
}

// Map RST IDs to the full configuration details of that RST. We don't use a Mapper here and just
// directly use a map since RSTs aren't yet "first class" BeeGFS entities managed by the mgmtd
// service so they don't have aliases or UIDs.
func MapRstIdToConfig(rsts *beeremote.GetRSTConfigResponse) map[uint32]*flex.RemoteStorageTarget {
	var RstMapper = map[uint32]*flex.RemoteStorageTarget{}
	for _, rst := range rsts.Rsts {
		RstMapper[rst.Id] = rst
	}
	return RstMapper
}
