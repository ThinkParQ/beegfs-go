package entry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/target"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// A generic mapper implementation for looking up different types of BeeGFS entities based on their
// entity ID. Before using a particular mapper first use the initXMapper function to fetch and
// initialize its mappings. These functions are always safe to call if it is uncertain a mapper has
// been initialized, but once mappings are initialized, future calls will not update the local
// mappings even if they are updated externally. If initializing a mapper returns an error, it is
// still safe to use the corresponding global mapper, the mapper.Get() function will just return an
// error. If calling initXMapper fails it can be called again later to try again. This design allows
// mappers to be initialized as part of setup code that has a context and makes decisions about when
// to return an error, but the mappers can always safely be used from code that should never return
// an error and doesn't need a context.
type mapper[T any] struct {
	mappingByUID      map[beegfs.Uid]T
	mappingByAlias    map[beegfs.Alias]T
	mappingByLegacyID map[beegfs.LegacyId]T
	mu                sync.RWMutex
}

var (
	ErrMapperNotInitialized = errors.New("mapper not initialized (this is likely a bug)")
	ErrMapperNotFound       = errors.New("requested ID not found")
)

func (m *mapper[T]) Get(id beegfs.EntityId) (t T, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.mappingByUID == nil ||
		m.mappingByAlias == nil ||
		m.mappingByLegacyID == nil {
		return t, ErrMapperNotInitialized
	}

	var ok bool
	switch v := id.(type) {
	case beegfs.Uid:
		t, ok = m.mappingByUID[v]
	case beegfs.Alias:
		t, ok = m.mappingByAlias[v]
	case beegfs.LegacyId:
		t, ok = m.mappingByLegacyID[v]
	}

	if !ok {
		return t, ErrMapperNotFound
	}
	return t, nil
}

// Map of target entity IDs to the entity ID set of their owning node.
var targetMapper = &mapper[beegfs.EntityIdSet]{}

func initTargetMapper(ctx context.Context) error {
	targetMapper.mu.Lock()
	defer targetMapper.mu.Unlock()
	if targetMapper.mappingByUID == nil &&
		targetMapper.mappingByAlias == nil &&
		targetMapper.mappingByLegacyID == nil {

		targets, err := target.GetTargets(ctx)
		if err != nil {
			return fmt.Errorf("unable to get storage target list from management: %w", err)
		}

		targetMapper.mappingByUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
		for _, tgt := range targets {
			targetMapper.mappingByUID[tgt.Target.Uid] = tgt.Node
		}

		targetMapper.mappingByAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
		for _, tgt := range targets {
			targetMapper.mappingByAlias[tgt.Target.Alias] = tgt.Node
		}

		targetMapper.mappingByLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
		for _, tgt := range targets {
			targetMapper.mappingByLegacyID[tgt.Target.LegacyId] = tgt.Node
		}
	}
	return nil
}

// Map of legacy storage pool IDs to pools.
var storagePoolMapper = &mapper[pool.GetStoragePools_Result]{}

func initStoragePoolMapper(ctx context.Context) error {
	storagePoolMapper.mu.Lock()
	defer storagePoolMapper.mu.Unlock()
	if storagePoolMapper.mappingByUID == nil &&
		storagePoolMapper.mappingByAlias == nil &&
		storagePoolMapper.mappingByLegacyID == nil {
		pools, err := pool.GetStoragePools(ctx, pool.GetStoragePools_Config{})
		if err != nil {
			return fmt.Errorf("unable to get storage pool list from management: %w", err)
		}

		storagePoolMapper.mappingByUID = make(map[beegfs.Uid]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolMapper.mappingByUID[pool.Pool.Uid] = pool
		}

		storagePoolMapper.mappingByAlias = make(map[beegfs.Alias]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolMapper.mappingByAlias[pool.Pool.Alias] = pool
		}

		storagePoolMapper.mappingByLegacyID = make(map[beegfs.LegacyId]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolMapper.mappingByLegacyID[pool.Pool.LegacyId] = pool
		}
	}
	return nil
}

// Map of RST IDs to their names. IMPORTANT: This is a little hacky since RSTs aren't "first class"
// BeeGFS entities managed by the mgmtd service so they do not have aliases or UIDs. The only way to
// lookup an RST is by its string ID, which here we treat as a legacy ID.
//
// For example to get RST ID 1: `rstMapper.Get(beegfs.LegacyId{NumId: beegfs.NumId(1)})`.
// Note the omission of a NodeType, which currently has no meaning for RSTs.
var rstMapper = &mapper[*flex.RemoteStorageTarget]{}

func initRSTMapper(ctx context.Context) error {
	rstMapper.mu.Lock()
	defer rstMapper.mu.Unlock()
	if rstMapper.mappingByUID == nil &&
		rstMapper.mappingByAlias == nil &&
		rstMapper.mappingByLegacyID == nil {
		beeRemote, err := config.BeeRemoteClient()
		if err != nil {
			return err
		}
		// Don't call into RST package because it needs to call GetEntry which would cause an import cycle.
		rsts, err := beeRemote.GetRSTConfig(ctx, &beeremote.GetRSTConfigRequest{})
		if err != nil {
			return err
		}
		rstMapper.mappingByAlias = make(map[beegfs.Alias]*flex.RemoteStorageTarget)
		rstMapper.mappingByUID = make(map[beegfs.Uid]*flex.RemoteStorageTarget)
		rstMapper.mappingByLegacyID = make(map[beegfs.LegacyId]*flex.RemoteStorageTarget)
		for _, rst := range rsts.Rsts {
			rstMapper.mappingByLegacyID[beegfs.LegacyId{NumId: beegfs.NumId(rst.Id)}] = rst
		}
	}
	return nil

}

// Map of buddy group entity IDs to the entity ID set of the primary node.
var metaBuddyMapper = &mapper[beegfs.EntityIdSet]{}

func initMetaBuddyMapper(ctx context.Context) error {
	metaBuddyMapper.mu.Lock()
	defer metaBuddyMapper.mu.Unlock()
	if metaBuddyMapper.mappingByUID == nil &&
		metaBuddyMapper.mappingByAlias == nil &&
		metaBuddyMapper.mappingByLegacyID == nil {
		buddyMirrors, err := buddygroup.GetBuddyGroups(ctx)
		if err != nil {
			return err
		}

		metaBuddyMapper.mappingByUID = make(map[beegfs.Uid]beegfs.EntityIdSet)
		for _, m := range buddyMirrors {
			metaBuddyMapper.mappingByUID[m.BuddyGroup.Uid] = m.PrimaryTarget
		}
		metaBuddyMapper.mappingByAlias = make(map[beegfs.Alias]beegfs.EntityIdSet)
		for _, m := range buddyMirrors {
			metaBuddyMapper.mappingByAlias[m.BuddyGroup.Alias] = m.PrimaryTarget
		}
		metaBuddyMapper.mappingByLegacyID = make(map[beegfs.LegacyId]beegfs.EntityIdSet)
		for _, m := range buddyMirrors {
			metaBuddyMapper.mappingByLegacyID[m.BuddyGroup.LegacyId] = m.PrimaryTarget
		}
	}
	return nil
}
