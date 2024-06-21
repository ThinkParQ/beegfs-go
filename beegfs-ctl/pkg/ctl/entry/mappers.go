package entry

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/target"
	"github.com/thinkparq/gobee/beegfs"
	"github.com/thinkparq/protobuf/go/flex"
)

// A generic mapper implementation for looking up different types of BeeGFS entities based on an ID.
// Before using a particular mapper first use the initXMapper function to fetch and initialize its
// mappings. These functions are always safe to call if it is uncertain a mapper has been
// initialized, but once mappings are initialized, future calls will not update the local mappings
// even if they are updated externally. If initializing a mapper returns an error, it is still safe
// to use the corresponding global mapper, the mapper.Get() function will just return an error. If
// calling initXMapper fails it can be called again later to try again. This design allows mappers
// to be initialized as part of setup code that has a context and makes decisions about when to
// return an error, but the mappers can always safely be used from code that should never return an
// error and doesn't need a context.
type mapper[C comparable, T any] struct {
	mapping map[C]T
}

var (
	ErrMapperNotInitialized = errors.New("mapper not initialized (this is likely a bug)")
	ErrMapperNotFound       = errors.New("requested ID not found")
)

func (m *mapper[C, T]) Get(id C) (t T, err error) {
	if m.mapping == nil {
		return t, ErrMapperNotInitialized
	}
	v, ok := m.mapping[id]
	if !ok {
		return t, ErrMapperNotFound
	}
	return v, nil
}

// Map of legacy storage targets IDs to the entity ID set of their owning node.
var storageTargetMapper = &mapper[beegfs.NumId, beegfs.EntityIdSet]{}

func initStorageTargetMapper(ctx context.Context) error {
	if storageTargetMapper.mapping == nil {
		targets, err := target.GetTargets(ctx)
		if err != nil {
			return fmt.Errorf("unable to get storage target list from management: %w", err)
		}

		storageTargetMapper.mapping = make(map[beegfs.NumId]beegfs.EntityIdSet)
		for _, tgt := range targets {
			if tgt.NodeType == beegfs.Storage {
				storageTargetMapper.mapping[tgt.Target.LegacyId.NumId] = tgt.Node
			}
		}
	}
	return nil
}

// Map of legacy storage pool IDs to pools.
var storagePoolMapper = &mapper[beegfs.NumId, pool.GetStoragePools_Result]{}

func initStoragePoolMapper(ctx context.Context) error {
	if storagePoolMapper.mapping == nil {
		pools, err := pool.GetStoragePools(ctx)
		if err != nil {
			return fmt.Errorf("unable to get storage pool list from management: %w", err)
		}

		storagePoolMapper.mapping = make(map[beegfs.NumId]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolMapper.mapping[pool.Pool.LegacyId.NumId] = pool
		}
	}
	return nil
}

// Map of storage pool UIDs to pools.
var storagePoolUIDMapper = &mapper[beegfs.Uid, pool.GetStoragePools_Result]{}

func initStoragePoolUIDMapper(ctx context.Context) error {
	if storagePoolUIDMapper.mapping == nil {
		pools, err := pool.GetStoragePools(ctx)
		if err != nil {
			return fmt.Errorf("unable to get storage pool list from management: %w", err)
		}

		storagePoolUIDMapper.mapping = make(map[beegfs.Uid]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolUIDMapper.mapping[pool.Pool.Uid] = pool
		}
	}
	return nil
}

// Map of storage pool aliases to pools.
var storagePoolAliasMapper = &mapper[beegfs.Alias, pool.GetStoragePools_Result]{}

func initStoragePoolAliasMapper(ctx context.Context) error {
	if storagePoolAliasMapper.mapping == nil {
		pools, err := pool.GetStoragePools(ctx)
		if err != nil {
			return fmt.Errorf("unable to get storage pool list from management: %w", err)
		}

		storagePoolAliasMapper.mapping = make(map[beegfs.Alias]pool.GetStoragePools_Result)
		for _, pool := range pools {
			storagePoolAliasMapper.mapping[pool.Pool.Alias] = pool
		}
	}
	return nil
}

// Map of RST IDs to their names.
var rstMapper = &mapper[string, *flex.RemoteStorageTarget]{}

func initRSTMapper(ctx context.Context) error {
	if rstMapper.mapping == nil {
		rsts, err := rst.GetRSTConfig(ctx)
		if err != nil {
			return fmt.Errorf("unable to get Remote Storage Target configuration from BeeRemote: %w", err)
		}

		rstMapper.mapping = make(map[string]*flex.RemoteStorageTarget)
		for _, rst := range rsts.Rsts {
			rstMapper.mapping[rst.Id] = rst
		}
	}
	return nil
}

// Map of legacy metadata buddy group IDs to the entity ID side of the primary metadata node.
var metaBuddyMapper = &mapper[beegfs.NumId, beegfs.EntityIdSet]{}

func initMetaBuddyMapper(ctx context.Context) error {

	if metaBuddyMapper.mapping == nil {
		buddyMirrors, err := buddygroup.GetBuddyGroups(ctx)
		if err != nil {
			return err
		}

		metaBuddyMapper.mapping = make(map[beegfs.NumId]beegfs.EntityIdSet)
		for _, m := range buddyMirrors {
			if m.NodeType == beegfs.Meta {
				metaBuddyMapper.mapping[m.BuddyGroup.LegacyId.NumId] = m.PrimaryTarget
			}
		}
	}
	return nil
}
