package health

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

func ptr[T any](v T) *T { return &v }

// storageTarget builds a storage target assigned to the pool with the given numeric ID. Capacity
// fields are set from the given values so tests can also exercise targets that report nothing.
func storageTarget(targetID uint64, poolID uint64, totalSpace, freeSpace, totalInodes, freeInodes *uint64) tgtBackend.GetTargets_Result {
	return tgtBackend.GetTargets_Result{
		Target:   beegfs.EntityIdSet{Uid: beegfs.Uid(targetID), LegacyId: beegfs.LegacyId{NumId: beegfs.NumId(targetID), NodeType: beegfs.Storage}},
		NodeType: beegfs.Storage,
		StoragePool: &beegfs.EntityIdSet{
			Uid:      beegfs.Uid(1000 + poolID),
			Alias:    beegfs.Alias("pool_" + string(rune('0'+poolID))),
			LegacyId: beegfs.LegacyId{NumId: beegfs.NumId(poolID), NodeType: beegfs.Storage},
		},
		ReachabilityState: tgtBackend.ReachabilityOnline,
		ConsistencyState:  tgtBackend.ConsistencyGood,
		CapacityPool:      tgtBackend.CapacityNormal,
		TotalSpaceBytes:   totalSpace,
		FreeSpaceBytes:    freeSpace,
		TotalInodes:       totalInodes,
		FreeInodes:        freeInodes,
	}
}

func TestAggregateStoragePoolsSumsCapacityPerPool(t *testing.T) {
	targets := []tgtBackend.GetTargets_Result{
		storageTarget(103, 2, ptr[uint64](300), ptr[uint64](30), ptr[uint64](3000), ptr[uint64](300)),
		storageTarget(101, 1, ptr[uint64](100), ptr[uint64](10), ptr[uint64](1000), ptr[uint64](100)),
		storageTarget(102, 1, ptr[uint64](200), ptr[uint64](20), ptr[uint64](2000), ptr[uint64](200)),
	}

	pools := aggregateStoragePools(targets)
	require.Len(t, pools, 2)

	// Pools are sorted by pool ID regardless of the order the targets arrived in.
	assert.Equal(t, beegfs.NumId(1), pools[0].pool.LegacyId.NumId)
	assert.Equal(t, beegfs.NumId(2), pools[1].pool.LegacyId.NumId)

	assert.Equal(t, uint64(2), pools[0].targets)
	assert.Equal(t, uint64(300), *pools[0].totalSpaceBytes)
	assert.Equal(t, uint64(30), *pools[0].freeSpaceBytes)
	assert.Equal(t, uint64(3000), *pools[0].totalInodes)
	assert.Equal(t, uint64(300), *pools[0].freeInodes)
	assert.Equal(t, map[string]uint64{tgtBackend.ReachabilityOnline: 2}, pools[0].reachability)
	assert.Equal(t, map[string]uint64{tgtBackend.ConsistencyGood: 2}, pools[0].consistency)
	assert.Equal(t, map[string]uint64{tgtBackend.CapacityNormal: 2}, pools[0].capacityPools)

	assert.Equal(t, uint64(1), pools[1].targets)
	assert.Equal(t, uint64(300), *pools[1].totalSpaceBytes)
}

func TestAggregateStoragePoolsSkipsTargetsWithoutAPool(t *testing.T) {
	metaTarget := tgtBackend.GetTargets_Result{
		Target:   beegfs.EntityIdSet{LegacyId: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta}},
		NodeType: beegfs.Meta,
		// A metadata target has no storage pool assignment.
		TotalSpaceBytes: ptr[uint64](999),
	}
	pools := aggregateStoragePools([]tgtBackend.GetTargets_Result{
		metaTarget,
		storageTarget(101, 1, ptr[uint64](100), ptr[uint64](10), nil, nil),
	})

	require.Len(t, pools, 1)
	assert.Equal(t, uint64(1), pools[0].targets)
	assert.Equal(t, uint64(100), *pools[0].totalSpaceBytes)
}

func TestAggregateStoragePoolsLeavesUnreportedCapacityNil(t *testing.T) {
	t.Run("no target reports", func(t *testing.T) {
		pools := aggregateStoragePools([]tgtBackend.GetTargets_Result{
			storageTarget(101, 1, nil, nil, nil, nil),
			storageTarget(102, 1, nil, nil, nil, nil),
		})
		require.Len(t, pools, 1)
		assert.Nil(t, pools[0].totalSpaceBytes)
		assert.Nil(t, pools[0].freeSpaceBytes)
		assert.Nil(t, pools[0].totalInodes)
		assert.Nil(t, pools[0].freeInodes)
	})

	t.Run("some targets report", func(t *testing.T) {
		pools := aggregateStoragePools([]tgtBackend.GetTargets_Result{
			storageTarget(101, 1, ptr[uint64](100), ptr[uint64](10), nil, nil),
			storageTarget(102, 1, nil, nil, nil, nil),
		})
		require.Len(t, pools, 1)
		// The sum covers only what was reported; inodes nothing reported stay nil.
		assert.Equal(t, uint64(100), *pools[0].totalSpaceBytes)
		assert.Equal(t, uint64(10), *pools[0].freeSpaceBytes)
		assert.Nil(t, pools[0].totalInodes)
	})
}

func TestAggregateStoragePoolsCountsMixedStates(t *testing.T) {
	online := storageTarget(101, 1, nil, nil, nil, nil)
	offline := storageTarget(102, 1, nil, nil, nil, nil)
	offline.ReachabilityState = tgtBackend.ReachabilityOffline
	offline.ConsistencyState = tgtBackend.ConsistencyNeedsResync
	offline.CapacityPool = tgtBackend.CapacityEmergency
	unset := storageTarget(103, 1, nil, nil, nil, nil)
	unset.ReachabilityState = ""
	unset.ConsistencyState = ""
	unset.CapacityPool = ""

	pools := aggregateStoragePools([]tgtBackend.GetTargets_Result{online, offline, unset})
	require.Len(t, pools, 1)

	assert.Equal(t, "1 online, 1 offline, 1 unknown", summarizeStates(pools[0].reachability, reachabilityOrder))
	assert.Equal(t, "1 good, 1 needs-resync, 1 unknown", summarizeStates(pools[0].consistency, consistencyOrder))
	assert.Equal(t, "1 normal, 1 emergency, 1 unknown", summarizeStates(pools[0].capacityPools, capacityPoolOrder))
}

func TestAggregateStoragePoolsEmptyInput(t *testing.T) {
	assert.Empty(t, aggregateStoragePools(nil))
}

func TestSummarizeStates(t *testing.T) {
	tests := []struct {
		name   string
		counts map[string]uint64
		want   string
	}{
		{"no counts", map[string]uint64{}, "-"},
		{"zero counts are omitted", map[string]uint64{tgtBackend.ReachabilityOnline: 0}, "-"},
		{"single state", map[string]uint64{tgtBackend.ReachabilityOnline: 3}, "3 online"},
		{
			"listed in order not map order",
			map[string]uint64{tgtBackend.ReachabilityOffline: 1, tgtBackend.ReachabilityOnline: 2, tgtBackend.ReachabilityProbablyOffline: 4},
			"2 online, 4 probably-offline, 1 offline",
		},
		{
			// A state from a newer management service must still be shown, after the known ones.
			"unrecognized states sort last alphabetically",
			map[string]uint64{"Zeta": 1, tgtBackend.ReachabilityOnline: 2, "Alpha": 3},
			"2 online, 3 Alpha, 1 Zeta",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, summarizeStates(tc.counts, reachabilityOrder))
		})
	}
}
