package msg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobStateStrings covers the two resync/rebalance job state enums. Neither declares a sentinel
// variant - every value they define is a real state mirroring its C++ counterpart - so an unmatched
// value read off the wire must keep its number instead of borrowing a name no constant uses.
func TestJobStateStrings(t *testing.T) {
	t.Run("BuddyResyncJobState", func(t *testing.T) {
		// 0-5 mirror BuddyResyncJobState in
		// beegfs-core/common/source/common/storage/mirroring/BuddyResyncJobStatistics.h.
		assert.Equal(t, "not-started", NotStarted.String())
		assert.Equal(t, "errors", Errors.String())
		assert.Equal(t, "unknown(6)", BuddyResyncJobState(6).String())
		assert.Equal(t, "unknown(-1)", BuddyResyncJobState(-1).String())

		data, err := json.Marshal(BuddyResyncJobState(6))
		require.NoError(t, err)
		assert.Equal(t, `"unknown(6)"`, string(data))
	})

	t.Run("ChunkBalancerJobState", func(t *testing.T) {
		assert.Equal(t, "not-started", ChunkBalancerJobState(ChunkBalancerJobStateNotStarted).String())
		assert.Equal(t, "idle", ChunkBalancerJobState(ChunkBalancerJobStateIdle).String())
		assert.Equal(t, "unknown(9)", ChunkBalancerJobState(9).String())

		data, err := json.Marshal(ChunkBalancerJobState(9))
		require.NoError(t, err)
		assert.Equal(t, `"unknown(9)"`, string(data))
	})

	// RebalanceIDType does declare a sentinel, so it must render as that name, not as a number.
	t.Run("RebalanceIDTypeSentinelKeepsItsName", func(t *testing.T) {
		assert.Equal(t, "invalid", RebalanceIDType(RebalanceIDTypeInvalid).String())
		assert.Equal(t, "unknown(9)", RebalanceIDType(9).String())
	})
}
