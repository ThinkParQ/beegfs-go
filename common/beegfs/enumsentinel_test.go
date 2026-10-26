package beegfs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSentinelAndUnmatchedEnumStrings pins the two halves of the enum display rule for the core
// domain enums, the same invariant TestStorageBenchEnumStrings covers for the storage bench ones: a
// declared sentinel renders as its constant name, and only a value matching no variant keeps a
// number as unknown(<n>). These are the enums whose sentinel used to be reached through the default
// case, so a value from a newer daemon was indistinguishable from the sentinel a local caller sets.
func TestSentinelAndUnmatchedEnumStrings(t *testing.T) {
	// EntryType and StripePatternType are read off the wire from meta as uint32, so an unmatched
	// value is the case that actually occurs in the field, against a newer or misbehaving daemon.
	t.Run("EntryType", func(t *testing.T) {
		assert.Equal(t, "file", EntryRegularFile.String())
		assert.Equal(t, "unknown", EntryUnknown.String())
		assert.Equal(t, "unknown(42)", EntryType(42).String())
		assert.NotEqual(t, EntryUnknown.String(), EntryType(42).String())
	})

	t.Run("StripePatternType", func(t *testing.T) {
		assert.Equal(t, "buddy-mirror", StripePatternBuddyMirror.String())
		assert.Equal(t, "invalid", StripePatternInvalid.String())
		assert.Equal(t, "unknown(9)", StripePatternType(9).String())
	})

	t.Run("NicType", func(t *testing.T) {
		assert.Equal(t, "rdma", Rdma.String())
		assert.Equal(t, "invalid", InvalidNicType.String())
		assert.Equal(t, "unknown(9)", NicType(9).String())
		assert.Equal(t, "unknown(-1)", NicType(-1).String())
	})

	t.Run("NodeType", func(t *testing.T) {
		assert.Equal(t, "meta", Meta.String())
		assert.Equal(t, "invalid", InvalidNodeType.String())
		assert.Equal(t, "unknown(9)", NodeType(9).String())
	})

	t.Run("ConsistencyState", func(t *testing.T) {
		assert.Equal(t, "needs-resync", NeedsResync.String())
		assert.Equal(t, "unspecified", ConsistencyStateUnspecified.String())
		assert.Equal(t, "unknown(9)", ConsistencyState(9).String())
	})

	// The unmatched form must survive into json too, since that is the output an operator reports
	// back when a value nothing recognizes shows up.
	t.Run("UnmatchedValueSurvivesMarshalJSON", func(t *testing.T) {
		for _, v := range []any{EntryType(42), StripePatternType(9), NicType(9), NodeType(9), ConsistencyState(9)} {
			data, err := json.Marshal(v)
			require.NoError(t, err, "value %v", v)
			assert.Contains(t, string(data), "unknown(", "value %v", v)
		}
	})
}

// TestNodeTypeStringConsumersTolerateUnmatchedValue guards the two places NodeType.String() is not
// display output. LegacyId slices its first byte and the ping ioctl copies it into a fixed 16 byte
// kernel buffer, so the unmatched form must stay non-empty and must not round-trip as a real type.
func TestNodeTypeStringConsumersTolerateUnmatchedValue(t *testing.T) {
	assert.NotEmpty(t, NodeType(9).String())
	assert.Equal(t, "u:5", LegacyId{NumId: 5, NodeType: NodeType(9)}.String())
	assert.Equal(t, InvalidNodeType, NodeTypeFromString(NodeType(9).String()))
	assert.Equal(t, InvalidNodeType, NodeTypeFromString(InvalidNodeType.String()))
}
