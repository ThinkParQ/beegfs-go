package beegfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStorageBenchEnumStrings pins the two halves of the enum display rule for the storage bench
// enums: a declared sentinel renders as its constant name, and only a value matching no variant
// keeps a number as unknown(<n>). The NONE values come from the C++ enums in
// beegfs-core/common/source/common/benchmark/StorageBench.h and are read off the wire, so they must
// resolve to a name rather than falling through to the unmatched case.
func TestStorageBenchEnumStrings(t *testing.T) {
	t.Run("StorageBenchAction", func(t *testing.T) {
		cases := []struct {
			val  StorageBenchAction
			want string
		}{
			{BenchStart, "start"},
			{BenchStop, "stop"},
			{BenchStatus, "status"},
			{BenchCleanup, "cleanup"},
			{BenchActionNone, "none"},         // C++ StorageBenchAction_NONE == 4
			{BenchUnspecified, "unspecified"}, // CTL side sentinel
			{StorageBenchAction(99), "unknown(99)"},
			{StorageBenchAction(-7), "unknown(-7)"},
		}
		for _, c := range cases {
			assert.Equal(t, c.want, c.val.String())
		}
	})

	t.Run("StorageBenchType", func(t *testing.T) {
		cases := []struct {
			val  StorageBenchType
			want string
		}{
			{ReadBench, "read"},
			{WriteBench, "write"},
			{BenchTypeNone, "none"}, // C++ StorageBenchType_NONE == 2
			{NoBench, "no-bench"},   // CTL side sentinel
			{StorageBenchType(42), "unknown(42)"},
		}
		for _, c := range cases {
			assert.Equal(t, c.want, c.val.String())
		}
	})

	t.Run("StorageBenchStatus", func(t *testing.T) {
		// 0-7 mirror the C++ enum and are all named; there is no sentinel to render.
		assert.Equal(t, "uninitialized", BenchUninitialized.String())
		assert.Equal(t, "finished", BenchFinished.String())
		assert.Equal(t, "unknown(8)", StorageBenchStatus(8).String())
	})

	// An unmatched value must not be mistaken for a sentinel, in the table or in JSON.
	t.Run("SentinelAndUnmatchedAreDistinct", func(t *testing.T) {
		assert.NotEqual(t, BenchUnspecified.String(), StorageBenchAction(99).String())
		assert.NotEqual(t, NoBench.String(), StorageBenchType(42).String())
	})
}
