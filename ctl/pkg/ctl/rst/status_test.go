package rst

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPathStatusMarshalJSON verifies PathStatus serializes to a stable machine-readable string,
// independent of its display-oriented String() (which might use emojis).
func TestPathStatusMarshalJSON(t *testing.T) {
	cases := map[PathStatus]string{
		Synchronized:   `"synchronized"`,
		Offloaded:      `"offloaded"`,
		Unsynchronized: `"unsynchronized"`,
		NotSupported:   `"not-supported"`,
		NoTargets:      `"no-targets"`,
		NotAttempted:   `"not-attempted"`,
		Directory:      `"directory"`,
		Unknown:        `"unknown"`,
	}
	for status, want := range cases {
		data, err := json.Marshal(status)
		require.NoError(t, err)
		assert.Equal(t, want, string(data))
	}
}
