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
		Synchronized:   `"Synchronized (1)"`,
		Offloaded:      `"Offloaded (2)"`,
		Unsynchronized: `"Unsynchronized (3)"`,
		NotSupported:   `"Not Supported (4)"`,
		NoTargets:      `"No Targets (5)"`,
		NotAttempted:   `"Not Attempted (6)"`,
		Directory:      `"Directory (7)"`,
		Unknown:        `"Unknown (0)"`,
	}
	for status, want := range cases {
		data, err := json.Marshal(status)
		require.NoError(t, err)
		assert.Equal(t, want, string(data))
	}
}
