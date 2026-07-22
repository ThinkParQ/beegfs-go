package beegfs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnumMarshalJSON verifies the domain enums serialize as their human-readable String() form
// rather than as integers. Values are cast to their enum type explicitly so the test exercises the
// MarshalJSON method regardless of whether the source constants are typed.
func TestEnumMarshalJSON(t *testing.T) {
	cases := []struct {
		name string
		val  any
		want string
	}{
		{"EntryType", EntryType(EntryRegularFile), `"file"`},
		{"OpsErr", OpsErr(OpsErr_SUCCESS), `"Success"`},
		{"AccessFlags", AccessFlags(AccessFlagUnlocked), `"Unlocked"`},
		{"DataState", DataState(DataStateAvailable), `"Available"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := json.Marshal(c.val)
			require.NoError(t, err)
			assert.Equal(t, c.want, string(data))
		})
	}
}
