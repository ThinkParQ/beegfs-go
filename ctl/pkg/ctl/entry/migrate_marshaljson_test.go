package entry

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateStatusMarshalJSON(t *testing.T) {
	data, err := json.Marshal(MigrateStatus(MigratedFile))
	require.NoError(t, err)
	assert.Equal(t, `"migrated file"`, string(data))
}
