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
	assert.Equal(t, `"migrated-file"`, string(data))
}

// TestMigrateStatusSentinelAndUnmatched pins that the declared sentinel and a value matching no
// variant no longer read alike, so a status that never got set stays distinguishable from one the
// enum does not cover.
func TestMigrateStatusSentinelAndUnmatched(t *testing.T) {
	assert.Equal(t, "migrated-file", MigratedFile.String())
	assert.Equal(t, "unknown", MigrateUnknown.String())
	assert.Equal(t, "unknown(42)", MigrateStatus(42).String())

	data, err := json.Marshal(MigrateStatus(42))
	require.NoError(t, err)
	assert.Equal(t, `"unknown(42)"`, string(data))
}
