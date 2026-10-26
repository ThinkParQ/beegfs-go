package benchmark

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

// TestStorageBenchResultJSON pins the benchmark structured-output contract: the enum fields serialize
// as their human-readable strings (not integers), and the throughput/detail fields are present.
func TestStorageBenchResultJSON(t *testing.T) {
	result := StorageBenchResult{
		Node:      beegfs.EntityIdSet{Alias: "storage_1"},
		ErrorCode: beegfs.BenchErr_NoError,
		Status:    beegfs.BenchRunning,
		Action:    beegfs.BenchStart,
		Type:      beegfs.WriteBench,
		TargetResults: []TargetResult{
			{ID: beegfs.EntityIdSet{Alias: "tgt_1"}, Throughput: 1288490188, Type: beegfs.WriteBench},
		},
	}

	data, err := json.Marshal([]StorageBenchResult{result})
	require.NoError(t, err)
	s := string(data)

	// Enums serialize as their String() form, never as bare integers.
	assert.Contains(t, s, `"status":"running"`)
	assert.Contains(t, s, `"action":"start"`)
	assert.Contains(t, s, `"type":"write"`)
	assert.Contains(t, s, `"errorCode":"executed benchmark command (no error)"`)
	assert.NotContains(t, s, `"status":3`)

	// Result detail is present.
	assert.Contains(t, s, `"throughput":1288490188`)
	assert.Contains(t, s, `"targetResults":`)

	// Valid JSON that decodes as an array.
	var out []map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Len(t, out, 1)
}
