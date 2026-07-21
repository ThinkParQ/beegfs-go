package resync

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func TestNewMetaResyncStats(t *testing.T) {
	r := msg.GetMetaResyncStatsResp{
		State:             msg.Running,
		StartTime:         1000,
		EndTime:           0, // still running
		DiscoveredDirs:    5,
		GatherErrors:      1,
		SyncedDirs:        4,
		SyncedFiles:       10,
		ErrorDirs:         2,
		ErrorFiles:        3,
		SessionsToSync:    7,
		SyncedSessions:    6,
		SessionSyncErrors: 1,
		ModObjectsSynced:  8,
		ModSyncErrors:     0,
	}

	stats := newMetaResyncStats(r)
	assert.Equal(t, "meta", stats.TargetType)
	assert.Equal(t, "Running", stats.State)
	assert.Equal(t, uint64(1), stats.DiscoveryErrors) // GatherErrors -> DiscoveryErrors
	assert.True(t, stats.SessionSyncErrors)           // uint8 1 -> bool
	assert.NotEmpty(t, stats.StartTime)
	assert.Empty(t, stats.EndTime) // zero time -> "" (omitted)

	data, err := json.Marshal(stats)
	require.NoError(t, err)
	s := string(data)
	assert.Contains(t, s, `"targetType":"meta"`)
	assert.Contains(t, s, `"state":"Running"`)
	assert.Contains(t, s, `"discoveryErrors":1`)
	assert.Contains(t, s, `"sessionSyncErrors":true`)
	assert.NotContains(t, s, `"endTime"`) // omitted while running
}

func TestNewStorageResyncStats(t *testing.T) {
	r := msg.GetStorageResyncStatsResp{
		State:           msg.Success,
		StartTime:       1000,
		EndTime:         2000,
		DiscoveredFiles: 100,
		DiscoveredDirs:  20,
		MatchedFiles:    50,
		MatchedDirs:     10,
		SyncedFiles:     45,
		SyncedDirs:      9,
		ErrorFiles:      1,
		ErrorDirs:       0,
	}

	stats := newStorageResyncStats(r)
	assert.Equal(t, "storage", stats.TargetType)
	assert.Equal(t, "Success", stats.State)
	assert.NotEmpty(t, stats.EndTime) // completed -> present

	data, err := json.Marshal(stats)
	require.NoError(t, err)
	s := string(data)
	assert.Contains(t, s, `"targetType":"storage"`)
	assert.Contains(t, s, `"matchedFiles":50`)
	assert.Contains(t, s, `"endTime":`)
}
