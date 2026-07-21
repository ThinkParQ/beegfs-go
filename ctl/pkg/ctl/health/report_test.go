package health

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/stats"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

// TestReportJSONSchema pins the structured-output contract: statuses render as stable strings, the
// detail projections are serialized, and the Raw fields (and the operational ConnCheckErr) are not.
func TestReportJSONSchema(t *testing.T) {
	total := uint64(1000)
	free := uint64(100)

	report := &Report{
		FS:           "beegfs://localhost:8010",
		FsUUID:       "uuid-1",
		Status:       Degraded,
		ConnCheckErr: errors.New("conn-err-should-not-appear"),
		Sections: []Section{
			{
				Title: "Busy Nodes",
				Checks: []Check{{
					Name:    "Busy Storage Nodes",
					Status:  Critical,
					Summary: "over threshold",
					Detail: BusyDetail{
						Nodes: []BusyNode{{
							Alias:          "storage_1",
							NumID:          1,
							NodeType:       "storage",
							QueuedRequests: 812,
						}},
						Raw: []stats.NodeStats{{}}, // must not be serialized
					},
				}},
			},
			{
				Title: "Targets",
				Detail: TargetsDetail{
					Targets: []TargetCapacity{{
						Target:          beegfs.EntityIdSet{Alias: "storage_1_tgt"},
						NodeType:        "storage",
						Reachability:    tgtBackend.ReachabilityOnline,
						Consistency:     tgtBackend.ConsistencyGood,
						CapacityPool:    tgtBackend.CapacityLow,
						TotalSpaceBytes: &total,
						FreeSpaceBytes:  &free,
					}},
					Raw: []tgtBackend.GetTargets_Result{{}}, // must not be serialized
				},
			},
			{
				Title:  "Connections to Server Nodes",
				Checks: []Check{{Name: "Fallbacks", Status: Degraded, Summary: "fallbacks in use"}},
				Detail: ConnectionsDetail{
					Clients: []ClientConn{{
						ID:      "client-1",
						Mount:   "/mnt/beegfs",
						Status:  Degraded,
						Summary: "fallbacks in use",
						MetaNodes: []NodeConn{{
							Alias: "meta_1",
							NumID: 1,
							Peers: []PeerConn{{Type: "TCP", IP: "10.0.0.1", Connections: 2, Fallback: false}},
						}},
						Raw: procfs.Client{ID: "raw-client-should-not-appear"},
					}},
				},
			},
		},
	}

	data, err := json.Marshal(report)
	require.NoError(t, err)
	s := string(data)

	// Statuses render as stable strings, never integers or emojis.
	assert.Contains(t, s, `"status":"degraded"`)
	assert.Contains(t, s, `"fallbackStatus":"degraded"`)

	// Busy detail is always present; thresholds are reported in the summary, not per node.
	assert.Contains(t, s, `"queuedRequests":812`)
	assert.NotContains(t, s, "degradedThreshold")
	assert.NotContains(t, s, "criticalThreshold")

	// Targets detail is serialized.
	assert.Contains(t, s, `"targets":`)
	assert.Contains(t, s, `"totalSpaceBytes":1000`)
	assert.Contains(t, s, `"capacityPool":"Low"`)

	// Connections detail is serialized down to per-peer entries.
	assert.Contains(t, s, `"clients":`)
	assert.Contains(t, s, `"mount":"/mnt/beegfs"`)
	assert.Contains(t, s, `"type":"TCP"`)

	// Raw fields and the operational ConnCheckErr must never appear in JSON.
	assert.NotContains(t, s, "raw-client-should-not-appear")
	assert.NotContains(t, s, "conn-err-should-not-appear")

	// The whole report must round-trip through a generic decode (valid JSON, no marshal panics).
	var out map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, "degraded", out["status"])
}

// TestClientConnectionsForJSON pins the `health network` JSON projection.
func TestClientConnectionsForJSON(t *testing.T) {
	clients := []procfs.Client{{
		ID:    "client-1",
		Mount: procfs.MountPoint{Path: "/mnt/beegfs"},
		MetaNodes: []procfs.Node{{
			Alias: "meta_1",
			NumID: 1,
			Peers: []procfs.Peer{{Type: beegfs.Tcp, IP: "10.0.0.1", Connections: 2, Fallback: true}},
		}},
	}}

	data, err := json.Marshal(ClientConnectionsFor(clients))
	require.NoError(t, err)
	s := string(data)

	assert.Contains(t, s, `"id":"client-1"`)
	assert.Contains(t, s, `"mount":"/mnt/beegfs"`)
	assert.Contains(t, s, `"metaNodes":`)
	assert.Contains(t, s, `"alias":"meta_1"`)
	assert.Contains(t, s, `"ip":"10.0.0.1"`)
	assert.Contains(t, s, `"connections":2`)
	assert.Contains(t, s, `"fallback":true`)

	// Valid JSON that decodes as an array.
	var out []map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Len(t, out, 1)
}
