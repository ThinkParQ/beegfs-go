package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete reproduces the reported issue in
// isolation: after every dispatched bulk-retrieve record is marked complete (exactly as
// CompleteWorkRequests -> xtreemstoreS3BulkMarkRequestComplete does for each individual sub-job),
// a fresh Execute() pass (as happens on every builder-job reschedule) should stop asking to
// reschedule.
func TestBulkRetrieveExecuteStopsReschedulingOnceAllComplete(t *testing.T) {
	tmpDir := t.TempDir()
	const stateMountPath = "state"
	const operation = "bulk-retrieve"

	newManager := func(t *testing.T) *xtreemstoreS3BulkRetrieveManager {
		m := &xtreemstoreS3BulkRetrieveManager{
			rstId:          1,
			mountPath:      tmpDir,
			stateMountPath: stateMountPath,
			operation:      operation,
			state:          &xtreemstoreS3BulkRetrieveManagerState{},
		}
		require.NoError(t, m.openState())
		return m
	}

	m := newManager(t)
	for _, p := range []string{"/a", "/b", "/c"} {
		req := &beeremote.JobRequest{
			Type:     &beeremote.JobRequest_Sync{Sync: &flex.SyncJob{RemotePath: p}},
			BulkInfo: &flex.BulkJobRequestInfo{},
		}
		require.NoError(t, m.AddRequest(context.Background(), req))
	}
	require.NoError(t, m.closeState())

	// First Execute pass: everything is Initialized, so all 3 get dispatched and marked Sent.
	m = newManager(t)
	walkCh, getResults, err := m.Execute(context.Background())
	require.NoError(t, err)

	var dispatched []*flex.BulkJobRequestInfo
	for r := range walkCh {
		require.NoError(t, r.Err)
		dispatched = append(dispatched, r.BulkInfo)
	}
	result := getResults()
	require.NoError(t, result.Err)
	assert.True(t, result.Reschedule, "should reschedule while records are still Sent")
	require.Len(t, dispatched, 3)
	require.NoError(t, m.closeState())

	// Simulate every dispatched sub-job completing successfully, exactly like
	// xtreemstoreS3BulkMarkRequestComplete does when CompleteWorkRequests is called.
	for _, bulkInfo := range dispatched {
		completeManager := &xtreemstoreS3BulkRetrieveManager{
			mountPath:      tmpDir,
			stateMountPath: bulkInfo.StateMountPath,
			operation:      bulkInfo.Operation,
		}
		require.NoError(t, completeManager.MarkComplete(bulkInfo.JobIndex))
	}

	// Second Execute pass (fresh manager instance, exactly as happens on a real builder-job
	// reschedule): every record is now Complete, so this should NOT ask to reschedule again.
	m = newManager(t)
	walkCh2, getResults2, err := m.Execute(context.Background())
	require.NoError(t, err)
	for r := range walkCh2 {
		t.Fatalf("expected no further dispatch once everything is complete, got: %+v", r)
	}
	result2 := getResults2()
	require.NoError(t, result2.Err)
	assert.False(t, result2.Reschedule, "bulk operation should stop rescheduling once every record is marked complete")
}
