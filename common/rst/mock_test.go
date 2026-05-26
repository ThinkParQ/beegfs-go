package rst

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMockClientGetJobRequestBuildsExecutableMockRequest(t *testing.T) {
	client := &MockClient{}
	cfg := &flex.JobRequestCfg{
		Path:                "/mnt/file",
		RemotePath:          "remote/file",
		RemoteStorageTarget: 7,
		Download:            true,
		LockedInfo: &flex.JobLockedInfo{
			ExternalId: "restore-123",
			RemoteSize: 4096,
		},
	}

	request := client.GetJobRequest(cfg)
	require.NotNil(t, request)
	require.True(t, request.HasMock())
	assert.Equal(t, "/mnt/file", request.GetPath())
	assert.Equal(t, uint32(7), request.GetRemoteStorageTarget())
	assert.Equal(t, int32(1), request.GetMock().GetNumTestSegments())
	assert.Equal(t, int64(4096), request.GetMock().GetFileSize())
	assert.Equal(t, "restore-123", request.GetMock().GetExternalId())
	require.NotNil(t, request.GetMock().GetCfg())
	assert.Equal(t, "remote/file", request.GetMock().GetCfg().GetRemotePath())

	job := &beeremote.Job{
		Id:         "job-1",
		ExternalId: "ext-1",
		Request:    request,
	}
	workRequests, err := client.GenerateWorkRequests(context.Background(), nil, job, 1)
	require.NoError(t, err)
	require.Len(t, workRequests, 1)
	assert.True(t, workRequests[0].HasMock())
	assert.Equal(t, int64(4096), workRequests[0].GetMock().GetFileSize())
}

func TestMockClientBulkOperationReplaysArchivedRequestsOnce(t *testing.T) {
	client := &MockClient{}
	cfg := &flex.JobRequestCfg{
		Path:                "/mnt/file",
		RemotePath:          "remote/file",
		RemoteStorageTarget: 7,
		Download:            true,
		LockedInfo: &flex.JobLockedInfo{
			IsArchived: true,
		},
	}

	request := client.GetJobRequest(cfg)
	include, operation := client.IncludeInBulkRequest(context.Background(), request)
	require.True(t, include)
	assert.Equal(t, "retrieve", operation)

	bulkOp, err := client.OpenBulkOperation(context.Background(), ".beegfs-rst/job/job-1/7", operation)
	require.NoError(t, err)
	require.NoError(t, bulkOp.AddRequest(context.Background(), request))

	walkCh, getResults, err := bulkOp.Execute(context.Background())
	require.NoError(t, err)

	var replayPaths []string
	for walkResp := range walkCh {
		require.NoError(t, walkResp.Err)
		replayPaths = append(replayPaths, walkResp.Path)
	}
	assert.Equal(t, []string{"remote/file"}, replayPaths)

	reschedule, delay, err := getResults()
	require.NoError(t, err)
	assert.False(t, reschedule)
	assert.Equal(t, time.Duration(0), delay)

	replayedRequest := client.GetJobRequest(cfg)
	include, operation = client.IncludeInBulkRequest(context.Background(), replayedRequest)
	assert.False(t, include)
	assert.Empty(t, operation)
}

func TestMockClientGetRemotePathInfoUsesLockedInfo(t *testing.T) {
	client := &MockClient{}
	expectedMtime := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	cfg := &flex.JobRequestCfg{
		LockedInfo: &flex.JobLockedInfo{
			RemoteSize:  8192,
			RemoteMtime: timestamppb.New(expectedMtime),
			IsArchived:  true,
		},
	}

	remoteSize, remoteMtime, isArchived, allowRestore, err := client.GetRemotePathInfo(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, int64(8192), remoteSize)
	assert.Equal(t, expectedMtime, remoteMtime)
	assert.True(t, isArchived)
	assert.True(t, allowRestore)
}
