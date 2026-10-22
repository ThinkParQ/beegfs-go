package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type alreadyExistsWorkerNodeServer struct {
	flex.UnimplementedWorkerNodeServer
}

func (s *alreadyExistsWorkerNodeServer) SubmitWork(ctx context.Context, request *flex.SubmitWorkRequest) (*flex.SubmitWorkResponse, error) {
	return nil, status.Errorf(codes.AlreadyExists, "already handling work request ID %s", request.GetRequest().GetRequestId())
}

// TestBeeSyncNodeSubmitWorkAlreadyExists checks that a node that already tracks a work request is
// treated as having it scheduled. This happens when a job is submitted again under the same job ID
// after remote lost the commit that recorded it. It must not crash remote.
func TestBeeSyncNodeSubmitWorkAlreadyExists(t *testing.T) {
	addr, cleanup := startWorkerNodeServer(t, &alreadyExistsWorkerNodeServer{})
	defer cleanup()

	node := newTestBeeSyncNode(t, addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	node.client = flex.NewWorkerNodeClient(conn)
	node.setState(ONLINE)

	request := flex.WorkRequest_builder{JobId: "job-1", RequestId: "0", Path: "/f"}.Build()
	var work *flex.Work
	require.NotPanics(t, func() { work, err = node.SubmitWork(request) })

	require.NoError(t, err)
	assert.Equal(t, flex.Work_SCHEDULED, work.GetStatus().GetState())
	assert.Equal(t, "job-1", work.GetJobId())
	assert.Equal(t, "0", work.GetRequestId())
	assert.Equal(t, "/f", work.GetPath())
}
