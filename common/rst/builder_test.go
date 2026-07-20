package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type trackingBulkOperation struct {
	cancelCalled bool
	cancelReason error
	waitCalled   bool
}

func (t *trackingBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return nil
}

func (t *trackingBulkOperation) Execute(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkExecuteResultFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() *SchedulingResult { return &SchedulingResult{} }, nil
}

func (t *trackingBulkOperation) Resume(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() error { return nil }, nil
}

func (t *trackingBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult)
	return walkCh, func() error {
		t.cancelCalled = true
		t.cancelReason = reason
		t.waitCalled = true
		close(walkCh)
		return nil
	}, nil
}

func TestGetBulkOperationsReturnsAllStartedBulkOperations(t *testing.T) {
	expected := []*flex.BulkOperation{
		flex.BulkOperation_builder{
			StateMountPath: ".beegfs-rst/job/job-1/1",
			RstId:          1,
			Operation:      "retrieve",
		}.Build(),
		flex.BulkOperation_builder{
			StateMountPath: ".beegfs-rst/job/job-1/2",
			RstId:          2,
			Operation:      "archive",
			Errors:         func() *string { s := "resume failed"; return &s }(),
		}.Build(),
	}

	workResults := []*flex.Work{
		flex.Work_builder{
			JobBuilderInfo: flex.Work_JobBuilderInfo_builder{
				BulkOperations: expected,
			}.Build(),
		}.Build(),
		flex.Work_builder{}.Build(),
	}

	assert.Equal(t, expected, getBulkOperations(workResults))
}

func TestCompleteWorkRequestsAbortCancelsAllStartedBulkOperations(t *testing.T) {
	mockRST := &MockClient{}
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: mockRST}, filesystem.NewMockFS())

	job := beeremote.Job_builder{
		Id: "builder-job",
		Request: beeremote.JobRequest_builder{
			Path:                "/test/builder",
			RemoteStorageTarget: JobBuilderRstId,
			Builder:             flex.BuilderJob_builder{}.Build(),
		}.Build(),
	}.Build()

	tracker := &trackingBulkOperation{}
	mockRST.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/builder-job/1", "retrieve").Return(tracker, nil).Once()

	workResults := []*flex.Work{
		flex.Work_builder{
			JobBuilderInfo: flex.Work_JobBuilderInfo_builder{
				BulkOperations: []*flex.BulkOperation{
					flex.BulkOperation_builder{
						StateMountPath: ".beegfs-rst/job/builder-job/1",
						RstId:          1,
						Operation:      "retrieve",
					}.Build(),
				},
			}.Build(),
		}.Build(),
	}

	err := client.CompleteWorkRequests(context.Background(), job, workResults, true)
	require.NoError(t, err)
	require.True(t, tracker.cancelCalled)
	require.True(t, tracker.waitCalled)
	require.Nil(t, tracker.cancelReason)
	mockRST.AssertExpectations(t)
}
