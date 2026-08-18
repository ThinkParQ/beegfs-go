package rst

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// fakeBulkOperation is a minimal clientBulkOperation stand-in for exercising bulkOperationRegistry
// and bulkOperationManager in isolation, without going through a real provider.
type fakeBulkOperation struct {
	addRequestErr error
	closeErr      error
}

func (f *fakeBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return f.addRequestErr
}

func (f *fakeBulkOperation) Execute(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
	return nil, nil, fmt.Errorf("fakeBulkOperation.Execute not implemented")
}

func (f *fakeBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
	return nil, nil, fmt.Errorf("fakeBulkOperation.Cancel not implemented")
}

func (f *fakeBulkOperation) Close(ctx context.Context) error {
	return f.closeErr
}

func (m *fakeBulkOperation) Destroy(ctx context.Context) error {
	return nil
}

// newTestBulkOperationRegistry builds a bulkOperationRegistry backed by a single rstId (1) mapped to
// client, mirroring what JobBuilderClient.newBulkOperationRegistry produces but without requiring a
// pre-populated builderBulkOperations slice.
func newTestBulkOperationRegistry(client Provider) *bulkOperationRegistry {
	bulkOperations := []*flex.BulkOperation{}
	return &bulkOperationRegistry{
		managers:              make(map[string]*bulkOperationManager),
		rstMap:                map[uint32]Provider{1: client},
		builderBulkOperations: &bulkOperations,
		builderJobId:          "job-1",
	}
}

func TestBulkOperationRegistry_AddRequestSkipsRequestsWithGenerationStatus(t *testing.T) {
	registry := newTestBulkOperationRegistry(&MockClient{})
	request := &beeremote.JobRequest{}
	request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{})

	skipSubmit, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)
	assert.False(t, skipSubmit)
	assert.Empty(t, registry.managers)
}

func TestBulkOperationRegistry_AddRequestNotIncludedDoesNotCreateManager(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(false, "")
	registry := newTestBulkOperationRegistry(client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)

	skipSubmit, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)
	assert.False(t, skipSubmit)
	assert.Empty(t, registry.managers)
}

// TestBulkOperationRegistry_AddRequestCreatesManagerOnDemandAndReusesIt asserts that the first
// request for a given rstId+operation lazily creates its manager (recording it on
// builderBulkOperations), and that subsequent requests for the same key reuse it rather than
// creating a second manager.
func TestBulkOperationRegistry_AddRequestCreatesManagerOnDemandAndReusesIt(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	registry := newTestBulkOperationRegistry(client)

	for i := 0; i < 2; i++ {
		request := &beeremote.JobRequest{}
		request.SetRemoteStorageTarget(1)

		skipSubmit, err := registry.AddRequest(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, skipSubmit)
	}

	assert.Len(t, registry.managers, 1)
	assert.Len(t, *registry.builderBulkOperations, 1)

	manager := registry.managers["1-retrieve"]
	require.NotNil(t, manager)
	assert.Equal(t, "retrieve", manager.operation)
}

func TestBulkOperationRegistry_AddRequestPropagatesManagerAddRequestError(t *testing.T) {
	addErr := fmt.Errorf("disk full")
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{addRequestErr: addErr}, nil)
	registry := newTestBulkOperationRegistry(client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)

	_, err := registry.AddRequest(context.Background(), request)
	require.ErrorIs(t, err, addErr)
}

func TestBulkOperationRegistry_CloseAggregatesManagerCloseErrors(t *testing.T) {
	closeErr := fmt.Errorf("failed to unmount")
	registry := &bulkOperationRegistry{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &fakeBulkOperation{closeErr: closeErr},
				operation:           "retrieve",
				errors:              new(string),
				failed:              new(bool),
			},
		},
	}

	err := registry.Close(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, closeErr)
	assert.Contains(t, err.Error(), "1-retrieve")
}

// TestNewBulkOperationManager_OpensHandleForAlreadyFailedOperation asserts that reopening a
// permanently failed operation still acquires a provider handle. Cancel, Close and Destroy all need
// it to release what the operation reserved (for xtreemstore, the active retrieve-session) and to
// delete its local state; without a handle a failed operation can never be torn down and leaks both.
func TestNewBulkOperationManager_OpensHandleForAlreadyFailedOperation(t *testing.T) {
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	bulkOperation := &flex.BulkOperation{RstId: 1, Operation: "retrieve", Failed: true}
	manager := newBulkOperationManager(context.Background(), client, "job-1", bulkOperation)

	assert.True(t, manager.IsFailed(), "reopening must not clear the failure")
	require.NotNil(t, manager.clientBulkOperation, "a failed operation still needs a handle to be torn down")
	assert.NoError(t, manager.Destroy(context.Background()))

	// New work must still be refused even though the handle now exists.
	err := manager.AddRequest(context.Background(), &beeremote.JobRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "previously failed permanently")
}

// TestNewBulkOperationManager_InterruptedOpenDoesNotFailOperation asserts that an open interrupted by
// shutdown leaves the operation resumable instead of permanently failing it.
func TestNewBulkOperationManager_InterruptedOpenDoesNotFailOperation(t *testing.T) {
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("open state: %w", context.Canceled))

	bulkOperation := &flex.BulkOperation{RstId: 1, Operation: "retrieve"}
	manager := newBulkOperationManager(context.Background(), client, "job-1", bulkOperation)

	assert.False(t, manager.IsFailed(), "an interrupted open must leave the operation resumable")
	assert.False(t, bulkOperation.Failed, "the persisted flag must stay clear")
	assert.NoError(t, manager.GetErrors())
}

// TestNewBulkOperationManager_FailedOpenFailsOperation is the counterpart: a genuine open failure is
// still permanent.
func TestNewBulkOperationManager_FailedOpenFailsOperation(t *testing.T) {
	openErr := fmt.Errorf("state file is corrupt")
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(nil, openErr)

	bulkOperation := &flex.BulkOperation{RstId: 1, Operation: "retrieve"}
	manager := newBulkOperationManager(context.Background(), client, "job-1", bulkOperation)

	assert.True(t, manager.IsFailed())
	assert.True(t, bulkOperation.Failed, "the failure must be persisted with the work request")
	require.Error(t, manager.GetErrors())
	assert.Contains(t, manager.GetErrors().Error(), openErr.Error())
}

func TestBulkOperationManager_AppendErrorAccumulatesAndGetErrorsFormats(t *testing.T) {
	manager := &bulkOperationManager{operation: "archive", errors: new(string), failed: new(bool)}
	assert.NoError(t, manager.GetErrors())

	manager.AppendError(fmt.Errorf("first"))
	manager.AppendError(fmt.Errorf("second"))
	manager.AppendError(nil)

	err := manager.GetErrors()
	require.Error(t, err)
	assert.Equal(t, "bulk operation archive: (first. second)", err.Error())
}
