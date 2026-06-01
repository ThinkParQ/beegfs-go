package rst

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type testBulkOperation struct {
	executeErr    error
	executeResult *SchedulingResult
	resumeErr     error
	cancelErr     error
	waitErr       error
}

func (t *testBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return nil
}

func (t *testBulkOperation) Execute(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkRequestWaitForResultFn, error) {
	if t.executeErr != nil {
		return nil, nil, t.executeErr
	}

	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() *SchedulingResult {
		if t.executeResult != nil {
			return t.executeResult
		}
		return &SchedulingResult{}
	}, nil
}

func (t *testBulkOperation) Resume(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	if t.resumeErr != nil {
		return nil, nil, t.resumeErr
	}

	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() error { return t.waitErr }, nil
}

func (t *testBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	if t.cancelErr != nil {
		return nil, nil, t.cancelErr
	}

	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() error { return t.waitErr }, nil
}

func TestJobBuilderBulkOperations_ManagerAbortReturnsNilAfterSuccessfulCancel(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 1),
	}

	cancelErrs := new(string)
	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{},
				Operation:           "retrieve",
				errors:              cancelErrs,
			},
		},
	}

	err := manager.Abort(context.Background(), controller, fmt.Errorf("abort requested"))
	assert.NoError(t, err)
	assert.Empty(t, *cancelErrs)
}

func TestJobBuilderBulkOperations_ManagerResumeReturnsNilAfterSuccessfulResume(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 1),
	}

	resumeErrs := new(string)
	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{},
				Operation:           "retrieve",
				errors:              resumeErrs,
			},
		},
	}

	wait, err := manager.Resume(context.Background(), controller)
	assert.NoError(t, err)
	require.NoError(t, wait())
	assert.Empty(t, *resumeErrs)
}

func TestJobBuilderBulkOperations_ManagerExecuteReturnsMergedSchedulingResult(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 2),
	}

	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{
					executeResult: &SchedulingResult{Reschedule: true, Delay: 5},
				},
				Operation: "retrieve",
				errors:    new(string),
			},
			"1-archive": {
				clientBulkOperation: &testBulkOperation{
					executeResult: &SchedulingResult{Reschedule: true, Delay: 3},
				},
				Operation: "archive",
				errors:    new(string),
			},
		},
	}

	result := manager.Execute(context.Background(), controller)
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.True(t, result.Reschedule)
	assert.Equal(t, 3, int(result.Delay))
}

func TestJobBuilderBulkOperations_ManagerExecuteReturnsErrorsWhenExecuteFails(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 2),
	}

	openErrs := new(string)
	waitErrs := new(string)
	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{executeErr: fmt.Errorf("failed to open execute state")},
				Operation:           "retrieve",
				errors:              openErrs,
			},
			"1-archive": {
				clientBulkOperation: &testBulkOperation{
					executeResult: &SchedulingResult{Err: fmt.Errorf("failed waiting for execute completion")},
				},
				Operation: "archive",
				errors:    waitErrs,
			},
		},
	}

	result := manager.Execute(context.Background(), controller)
	require.NotNil(t, result)
	if assert.Error(t, result.Err) {
		assert.True(t, strings.Contains(result.Err.Error(), "bulk operation retrieve: (failed to open execute state)"))
		assert.True(t, strings.Contains(result.Err.Error(), "bulk operation archive: (failed waiting for execute completion)"))
	}
	assert.Equal(t, "failed to open execute state", *openErrs)
	assert.Equal(t, "failed waiting for execute completion", *waitErrs)
}

func TestJobBuilderBulkOperations_ManagerResumeReturnsErrorsWhenResumeFails(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 1),
	}

	openErrs := new(string)
	waitErrs := new(string)
	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{resumeErr: fmt.Errorf("failed to open resume state")},
				Operation:           "retrieve",
				errors:              openErrs,
			},
			"1-archive": {
				clientBulkOperation: &testBulkOperation{waitErr: fmt.Errorf("failed waiting for resume completion")},
				Operation:           "archive",
				errors:              waitErrs,
			},
		},
	}

	wait, err := manager.Resume(context.Background(), controller)
	if assert.Error(t, err) {
		assert.True(t, strings.Contains(err.Error(), "bulk operation retrieve: (failed to open resume state)"))
	}
	waitErr := wait()
	if assert.Error(t, waitErr) {
		assert.True(t, strings.Contains(waitErr.Error(), "bulk operation archive: (failed waiting for resume completion)"))
	}
	assert.Equal(t, "failed to open resume state", *openErrs)
	assert.Equal(t, "failed waiting for resume completion", *waitErrs)
}

func TestJobBuilderBulkOperations_ManagerAbortReturnsErrorsWhenCancelFails(t *testing.T) {
	controller := &requestBuildController{
		walkMultiplexer: filesystem.NewWalkMultiplexer(context.Background(), 1),
	}

	openErrs := new(string)
	waitErrs := new(string)
	manager := &jobBuilderBulkOperationsManager{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &testBulkOperation{cancelErr: fmt.Errorf("failed to open cancel state")},
				Operation:           "retrieve",
				errors:              openErrs,
			},
			"1-archive": {
				clientBulkOperation: &testBulkOperation{waitErr: fmt.Errorf("failed waiting for cancel completion")},
				Operation:           "archive",
				errors:              waitErrs,
			},
		},
	}

	err := manager.Abort(context.Background(), controller, fmt.Errorf("abort requested"))
	if assert.Error(t, err) {
		assert.True(t, strings.Contains(err.Error(), "bulk operation retrieve: (failed to open cancel state)"))
		assert.True(t, strings.Contains(err.Error(), "bulk operation archive: (failed waiting for cancel completion)"))
	}
	assert.Equal(t, "failed to open cancel state", *openErrs)
	assert.Equal(t, "failed waiting for cancel completion", *waitErrs)
}
