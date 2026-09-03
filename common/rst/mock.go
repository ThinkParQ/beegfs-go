package rst

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// MockClient can be used to mock RST client behavior. This is mostly useful when testing other
// packages that aren't concerned about testing the behavior of a particular RST type. When used
// with the MockJob request type, behavior can be customized through the MockJob without needing to
// use `Mock.On`. This drastically simplifies most test cases which just need to generate some
// number of work requests so all they need to do is specify NumTestSegments in the MockJob request.
// The behavior of any other job type (i.e., SyncJob) can also be mocked by setting up the
// appropriate `Mock.On` to setup the methods to return what you want for the test.
//
// To test directly (for example the RST package tests):
//
//	rstClient := &rst.MockClient{}
//	mockClient.On("GenerateWorkRequests",mock.Anything, fileSize, availWorkers).Return(externalID, requests, false, nil)
//
// To test indirectly use the Mock RST type when initializing WorkerMgr:
//
//	rsts := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{}}}
//	wm, err := workermgr.NewManager(logger, workermgr.Config{}, []worker.Config{}, rsts)
//
// If you are using the client directly, use type assertion to get at the underlying mock client to setup expectations:
//
//	mockClient, _ := workerManager.RemoteStorageTargets["0"].(*rst.MockClient)
//	mockClient.On("GenerateWorkRequests",mock.Anything, fileSize, availWorkers).Return(externalID, requests, false, nil)
//
// Or if you are using the ClientStore, use the testing hook:
//
//	clientStore := NewClientStore()
//	mockRST := &rst.MockClient{}
//	clientStore.SetMockClientForTesting("0", mockRST)
//	mockRST.On("ExecuteWorkRequestPart", mock.Anything, mock.Anything, mock.Anything).Return(nil)
//
// IMPORTANT:
//   - You CANNOT use `Mock.On` with the `MockJob` request type.
type MockClient struct {
	mock.Mock
}

var _ Provider = &MockClient{}

func (r *MockClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return nil
}

func (rst *MockClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {

	if job.Request.GetMock() != nil {
		if job.Request.GetMock().ShouldFail {
			return nil, fmt.Errorf("test requested an error")
		}

		numSegments := int64(job.Request.GetMock().NumTestSegments)
		if numSegments <= 0 {
			numSegments = 1
		}

		// Most tests only care how many work requests come back and leave FileSize unset. Segments
		// must contain at least one byte each to stay distinguishable, so give the mock file just
		// enough bytes to hand out one per requested segment.
		fileSize := job.Request.GetMock().FileSize
		if fileSize < numSegments {
			fileSize = numSegments
		}

		workRequests := RecreateWorkRequests(job, generateSegments(fileSize, numSegments, 1))
		return workRequests, nil
	}

	args := rst.Called(job, availableWorkers)
	if args.Error(2) != nil {
		return nil, args.Error(2)
	}
	return args.Get(0).([]*flex.WorkRequest), nil
}

func (m *MockClient) ExecuteWorkRequestPart(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest, part *flex.Work_Part) *SchedulingResult {

	if request.GetMock() != nil {
		if request.GetMock().ShouldFail {
			return &SchedulingResult{Err: fmt.Errorf("test requested an error")}
		}
		part.Completed = true
		return nil
	}

	args := m.Called(workCtx, request, part)
	err := args.Error(0)
	if err != nil {
		return &SchedulingResult{Err: err}
	}
	part.Completed = true
	return nil
}

func (m *MockClient) ExecuteJobBuilderRequest(shutdownCtx context.Context, workCtx context.Context, log *zap.Logger, workRequest *flex.WorkRequest, submitRequest SubmitRequestFn, workerSaturation []func() float64) *SchedulingResult {
	if !m.hasExpectedCall("ExecuteJobBuilderRequest") {
		return &SchedulingResult{Err: ErrUnsupportedOpForRST}
	}

	args := m.Called(workCtx, workRequest, submitRequest)
	delay, _ := args.Get(1).(time.Duration)
	return &SchedulingResult{
		Reschedule: args.Bool(0),
		Delay:      delay,
		Err:        args.Error(2),
	}
}

// ExecuteJobBuilderRequest is not implemented and should never be called.
func (r *MockClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (bool, error) {
	return false, ErrUnsupportedOpForRST
}

func (m *MockClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	if !abort {
		switch GetWorkResultsState(workResults) {
		case flex.Work_CANCELLED, flex.Work_COMPLETED:
		default:
			return fmt.Errorf("unable to resolve failure")
		}
	}

	if job.Request.GetMock() != nil {
		if job.Request.GetMock().ShouldFail {
			return fmt.Errorf("test requested an error")
		}
		return nil
	}

	args := rst.Called(job, workResults, abort)
	return args.Error(0)
}

func (rst *MockClient) GetConfig() *flex.RemoteStorageTarget {
	args := rst.Called()
	return args.Get(0).(*flex.RemoteStorageTarget)
}

func (m *MockClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string) (walk <-chan *filesystem.StreamPathResult, stopWalk func(), err error) {
	return nil, func() {}, ErrUnsupportedOpForRST
}

func (r *MockClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

func (r *MockClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, bool, bool, error) {
	return 0, time.Time{}, false, false, ErrUnsupportedOpForRST
}

func (r *MockClient) ReleaseExternalId(ctx context.Context, cfg *flex.JobRequestCfg, externalId string) error {
	return nil
}

func (r *MockClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	return "", ErrUnsupportedOpForRST
}

func (m *MockClient) IsWorkRequestReady(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	args := m.Called(request)
	return args.Bool(0), args.Get(1).(time.Duration), args.Error(2)
}
