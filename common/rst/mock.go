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

func (m *MockClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return nil
}

func (m *MockClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {

	if job.Request.GetMock() != nil {
		if job.Request.GetMock().ShouldFail {
			return nil, fmt.Errorf("test requested an error")
		}

		workRequests := RecreateWorkRequests(job, generateSegments(job.Request.GetMock().FileSize, int64(job.Request.GetMock().NumTestSegments), 1))
		return workRequests, nil
	}

	args := m.Called(job, availableWorkers)
	if args.Error(2) != nil {
		return nil, args.Error(2)
	}
	return args.Get(0).([]*flex.WorkRequest), nil
}

func (m *MockClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {

	if request.GetMock() != nil {
		if request.GetMock().ShouldFail {
			return fmt.Errorf("test requested an error")
		}
		part.Completed = true
		return nil
	}

	args := m.Called(ctx, request, part)
	err := args.Error(0)
	if err == nil {
		part.Completed = true
	}
	return err
}

// ExecuteJobBuilderRequest is not implemented and should never be called.
func (m *MockClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (bool, time.Duration, error) {
	return false, 0, ErrUnsupportedOpForRST
}

func (m *MockClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (m *MockClient) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest) (reschedule bool, delay time.Duration, err error) {
	return false, 0, ErrUnsupportedOpForRST
}

func (m *MockClient) CompleteBulkRequest(ctx context.Context, stateMountPath string, operation string) error {
	return nil
}

func (m *MockClient) CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error) error {
	return nil
}

func (m *MockClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {

	if job.Request.GetMock() != nil {
		if job.Request.GetMock().ShouldFail {
			return fmt.Errorf("test requested an error")
		}
		return nil
	}

	args := m.Called(job, workResults, abort)
	return args.Error(0)
}

func (m *MockClient) GetConfig() *flex.RemoteStorageTarget {
	args := m.Called()
	return args.Get(0).(*flex.RemoteStorageTarget)
}

func (m *MockClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string, maxRequests int) (<-chan *filesystem.StreamPathResult, error) {
	return nil, ErrUnsupportedOpForRST
}

func (m *MockClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

func (m *MockClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, bool, bool, error) {
	return 0, time.Time{}, false, false, ErrUnsupportedOpForRST
}

func (m *MockClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	return "", ErrUnsupportedOpForRST
}

func (m *MockClient) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	args := m.Called(request)
	return args.Bool(0), args.Get(1).(time.Duration), args.Error(2)
}
