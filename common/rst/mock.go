package rst

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
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
	bulkMu             sync.Mutex
	completedBulkPaths map[string]struct{}
}

var _ Provider = &MockClient{}

func (m *MockClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	if m.hasExpectedCall("GetJobRequest") {
		args := m.Called(cfg)
		return args.Get(0).(*beeremote.JobRequest)
	}

	mockJob := &flex.MockJob{
		NumTestSegments: 1,
		Cfg:             proto.Clone(cfg).(*flex.JobRequestCfg),
	}
	if cfg.LockedInfo != nil {
		mockJob.LockedInfo = proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)
		if cfg.Download {
			mockJob.FileSize = cfg.LockedInfo.GetRemoteSize()
		} else {
			mockJob.FileSize = cfg.LockedInfo.GetSize()
		}
		mockJob.ExternalId = cfg.LockedInfo.GetExternalId()
	}

	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: cfg.RemoteStorageTarget,
		StubLocal:           cfg.StubLocal,
		Priority:            cfg.GetPriority(),
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Mock{
			Mock: mockJob,
		},
		Update: cfg.Update,
	}
}

func (m *MockClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {

	if job.Request.GetMock() != nil {
		if job.Request.GetMock().ShouldFail {
			return nil, fmt.Errorf("test requested an error")
		}

		numSegments := int64(job.Request.GetMock().NumTestSegments)
		if numSegments <= 0 {
			numSegments = 1
		}

		workRequests := RecreateWorkRequests(job, generateSegments(job.Request.GetMock().FileSize, numSegments, 1))
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

func (m *MockClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (bool, time.Duration, error) {
	if m.hasExpectedCall("ExecuteJobBuilderRequest") {
		args := m.Called(ctx, workRequest, jobSubmissionChan)
		delay, _ := args.Get(1).(time.Duration)
		return args.Bool(0), delay, args.Error(2)
	}

	return false, 0, ErrUnsupportedOpForRST
}

func (m *MockClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	if m.hasExpectedCall("IncludeInBulkRequest") {
		args := m.Called(ctx, request)
		return args.Bool(0), args.String(1)
	}

	lockedInfo := getMockRequestLockedInfo(request)
	if lockedInfo == nil || !lockedInfo.GetIsArchived() {
		return false, ""
	}

	operation = "retrieve"
	if m.isBulkPathCompleted(operation, getMockBulkReplayPath(request)) {
		return false, ""
	}

	return true, operation
}

func (m *MockClient) OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error) {
	if m.hasExpectedCall("OpenBulkOperation") {
		args := m.Called(ctx, stateMountPath, operation)
		if args.Error(1) != nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(clientBulkOperation), nil
	}

	return &mockBulkOperation{
		client:         m,
		stateMountPath: stateMountPath,
		operation:      operation,
	}, nil
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
	if m.hasExpectedCall("GetRemotePathInfo") {
		args := m.Called(ctx, cfg)
		return args.Get(0).(int64), args.Get(1).(time.Time), args.Bool(2), args.Bool(3), args.Error(4)
	}

	lockedInfo := cfg.GetLockedInfo()
	if lockedInfo == nil {
		return 0, time.Time{}, false, false, os.ErrNotExist
	}

	remoteMtime := time.Time{}
	if lockedInfo.GetRemoteMtime() != nil {
		remoteMtime = lockedInfo.GetRemoteMtime().AsTime()
	}

	return lockedInfo.GetRemoteSize(), remoteMtime, lockedInfo.GetIsArchived(), true, nil
}

func (m *MockClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	if m.hasExpectedCall("GenerateExternalId") {
		args := m.Called(ctx, cfg)
		return args.String(0), args.Error(1)
	}

	if cfg.GetLockedInfo() != nil && cfg.GetLockedInfo().GetExternalId() != "" {
		return cfg.GetLockedInfo().GetExternalId(), nil
	}
	if cfg.GetRemotePath() != "" {
		return cfg.GetRemotePath(), nil
	}
	return cfg.GetPath(), nil
}

func (m *MockClient) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (bool, time.Duration, error) {
	args := m.Called(request)
	return args.Bool(0), args.Get(1).(time.Duration), args.Error(2)
}

func (m *MockClient) hasExpectedCall(method string) bool {
	for _, call := range m.ExpectedCalls {
		if call.Method == method {
			return true
		}
	}
	return false
}

func (m *MockClient) isBulkPathCompleted(operation string, path string) bool {
	m.bulkMu.Lock()
	defer m.bulkMu.Unlock()
	_, ok := m.completedBulkPaths[m.getCompletedBulkPathKey(operation, path)]
	return ok
}

func (m *MockClient) markBulkPathCompleted(operation string, path string) {
	m.bulkMu.Lock()
	defer m.bulkMu.Unlock()
	if m.completedBulkPaths == nil {
		m.completedBulkPaths = map[string]struct{}{}
	}
	m.completedBulkPaths[m.getCompletedBulkPathKey(operation, path)] = struct{}{}
}

func (m *MockClient) getCompletedBulkPathKey(operation string, path string) string {
	return fmt.Sprintf("%s\x00%s", operation, path)
}

type mockBulkOperation struct {
	client         *MockClient
	stateMountPath string
	operation      string
	requests       []*beeremote.JobRequest
}

func (m *mockBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	m.requests = append(m.requests, proto.Clone(request).(*beeremote.JobRequest))
	return nil
}

func (m *mockBulkOperation) Execute(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkRequestWaitForResultFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult, len(m.requests))
	for _, request := range m.requests {
		path := getMockBulkReplayPath(request)
		m.client.markBulkPathCompleted(m.operation, path)
		walkCh <- &filesystem.StreamPathResult{Path: path}
	}
	close(walkCh)
	return walkCh, func() (bool, time.Duration, error) { return false, 0, nil }, nil
}

func (m *mockBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() error { return nil }, nil
}

func (m *mockBulkOperation) Resume(ctx context.Context) (<-chan *filesystem.StreamPathResult, BulkWaitFn, error) {
	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)
	return walkCh, func() error { return nil }, nil
}

func getMockRequestLockedInfo(request *beeremote.JobRequest) *flex.JobLockedInfo {
	switch request.WhichType() {
	case beeremote.JobRequest_Sync_case:
		return request.GetSync().GetLockedInfo()
	case beeremote.JobRequest_Mock_case:
		if request.GetMock().GetLockedInfo() != nil {
			return request.GetMock().GetLockedInfo()
		}
		if request.GetMock().GetCfg() != nil {
			return request.GetMock().GetCfg().GetLockedInfo()
		}
	}
	return nil
}

func getMockBulkReplayPath(request *beeremote.JobRequest) string {
	switch request.WhichType() {
	case beeremote.JobRequest_Sync_case:
		if request.GetSync().GetOperation() == flex.SyncJob_DOWNLOAD && request.GetSync().GetRemotePath() != "" {
			return request.GetSync().GetRemotePath()
		}
	case beeremote.JobRequest_Mock_case:
		cfg := request.GetMock().GetCfg()
		if cfg != nil {
			if cfg.GetDownload() && cfg.GetRemotePath() != "" {
				return cfg.GetRemotePath()
			}
			if cfg.GetPath() != "" {
				return cfg.GetPath()
			}
		}
	}
	return request.GetPath()
}
