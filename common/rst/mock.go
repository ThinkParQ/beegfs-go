package rst

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/flex"
)

// MockClient can be used to mock the behavior of any real client type.
//
// To test directly (for example the RST package tests):
// rstClient := &rst.MockClient{}
// mockClient.On("RecommendedSegments",fileSize).Return(rst.S3, segmentCount, partsPerSegment)
//
// To test indirectly use the Mock RST type when initializing WorkerMgr:
// rsts := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{}}}
// wm, err := workermgr.NewManager(logger, workermgr.Config{}, []worker.Config{}, rsts)
//
// Then use type assertion to get at the underlying mock client to setup expectations:
// mockClient, _ := workerManager.RemoteStorageTargets["0"].(*rst.MockClient)
// mockClient.On("RecommendedSegments", int64(1<<20)).Return(rst.S3, 4, 2)
type MockClient struct {
	mock.Mock
}

var _ Client = &MockClient{}

func (rst *MockClient) GetType() Type {
	args := rst.Called()
	// Any string type could also be used here even if it isn't one of the typed
	// constants. This is useful if you want to mock an RST type that doesn't
	// exist for some tests.
	clientType, ok := args.Get(0).(Type)
	if !ok {
		panic("client type used for test is invalid")
	}
	return clientType
}

func (rst *MockClient) RecommendedSegments(fileSize int64) (int64, int32) {
	args := rst.Called(fileSize)

	return int64(args.Int(0)), int32(args.Int(1))
}

func (rst *MockClient) CreateUpload(path string) (uploadID string, err error) {
	args := rst.Called()
	return args.String(0), args.Error(1)
}

func (rst *MockClient) AbortUpload(uploadID string, path string) error {
	args := rst.Called()
	return args.Error(0)
}

func (rst *MockClient) FinishUpload(uploadID string, path string, parts []*flex.WorkResponse_Part) error {
	args := rst.Called()
	return args.Error(0)
}

func (rst *MockClient) UploadPart(uploadID string, part int32, path string, offsetStart int64, offsetStop int64) (string, error) {
	args := rst.Called()
	return args.String(0), args.Error(1)
}

func (rst *MockClient) DownloadPart(path string, offsetStart int64, offsetStop int64) error {
	return rst.Called().Error(0)
}

func (rst *MockClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.WorkResponse_Part) error {
	args := rst.Called(ctx, request, part)
	err, ok := args.Get(0).(error)

	if err != nil && !ok {
		panic("error type used for test is invalid")
	}

	if err == nil {
		part.Completed = true
	}

	return err
}
