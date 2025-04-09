package worker

import (
	"fmt"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/flex"
)

type MockNode struct {
	*baseNode
	mock.Mock
}

var _ Worker = &MockNode{}
var _ grpcClientHandler = &MockNode{}

func newMockNode(baseNode *baseNode) Worker {
	mockNode := &MockNode{baseNode: baseNode}
	mockNode.baseNode.grpcClientHandler = mockNode
	for _, e := range baseNode.config.MockConfig.Expectations {
		mockNode.Mock.On(e.MethodName, e.Args...).Return(e.ReturnArgs...)
	}
	return mockNode
}

func (n *MockNode) connect(config *flex.UpdateConfigRequest, bulkUpdate *flex.BulkUpdateWorkRequest) (bool, error) {
	args := n.Called()
	return args.Bool(0), args.Error(1)
}

func (n *MockNode) heartbeat(request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	// We could mock heartbeat responses if it ever becomes necessary. However for now they are
	// disabled because it causes confusing errors when tests run longer than 10 seconds because it
	// will panic unless the test mocked a heartbeat (which is not the focus of most tests).
	//  args := n.Called()
	// return args.Get(0).(*flex.HeartbeatResponse), args.Error(1)
	return flex.HeartbeatResponse_builder{
		IsReady: true,
	}.Build(), nil
}

func (n *MockNode) disconnect() error {
	args := n.Called()
	return args.Error(0)
}

func (n *MockNode) SubmitWork(request *flex.WorkRequest) (*flex.Work, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	args := n.Called(request)

	if args.Error(1) != nil {
		select {
		case n.rpcErr <- args.Error(1):
		default:
		}
		return nil, args.Error(1)
	}

	// Echo the work response back to the caller with whatever status was set
	// using MockExpectations. It is very very important we copy the status here
	// and return a pointer to a new status (not reuse the status from the
	// expectation), otherwise all test requests will share the same status
	// which causes very confusing test failures.
	status := flex.Work_Status_builder{
		State:   args.Get(0).(*flex.Work_Status).GetState(),
		Message: args.Get(0).(*flex.Work_Status).GetMessage(),
	}.Build()

	return flex.Work_builder{
		Path:      request.GetPath(),
		JobId:     request.GetJobId(),
		RequestId: request.GetRequestId(),
		Status:    status,
	}.Build(), args.Error(1)
}

func (n *MockNode) UpdateWork(request *flex.UpdateWorkRequest) (*flex.Work, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	args := n.Called(request)

	if args.Error(1) != nil {
		select {
		case n.rpcErr <- args.Error(1):
		default:
		}
		return nil, args.Error(1)
	}

	// Echo the work response back to the caller with whatever status was set
	// using MockExpectations. It is very very important we copy the status here
	// and return a pointer to a new status (not reuse the status from the
	// expectation), otherwise all test requests will share the same status
	// which causes very confusing test failures.
	status := flex.Work_Status_builder{
		State:   args.Get(0).(*flex.Work_Status).GetState(),
		Message: args.Get(0).(*flex.Work_Status).GetMessage(),
	}.Build()
	return flex.Work_builder{
		// The update request does not contain "path" so we cannot set that field in the response.
		// Currently this doesn't break anything, but this may be the source of future test
		// failures.
		JobId:     request.GetJobId(),
		RequestId: request.GetRequestId(),
		Status:    status,
	}.Build(), args.Error(1)
}
