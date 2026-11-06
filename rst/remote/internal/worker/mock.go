package worker

import (
	"context"
	"errors"
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

func (n *MockNode) connect(ctx context.Context, config *flex.UpdateConfigRequest, bulkUpdate *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) error {
	args := n.Called()
	return args.Error(0)
}

func (n *MockNode) heartbeat(ctx context.Context, request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	// We could mock heartbeat responses if it ever becomes necessary. However for now they are
	// disabled because it causes confusing errors when tests run longer than 10 seconds because it
	// will panic unless the test mocked a heartbeat (which is not the focus of most tests).
	//  args := n.Called()
	// return args.Get(0).(*flex.HeartbeatResponse), args.Error(1)
	return flex.HeartbeatResponse_builder{
		State: flex.HeartbeatResponse_READY,
	}.Build(), nil
}

func (n *MockNode) disconnect() error {
	args := n.Called()
	return args.Error(0)
}

func (n *MockNode) SubmitWork(request *flex.WorkRequest) (*flex.Work, error) {
	// Matches BeeSyncNode: a draining node may still be offered work as a last resort, so only an
	// unreachable node is refused outright here.
	if _, ok := n.beginRPC(); !ok {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	defer n.rpcWG.Done()
	args := n.Called(request)

	// A mock node simulates declining while draining by returning ErrNodeDraining, which is handled
	// the same way BeeSyncNode handles a node that answers with SubmitWorkResponse_DRAINING.
	if errors.Is(args.Error(1), ErrNodeDraining) {
		n.compareAndSetState(ONLINE, DRAINING)
		return nil, ErrNodeDraining
	}

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
	// Matches BeeSyncNode: work already assigned to a draining node can still be updated.
	state, ok := n.beginRPC()
	if !ok {
		return nil, fmt.Errorf("unable to update work request on a node that is %s", state)
	}
	defer n.rpcWG.Done()
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

// VerifyBackend behaves like BeeSyncNode.VerifyBackend. A node with no expectation for
// VerifyBackend passes every verify, as most tests never verify. A test that registers one decides
// the note and the error.
func (n *MockNode) VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget) (string, error) {
	if _, ok := n.beginRPC(); !ok {
		return "", ErrNodeNotConnected
	}
	defer n.rpcWG.Done()
	for _, call := range n.ExpectedCalls {
		if call.Method == "VerifyBackend" {
			args := n.Called(backend)
			return args.String(0), args.Error(1)
		}
	}
	return "", nil
}

// UpdateConfig behaves like BeeSyncNode.UpdateConfig. Most tests never push a configuration, so a
// node with no expectation for UpdateConfig accepts every push. A test that registers one decides
// the result, and an error places the node offline as a real node's would.
func (n *MockNode) UpdateConfig(request *flex.UpdateConfigRequest) error {
	if _, ok := n.beginRPC(); !ok {
		return ErrNodeNotConnected
	}
	defer n.rpcWG.Done()
	expected := false
	for _, call := range n.ExpectedCalls {
		if call.Method == "UpdateConfig" {
			expected = true
			break
		}
	}
	if !expected {
		return nil
	}
	err := n.Called(request).Error(0)
	if err != nil {
		select {
		case n.rpcErr <- err:
		default:
		}
	}
	return err
}
