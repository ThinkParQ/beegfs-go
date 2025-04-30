package beeremote

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// To test indirectly when initializing WorkMgr in the config specify "mock:0" as the
// "BeeRemoteAddress" then before making any requests use type assertion to get the underlying mock
// client to setup expectations:
//
// mockBeeRemote, _ := mgr.beeRemoteClient.(*beeremote.MockProvider)
// mockBeeRemote.On("UpdateWorkRequest", mock.Anything).Return(&flex.Response{Success: true,
// Message: "test requests a success response"}, nil)
type MockProvider struct {
	mock.Mock
}

var _ Provider = &MockProvider{}

// Currently there is no reason we want to mock initializing a mock provider.
func (c *MockProvider) init(config Config) error {
	return nil
}

// Currently there is no reason we want to mock disconnecting a mock provider.
func (c *MockProvider) disconnect() error {
	return nil
}

func (c *MockProvider) updateWork(ctx context.Context, workResult *flex.Work) error {
	args := c.Called(workResult)
	err, ok := args.Get(0).(error)
	if err != nil && !ok {
		panic("error type used for test is invalid")
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *MockProvider) submitJob(ctx context.Context, jobRequest *beeremote.JobRequest) error {
	args := c.Called(jobRequest)
	err, ok := args.Get(0).(error)
	if err != nil && !ok {
		panic("error type used for test is invalid")
	}
	if err != nil {
		return err
	}
	return nil
}
