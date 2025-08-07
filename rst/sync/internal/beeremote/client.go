package beeremote

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	// Dynamic configuration is typically set by BeeRemote through the gRPC server.
	dynamic                *flex.BeeRemoteNode
	TlsCertFile            string `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool   `mapstructure:"tls-disable-verification"`
	TlsDisable             bool   `mapstructure:"tls-disable"`
	UseProxy               bool   `mapstructure:"use-http-proxy"`
}

// Client is setup so the provider can be swapped out after the client is initialized.
type Client struct {
	Provider
	config  Config
	readyMu *sync.RWMutex
}

// While we only ever expect to communicate with BeeRemote over gRPC, the use of an interface means
// we can easily mock BeeRemote for testing.
type Provider interface {
	init(config Config) error
	disconnect() error
	updateWork(ctx context.Context, workResult *flex.Work) error
	submitJob(ctx context.Context, jobRequest *beeremote.JobRequest) error
}

// New returns an initialized BeeRemote client. The dynamic portion of the initialCfg can be set to
// nil if the caller needs the base client initialization to be separated from the configuration and
// initialization of the provider. This is the typical approach to used to configure the client so
// WorkMgr can be started then later receive this configuration from BeeRemote.
//
// IMPORTANT: Ensure to call Disconnect() to cleanup gRPC connections when shutting down.
func New(initialCfg Config) (*Client, error) {

	c := &Client{
		readyMu: &sync.RWMutex{},
	}

	c.config = initialCfg
	err := c.UpdateConfig(initialCfg.dynamic)
	if errors.Is(err, ErrNilConfiguration) {
		return c, nil
	} else if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) CompareConfig(newConfig *flex.BeeRemoteNode) bool {
	return proto.Equal(c.config.dynamic, newConfig)
}

// UpdateConfig allows the client provider to be swapped out dynamically. It works by first
// obtaining a write lock on the client, which will block until all existing requests complete
// before blocking new requests until the client is updated. Note this implementation does not
// provide a way to force outstanding UpdateWorkRequest() calls to be terminated. However this
// should not be a problem because worker goroutines will get an error if there is some
// misconfiguration that would prevent ever sending a response, then wait some time before trying to
// resend the result, which gives enough time for the configuration to be updated.
func (c *Client) UpdateConfig(newCfg *flex.BeeRemoteNode) error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	if newCfg == nil {
		return ErrNilConfiguration
	}

	// Cleanup the old provider before swapping it out.
	if c.Provider != nil {
		c.disconnect()
	}

	c.config.dynamic = newCfg
	if c.config.dynamic.GetAddress() == "mock:0" {
		// Note this handles setting up the MockClient for the purposes of WorkMgr. However if the
		// client needs to actually be used then expectations must be setup (see mock.go).
		c.Provider = &MockProvider{}
		return nil
	} else if c.config.dynamic.GetAddress() != "" {
		newProvider := &grpcProvider{}
		if err := newProvider.init(c.config); err != nil {
			return fmt.Errorf("unable to initialize Remote client: %w", err)
		}
		// Note when adding new Providers, this should only be set to a fully initialized provider.
		// Even if this function returns an error, the connection handling logic will still call
		// disconnect if the provider is not nil. Depending on the Provider this may cause a panic.
		c.Provider = newProvider
		return nil
	}
	return ErrInvalidAddress
}

// UpdateWorkRequest will forward the provided work result to BeeRemote. If the provider has not
// been configured yet it will return an error. When an error occurs the caller should check
// canRetry to see if it should continue to try to send the work response.
func (c *Client) UpdateWorkRequest(ctx context.Context, workResult *flex.Work) (canRetry bool, err error) {
	c.readyMu.RLock()
	defer c.readyMu.RUnlock()
	if c.Provider == nil {
		return true, fmt.Errorf("BeeRemote client is not ready")
	}

	err = c.updateWork(ctx, workResult)
	if err != nil {
		if st, ok := status.FromError(err); !ok {
			// All real gRPC errors should have a status. If one does not this is a bug. Generally the
			// only reason we should stop trying is for a NotFound code, so default to retrying the
			// request indefinitely. Then at least we aren't silently discarding the response and
			// eventually someone should notice this error and can add a status and include it below
			// if we should not retry when we receive those errors. Note errors generated from mock
			// gRPC providers may not have real gRPC statuses, so seeing this error in tests is ok.
			return true, fmt.Errorf("received an unknown error (no gRPC status), most likely this is a bug: %w", err)
		} else {
			switch st.Code() {
			case codes.NotFound:
				// There is no point continuing to retry of the job/work request we tried to update
				// no longer exists. This generally shouldn't ever happen unless someone force
				// deleted the job while BeeRemote could not talk to BeeSync to cancel it first. In
				// any case there is no point continuing to try sending a response.
				return false, err
			default:
				return true, err
			}
		}
	}
	return false, nil
}

// SubmitJobRequest will forward the provided job request to BeeRemote.
func (c *Client) SubmitJobRequest(ctx context.Context, jobRequest *beeremote.JobRequest) error {
	c.readyMu.RLock()
	defer c.readyMu.RUnlock()
	if c.Provider == nil {
		return fmt.Errorf("BeeRemote client is not ready")
	}

	if err := c.submitJob(ctx, jobRequest); err != nil {
		if _, ok := status.FromError(err); !ok {
			return fmt.Errorf("received an unknown error (no gRPC status), most likely this is a bug: %w", err)
		}
		return err
	}
	return nil
}

func (c *Client) Disconnect() error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	if c.Provider == nil {
		return nil // nothing to do
	}
	return c.Provider.disconnect()
}
