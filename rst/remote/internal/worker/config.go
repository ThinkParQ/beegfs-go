package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/thinkparq/beegfs-go/common/types"
	"go.uber.org/zap"
)

// Supported worker nodes are organized into pools based on their Type. In
// addition to implementing the Worker and grpcClientHandler interfaces, expand
// the list of constants below when adding new worker node types. This will
// allow a pool for the new worker type to automatically be created whenever new
// workers of that type are configured.
type Type string

const (
	Unknown Type = "unknown"
	BeeSync Type = "beesync"
	Mock    Type = "mock"
)

// Configuration for a single worker node.
type Config struct {
	ID                     string `mapstructure:"id"`
	Name                   string `mapstructure:"name"`
	Type                   Type   `mapstructure:"type"`
	Address                string `mapstructure:"address"`
	TlsCertFile            string `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool   `mapstructure:"tls-disable-verification"`
	TlsDisable             bool   `mapstructure:"tls-disable"`
	UseProxy               bool   `mapstructure:"use-http-proxy"`
	MaxReconnectBackOff    int    `mapstructure:"max-reconnect-back-off"`
	DisconnectTimeout      int    `mapstructure:"disconnect-timeout"`
	SendRetries            int    `mapstructure:"send-retries"`
	RetryInterval          int    `mapstructure:"retry-interval"`
	HeartbeatInterval      int    `mapstructure:"heartbeat-interval"`
	// All embedded subscriber types must specify `mapstructure:",squash"` to tell
	// Viper to squash the fields of the embedded struct into the worker Config.
	// Without this viper.Unmarshal(&newConfig) will omit their configuration.
	// Across all embedded structs, the field tags must be unique for proper unmarshalling.
	// The standard is to prefix field tags with the subscriber type.
	BeeSyncConfig `mapstructure:",squash"` // Configuration options for type BeeSync.
	MockConfig                             // Allow mocking worker node behavior from other packages. We could add a map structure tag here if we wanted to be able to setup expectations using a TOML file.
}

// Configuration specific to BeeSync nodes.
type BeeSyncConfig struct {
}

// Mock worker nodes represent simulated worker nodes. They are configured just
// like other worker node types, except their configuration defines how they
// should behave for a particular test. An entry must exist in Expectations for
// each method that would be called during a particular test run.
type MockConfig struct {
	Expectations []MockExpectation
}

// MockExpectation is a wrapper around a single call to setup expectations for
// Testify (i.e., `testObj.On("DoSomething", 123).Return(true, nil)`
// https://github.com/stretchr/testify#mock-package). This allows us to mock
// worker nodes from other packages which cannot directly modify worker nodes.
type MockExpectation struct {
	MethodName string
	Args       []interface{}
	ReturnArgs []interface{}
}

// newWorkerNodesFromConfig is the standard way for initializing worker nodes.
// It accepts a slice of Worker Config defining the configuration for one or
// more worker nodes. It returns a slice of all nodes that could be successfully
// initialized. If it was unable to initialize any nodes due to bad
// configuration/fields it also returns an error indicating the invalid nodes.
// It is up to the caller to decide to act on the configuration if an error is
// returned. For example the caller could choose to move forward with whatever
// good Nodes were returned and log a warning about the misconfigured nodes.
func NewWorkerNodesFromConfig(log *zap.Logger, configs []Config) ([]Worker, error) {

	var newWorkers []Worker
	var multiErr types.MultiError

	for _, config := range configs {
		w, err := newWorkerNodeFromConfig(log, config)
		if err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
			continue
		}
		newWorkers = append(newWorkers, w)
	}

	if len(multiErr.Errors) != 0 {
		return newWorkers, &multiErr
	}

	return newWorkers, nil
}

// newWorkerNodeFromConfig() is intended to be used with newWorkerNodesFromConfig().
// It accepts a single worker node configuration and returns either an
// initialized worker node or an error.
func newWorkerNodeFromConfig(log *zap.Logger, config Config) (Worker, error) {

	log = log.With(zap.String("nodeID", config.ID), zap.String("nodeName", config.Name))
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	rpcCtx, rpcCancel := context.WithCancel(context.Background())

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/36
	// Allow configuration that should apply to all nodes to be set using configmgr, including the
	// default values (which should be set using flags). For now just ensure these are not set to
	// zero here, otherwise weird things can happen.

	if config.MaxReconnectBackOff == 0 {
		config.MaxReconnectBackOff = 60
	}

	if config.DisconnectTimeout == 0 {
		config.DisconnectTimeout = 30
	}

	if config.SendRetries == 0 {
		config.SendRetries = 10
	}

	if config.RetryInterval == 0 {
		config.RetryInterval = 1
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 10
	}

	baseNode := &baseNode{
		config:     config,
		State:      OFFLINE,
		nodeCtx:    nodeCtx,
		nodeCancel: nodeCancel,
		rpcWG:      &sync.WaitGroup{},
		rpcCtx:     rpcCtx,
		rpcCancel:  rpcCancel,
		log:        log,
		rpcErr:     make(chan error),
	}

	switch baseNode.GetNodeType() {
	case BeeSync:
		return newBeeSyncNode(baseNode), nil
	case Mock:
		return newMockNode(baseNode), nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", config.Type)
	}
}
