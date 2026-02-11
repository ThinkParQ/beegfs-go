package worker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// Worker defines the external facing interface used to interact with all worker
// node types. Specific worker node types must implement this interface along
// with the grpcClientHandler interface and add a typed constant in Config
// before they will be usable. Note common methods are implemented by the
// baseNode type, which all worker node types are expected to embed.
type Worker interface {
	// Implemented by the base node type:
	GetID() string
	GetState() State
	GetNodeType() Type
	Handle(*sync.WaitGroup, *flex.UpdateConfigRequest, *flex.BulkUpdateWorkRequest, map[string]*flex.Feature)
	Stop()
	// Implemented by specific node types:
	//
	// SubmitWork() should only return an error if the request was definitely not created on
	// the node. Otherwise this can lead to orphaned requests because BeeRemote assumes an error
	// means the request could not be sent to the node or created due to an internal node error.
	SubmitWork(*flex.WorkRequest) (*flex.Work, error)
	UpdateWork(*flex.UpdateWorkRequest) (*flex.Work, error)
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/55
	// Require UpdateConfig() once dynamic configuration updates are supported.
	//   UpdateConfig(*flex.WorkerNodeConfigRequest) (*flex.WorkerNodeConfigResponse, error)
}

// grpcClientHandler defines the interface for managing gRPC connections in
// worker nodes. Implementers of this interface are responsible for establishing
// and terminating gRPC connections and clients specific to their node type.
// This interface enables a common handler implementation through dependency
// injection. Implementations must ensure that they set the grpcClientHandler in
// their respective constructor functions.
//
// The interface consists of two methods:
//   - connect: Establishes a gRPC connection and initializes a gRPC client of the appropriate type
//     that is reused for all unary RPCs. After the new client is setup it should update the
//     configuration and state of any existing work requests on the node. It should return false if
//     an error occurred that is fatal (i.e., remote node was unable to update config or WRs) or
//     true if an transient error occurred that can be retried (i.e., network connectivity).
//   - disconnect: Cleanup the gRPC connection and client that was created for this node.
//     It should return an error if there were any problems freeing these resources.
//
// Note: Connect and disconnect operations should be idempotent and safe to call
// multiple times.
type grpcClientHandler interface {
	connect(*flex.UpdateConfigRequest, *flex.BulkUpdateWorkRequest, map[string]*flex.Feature) (retry bool, err error)
	heartbeat(*flex.HeartbeatRequest) (*flex.HeartbeatResponse, error)
	disconnect() error
}

// All worker node implementations should embed the baseNode type.
type baseNode struct {
	log *zap.Logger
	grpcClientHandler
	// State should not be used directly. It should be set/inspected using the
	// exported thread safe State methods that first lock stateMu.
	State   State
	stateMu sync.RWMutex
	config  Config
	// The mutex serves two purposes: (1) guarantee only one Handle() methods
	// for each node at a time. (2) guarantee when dynamic configuration updates
	// happen the old node handler has finished shutting down before we swap out
	// the node or delete it.
	nodeMu sync.Mutex
	// Context for the overall node. When cancelled the node will wait up to the
	// DisconnectTimeout for any outstanding RPCs to complete before they are
	// forcibly cancelled. IMPORTANT: After the node context is cancelled the
	// node must be recreated using newWorkerNodeFromConfig() before it can be
	// used again.
	nodeCtx    context.Context
	nodeCancel context.CancelFunc
	// When an RPC request is made the WG is incremented then deincremented
	// when the RPC completes. This is used to check for outstanding RPCs
	// when a disconnect is requested, allowing us to wait up to the
	// DisconnectTimeout for RPCs to complete before cancelling them.
	rpcWG *sync.WaitGroup
	// Context used for all RPCs. Can be cancelled to force outstanding RPCs to
	// complete if the DisconnectTimeout is exceeded. Note tis context is
	// initialized when the node is created using newWorkerNodeFromConfig(),
	// then also reinitialized every time connect is called otherwise we'd
	// never be able to reconnect or call any RPCs after a disconnect. Note the
	// initialization in newWorkerNodeFromConfig() shouldn't be required, but is
	// done to avoid a segmentation fault if the handle method were to change in
	// a way where the context could be cancelled before connect was called.
	rpcCtx    context.Context
	rpcCancel context.CancelFunc
	// rpcErr is used by unary RPC functions to notify the handler in the event
	// of an unrecoverable error to indicate the worker node should be marked as
	// offline. When sending to this channel it is important to use a
	// non-blocking send pattern using a select statement with a default case.
	// This is necessary because multiple RPCs may simultaneously encounter
	// errors, but only a single notification is needed to inform the handler to
	// set the node offline. A non-blocking send prevents RPCs from being
	// blocked and ensures after the node reconnects an RPC that was previously
	// blocked doesn't send a stale offline notification which would make the
	// node offline again.
	rpcErr chan error
}

// While gRPC handles most aspects of managing connections with worker nodes,
// because these nodes are stateless we must first send them configuration and
// tell them what to do with any outstanding work requests before they can
// handle new work requests and are considered "online". After a node is online,
// if any unary RPC results in an error the state will move to offline and
// we'll verify we can reconnect to the node and send the configuration. This
// way if a worker node was rebooted or the service restarted, it gets the
// correct configuration and knows how to handle any outstanding WRs.
type State string

const (
	UNKNOWN State = "unknown"
	OFFLINE State = "offline"
	ONLINE  State = "online"
)

func (n *baseNode) setState(state State) {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	n.State = state
}

func (n *baseNode) GetState() State {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return n.State
}

func (n *baseNode) GetNodeType() Type {
	return n.config.Type
}

func (n *baseNode) GetID() string {
	return n.config.ID
}

// Handle() should be run as a goroutine and is a common handler for all node
// types to manage the overall state of the node. It handles initializing the
// gRPC connection and client reused by all RPCs. It is also responsible for
// sending the node its configuration and telling the node what to do with
// outstanding work requests whenever the node transitions from offline->online.
// It also coordinates placing the node offline by first giving outstanding RPCs
// time to complete before forcibly disconnecting them. To allow this to happen
// it also requires a wait group that should be used to ensure nodes are
// disconnected cleanly when the application is shutting down.
func (n *baseNode) Handle(wg *sync.WaitGroup, config *flex.UpdateConfigRequest, wrUpdates *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) {

	wg.Add(1)
	defer wg.Done()
	n.nodeMu.Lock()
	defer n.nodeMu.Unlock()

	// Ticker used to control the interval at which heartbeat requests are send to worker nodes.
	ticker := time.NewTicker(time.Duration(n.config.HeartbeatInterval) * time.Second)
	defer ticker.Stop()
	// Set to true if the handler was stopped.
	done := false
	for {
		if n.GetState() == OFFLINE {
			if n.connectLoop(config, wrUpdates, requiredFeatures) {
				n.setState(ONLINE)
			connectedLoop:
				for {
					select {
					case <-n.nodeCtx.Done():
						n.log.Debug("node is shutting down because its context was cancelled")
						done = true
						break connectedLoop
					case err := <-n.rpcErr:
						n.log.Error("placing node offline due to an error", zap.Error(err))
						break connectedLoop
					case <-ticker.C:
						// When worker nodes start up they wait for BeeRemote to configure them and
						// tell what to do with any outstanding requests (in case they were
						// cancelled while the node was offline). Because BeeRemote and worker nodes
						// communicate using unary RPCs, unless requests are actively being assigned
						// to this worker, it is possible for the worker to reboot and BeeRemote to
						// never notice. This is because BeeRemote doesn't poll the status of
						// requests once they are assigned to a node, and worker nodes don't know
						// where to reach BeeRemote until it tells them. While it is not important
						// we immediately detect when a node is offline or rebooted (because unary
						// RPCs will trigger a reconnect and retry automatically), the heartbeat
						// mechanism ensures worker nodes aren't stuck indefinitely waiting for
						// configuration from BeeRemote.
						resp, err := n.heartbeat(&flex.HeartbeatRequest{})
						if err != nil {
							n.log.Error("failed to receive heartbeat response from node, placing offline and attempting to reconnect", zap.Error(err))
							break connectedLoop
						} else if !resp.GetIsReady() {
							n.log.Error("received a heartbeat response but the node is not ready, placing offline and attempting to update its configuration")
							break connectedLoop
						}
						n.log.Debug("received heartbeat from worker node", zap.Any("response", resp))
						// Otherwise continue.
					}
				}
			}
		}
		// We'll first set the node state to offline so the node is not assigned
		// more WRs and any RPC requests that do/did make it through are
		// rejected. We'll then wait up to the disconnect timeout for any active
		// RPCs to gracefully complete before cancelling the shared RPC context
		// and immediately trying to disconnect the node. Probably this is a bit
		// excessive, but allows for tight control over the shutdown process.
		n.setState(OFFLINE)
		allDone := make(chan struct{})
		go func() {
			n.rpcWG.Wait()
			select {
			case allDone <- struct{}{}:
			default:
				// Don't leak the goroutine if we reached the timeout and aren't
				// listening on the channel anymore.
				return
			}
		}()
		select {
		case <-allDone:
		case <-time.After(time.Duration(n.config.DisconnectTimeout) * time.Second):
		}
		// If we hit the timeout this allows us to cancel the context for any
		// outstanding RPCs and ensure they complete before disconnecting.
		// Otherwise this is essentially a no-op and we'll go straight to the
		// disconnect without further waiting.
		n.rpcCancel()
		n.rpcWG.Wait()
		err := n.disconnect()
		if err != nil {
			n.log.Error("error disconnecting node", zap.Error(err))
		}
		if done {
			return
		}
	}
}

// connectLoop() attempts to connect to a worker node. If the node is not ready
// or there is an error it will attempt to reconnect with an exponential
// backoff. If it returns false there was an unrecoverable error and the caller
// should first call doDisconnect() before reconnecting.
func (n *baseNode) connectLoop(config *flex.UpdateConfigRequest, wrUpdates *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) bool {
	n.log.Info("connecting to node")
	var reconnectBackOff float64 = 1

	// If a disconnect happened previously the RPC context would be cancelled.
	// Ensure it is initialized when connecting.
	n.rpcCtx, n.rpcCancel = context.WithCancel(context.Background())

	for {
		select {
		case <-n.nodeCtx.Done():
			n.log.Info("not attempting to connect to node because its context was cancelled")
			return false
		case <-time.After(time.Second * time.Duration(reconnectBackOff)):
			retry, err := n.connect(config, wrUpdates, requiredFeatures)
			if err != nil {
				if !retry {
					n.log.Error("unable to connect to node (unable to retry)", zap.Error(err))
					return false
				}
				// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
				reconnectBackOff *= 2 + rand.Float64()
				if reconnectBackOff > float64(n.config.MaxReconnectBackOff) {
					reconnectBackOff = float64(n.config.MaxReconnectBackOff) - rand.Float64()
				}

				n.log.Warn("unable to connect to node (retrying)", zap.Error(err), zap.Any("retry_in_seconds", reconnectBackOff))
				continue
			}
			n.log.Info("connected to node")
			return true
		}
	}
}

func (n *baseNode) Stop() {
	n.nodeCancel()
}
