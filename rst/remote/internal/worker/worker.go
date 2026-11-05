package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// connectTimeout bounds one connect attempt. Without it, a node that accepts the connection but
// never answers would block Handle forever, and on a first attempt it would never leave UNKNOWN. It
// is generous because the node only answers UpdateConfig after it has checked back with remote and
// set up its remote storage targets. It is a variable so tests can shorten it.
var connectTimeout = time.Minute

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
	GetNumWorkers() int
	Handle(*sync.WaitGroup, ConfigFn, *flex.BulkUpdateWorkRequest, map[string]*flex.Feature)
	Stop()
	// Implemented by specific node types:
	//
	// SubmitWork() should only return an error if the request was definitely not created on
	// the node. Otherwise this can lead to orphaned requests because BeeRemote assumes an error
	// means the request could not be sent to the node or created due to an internal node error.
	SubmitWork(*flex.WorkRequest) (*flex.Work, error)
	UpdateWork(*flex.UpdateWorkRequest) (*flex.Work, error)
	// UpdateConfig pushes a new configuration to a connected node. BeeRemote calls it after every
	// change to its remote storage targets. It returns:
	//   - ErrNodeNotConnected when the node is not connected. The node gets the current
	//     configuration from its ConfigFn when it connects.
	//   - An error wrapping ErrConfigRejected when the node refused the configuration.
	//   - Any other error when the RPC failed.
	// After a rejection or a failed RPC the node is placed offline and reconnects, which sends the
	// current configuration again. So a node never keeps working with a configuration BeeRemote no
	// longer serves.
	UpdateConfig(*flex.UpdateConfigRequest) error
}

// ConfigFn returns the configuration to send to a node, and a release function. A node calls it on
// every connect attempt, so an attempt always sends the configuration BeeRemote serves at that
// moment. The node calls release once the attempt ends, after it marked itself ONLINE or failed.
//
// Until release, BeeRemote holds back any change to the configuration. That closes a gap: a change
// pushed while the node is still connecting finds it not connected, and is skipped, while the
// attempt already sent the previous configuration. Held back, the change waits until the node is
// ONLINE, and then its push reaches the node.
type ConfigFn func() (config *flex.UpdateConfigRequest, release func())

// grpcClientHandler defines the interface for managing gRPC connections in worker nodes.
// Implementers of this interface are responsible for establishing and terminating gRPC connections
// and clients specific to their node type. This interface enables a common handler implementation
// through dependency injection. Implementations must ensure that they set the grpcClientHandler in
// their respective constructor functions.
//
// The interface consists of three methods:
//
//   - connect: Establishes a gRPC connection and initializes a gRPC client of the appropriate type
//     that is reused for all unary RPCs. After the new client is setup it should update the
//     configuration and state of any existing work requests on the node. Its RPCs must use ctx,
//     which bounds one attempt and is cancelled by Stop().
//   - heartbeat: Sends a heartbeat request on ctx to verify the node is online and ready.
//   - disconnect: Cleanup the gRPC connection and client that was created for this node.
//     It should return an error if there were any problems freeing these resources.
//
// Note: Connect and disconnect operations should be idempotent and safe to call multiple times.
type grpcClientHandler interface {
	connect(context.Context, *flex.UpdateConfigRequest, *flex.BulkUpdateWorkRequest, map[string]*flex.Feature) error
	heartbeat(context.Context, *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error)
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
	// Context used by RPCs that run outside Handle, such as SubmitWork and
	// UpdateWork. RPCs made by Handle itself (connect and heartbeat) use
	// contexts derived from nodeCtx instead, so Stop() can interrupt them. rpcCtx is not derived from
	// nodeCtx because Stop() must not cancel outstanding work RPCs at once. Can
	// be cancelled to force outstanding RPCs to complete if the
	// DisconnectTimeout is exceeded. Note this context is
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
	// numWorkers is how many work requests this node runs concurrently, as reported by its last
	// heartbeat. It is guarded by stateMu alongside State because the two are updated together and
	// read together when sizing jobs. Zero means the node has not reported one yet, either because
	// it has not been heard from or because it predates the field.
	numWorkers int
	// warnedNoNumWorkers suppresses repeating the "node did not report num_workers" warning on
	// every heartbeat. It is reset whenever the node reconnects so an upgrade is noticed.
	warnedNoNumWorkers bool
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
	// UNKNOWN is the state of a node whose first connect attempt has not finished yet.
	UNKNOWN State = "unknown"
	// OFFLINE is the state of a node that is not connected after at least one connect attempt.
	OFFLINE State = "offline"
	ONLINE  State = "online"
	// DRAINING nodes are reachable and still finishing the work they were assigned. They cannot run
	// anything new before they restart, so they are offered a request only as a last resort, once
	// every online node in their pool has declined it. See Pool.assignmentCandidates.
	DRAINING State = "draining"
)

// AssumedNumWorkers is the worker count assumed for nodes that predate num_workers in the heartbeat
// response. Overestimating only queues extra work requests on a node, while underestimating
// permanently under parallelizes a job because its segments are fixed once generated, so this sits
// toward the high end of a typical GOMAXPROCS rather than the low end.
const AssumedNumWorkers = 32

func (n *baseNode) heartbeatState(resp *flex.HeartbeatResponse) State {
	switch resp.GetState() {
	case flex.HeartbeatResponse_DRAINING:
		return DRAINING
	case flex.HeartbeatResponse_READY:
		return ONLINE
	case flex.HeartbeatResponse_NOT_READY:
		return OFFLINE
	default:
		// Worker nodes predating HeartbeatResponse.State will be UNSPECIFIED in which case the
		// deprecated IsReady flag is the only thing they can tell us.
		if resp.GetIsReady() {
			return ONLINE
		}
		return OFFLINE
	}
}

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

// compareAndSetState sets the state to next only if it is still expected, and reports whether it
// did. RPC methods use it to change the state based on a response, so a state Handle set while the
// RPC was running is not overwritten.
func (n *baseNode) compareAndSetState(expected State, next State) bool {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.State != expected {
		return false
	}
	n.State = next
	return true
}

// beginRPC is called by every RPC method before it uses the node's client. It registers the RPC
// with rpcWG, but only while the node is ONLINE or DRAINING. It returns the state it read and
// whether the RPC was registered. A caller that gets true must call rpcWG.Done when the RPC
// finishes. A caller that gets false must not call the node at all.
//
// The state check and the Add happen under one stateMu lock, which is what makes rpcWG safe:
//   - setOffline sets OFFLINE under stateMu before it waits on rpcWG, so every Add happens before
//     that Wait starts.
//   - No Add happens again until Handle sets ONLINE after a reconnect, and setOffline has returned
//     from all its waits by then.
//
// sync.WaitGroup panics when an Add from zero runs concurrently with a Wait. An Add made before
// the state check allowed exactly that whenever an RPC was attempted on an offline node.
func (n *baseNode) beginRPC() (State, bool) {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	if n.State != ONLINE && n.State != DRAINING {
		return n.State, false
	}
	n.rpcWG.Add(1)
	return n.State, true
}

// GetNumWorkers returns how many work requests this node runs concurrently, or zero when the node
// has not reported a count. Callers are responsible for deciding what to assume when it is zero.
func (n *baseNode) GetNumWorkers() int {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return n.numWorkers
}

// recordNumWorkers stores the worker count from a heartbeat response. Nodes predating the field
// report zero, which is left as-is so the caller can substitute its own assumption. That is warned
// about once per connection rather than on every heartbeat, which would otherwise repeat for as
// long as the node stays connected.
func (n *baseNode) recordNumWorkers(resp *flex.HeartbeatResponse) {
	numWorkers := int(resp.GetNumWorkers())

	n.stateMu.Lock()
	n.numWorkers = numWorkers
	warn := numWorkers == 0 && !n.warnedNoNumWorkers
	if warn {
		n.warnedNoNumWorkers = true
	}
	n.stateMu.Unlock()

	if warn {
		n.log.Warn("node did not report how many workers it runs, assuming a default when sizing jobs (hint: upgrade the node so jobs are split to match its actual capacity)",
			zap.Int("assumedNumWorkers", AssumedNumWorkers))
	}
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
func (n *baseNode) Handle(wg *sync.WaitGroup, config ConfigFn, wrUpdates *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) {

	wg.Add(1)
	defer wg.Done()
	n.nodeMu.Lock()
	defer n.nodeMu.Unlock()

	shutdown := func(reason error) {
		n.log.Info("node is shutting down", zap.Error(reason))
		n.setOffline()
	}

	for {
		if err := n.waitUntilConnected(config, wrUpdates, requiredFeatures); err != nil {
			shutdown(err)
			return
		}

		err := n.manageConnection()
		if n.nodeCtx.Err() != nil {
			shutdown(n.nodeCtx.Err())
			return
		}
		n.log.Error("placing node offline due to an error", zap.Error(err))
		n.setOffline()
	}
}

// waitUntilConnected attempts to connect to a worker node until either a connection succeeds or the
// node context is cancelled. For any error, an exponential backoff delay will be observed before
// retrying.
func (n *baseNode) waitUntilConnected(config ConfigFn, wrUpdates *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) error {
	var reconnectBackOff float64 = 1
	var retryDelay time.Duration
	// A rejected config repeats on every retry until an operator fixes it or the node is stopped.
	// It is logged as an error once, and later rejections use the normal retry warning.
	loggedConfigRejected := false

	// If a disconnect happened previously the RPC context would be cancelled.
	// Ensure it is initialized when connecting.
	n.rpcCtx, n.rpcCancel = context.WithCancel(context.Background())

	n.log.Info("connecting to node")
	for {
		select {
		case <-n.nodeCtx.Done():
			return n.nodeCtx.Err()
		case <-time.After(retryDelay):
			if err := n.nodeCtx.Err(); err != nil {
				return err
			}
			connectCtx, cancel := context.WithTimeout(n.nodeCtx, connectTimeout)
			sent, release := config()
			err := n.connect(connectCtx, sent, wrUpdates, requiredFeatures)
			cancel()
			if err != nil {
				release()
				// Leave UNKNOWN after the first failed attempt. The pool briefly waits for UNKNOWN
				// nodes to finish starting, but the next attempt only comes after a backoff.
				n.setState(OFFLINE)

				// connect() may fail after it has already opened n.conn. Close that connection before retrying
				// because the next connect() overwrites n.conn and the old connection would otherwise never close.
				if disconnectErr := n.disconnect(); disconnectErr != nil {
					n.log.Debug("error disconnecting before reconnect", zap.Error(disconnectErr))
				}

				// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
				reconnectBackOff *= 2 + rand.Float64()
				if reconnectBackOff > float64(n.config.MaxReconnectBackOff) {
					reconnectBackOff = float64(n.config.MaxReconnectBackOff) - rand.Float64()
				}
				retryDelay = time.Duration(reconnectBackOff * float64(time.Second))
				if errors.Is(err, ErrConfigRejected) && !loggedConfigRejected {
					loggedConfigRejected = true
					n.log.Error("node rejected its configuration, retrying until it is accepted (hint: check the node's log for why it was rejected)", zap.Error(err), zap.Duration("retry_in", retryDelay))
				} else {
					n.log.Warn("unable to connect to node (retrying)", zap.Error(err), zap.Duration("retry_in", retryDelay))
				}
			} else {
				n.setState(ONLINE)
				release()
				n.log.Info("connected to node")
				return nil
			}
		}
	}
}

// manageConnection checks for node heartbeats and returns on a rpc error or context cancellation.
func (n *baseNode) manageConnection() error {
	// Ticker used to control the interval at which heartbeat requests are send to worker nodes.
	ticker := time.NewTicker(time.Duration(n.config.HeartbeatInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.nodeCtx.Done():
			return n.nodeCtx.Err()
		case err := <-n.rpcErr:
			return err
		case <-ticker.C:
			// When worker nodes start up they wait for BeeRemote to configure them and tell what to
			// do with any outstanding requests (in case they were cancelled while the node was
			// offline). Because BeeRemote and worker nodes communicate using unary RPCs, unless
			// requests are actively being assigned to this worker, it is possible for the worker to
			// reboot and BeeRemote to never notice. This is because BeeRemote doesn't poll the
			// status of requests once they are assigned to a node, and worker nodes don't know
			// where to reach BeeRemote until it tells them. While it is not important we
			// immediately detect when a node is offline or rebooted (because unary RPCs will
			// trigger a reconnect and retry automatically), the heartbeat mechanism ensures worker
			// nodes aren't stuck indefinitely waiting for configuration from BeeRemote.
			// The heartbeat is bounded by the interval. A node that accepts the connection but
			// never answers is placed offline instead of staying ONLINE and getting work. Its context
			// is derived from nodeCtx so Stop() interrupts it.
			heartbeatCtx, cancel := context.WithTimeout(n.nodeCtx, time.Duration(n.config.HeartbeatInterval)*time.Second)
			resp, err := n.heartbeat(heartbeatCtx, &flex.HeartbeatRequest{})
			cancel()
			if err != nil {
				return fmt.Errorf("failed to receive heartbeat response: %w", err)
			}

			n.log.Debug("received heartbeat from worker node", zap.Any("response", resp))
			n.recordNumWorkers(resp)
			switch n.heartbeatState(resp) {
			case DRAINING:
				// Deliberately stay connected. A draining node is shutting down cleanly and we want
				// to keep the connection so outstanding requests can still be cancelled or updated,
				// and so we notice when it does go away. As soon as the state is set the pool stops
				// treating it as a place work can run now, and only falls back to it when nothing
				// else in the pool will take a request.
				if n.GetState() != DRAINING {
					n.log.Info("node reports it is draining, new work requests will only be assigned to it as a last resort")
					n.setState(DRAINING)
				}
			case ONLINE:
				if n.GetState() == DRAINING {
					// Only reachable if a node reports draining then reports ready again without
					// ever becoming unreachable. Not expected during shutdown, but recovering is
					// harmless and better than staying unusable.
					n.log.Info("node is no longer draining, it is a normal candidate for work requests again")
					n.setState(ONLINE)
				}
			default:
				return fmt.Errorf("received a heartbeat response but the node is not ready")
			}
		}
	}
}

// setOffline stops all new work request assigments and waits any for any outstanding rpcs to
// complete before disconnecting. If the configured DisconnectTimeout time expires before the rpcs
// can gracefully complete, the rpc context will be cancelled.
func (n *baseNode) setOffline() {
	// We'll first set the node state to offline so the node is not assigned more WRs and any RPC
	// requests that do/did make it through are rejected. We'll then wait up to the disconnect
	// timeout for any active RPCs to gracefully complete before cancelling the shared RPC context
	// and immediately trying to disconnect the node. Probably this is a bit excessive, but allows
	// for tight control over the shutdown process.
	n.setState(OFFLINE)
	n.stateMu.Lock()
	n.numWorkers = 0
	n.warnedNoNumWorkers = false
	n.stateMu.Unlock()

	allDone := make(chan struct{})
	go func() {
		n.rpcWG.Wait()
		close(allDone)
	}()
	select {
	case <-allDone:
	case <-time.After(time.Duration(n.config.DisconnectTimeout) * time.Second):
	}

	// If we hit the timeout this allows us to cancel the context for any outstanding RPCs and
	// ensure they complete before disconnecting.
	n.rpcCancel()
	n.rpcWG.Wait()
	// Also wait for the goroutine's Wait to return. A reconnect can register new RPCs, and
	// sync.WaitGroup panics if an Add runs before every earlier Wait has returned.
	<-allDone

	err := n.disconnect()
	if err != nil {
		n.log.Error("error disconnecting node", zap.Error(err))
	}
}

func (n *baseNode) Stop() {
	n.nodeCancel()
}
