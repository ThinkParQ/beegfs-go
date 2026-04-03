/*
Package subscriber is where each subscriber type is implemented.

Each subscriber must embed the BaseSubscriber type and implement the Subscriber interface to be a valid
concrete implementation of a Subscriber that can be managed by a handler.
*/
package subscriber

import (
	"sync"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// This interface defines the methods all subscriber types (such as gRPC) are expected to implement.
// These methods are used by subscriber handlers to manage the connection lifecycle.
type Interface interface {
	// Connect establishes a new connection to a subscriber.
	// If it returns an error retry should be true if there was a transient issue that can be retried (i.e., subscriber not ready).
	// Otherwise retry should be false if there is a fatal error connecting to the subscriber (i.e., subscriber is misconfigured).
	// In either case it is not expected to be idempotent, and if an error occurs Disconnect() should be called to cleanup before reconnecting.
	Connect(forMetaID beegfs.NumId) (retry bool, err error)
	// Send transmits a single event to a subscriber.
	Send(*pb.Event) (err error)
	// Receive starts a goroutine that listens responses from the subscriber and posts them to the returned channel.
	// It is idempotent in that multiple calls are expected to return the same channel and not start a new goroutine.
	Receive() chan *pb.Response
	// Disconnect must be idempotent and not return an error even if called against an already disconnected subscriber.
	Disconnect() (err error)
}

// state should not be used directly.
// It should be set/inspected using the exported thread safe State methods.
type state string

const (
	DISCONNECTED  state = "disconnected"
	CONNECTING    state = "connecting"
	CONNECTED     state = "connected"
	DISCONNECTING state = "disconnecting"
)

// The GetState method provided by State are used to communicate the state of a subscriber to the outside world.
// External readers SHOULD NOT inspect the state directly, but instead use the thread safe GetState() method.
// The Handler.Handle() method is the ONLY place where SubscriberStates should change.
// All internal handler methods and subscriber methods such as connect(), send(), disconnect(), etc. should not affect the state.
type State struct {
	// state should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	state state
	mutex sync.RWMutex
}

// GetState is a thread safe mechanism to get the current state and status of a subscriber.
func (s *State) GetState() state {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// SetState is a thread safe mechanism to set the current state and status of a subscriber.
func (s *State) SetState(newState state) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = newState
}

// Subscriber encapsulates the configuration and functionality implemented by
// all subscribers. It uses an Interface to abstract implementation details for
// a particular subscriber type to allow a common handler.
type Subscriber struct {
	Config
	State
	Interface
}

// ComparableSubscriber is a "comparable" view of the Subscriber struct used for testing.
// When Subscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the interface is omitted and each Subscriber should implement its own comparable type.
type ComparableSubscriber struct {
	Config
	State
}
