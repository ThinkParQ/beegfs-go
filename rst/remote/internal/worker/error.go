package worker

import "errors"

var (
	ErrWorkRequestNotFound = errors.New("work request not found on worker node")
	ErrNodeDraining        = errors.New("worker node is draining and declined the work request")
	// ErrConfigRejected is wrapped by connect and UpdateConfig when the node received its
	// configuration but refused to apply it. A retry sends the configuration BeeRemote serves at
	// that moment, so it succeeds only once the configuration or the node is fixed.
	ErrConfigRejected = errors.New("node rejected config update")
	// ErrNodeNotConnected is returned by UpdateConfig for a node that is not connected.
	ErrNodeNotConnected = errors.New("worker node is not connected")
)
