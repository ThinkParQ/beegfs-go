package worker

import "errors"

var (
	ErrWorkRequestNotFound = errors.New("work request not found on worker node")
	ErrNodeDraining        = errors.New("worker node is draining and declined the work request")
	// ErrConfigRejected is wrapped by connect when the node received its configuration but refused
	// to apply it. Retrying sends the same configuration again, so it usually needs an operator.
	ErrConfigRejected = errors.New("node rejected config update")
)
