package worker

import "errors"

var (
	ErrWorkRequestNotFound = errors.New("work request not found on worker node")
	ErrNodeDraining        = errors.New("worker node is draining and is not accepting new work requests")
)
