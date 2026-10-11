package workmgr

import "errors"

var (
	ErrNotReady               = errors.New("work manager is not ready yet")
	ErrStopping               = errors.New("work manager is shutting down")
	ErrConfigUpdateNotAllowed = errors.New("updating BeeSync configuration after it was initially set is not currently supported")
)
