package registry

import "errors"

var (
	ErrUnsupportedFeature   = errors.New("unsupported feature")
	ErrRegistryUninitalized = errors.New("component registry uninitialized")
)
