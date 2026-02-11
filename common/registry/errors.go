package registry

import "errors"

var (
	ErrUnsupportedFeature        = errors.New("unsupported feature")
	ErrRegistryClientUnavailable = errors.New("registry client is not available")
	ErrRegistryUninitialized     = errors.New("component registry uninitialized")
	ErrCapabilitiesNotSupported  = errors.New("server does not support capabilities")
)
