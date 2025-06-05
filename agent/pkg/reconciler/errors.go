package reconciler

import "errors"

var (
	ErrLoadingManifest = errors.New("unable to load manifest from disk")
	ErrSavingManifest  = errors.New("unable to save manifest to disk")
	ErrBadManifest     = errors.New("manifest failed verification")
)
