package rst

import (
	"errors"
)

var (
	ErrConfigRSTTypeNotSet          = errors.New("error creating new RST client: configuration does not specify the type")
	ErrConfigRSTTypeIsUnknown       = errors.New("error creating new RST client: configuration specified an unknown type")
	ErrReqAndRSTTypeMismatch        = errors.New("the request type is not supported by this RST type")
	ErrUnsupportedOpForRST          = errors.New("operation is not supported by this RST type")
	ErrConfigUpdateNotAllowed       = errors.New("updating RST configuration after it was initially set is not currently supported")
	ErrJobAlreadyHasExternalID      = errors.New("cannot generate requests: job is already associated with an external ID (clean it up or delete before retrying)")
	ErrPartialPartDownload          = errors.New("data written to disk does not match the actual amount of data in the part")
	ErrJobAlreadyComplete           = errors.New("file already synced with RST")
	ErrJobAlreadyOffloaded          = errors.New("file already offloaded to RST")
	ErrJobAlreadyInProgress         = errors.New("job already in progress")
	ErrJobNotAllowed                = errors.New("submitting a new job is not allowed in this state")
	ErrJobAlreadyExists             = errors.New("no changes to entry detected since the last job")
	ErrFileHasNoRSTs                = errors.New("entry does not have any remote storage target IDs configured")
	ErrFileHasAmbiguousRSTs         = errors.New("ambiguous remote source! There must only be one rst for downloads")
	ErrFileOpenForWriting           = errors.New("entry is opened for writing on one or more clients")
	ErrFileOpenForReading           = errors.New("entry is opened for reading on one or more clients")
	ErrFileOpenForReadingAndWriting = errors.New("entry is opened for reading and writing on one or more clients")
	ErrFileTypeUnsupported          = errors.New("entry type is not supported")
	ErrOffloadFileCreate            = errors.New("unable to create offload file")
	ErrOffloadFileUrlMismatch       = errors.New("offload file url does not match")
)

func IsErrJobTerminalSentinel(err error) bool {
	return errors.Is(err, ErrJobAlreadyComplete) || errors.Is(err, ErrJobAlreadyOffloaded)
}
