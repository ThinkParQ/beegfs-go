package rst

import "errors"

var (
	ErrConfigRSTTypeNotSet     = errors.New("error creating new RST client: configuration does not specify the type")
	ErrConfigRSTTypeIsUnknown  = errors.New("error creating new RST client: configuration specified an unknown type")
	ErrReqAndRSTTypeMismatch   = errors.New("the request type is not supported by this RST type")
	ErrUnsupportedOpForRST     = errors.New("operation is not supported by this RST type")
	ErrConfigUpdateNotAllowed  = errors.New("updating RST configuration after it was initially set is not currently supported (hint: all nodes must be restarted)")
	ErrJobAlreadyHasExternalID = errors.New("cannot generate requests: job is already associated with an external ID (clean it up or delete before retrying)")
	ErrPartialPartDownload     = errors.New("data written to disk does not match the actual amount of data in the part")
	ErrJobAlreadyExists        = errors.New("no changes to entry detected since the last job")
	ErrJobNotAllowed           = errors.New("submitting a new job is not allowed in this state")
)
