package rst

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrConfigRSTTypeNotSet          = errors.New("error creating new RST client: configuration does not specify the type")
	ErrConfigRSTTypeIsUnknown       = errors.New("error creating new RST client: configuration specified an unknown type")
	ErrReqAndRSTTypeMismatch        = errors.New("the request type is not supported by this RST type")
	ErrUnsupportedOpForRST          = errors.New("operation is not supported by this RST type")
	ErrConfigUpdateNotAllowed       = errors.New("updating RST configuration after it was initially set is not currently supported (hint: all nodes must be restarted)")
	ErrJobAlreadyHasExternalID      = errors.New("cannot generate requests: job is already associated with an external ID (clean it up or delete before retrying)")
	ErrPartialPartDownload          = errors.New("data written to disk does not match the actual amount of data in the part")
	ErrJobAlreadyComplete           = errors.New("file already synced with RST")
	ErrJobAlreadyOffloaded          = errors.New("file already offloaded to RST")
	ErrJobFailedPrecondition        = errors.New("job failed precondition")
	ErrJobNotAllowed                = errors.New("submitting a new job is not allowed in this state")
	ErrJobAlreadyExists             = errors.New("no changes to entry detected since the last job")
	ErrEntryNotFound                = errors.New("entry was not found")
	ErrFileHasNoRSTs                = errors.New("entry does not have any remote storage target IDs configured")
	ErrFileHasAmbiguousRSTs         = errors.New("ambiguous remote source! There must only be one rst for downloads")
	ErrFileOpenForWriting           = errors.New("entry is opened for writing on one or more clients")
	ErrFileOpenForReading           = errors.New("entry is opened for reading on one or more clients")
	ErrFileOpenForReadingAndWriting = errors.New("entry is opened for reading and writing on one or more clients")
	ErrFileTypeUnsupported          = errors.New("entry type is not supported")
	ErrOffloadFileCreate            = errors.New("unable to create offload file")
	ErrOffloadFileUrlMismatch       = errors.New("offload file url does not match")
	ErrOffloadFileNotReadable       = errors.New("unable to read stub file")
	ErrRSTUnavailable               = errors.New("remote target is unavailable")
)

func IsErrJobTerminalSentinel(err error) bool {
	return errors.Is(err, ErrJobAlreadyComplete) || errors.Is(err, ErrJobAlreadyOffloaded)
}

type MtimeErr struct {
	Time time.Time
	Err  error
}

func (m *MtimeErr) Error() string {
	return fmt.Sprintf("%s (mtime %s)", m.Err.Error(), m.Time.String())
}
func (m *MtimeErr) Mtime() time.Time { return m.Time }
func (m *MtimeErr) Unwrap() error    { return m.Err }
func GetErrJobAlreadyCompleteWithMtime(mtime time.Time) *MtimeErr {
	return &MtimeErr{Err: ErrJobAlreadyComplete, Time: mtime}
}
