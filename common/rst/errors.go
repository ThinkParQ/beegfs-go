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
	ErrJobNotReserved               = errors.New("the job is no longer reserved: it was cancelled or already claimed")
	ErrRequestNotDelivered          = errors.New("job request was not delivered to remote")
	ErrReservationMissing           = &reservationMissingError{}
	ErrJobBlockedByActiveJob        = &blockedByActiveJobError{}
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
	ErrGetPathStateFatal            = errors.New("fatal error collecting state path info")
	ErrBulkOperationCancelRequest   = errors.New("bulk operation cancel request")
)

// blockedByActiveJobError reports that a job cannot start because another active job holds the same
// path. The blocking job may be for the requested RST or for another one. It unwraps to
// ErrJobNotAllowed so callers that only care that the job was refused keep working, including the
// gRPC layer, which maps ErrJobNotAllowed to the NOT_ALLOWED response status. Callers that can retry
// check ErrJobBlockedByActiveJob first, because this block clears on its own when the other job
// finishes, while the other causes of ErrJobNotAllowed need an operator to clear an inactive job.
type blockedByActiveJobError struct{}

func (e *blockedByActiveJobError) Error() string {
	return "another job is active on this path (retry once it finishes)"
}
func (e *blockedByActiveJobError) Unwrap() error { return ErrJobNotAllowed }

// reservationMissingError reports that a claimed job ID is one remote has no record of at all, as
// opposed to one it holds in a state that can no longer be claimed. It unwraps to ErrJobNotReserved
// so callers that only care that the claim was refused keep working, including the gRPC layer,
// which maps ErrJobNotReserved to the NOT_RESERVED response status.
//
// Both cases are final, and the builder does not retry either one. Remote cannot tell why an ID is
// missing. These are the ways it happens:
//   - The builder crashed after the bulk operation recorded the request but before the reserve
//     reached remote.
//   - The job reached a terminal state and its record was removed afterwards, by a delete or by
//     garbage collection of old jobs.
//
// Reserving the ID again would recover the first case, but it would bring back a job that was
// cancelled and deleted in the second, or redo work that already completed. The separate error only
// gives the message a clearer reason.
//
// errors.Is matches this error for ErrJobNotReserved too, so anything that distinguishes them has
// to test this one first.
type reservationMissingError struct{}

func (e *reservationMissingError) Error() string {
	return "no job with the claimed ID exists (the reservation was never recorded or has been cleaned up)"
}
func (e *reservationMissingError) Unwrap() error { return ErrJobNotReserved }

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

// Bulk operations may need to be cancelled and in some cases, the unsent job requests should be
// cancelled. Pass &RequestCancelError{Reason: err} to the filesystem.StreamPathResult Err to
// ensure the request is submitted as a failed-precondition rather than an error.
type RequestCancelError struct {
	Reason error
}

func (e *RequestCancelError) Error() string { return e.Reason.Error() }
func (e *RequestCancelError) Unwrap() error { return ErrBulkOperationCancelRequest }
