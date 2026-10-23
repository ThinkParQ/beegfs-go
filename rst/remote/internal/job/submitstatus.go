package job

import (
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// SubmitStatus maps the error SubmitJobRequest returned to the status remote reports to the
// submitter. It is the one mapping for the wire answer and for the answer a job stores, so a
// replayed request gets back exactly what the first request got.
//
//   - nil maps to CREATED.
//   - A known sentinel maps to its status. More specific sentinels are checked before the ones
//     they wrap: ErrJobBlockedByActiveJob before ErrJobNotAllowed, and ErrReservationMissing
//     before ErrJobNotReserved.
//   - Any other error maps to INVALID with ok false. The server returns those as gRPC errors.
func SubmitResponseStatus(err error) (status beeremote.SubmitJobResponse_ResponseStatus, ok bool) {
	switch {
	case err == nil:
		return beeremote.SubmitJobResponse_CREATED, true
	case errors.Is(err, rst.ErrJobAlreadyComplete):
		return beeremote.SubmitJobResponse_ALREADY_COMPLETE, true
	case errors.Is(err, rst.ErrJobAlreadyOffloaded):
		return beeremote.SubmitJobResponse_ALREADY_OFFLOADED, true
	case errors.Is(err, rst.ErrJobAlreadyExists):
		return beeremote.SubmitJobResponse_EXISTING, true
	case errors.Is(err, rst.ErrJobFailedPrecondition):
		return beeremote.SubmitJobResponse_FAILED_PRECONDITION, true
	case errors.Is(err, rst.ErrJobBlockedByActiveJob):
		return beeremote.SubmitJobResponse_BLOCKED_BY_ACTIVE_JOB, true
	case errors.Is(err, rst.ErrJobNotAllowed):
		return beeremote.SubmitJobResponse_NOT_ALLOWED, true
	case errors.Is(err, rst.ErrReservationMissing):
		return beeremote.SubmitJobResponse_RESERVATION_MISSING, true
	case errors.Is(err, rst.ErrJobNotReserved):
		return beeremote.SubmitJobResponse_NOT_RESERVED, true
	}
	return beeremote.SubmitJobResponse_INVALID, false
}

// submitRefused reports whether the request that created job was refused. It reads the stored
// answer. A job with no stored answer (INVALID) was recorded from an error that has no status of
// its own. Such a job counts as refused only when its state says so: FAILED or CANCELLED.
func submitRefused(job *Job) bool {
	switch job.GetSubmitStatus() {
	case beeremote.SubmitJobResponse_CREATED:
		return false
	case beeremote.SubmitJobResponse_INVALID:
		state := job.GetStatus().GetState()
		return state == beeremote.Job_FAILED || state == beeremote.Job_CANCELLED
	}
	return true
}

// storedSubmitError rebuilds the error a replayed request gets back from the answer its job
// stored. The sentinel comes from the stored status. The detail comes from the job's status
// message, because the original error text is not kept. A job with no stored answer (INVALID)
// answers from its state, as submitRefused decides.
func storedSubmitError(job *Job) error {
	message := job.GetStatus().GetMessage()
	var sentinel error
	switch job.GetSubmitStatus() {
	case beeremote.SubmitJobResponse_CREATED:
		return nil
	case beeremote.SubmitJobResponse_INVALID:
		if !submitRefused(job) {
			return nil
		}
		return fmt.Errorf("job %s was not accepted: %s", job.GetId(), message)
	case beeremote.SubmitJobResponse_ALREADY_COMPLETE:
		sentinel = rst.ErrJobAlreadyComplete
	case beeremote.SubmitJobResponse_ALREADY_OFFLOADED:
		sentinel = rst.ErrJobAlreadyOffloaded
	case beeremote.SubmitJobResponse_EXISTING:
		sentinel = rst.ErrJobAlreadyExists
	case beeremote.SubmitJobResponse_FAILED_PRECONDITION:
		sentinel = rst.ErrJobFailedPrecondition
	case beeremote.SubmitJobResponse_BLOCKED_BY_ACTIVE_JOB:
		sentinel = rst.ErrJobBlockedByActiveJob
	case beeremote.SubmitJobResponse_NOT_ALLOWED:
		sentinel = rst.ErrJobNotAllowed
	case beeremote.SubmitJobResponse_RESERVATION_MISSING:
		sentinel = rst.ErrReservationMissing
	case beeremote.SubmitJobResponse_NOT_RESERVED:
		sentinel = rst.ErrJobNotReserved
	default:
		return fmt.Errorf("job %s has an unknown submit status %s: %s", job.GetId(), job.GetSubmitStatus(), message)
	}
	return fmt.Errorf("%w: %s", sentinel, message)
}
